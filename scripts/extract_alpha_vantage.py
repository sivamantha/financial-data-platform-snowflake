"""
Alpha Vantage Data Extraction
─────────────────────────────
Extracts:
  1. Daily stock prices (OHLCV) for target tickers
  2. Sector performance metrics
  3. Company overview fundamentals (market cap, P/E, dividends)

API Docs: https://www.alphavantage.co/documentation/
Rate limit: 25 requests/day (free tier), 5 requests/minute
"""

import os
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from snowflake_utils import get_snowflake_connection, load_json_to_snowflake

load_dotenv(Path(__file__).parent.parent / '.env')

# ── Configuration ─────────────────────────────────────────────
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
BASE_URL = 'https://www.alphavantage.co/query'

# Same companies as SEC EDGAR for cross-source joins
TARGET_TICKERS = ['AAPL', 'MSFT', 'JPM', 'JNJ', 'XOM',
                  'WMT', 'PG', 'BAC', 'DIS', 'CAT']

# Free tier: 25 calls/day — prioritize carefully
# 10 daily prices + 10 overviews + 1 sector = 21 calls
CALLS_MADE = 0
MAX_DAILY_CALLS = 25


def rate_limit():
    """Alpha Vantage free tier: 5 requests/minute, 25/day."""
    global CALLS_MADE
    CALLS_MADE += 1
    if CALLS_MADE >= MAX_DAILY_CALLS:
        print(f"  WARNING: Approaching daily limit ({CALLS_MADE}/{MAX_DAILY_CALLS})")
    time.sleep(13)  # ~5 requests/minute to stay safe


def extract_daily_prices():
    """
    Extract daily OHLCV data for target tickers.
    Uses TIME_SERIES_DAILY — returns last 100 trading days.
    """
    print("Extracting daily stock prices...")
    results = []

    for ticker in TARGET_TICKERS:
        try:
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': ticker,
                'outputsize': 'compact',  # Last 100 days
                'apikey': API_KEY
            }
            resp = requests.get(BASE_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            rate_limit()

            # Check for API limit message
            if 'Note' in data or 'Information' in data:
                print(f"  {ticker}: API limit reached. Stopping.")
                break

            time_series = data.get('Time Series (Daily)', {})
            count = 0

            for date, values in time_series.items():
                results.append({
                    'ticker': ticker,
                    'trade_date': date,
                    'open': float(values.get('1. open', 0)),
                    'high': float(values.get('2. high', 0)),
                    'low': float(values.get('3. low', 0)),
                    'close': float(values.get('4. close', 0)),
                    'volume': int(values.get('5. volume', 0)),
                    'extract_date': datetime.utcnow().isoformat()
                })
                count += 1

            print(f"  {ticker}: {count} trading days")

        except Exception as e:
            print(f"  Warning: Could not fetch prices for {ticker}: {e}")

    print(f"  Total: {len(results)} price records extracted")
    return results


def extract_company_overview():
    """
    Extract company fundamentals: market cap, P/E, dividend yield, etc.
    One API call per ticker.
    """
    print("Extracting company overviews...")
    results = []

    for ticker in TARGET_TICKERS:
        try:
            params = {
                'function': 'OVERVIEW',
                'symbol': ticker,
                'apikey': API_KEY
            }
            resp = requests.get(BASE_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            rate_limit()

            if 'Note' in data or 'Information' in data:
                print(f"  {ticker}: API limit reached. Stopping.")
                break

            if not data or 'Symbol' not in data:
                print(f"  {ticker}: No data returned")
                continue

            results.append({
                'ticker': data.get('Symbol'),
                'company_name': data.get('Name'),
                'exchange': data.get('Exchange'),
                'sector': data.get('Sector'),
                'industry': data.get('Industry'),
                'market_cap': data.get('MarketCapitalization'),
                'pe_ratio': data.get('PERatio'),
                'peg_ratio': data.get('PEGRatio'),
                'book_value': data.get('BookValue'),
                'dividend_per_share': data.get('DividendPerShare'),
                'dividend_yield': data.get('DividendYield'),
                'eps': data.get('EPS'),
                'revenue_per_share': data.get('RevenuePerShareTTM'),
                'profit_margin': data.get('ProfitMargin'),
                'operating_margin': data.get('OperatingMarginTTM'),
                'return_on_assets': data.get('ReturnOnAssetsTTM'),
                'return_on_equity': data.get('ReturnOnEquityTTM'),
                'revenue_ttm': data.get('RevenueTTM'),
                'gross_profit_ttm': data.get('GrossProfitTTM'),
                'fifty_two_week_high': data.get('52WeekHigh'),
                'fifty_two_week_low': data.get('52WeekLow'),
                'fifty_day_moving_avg': data.get('50DayMovingAverage'),
                'two_hundred_day_moving_avg': data.get('200DayMovingAverage'),
                'beta': data.get('Beta'),
                'shares_outstanding': data.get('SharesOutstanding'),
                'fiscal_year_end': data.get('FiscalYearEnd'),
                'latest_quarter': data.get('LatestQuarter'),
                'extract_date': datetime.utcnow().isoformat()
            })
            print(f"  {ticker}: {data.get('Name', 'Unknown')}")

        except Exception as e:
            print(f"  Warning: Could not fetch overview for {ticker}: {e}")

    print(f"  Extracted {len(results)} company overviews")
    return results


def main():
    """Run full Alpha Vantage extraction pipeline."""
    global CALLS_MADE

    print("=" * 60)
    print("Alpha Vantage Extraction Pipeline")
    print(f"Started: {datetime.utcnow().isoformat()}")
    print(f"Daily API limit: {MAX_DAILY_CALLS} calls")
    print("=" * 60)

    if not API_KEY or API_KEY == 'your_alpha_vantage_key_here':
        print("ERROR: ALPHA_VANTAGE_API_KEY not set in .env file")
        print("Get yours at: https://www.alphavantage.co/support/#api-key")
        return

    conn = get_snowflake_connection()

    try:
        # 1. Daily prices (10 API calls)
        prices_data = extract_daily_prices()
        load_json_to_snowflake(conn, prices_data, 'MARKET_DATA', 'DAILY_PRICES_RAW')

        # 2. Company overviews (10 API calls)
        overview_data = extract_company_overview()
        load_json_to_snowflake(conn, overview_data, 'MARKET_DATA', 'COMPANY_OVERVIEW_RAW')

        print("\n" + "=" * 60)
        print("Alpha Vantage extraction complete!")
        print(f"  Price records:     {len(prices_data)}")
        print(f"  Company overviews: {len(overview_data)}")
        print(f"  API calls used:    {CALLS_MADE}/{MAX_DAILY_CALLS}")
        print("=" * 60)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
