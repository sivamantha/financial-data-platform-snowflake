"""
Open Exchange Rates Data Extraction
────────────────────────────────────
Extracts daily exchange rates for major currency pairs.
Free tier: 1,000 requests/month, hourly updates, USD base only.

Target currencies: CAD, EUR, GBP, JPY, CHF, AUD, CNY, INR, BRL, MXN

API Docs: https://docs.openexchangerates.org/reference/api-introduction
"""

import os
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path
from snowflake_utils import get_snowflake_connection, load_json_to_snowflake

load_dotenv(Path(__file__).parent.parent / '.env')

# ── Configuration ─────────────────────────────────────────────
APP_ID = os.getenv('OPEN_EXCHANGE_RATES_APP_ID')
BASE_URL = 'https://openexchangerates.org/api'

# Major currencies for financial analysis
TARGET_CURRENCIES = ['CAD', 'EUR', 'GBP', 'JPY', 'CHF',
                     'AUD', 'CNY', 'INR', 'BRL', 'MXN']

# Historical date range (free tier allows historical endpoint)
# Pull last 30 days to stay within monthly limit
LOOKBACK_DAYS = 30


def rate_limit():
    """Be conservative with the 1,000/month limit."""
    time.sleep(1)


def extract_latest_rates():
    """
    Extract the latest exchange rates.
    Free tier always returns USD as base currency.
    """
    print("Extracting latest exchange rates...")
    results = []

    try:
        params = {
            'app_id': APP_ID,
            'symbols': ','.join(TARGET_CURRENCIES),
            'show_alternative': 'false'
        }
        resp = requests.get(f"{BASE_URL}/latest.json", params=params)
        resp.raise_for_status()
        data = resp.json()
        rate_limit()

        timestamp = data.get('timestamp')
        base = data.get('base', 'USD')
        rates = data.get('rates', {})

        for currency, rate in rates.items():
            results.append({
                'base_currency': base,
                'target_currency': currency,
                'rate': rate,
                'rate_date': datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d'),
                'timestamp': datetime.utcfromtimestamp(timestamp).isoformat(),
                'extract_date': datetime.utcnow().isoformat()
            })

        print(f"  Latest rates: {len(results)} currency pairs (base: {base})")

    except Exception as e:
        print(f"  Warning: Could not fetch latest rates: {e}")

    return results


def extract_historical_rates():
    """
    Extract historical daily rates for the lookback period.
    One API call per day — uses ~30 of the 1,000 monthly limit.
    """
    print(f"Extracting historical rates (last {LOOKBACK_DAYS} days)...")
    results = []

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    current_date = start_date

    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')

        try:
            params = {
                'app_id': APP_ID,
                'symbols': ','.join(TARGET_CURRENCIES),
                'show_alternative': 'false'
            }
            resp = requests.get(f"{BASE_URL}/historical/{date_str}.json", params=params)
            resp.raise_for_status()
            data = resp.json()
            rate_limit()

            base = data.get('base', 'USD')
            rates = data.get('rates', {})

            for currency, rate in rates.items():
                results.append({
                    'base_currency': base,
                    'target_currency': currency,
                    'rate': rate,
                    'rate_date': date_str,
                    'extract_date': datetime.utcnow().isoformat()
                })

            print(f"  {date_str}: {len(rates)} pairs")

        except Exception as e:
            print(f"  Warning: Could not fetch rates for {date_str}: {e}")

        current_date += timedelta(days=1)

    print(f"  Total: {len(results)} historical rate records")
    return results


def main():
    """Run full FX rates extraction pipeline."""
    print("=" * 60)
    print("Open Exchange Rates Extraction Pipeline")
    print(f"Started: {datetime.utcnow().isoformat()}")
    print(f"Lookback: {LOOKBACK_DAYS} days")
    print("=" * 60)

    if not APP_ID or APP_ID == 'your_app_id_here':
        print("ERROR: OPEN_EXCHANGE_RATES_APP_ID not set in .env file")
        print("Get yours at: https://openexchangerates.org/signup/free")
        return

    conn = get_snowflake_connection()

    try:
        # 1. Latest rates (1 API call)
        latest_data = extract_latest_rates()
        load_json_to_snowflake(conn, latest_data, 'FX_RATES', 'EXCHANGE_RATES_RAW')

        # 2. Historical rates (~30 API calls)
        historical_data = extract_historical_rates()
        load_json_to_snowflake(conn, historical_data, 'FX_RATES', 'EXCHANGE_RATES_RAW')

        total_records = len(latest_data) + len(historical_data)
        print("\n" + "=" * 60)
        print("FX rates extraction complete!")
        print(f"  Latest rates:     {len(latest_data)}")
        print(f"  Historical rates: {len(historical_data)}")
        print(f"  Total records:    {total_records}")
        print(f"  API calls used:   ~{LOOKBACK_DAYS + 1}")
        print("=" * 60)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
