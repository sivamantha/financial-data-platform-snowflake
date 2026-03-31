"""
FRED API Data Extraction
────────────────────────
Extracts macroeconomic time series data from the Federal Reserve:
  - Federal Funds Rate (DFF)
  - 10-Year Treasury Yield (DGS10)
  - Consumer Price Index (CPIAUCSL)
  - Unemployment Rate (UNRATE)
  - GDP Growth (GDP)
  - PCE Inflation (PCEPI)
  - S&P 500 Index (SP500)
  - Housing Starts (HOUST)

API Docs: https://fred.stlouisfed.org/docs/api/fred/
Rate limit: 120 requests/minute
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
API_KEY = os.getenv('FRED_API_KEY')
BASE_URL = 'https://api.stlouisfed.org/fred'

# Key economic indicators with descriptions
SERIES = {
    'DFF':      'Federal Funds Effective Rate (daily)',
    'DGS10':    '10-Year Treasury Constant Maturity Rate (daily)',
    'CPIAUCSL': 'Consumer Price Index for All Urban Consumers (monthly)',
    'UNRATE':   'Unemployment Rate (monthly)',
    'GDP':      'Gross Domestic Product (quarterly)',
    'PCEPI':    'Personal Consumption Expenditures Price Index (monthly)',
    'SP500':    'S&P 500 Index (daily)',
    'HOUST':    'Housing Starts Total (monthly)',
    'FEDFUNDS': 'Federal Funds Rate (monthly average)',
    'T10Y2Y':   '10Y-2Y Treasury Spread (daily)',
}

# How far back to pull data
OBSERVATION_START = '2015-01-01'


def rate_limit():
    """FRED allows 120 requests/minute."""
    time.sleep(0.6)


def extract_series_metadata():
    """
    Extract metadata for each series (units, frequency, description).
    """
    print("Extracting series metadata...")
    results = []

    for series_id, description in SERIES.items():
        try:
            url = f"{BASE_URL}/series"
            params = {
                'series_id': series_id,
                'api_key': API_KEY,
                'file_type': 'json'
            }
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            rate_limit()

            series_info = data.get('seriess', [{}])[0]
            results.append({
                'series_id': series_id,
                'title': series_info.get('title', description),
                'frequency': series_info.get('frequency', ''),
                'units': series_info.get('units', ''),
                'seasonal_adjustment': series_info.get('seasonal_adjustment', ''),
                'observation_start': series_info.get('observation_start', ''),
                'observation_end': series_info.get('observation_end', ''),
                'last_updated': series_info.get('last_updated', ''),
                'extract_date': datetime.utcnow().isoformat()
            })
            print(f"  {series_id}: {series_info.get('title', description)[:50]}")

        except Exception as e:
            print(f"  Warning: Could not fetch metadata for {series_id}: {e}")

    print(f"  Extracted metadata for {len(results)} series")
    return results


def extract_observations():
    """
    Extract time series observations for all configured series.
    Pulls data from OBSERVATION_START to present.
    """
    print(f"Extracting observations (from {OBSERVATION_START})...")
    results = []

    for series_id, description in SERIES.items():
        try:
            url = f"{BASE_URL}/series/observations"
            params = {
                'series_id': series_id,
                'api_key': API_KEY,
                'file_type': 'json',
                'observation_start': OBSERVATION_START,
                'sort_order': 'asc'
            }
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            rate_limit()

            observations = data.get('observations', [])
            count = 0

            for obs in observations:
                # Skip missing values (FRED uses '.' for missing)
                value = obs.get('value', '.')
                if value == '.':
                    continue

                results.append({
                    'series_id': series_id,
                    'observation_date': obs.get('date'),
                    'value': float(value),
                    'realtime_start': obs.get('realtime_start'),
                    'realtime_end': obs.get('realtime_end'),
                    'extract_date': datetime.utcnow().isoformat()
                })
                count += 1

            print(f"  {series_id}: {count} observations")

        except Exception as e:
            print(f"  Warning: Could not fetch observations for {series_id}: {e}")

    print(f"  Total: {len(results)} observations extracted")
    return results


def main():
    """Run full FRED extraction pipeline."""
    print("=" * 60)
    print("FRED API Extraction Pipeline")
    print(f"Started: {datetime.utcnow().isoformat()}")
    print("=" * 60)

    if not API_KEY or API_KEY == 'your_fred_api_key_here':
        print("ERROR: FRED_API_KEY not set in .env file")
        print("Get yours at: https://fred.stlouisfed.org/docs/api/api_key.html")
        return

    conn = get_snowflake_connection()

    try:
        # 1. Series metadata
        metadata = extract_series_metadata()
        load_json_to_snowflake(conn, metadata, 'FRED', 'SERIES_METADATA_RAW')

        # 2. Time series observations
        observations = extract_observations()
        load_json_to_snowflake(conn, observations, 'FRED', 'ECONOMIC_INDICATORS_RAW')

        print("\n" + "=" * 60)
        print("FRED extraction complete!")
        print(f"  Series:       {len(metadata)}")
        print(f"  Observations: {len(observations)}")
        print("=" * 60)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
