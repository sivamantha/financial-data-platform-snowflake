"""
SEC EDGAR Data Extraction
─────────────────────────
Extracts:
  1. Company reference data (CIK, name, SIC codes)
  2. Company filings metadata (10-K, 10-Q, 8-K)
  3. Financial statement data (revenue, net income, assets, EPS)

SEC EDGAR API: No key required, but User-Agent header with email is mandatory.
Rate limit: 10 requests/second
Docs: https://www.sec.gov/edgar/sec-api-documentation

Verified endpoints (2026-03-30):
  - https://data.sec.gov/submissions/CIK{cik}.json            — company info + filings
  - https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json — financials

NOTE: companytickers.json is dead (S3 NoSuchKey). Company info is pulled
      directly from each company's submissions endpoint instead.
"""

import os
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path
from snowflake_utils import get_snowflake_connection, load_json_to_snowflake

load_dotenv(Path(__file__).parent.parent / '.env')

# ── Configuration ─────────────────────────────────────────────
USER_AGENT = os.getenv('SEC_EDGAR_USER_AGENT', 'DataEngProject dev@example.com')
DATA_URL   = 'https://data.sec.gov'
HEADERS    = {'User-Agent': USER_AGENT, 'Accept-Encoding': 'gzip, deflate'}

# Target companies — large cap, diverse sectors, reliable filing history
# CIK numbers are stable permanent identifiers, no lookup endpoint needed
TARGET_COMPANIES = {
    'AAPL': '0000320193',  # Apple           - Technology
    'MSFT': '0000789019',  # Microsoft       - Technology
    'JPM':  '0000019617',  # JPMorgan Chase  - Financials
    'JNJ':  '0000200406',  # Johnson & Johnson - Healthcare
    'XOM':  '0000034088',  # ExxonMobil      - Energy
    'WMT':  '0000104169',  # Walmart         - Consumer Staples
    'PG':   '0000080424',  # Procter & Gamble - Consumer Goods
    'BAC':  '0000070858',  # Bank of America - Financials
    'DIS':  '0001744489',  # Disney          - Communication
    'CAT':  '0000018230',  # Caterpillar     - Industrials
}

FILING_TYPES = ['10-K', '10-Q', '8-K']

FINANCIAL_TAGS = [
    'Revenues',
    'RevenueFromContractWithCustomerExcludingAssessedTax',
    'NetIncomeLoss',
    'Assets',
    'EarningsPerShareBasic',
    'StockholdersEquity',
    'OperatingIncomeLoss',
]


def rate_limit():
    """SEC requires max 10 requests/second — 0.12s gap is safe."""
    time.sleep(0.12)


def fetch_json(url: str) -> dict:
    """Fetch JSON with retries and exponential back-off."""
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError:
            if resp.status_code == 404:
                raise
            print(f"  HTTP {resp.status_code} on attempt {attempt + 1}: {url}")
            time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as e:
            print(f"  Network error attempt {attempt + 1}: {e}")
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed after 3 attempts: {url}")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Extract 1: Company reference data ─────────────────────────
def extract_company_info() -> list:
    """
    Pull company metadata from each company's submissions endpoint.
    No bulk lookup endpoint needed — CIKs are hardcoded constants.
    Verified URL: /submissions/CIK{cik}.json
    """
    print("Extracting company info...")
    results = []

    for ticker, cik in TARGET_COMPANIES.items():
        try:
            url  = f"{DATA_URL}/submissions/CIK{cik}.json"
            data = fetch_json(url)
            rate_limit()

            results.append({
                'cik':                    cik,
                'ticker':                 ticker,
                'company_name':           data.get('name', ''),
                'sic_code':               data.get('sic', ''),
                'sic_description':        data.get('sicDescription', ''),
                'state_of_incorporation': data.get('stateOfIncorporation', ''),
                'fiscal_year_end':        data.get('fiscalYearEnd', ''),
                'entity_type':            data.get('entityType', ''),
                'exchanges':              ','.join(data.get('exchanges', [])),
                'extract_date':           now_iso(),
            })
            print(f"  {ticker}: OK")

        except Exception as e:
            print(f"  Warning: Could not fetch company info for {ticker}: {e}")

    print(f"  Extracted {len(results)} companies")
    return results


# ── Extract 2: Filings metadata ────────────────────────────────
def extract_filings() -> list:
    """
    Pull recent 10-K, 10-Q, 8-K filings from submissions endpoint.
    Verified URL: /submissions/CIK{cik}.json
    """
    print("Extracting filings metadata...")
    results = []

    for ticker, cik in TARGET_COMPANIES.items():
        try:
            url    = f"{DATA_URL}/submissions/CIK{cik}.json"
            data   = fetch_json(url)
            rate_limit()

            recent = data.get('filings', {}).get('recent', {})
            if not recent:
                print(f"  {ticker}: no recent filings found")
                continue

            forms        = recent.get('form', [])
            dates        = recent.get('filingDate', [])
            accessions   = recent.get('accessionNumber', [])
            primary_docs = recent.get('primaryDocument', [])

            ticker_count = 0
            for i in range(len(forms)):
                if forms[i] not in FILING_TYPES:
                    continue

                accession = accessions[i] if i < len(accessions) else None
                primary   = primary_docs[i] if i < len(primary_docs) else None

                filing_url = None
                if accession and primary:
                    acc_clean  = accession.replace('-', '')
                    filing_url = (
                        f"https://www.sec.gov/Archives/edgar/data/"
                        f"{int(cik)}/{acc_clean}/{primary}"
                    )

                results.append({
                    'cik':              cik,
                    'ticker':           ticker,
                    'filing_type':      forms[i],
                    'filing_date':      dates[i] if i < len(dates) else None,
                    'accession_number': accession,
                    'primary_document': primary,
                    'filing_url':       filing_url,
                    'extract_date':     now_iso(),
                })
                ticker_count += 1

            print(f"  {ticker}: {ticker_count} filings")

        except Exception as e:
            print(f"  Warning: Could not fetch filings for {ticker}: {e}")

    print(f"  Total: {len(results)} filings extracted")
    return results


# ── Extract 3: Financial statements ───────────────────────────
def extract_financial_statements() -> list:
    """
    Pull XBRL financial facts (revenue, net income, assets, EPS).
    Verified URL: /api/xbrl/companyfacts/CIK{cik}.json
    """
    print("Extracting financial statements...")
    results = []

    for ticker, cik in TARGET_COMPANIES.items():
        try:
            url  = f"{DATA_URL}/api/xbrl/companyfacts/CIK{cik}.json"
            data = fetch_json(url)
            rate_limit()

            facts        = data.get('facts', {}).get('us-gaap', {})
            ticker_count = 0

            for tag_name in FINANCIAL_TAGS:
                tag_data = facts.get(tag_name, {})
                units    = tag_data.get('units', {})

                for unit_type, values in units.items():
                    for entry in values:
                        form = entry.get('form', '')
                        if form not in ['10-K', '10-Q']:
                            continue

                        results.append({
                            'cik':              cik,
                            'ticker':           ticker,
                            'metric_name':      tag_name,
                            'value':            entry.get('val'),
                            'unit':             unit_type,
                            'period_end':       entry.get('end'),
                            'period_start':     entry.get('start'),
                            'fiscal_year':      entry.get('fy'),
                            'fiscal_period':    entry.get('fp'),
                            'form_type':        form,
                            'filed_date':       entry.get('filed'),
                            'accession_number': entry.get('accn'),
                            'extract_date':     now_iso(),
                        })
                        ticker_count += 1

            print(f"  {ticker}: {ticker_count} financial data points")

        except Exception as e:
            print(f"  Warning: Could not fetch financials for {ticker}: {e}")

    print(f"  Total: {len(results)} financial data points extracted")
    return results


# ── Main ───────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("SEC EDGAR Extraction Pipeline")
    print(f"Started: {now_iso()}")
    print("=" * 60)

    conn = get_snowflake_connection()

    try:
        company_data = extract_company_info()
        load_json_to_snowflake(conn, company_data, 'SEC_EDGAR', 'COMPANY_INFO_RAW')

        filings_data = extract_filings()
        load_json_to_snowflake(conn, filings_data, 'SEC_EDGAR', 'FILINGS_RAW')

        financials_data = extract_financial_statements()
        load_json_to_snowflake(conn, financials_data, 'SEC_EDGAR', 'FINANCIAL_STATEMENTS_RAW')

        print("\n" + "=" * 60)
        print("SEC EDGAR extraction complete!")
        print(f"  Companies:  {len(company_data)}")
        print(f"  Filings:    {len(filings_data)}")
        print(f"  Financials: {len(financials_data)}")
        print("=" * 60)

    finally:
        conn.close()


if __name__ == '__main__':
    main()