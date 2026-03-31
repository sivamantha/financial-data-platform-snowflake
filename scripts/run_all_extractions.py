"""
Master Extraction Runner
────────────────────────
Runs all extraction pipelines in sequence.
Usage: python run_all_extractions.py [--source SOURCE]

Options:
  --source    Run a single source: sec_edgar, fred, alpha_vantage, fx_rates
  (no args)   Runs all four sources in sequence
"""

import sys
import argparse
from datetime import datetime

from extract_sec_edgar import main as extract_sec_edgar
from extract_fred import main as extract_fred
from extract_alpha_vantage import main as extract_alpha_vantage
from extract_fx_rates import main as extract_fx_rates


EXTRACTORS = {
    'sec_edgar':     ('SEC EDGAR',       extract_sec_edgar),
    'fred':          ('FRED API',        extract_fred),
    'alpha_vantage': ('Alpha Vantage',   extract_alpha_vantage),
    'fx_rates':      ('FX Rates',        extract_fx_rates),
}


def main():
    parser = argparse.ArgumentParser(description='Run data extraction pipelines')
    parser.add_argument(
        '--source',
        choices=list(EXTRACTORS.keys()),
        help='Run a single source extraction'
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Financial Data Platform - Extraction Runner")
    print(f"Started: {datetime.utcnow().isoformat()}")
    print("=" * 60)

    if args.source:
        sources = {args.source: EXTRACTORS[args.source]}
    else:
        sources = EXTRACTORS

    results = {}
    for key, (name, extractor) in sources.items():
        print(f"\n{'─' * 60}")
        print(f"Running: {name}")
        print(f"{'─' * 60}")
        try:
            extractor()
            results[name] = 'SUCCESS'
        except Exception as e:
            print(f"ERROR in {name}: {e}")
            results[name] = f'FAILED: {e}'

    print(f"\n{'=' * 60}")
    print("Extraction Summary")
    print(f"{'=' * 60}")
    for name, status in results.items():
        icon = 'OK' if status == 'SUCCESS' else 'FAIL'
        print(f"  [{icon}] {name}: {status}")
    print(f"\nFinished: {datetime.utcnow().isoformat()}")
    print("=" * 60)


if __name__ == '__main__':
    main()
