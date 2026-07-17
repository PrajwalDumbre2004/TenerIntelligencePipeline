"""
Quick Validation - Fast subset of critical checks

Runs only the most important consistency checks for rapid feedback.
Use this during development; run full validation before deployment.

Usage:
    python test/quick_validate.py
    python test/quick_validate.py --month 2026-01
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
from validate_data_consistency import (
    get_client, ValidationReport,
    validate_count_match, validate_no_orphans,
    validate_seller_math, validate_no_duplicates
)
from pipeline4.config import SOURCE_DB, TARGET_DB


def main():
    parser = argparse.ArgumentParser(description="Quick data validation (critical checks only)")
    parser.add_argument("--month", help="Validate specific month (YYYY-MM)")
    args = parser.parse_args()
    
    print("=" * 70)
    print("  QUICK VALIDATION (Critical Checks Only)")
    print("=" * 70)
    
    client = get_client()
    source_db = client[SOURCE_DB]
    target_db = client[TARGET_DB]
    
    report = ValidationReport()
    months = [args.month] if args.month else None
    
    # Run only critical tests (fast)
    validate_count_match(source_db, report, months)
    validate_no_orphans(source_db, report, months)
    validate_seller_math(target_db, report, sample_size=10)  # Small sample
    validate_no_duplicates(target_db, report)
    
    success = report.summary()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
