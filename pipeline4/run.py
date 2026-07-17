"""
Pipeline4 Entry Point: Incremental derived collection updater.

Reads tenders where derived_at=None from TenderIntelligence,
extracts participants, and updates TenderAnalytics collections.

Usage:
    python -m pipeline4.run                    # Full run (all phases)
    python -m pipeline4.run --skip-competitors # Skip competitor computation
    python -m pipeline4.run --phase1-only      # Only extract + upsert seller_tenders
"""
import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline4.orchestrator import run_pipeline, process_new_tenders, recompute_seller_aggregates, recompute_competitors


def parse_args():
    parser = argparse.ArgumentParser(
        description="Pipeline4: Incremental derived collection updater."
    )
    parser.add_argument(
        "--skip-competitors",
        action="store_true",
        help="Skip Phase 3 (competitor recomputation). Useful for quick runs.",
    )
    parser.add_argument(
        "--phase1-only",
        action="store_true",
        help="Only run Phase 1 (extract + upsert seller_tenders). Skip aggregates and competitors.",
    )
    parser.add_argument(
        "--recompute-only",
        nargs="+",
        metavar="SELLER_ID",
        help="Skip Phase 1; recompute aggregates + competitors for specific sellers only.",
    )
    parser.add_argument(
        "--month",
        default=None,
        help="Process only this month from raw tenders (format: YYYY-MM).",
    )
    parser.add_argument(
        "--months",
        nargs="+",
        default=None,
        help="Process only these months from raw tenders (format: YYYY-MM YYYY-MM ...).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    month_filter = args.months if args.months else args.month

    if args.recompute_only:
        # Manual recompute mode — skip Phase 1, directly recompute specified sellers
        from pipeline4.db import ensure_indexes
        ensure_indexes()
        sellers = set(s.strip().upper() for s in args.recompute_only)
        print(f"  Manual recompute for {len(sellers)} sellers: {sorted(sellers)[:5]}...")
        recompute_seller_aggregates(sellers)
        if not args.skip_competitors:
            recompute_competitors(sellers)
    elif args.phase1_only:
        from pipeline4.db import ensure_indexes
        ensure_indexes()
        result = process_new_tenders(month_filter=month_filter)
        print(f"\n  Done. {result['tenders_processed']} tenders processed, "
              f"{len(result['impacted_sellers'])} sellers impacted.")
    else:
        run_pipeline(skip_competitors=args.skip_competitors, month_filter=month_filter)


if __name__ == "__main__":
    main()
