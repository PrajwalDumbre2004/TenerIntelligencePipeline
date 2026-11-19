"""Combined Pipeline: Runs Pipeline2 and Pipeline3 for each month.

Flow per month:
  1. Pipeline2: Multi-pass fetch all listing data → store in DB
  2. Pipeline3: Fetch bid results/RA results for all tenders in that month → update DB
  3. Move to next month

Usage:
    python pipeline_main/run.py                  # Process all months in range below
    python pipeline_main/run.py 2024-01 2024-06  # Custom range
    python pipeline_main/run.py 2025-07          # One month (re-running a month fetches
                                                 # only what is missing: new tech/fin/awarded
                                                 # bids, late awards, stage/cancel changes)
    python pipeline_main/run.py 2025-07 --days 15-16   # TEST ONLY: one month, bid end
                                                       # dates 15..16 only

Each month lists three GeM statuses (pipeline2/config.py BY_STATUSES):
technically evaluated, financially evaluated and awarded -- cancelled bids at
those stages included.

IMPORTANT: after a month finishes here, run pipeline4 before re-running that
same month for fresh data:
    psql "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -f pipeline4/sql/00_run_all.sql
Until pipeline4's cleanup runs, every bid listed for the month stays in
staging (already-known bids are only marked known_at), so a re-run counts the
month's lists as captured and skips them -- that is what lets a killed run
resume cheaply, but it also means a re-run before pipeline4 won't re-list.
The cleanup also removes rejected bids (no participant data / all names
masked), so the next re-run retries them.

Pipeline3 runs for a month only if pipeline2 finished it: before that,
already-known bids aren't marked yet and pipeline3 would re-scrape them.

Features:
  - Fault tolerance: Each pipeline wrapped in try-catch
  - Graceful degradation: If one pipeline fails, others still run
  - Progress tracking: Summary of successes/failures at end
  - Error logging: Failures saved to logs/pipeline_failures.log
"""
import sys
import os
import time
import signal
import asyncio
from datetime import datetime, timezone

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline2.orchestrator import process_month as pipeline2_process_month, gem_date
from pipeline3.orchestrator import process_month as pipeline3_process_month
from pipeline2.db import create_indexes as p2_create_indexes, init_month_status, reset_in_progress_months

# ═══════════════════════════════════════════════════════════════
# CONFIG — month range, shared with pipeline2/pipeline3 via common/config.py.
# Change it there (or pass CLI args below) to affect all three.
# ═══════════════════════════════════════════════════════════════
from common.config import START_YEAR, START_MONTH, END_YEAR, END_MONTH

_shutdown = False


def _log_failure_to_file(month, pipeline, error):
    """Log pipeline failure to logs/pipeline_failures.log"""
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "pipeline_failures.log")
    
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*70}\n")
            f.write(f"[{timestamp}] {month} - {pipeline}\n")
            f.write(f"{'-'*70}\n")
            f.write(f"{error}\n")
    except Exception:
        pass  # Silent fail on logging


def _signal_handler(sig, frame):
    print("\n  [!] Force shutdown — killing all workers.")
    os._exit(1)


def generate_month_list(start_year, start_month, end_year, end_month):
    months = []
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def main():
    global _shutdown
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # Optional test-only day window: --days 15-16 (single month only)
    args = sys.argv[1:]
    day_window = None
    if "--days" in args:
        i = args.index("--days")
        try:
            d1, d2 = (int(x) for x in args[i + 1].split("-"))
        except (IndexError, ValueError):
            sys.exit("--days expects a range like 15-16")
        day_window = (d1, d2)
        del args[i:i + 2]

    # Parse optional CLI args: start_month end_month
    if len(args) >= 2:
        start = args[0]  # e.g. "2024-01"
        end = args[1]    # e.g. "2024-06"
        sy, sm = int(start[:4]), int(start[5:7])
        ey, em = int(end[:4]), int(end[5:7])
    elif len(args) == 1:
        # Single month
        start = args[0]
        sy, sm = int(start[:4]), int(start[5:7])
        ey, em = sy, sm
    else:
        sy, sm = START_YEAR, START_MONTH
        ey, em = END_YEAR, END_MONTH

    months = generate_month_list(sy, sm, ey, em)

    window_from = window_to = None
    if day_window:
        if len(months) != 1:
            sys.exit("--days can only be used with a single month")
        window_from = gem_date(sy, sm, day_window[0])
        window_to = gem_date(sy, sm, day_window[1])
        print(f"  [TEST] Day window: {window_from} to {window_to}")

    print("=" * 70)
    print("  COMBINED PIPELINE — Pipeline2 + Pipeline3")
    print("=" * 70)
    print(f"  Months to process: {len(months)} ({months[0]} to {months[-1]})")
    print("=" * 70)

    # Initialize DB indexes
    print("\n  Initializing database indexes...")
    try:
        p2_create_indexes()
        print("  ✓ Indexes created successfully")
    except Exception as e:
        print(f"  ✗ WARNING: Index creation failed: {str(e)[:200]}")
        print("  Continuing anyway (indexes may already exist)...")

    # Reset any months stuck as in_progress from a previous crashed run
    try:
        reset_count = reset_in_progress_months()
        if reset_count > 0:
            print(f"  [!] Reset {reset_count} in_progress month(s) to pending (from previous crash)")
    except Exception as e:
        print(f"  ✗ WARNING: Failed to reset in-progress months: {str(e)[:200]}")
        print("  Continuing anyway...")

    # Track success/failure for final summary
    pipeline_stats = {
        "months_completed": 0,
        "months_partial": 0,
        "months_failed": 0,
        "failures": []
    }

    for i, month in enumerate(months):
        if _shutdown:
            print(f"\n  [!] Stopped. Completed {i}/{len(months)} months.")
            break

        t_start = time.time()

        print(f"\n{'#' * 70}")
        print(f"  MONTH {i+1}/{len(months)}: {month}")
        print(f"{'#' * 70}")

        p2_success = False
        p3_success = False

        # Step 1: Pipeline2 — Extract all listings
        print(f"\n  [STEP 1] Pipeline2: Extracting all listings for {month}...")
        try:
            init_month_status(month)
            p2_success = bool(pipeline2_process_month(month, window_from, window_to))
            if not p2_success:
                raise RuntimeError("pipeline2 did not complete the month (see log above)")
            print(f"  ✓ Pipeline2 completed successfully")
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            print(f"  ✗ Pipeline2 FAILED: {error_msg[:200]}")
            _log_failure_to_file(month, "Pipeline2", error_msg)
            pipeline_stats["failures"].append({
                "month": month,
                "pipeline": "Pipeline2",
                "error": error_msg[:500]
            })

        if _shutdown:
            break

        # Step 2: Pipeline3 — Extract results for this month. Only after
        # pipeline2 finished the month: until then known bids aren't marked
        # and pipeline3 would re-scrape bids already in production.
        if not p2_success:
            print(f"\n  [STEP 2] Pipeline3: skipped for {month} (pipeline2 did not finish the month; "
                  f"re-run it to resume)")
        else:
            print(f"\n  [STEP 2] Pipeline3: Extracting results for {month}...")
            try:
                asyncio.run(pipeline3_process_month(month))
                p3_success = True
                print(f"  ✓ Pipeline3 completed successfully")
            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                print(f"  ✗ Pipeline3 FAILED: {error_msg[:200]}")
                _log_failure_to_file(month, "Pipeline3", error_msg)
                pipeline_stats["failures"].append({
                    "month": month,
                    "pipeline": "Pipeline3",
                    "error": error_msg[:500]
                })

        elapsed = time.time() - t_start

        # Track month completion status
        if p2_success and p3_success:
            pipeline_stats["months_completed"] += 1
            print(f"\n  ✓ Month {month} FULLY COMPLETE — Total time: {elapsed:.0f}s ({elapsed/60:.1f}min)")
        elif p2_success or p3_success:
            pipeline_stats["months_partial"] += 1
            print(f"\n  ⚠ Month {month} PARTIALLY COMPLETE — Total time: {elapsed:.0f}s ({elapsed/60:.1f}min)")
        else:
            pipeline_stats["months_failed"] += 1
            print(f"\n  ✗ Month {month} FAILED — All pipelines failed — Total time: {elapsed:.0f}s ({elapsed/60:.1f}min)")

    # Final summary
    print(f"\n{'=' * 70}")
    print("  PIPELINE EXECUTION SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Total months processed    : {len(months)}")
    print(f"  Fully completed           : {pipeline_stats['months_completed']}")
    print(f"  Partially completed       : {pipeline_stats['months_partial']}")
    print(f"  Failed                    : {pipeline_stats['months_failed']}")
    
    if pipeline_stats["failures"]:
        print(f"\n  ⚠ FAILURES ({len(pipeline_stats['failures'])}):")
        for fail in pipeline_stats["failures"]:
            print(f"    • {fail['month']} - {fail['pipeline']}: {fail['error'][:150]}")
        print(f"\n  Detailed errors logged to: logs/pipeline_failures.log")
    else:
        print(f"\n  ✓ All pipelines completed successfully!")
    
    print(f"{'=' * 70}")
    
    # Exit with error code if there were any failures
    if pipeline_stats["failures"]:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
