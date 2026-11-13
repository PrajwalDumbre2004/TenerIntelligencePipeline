"""
Orchestrator for Pipeline3: Sequential month processing with async multi-page workers.

Flow:
  1. For each month in range, get tenders from DB that need results (scraped_at=None)
  2. Divide tenders among NUM_WORKERS async browser workers
  3. Each browser worker uses PAGES_PER_BROWSER concurrent pages
  4. Move to next month when done
"""
import sys
import time
import signal
import asyncio

from playwright.async_api import async_playwright

from pipeline3.config import (
    NUM_WORKERS, PAGES_PER_BROWSER, START_YEAR, START_MONTH, END_YEAR, END_MONTH
)
from pipeline3.db import (
    get_tenders_needing_results, get_month_result_stats,
    update_p3_status, get_p3_status
)
from pipeline3.worker import browser_worker
from pipeline3.derive import derive_month

MAX_P3_PASSES = 3  # Max retry passes for pipeline3 per month

# Graceful shutdown
_shutdown = False


def _signal_handler(sig, frame):
    global _shutdown
    print("\n  [!] Shutdown signal received. Finishing current batch...")
    _shutdown = True


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


async def process_month(month_str):
    """Process all tenders needing results for a given month, with retry passes."""
    print(f"\n{'=' * 70}")
    print(f"  PIPELINE3 — Processing results for: {month_str}")
    print(f"{'=' * 70}")

    total_concurrency = NUM_WORKERS * PAGES_PER_BROWSER

    for pass_num in range(1, MAX_P3_PASSES + 1):
        # Get tenders that need results
        tenders = get_tenders_needing_results(month_str)
        if not tenders:
            stats = get_month_result_stats(month_str)
            print(f"  No pending tenders. (Total: {stats['total']}, Done: {stats['done']})")
            update_p3_status(month_str, "completed",
                             pending=0, done=stats["done"], total=stats["total"],
                             passes=pass_num - 1)
            derive_month(month_str)
            return True

        stats = get_month_result_stats(month_str)
        if pass_num > 1:
            print(f"\n  --- Pass {pass_num}/{MAX_P3_PASSES}: {len(tenders)} remaining ---")

        print(f"  Tenders to process: {len(tenders)}")
        print(f"  Browsers: {NUM_WORKERS} x {PAGES_PER_BROWSER} pages = {total_concurrency} concurrent")

        update_p3_status(month_str, "in_progress",
                         pending=stats["pending"], done=stats["done"],
                         total=stats["total"], passes=pass_num)

        # Divide tenders among browser workers
        batches = [[] for _ in range(NUM_WORKERS)]
        for i, tender in enumerate(tenders):
            batches[i % NUM_WORKERS].append(tender)
        del tenders

        # Remove empty batches
        batches = [b for b in batches if b]
        actual_workers = len(batches)

        for i, batch in enumerate(batches):
            print(f"    Browser {i+1}: {len(batch)} tenders")

        t_start = time.time()

        # Launch all browser workers concurrently
        async with async_playwright() as pw:
            tasks = [
                browser_worker(wid + 1, batch, pw, month_str)
                for wid, batch in enumerate(batches)
            ]
            worker_results = await asyncio.gather(*tasks)

        elapsed = time.time() - t_start

        # Summary
        total_processed = 0
        total_success = 0
        total_no_data = 0
        total_failed = 0

        print(f"\n  {'Worker':>7s} {'Processed':>10s} {'Success':>8s} {'No Data':>8s} {'Failed':>7s}")
        print(f"  {'-'*7} {'-'*10} {'-'*8} {'-'*8} {'-'*7}")

        for r in sorted(worker_results, key=lambda x: x["worker_id"]):
            if r["error"]:
                print(f"  W{r['worker_id']:>5d}   ERROR: {r['error']}")
                continue
            print(f"  W{r['worker_id']:>5d} {r['processed']:>10d} {r['success']:>8d} "
                  f"{r['no_data']:>8d} {r['failed']:>7d}")
            total_processed += r["processed"]
            total_success += r["success"]
            total_no_data += r["no_data"]
            total_failed += r["failed"]

        stats = get_month_result_stats(month_str)
        print(f"\n  Month {month_str}: {stats['done']}/{stats['total']} done, "
              f"{stats['pending']} pending | Time: {elapsed:.0f}s")
        print(f"  This pass: {total_success} with data, {total_no_data} no data, {total_failed} failed")

        # Check if done
        if stats["pending"] == 0:
            update_p3_status(month_str, "completed",
                             pending=0, done=stats["done"], total=stats["total"],
                             passes=pass_num)
            derive_month(month_str)
            return True

        # If pending remains but no progress was made, stop retrying
        if total_processed == 0:
            print(f"  [!] No progress made — stopping retries for {month_str}")
            break

    # After all passes, update final status
    stats = get_month_result_stats(month_str)
    if stats["pending"] > 0:
        update_p3_status(month_str, "partial",
                         pending=stats["pending"], done=stats["done"],
                         total=stats["total"], passes=MAX_P3_PASSES)
        print(f"  [!] {month_str}: {stats['pending']} tenders still pending after {MAX_P3_PASSES} passes")
    else:
        update_p3_status(month_str, "completed",
                         pending=0, done=stats["done"], total=stats["total"],
                         passes=MAX_P3_PASSES)

    # Flatten whatever did get scraped this run, even if some tenders are
    # still pending (they'll be picked up on the next pass/retry).
    derive_month(month_str)
    return stats["pending"] == 0


async def run_pipeline():
    """Main entry point: process months sequentially."""
    global _shutdown

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    total_concurrency = NUM_WORKERS * PAGES_PER_BROWSER

    print("=" * 70)
    print("  PIPELINE 3 — Bid Result Extraction (Async Multi-Page)")
    print("=" * 70)
    print(f"  Browsers       : {NUM_WORKERS}")
    print(f"  Pages/browser  : {PAGES_PER_BROWSER}")
    print(f"  Total parallel : {total_concurrency}")
    print(f"  Date range     : {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}")
    print("=" * 70)

    months = generate_month_list(START_YEAR, START_MONTH, END_YEAR, END_MONTH)
    print(f"  Total months: {len(months)}")

    completed = 0
    for month in months:
        if _shutdown:
            print("\n  [!] Shutdown. Stopping.")
            break

        success = await process_month(month)
        if success:
            completed += 1

    # Final summary
    print(f"\n{'=' * 70}")
    print(f"  DONE — {completed}/{len(months)} months fully processed")
    print(f"{'=' * 70}")
