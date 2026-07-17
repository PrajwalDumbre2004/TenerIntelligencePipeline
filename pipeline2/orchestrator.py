"""
Orchestrator for Pipeline2: Sequential month processing with multi-pass per month.

Flow:
  1. Pick next pending month (sequential, one at a time)
  2. Get numFound for that month
  3. Run passes (up to MAX_PASSES) with NUM_WORKERS parallel page-fetchers
  4. After each pass, bulk-upsert new docs to MongoDB (dedup by bid_id)
  5. Stop when unique_captured >= numFound or MAX_PASSES reached
  6. Move to next month
"""
import sys
import time
import signal
import calendar
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

from pipeline2.config import (
    NUM_WORKERS, MAX_PASSES, DELAY_BETWEEN_PASSES,
    START_YEAR, START_MONTH, END_YEAR, END_MONTH,
    USER_AGENT, BROWSER_LOCALE, BROWSER_VIEWPORT, PAGE_SIZE,
    SORT_BY
)
from pipeline2.db import (
    create_indexes, init_month_status, start_month,
    update_month_progress, complete_month, fail_month,
    get_next_pending_month, get_all_month_statuses,
    bulk_upsert_tenders, get_month_bid_count
)
from pipeline2.worker import (
    worker_fetch_pages, capture_csrf, fetch_page, _init_worker
)

# Graceful shutdown
_shutdown = False


def _signal_handler(sig, frame):
    global _shutdown
    print("\n  [!] Shutdown signal received. Finishing current pass...")
    _shutdown = True


def gem_date(year, month, day):
    names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
             "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    return f"{day:02d}-{names[month - 1]}-{year}"


def month_to_date_range(month_str):
    y, m = int(month_str[:4]), int(month_str[5:7])
    last_day = calendar.monthrange(y, m)[1]
    return gem_date(y, m, 1), gem_date(y, m, last_day)


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


def get_num_found(from_date, to_date):
    """Quick single request to get numFound for a month."""
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = browser.new_context(
            user_agent=USER_AGENT,
            locale=BROWSER_LOCALE,
            viewport=BROWSER_VIEWPORT
        )
        page = context.new_page()
        csrf = capture_csrf(page)
        _, num_found = fetch_page(page, csrf, from_date, to_date, 1)
        browser.close()
    return num_found


def run_pass(pass_num, from_date, to_date, month, expected_pages):
    """
    Run one pass: divide all pages among NUM_WORKERS (round-robin), fetch in parallel.
    Returns (list of tender_docs, total_new_ids_in_pass, stats).
    """
    # Round-robin: page 1→W1, page 2→W2, ..., page N→W(N%workers)+1
    page_assignments = [[] for _ in range(NUM_WORKERS)]
    for pg in range(1, expected_pages + 1):
        page_assignments[(pg - 1) % NUM_WORKERS].append(pg)

    # Remove empty assignments
    page_assignments = [pages for pages in page_assignments if pages]

    actual_workers = len(page_assignments)

    # Shared progress counters
    progress_array = [multiprocessing.Value('i', 0) for _ in range(actual_workers)]

    worker_results = []
    with ProcessPoolExecutor(
        max_workers=actual_workers,
        initializer=_init_worker,
        initargs=(progress_array,)
    ) as executor:
        futures = {
            executor.submit(
                worker_fetch_pages, wid + 1, from_date, to_date, month, page_assignments[wid]
            ): wid + 1
            for wid in range(actual_workers)
        }

        # Poll progress
        while True:
            done_count = sum(1 for f in futures if f.done())
            parts = []
            for i in range(actual_workers):
                wp = progress_array[i].value
                wp_total = len(page_assignments[i])
                parts.append(f"W{i+1}:{wp}/{wp_total}")
            progress_line = " | ".join(parts)
            print(f"\r    {progress_line}  [{done_count}/{actual_workers} done]",
                  end="", flush=True)
            if done_count >= len(futures):
                break
            time.sleep(3)
        print(flush=True)

        for future in futures:
            worker_results.append(future.result())

    # Collect results
    all_docs = []
    all_ids = set()
    total_errors = 0

    for r in sorted(worker_results, key=lambda x: x["worker_id"]):
        if r["error"]:
            print(f"    Worker {r['worker_id']} ERROR: {r['error']}")
            total_errors += 1
            continue
        for doc in r["tender_docs"]:
            bid_id = doc["bid_id"]
            if bid_id not in all_ids:
                all_ids.add(bid_id)
                all_docs.append(doc)

    # Free worker results (each holds its own tender_docs list)
    del worker_results

    return all_docs, len(all_ids), total_errors


def process_month(month_str):
    """
    Process a single month with multi-pass until 100% captured or MAX_PASSES.
    """
    from_date, to_date = month_to_date_range(month_str)

    print(f"\n{'=' * 70}")
    print(f"  PROCESSING: {month_str} ({from_date} to {to_date})")
    print(f"{'=' * 70}")

    # Get numFound
    print("  Getting numFound...", end=" ", flush=True)
    try:
        num_found = get_num_found(from_date, to_date)
    except Exception as e:
        print(f"FAILED: {e}")
        fail_month(month_str, str(e))
        return False

    if num_found == 0:
        print("0 tenders — skipping.")
        complete_month(month_str, 0, 0)
        return True

    expected_pages = (num_found + PAGE_SIZE - 1) // PAGE_SIZE
    print(f"{num_found} tenders, {expected_pages} pages")

    start_month(month_str)

    # Check how many we already have in DB for this month
    existing_count = get_month_bid_count(month_str)
    if existing_count >= num_found:
        print(f"  Already have {existing_count}/{num_found} in DB — marking complete.")
        complete_month(month_str, existing_count, 0)
        return True

    for pass_num in range(1, MAX_PASSES + 1):
        if _shutdown:
            print("  [!] Shutdown requested, stopping passes.")
            break

        t_start = time.time()
        print(f"\n  Pass {pass_num}/{MAX_PASSES}:", flush=True)

        # Run the pass
        docs, pass_unique, errors = run_pass(
            pass_num, from_date, to_date, month_str, expected_pages
        )

        # Bulk upsert to MongoDB (only new ones get inserted)
        newly_inserted = bulk_upsert_tenders(docs, month_str)

        # Free pass memory immediately
        del docs

        # Get exact count from DB (not accumulated — avoids drift)
        total_inserted = get_month_bid_count(month_str)

        elapsed = time.time() - t_start
        capture_pct = (total_inserted / num_found * 100) if num_found > 0 else 0
        gap = num_found - total_inserted

        print(f"    Pass {pass_num}: +{newly_inserted} new to DB | "
              f"Total: {total_inserted}/{num_found} ({capture_pct:.1f}%) | "
              f"Gap: {gap} | Time: {elapsed:.0f}s")

        # Update status in DB
        update_month_progress(month_str, pass_num, total_inserted,
                              newly_inserted, num_found)

        # Check completion — exact count from DB vs numFound
        if total_inserted >= num_found:
            print(f"\n  *** 100% CAPTURED for {month_str} after {pass_num} passes! ***")
            complete_month(month_str, total_inserted, pass_num)
            return True

        time.sleep(DELAY_BETWEEN_PASSES)

    # Hit MAX_PASSES
    total_inserted = get_month_bid_count(month_str)
    gap = num_found - total_inserted
    capture_pct = (total_inserted / num_found * 100) if num_found > 0 else 0
    print(f"\n  Max passes reached. Final: {total_inserted}/{num_found} ({capture_pct:.1f}%) gap={gap}")
    complete_month(month_str, total_inserted, MAX_PASSES)
    return True


def run_pipeline():
    """Main entry point: process months sequentially with multi-pass each."""
    global _shutdown

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    print("=" * 70)
    print("  PIPELINE 2 — Multi-Pass Listing Extraction")
    print("=" * 70)
    print(f"  Workers per pass : {NUM_WORKERS}")
    print(f"  Max passes/month : {MAX_PASSES}")
    print(f"  Sort             : bid_id")
    print(f"  Date range       : {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}")
    print("=" * 70)

    # Initialize DB
    create_indexes()

    # Generate and init all months
    months = generate_month_list(START_YEAR, START_MONTH, END_YEAR, END_MONTH)
    for m in months:
        init_month_status(m)

    print(f"  Total months: {len(months)}")
    print()

    # Process months sequentially
    completed = 0
    while not _shutdown:
        month = get_next_pending_month()
        if month is None:
            print("\n  All months completed!")
            break

        success = process_month(month)
        if success:
            completed += 1

    # Final summary
    print(f"\n{'=' * 70}")
    print("  PIPELINE SUMMARY")
    print(f"{'=' * 70}")
    statuses = get_all_month_statuses()
    total_captured = sum(s.get("unique_captured", 0) for s in statuses)
    total_target = sum(s.get("total_tenders", 0) for s in statuses)
    done = sum(1 for s in statuses if s.get("status") == "completed")
    pending = sum(1 for s in statuses if s.get("status") in ("pending", "failed"))

    print(f"  Completed months   : {done}/{len(statuses)}")
    print(f"  Pending/failed     : {pending}")
    print(f"  Total captured     : {total_captured}")
    print(f"  Total target       : {total_target}")
    if total_target > 0:
        print(f"  Overall capture %  : {total_captured/total_target*100:.1f}%")
    print(f"{'=' * 70}")
