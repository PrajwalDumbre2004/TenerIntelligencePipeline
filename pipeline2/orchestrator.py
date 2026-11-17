"""
Orchestrator for Pipeline2: Sequential month processing with multi-pass per status.

Flow:
  1. Pick next pending month (sequential, one at a time)
  2. For each status in BY_STATUSES (tech_evaluated -> fin_evaluated -> bid_awarded):
       a. Get numFound for that status (live + cancelled bids at that stage)
       b. Run passes (up to MAX_PASSES) with NUM_WORKERS parallel page-fetchers
       c. After each pass, bulk-upsert docs into staging (dedup by bid_id)
       d. Stop when the bids seen for this status >= numFound, when a pass finds
          nothing new (bids that moved to a higher status mid-crawl can never be
          found in this list again), or at MAX_PASSES
  3. complete_month(): prune bids already stored at the same stage
  4. Move to next month
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
    SORT_BY, BY_STATUSES, STAGE_OF
)
from pipeline2.db import (
    create_indexes, init_month_status, start_month,
    update_month_progress, complete_month, fail_month,
    get_next_pending_month, get_all_month_statuses,
    bulk_upsert_tenders, get_month_bid_count, get_staged_bid_ids,
    prune_known_bid_ids
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


def get_num_found(from_date, to_date, by_status):
    """Quick single request to get numFound for one status in a date range."""
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
        _, num_found = fetch_page(page, csrf, from_date, to_date, 1, by_status)
        browser.close()
    return num_found


def run_pass(pass_num, from_date, to_date, month, expected_pages, by_status):
    """
    Run one pass over one status list: divide all pages among NUM_WORKERS
    (round-robin), fetch in parallel.
    Returns (list of tender_docs, unique ids in pass, errors) where errors counts
    crashed workers plus pages that failed after retry.
    """
    # Round-robin: page 1→W1, page 2→W2, ..., page N→W(N%workers)+1
    page_assignments = [[] for _ in range(NUM_WORKERS)]
    for pg in range(1, expected_pages + 1):
        page_assignments[(pg - 1) % NUM_WORKERS].append(pg)

    # Remove empty assignments
    page_assignments = [pages for pages in page_assignments if pages]

    actual_workers = len(page_assignments)

    # Always "spawn" (fresh worker processes), as on Windows. Linux would
    # otherwise fork the parent, copying its DB connection and Playwright state.
    ctx = multiprocessing.get_context("spawn")

    # Shared progress counters
    progress_array = [ctx.Value('i', 0) for _ in range(actual_workers)]

    worker_results = []
    with ProcessPoolExecutor(
        max_workers=actual_workers,
        mp_context=ctx,
        initializer=_init_worker,
        initargs=(progress_array,)
    ) as executor:
        futures = {
            executor.submit(
                worker_fetch_pages, wid + 1, from_date, to_date, month, page_assignments[wid], by_status
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
        total_errors += r.get("errors", 0)
        for doc in r["tender_docs"]:
            bid_id = doc["bid_id"]
            if bid_id not in all_ids:
                all_ids.add(bid_id)
                all_docs.append(doc)

    # Free worker results (each holds its own tender_docs list)
    del worker_results

    return all_docs, len(all_ids), total_errors


def process_status(month_str, from_date, to_date, by_status):
    """
    List one status (byStatus) for one date range with multi-pass.

    Completion is tracked with the set of bid_ids SEEN in this status (in memory,
    seeded from rows already staged at this stage so a crashed run resumes),
    not with a DB count: a bid that moves up to a higher status mid-crawl is
    staged under its higher stage, and it can never be found in this list again.

    Stops when seen >= numFound, when a clean pass (no failed pages) finds no
    new bid, or at MAX_PASSES.

    Returns {"num_found", "captured", "passes"} or None if numFound failed.
    """
    stage = STAGE_OF[by_status]
    print(f"\n  --- {by_status} (stage {stage}) ---")
    print("  Getting numFound...", end=" ", flush=True)
    try:
        num_found = get_num_found(from_date, to_date, by_status)
    except Exception as e:
        print(f"FAILED: {e}")
        return None

    seen = get_staged_bid_ids(month_str, stage)
    info = {"num_found": num_found, "captured": len(seen), "passes": 0}

    if num_found == 0:
        print("0 tenders — skipping.")
        return info

    expected_pages = (num_found + PAGE_SIZE - 1) // PAGE_SIZE
    print(f"{num_found} tenders, {expected_pages} pages")

    if len(seen) >= num_found:
        print(f"  Already have {len(seen)}/{num_found} staged — skipping.")
        return info

    for pass_num in range(1, MAX_PASSES + 1):
        if _shutdown:
            print("  [!] Shutdown requested, stopping passes.")
            break

        t_start = time.time()
        print(f"\n  [{by_status}] Pass {pass_num}/{MAX_PASSES}:", flush=True)

        docs, pass_unique, errors = run_pass(
            pass_num, from_date, to_date, month_str, expected_pages, by_status
        )

        pass_ids = {d["bid_id"] for d in docs}
        new_in_status = len(pass_ids - seen)
        seen |= pass_ids

        newly_inserted = bulk_upsert_tenders(docs, month_str)
        del docs

        info["captured"] = len(seen)
        info["passes"] = pass_num

        elapsed = time.time() - t_start
        capture_pct = len(seen) / num_found * 100
        print(f"    Pass {pass_num}: +{new_in_status} new in {by_status} (+{newly_inserted} new rows) | "
              f"Seen: {len(seen)}/{num_found} ({capture_pct:.1f}%) | "
              f"Gap: {num_found - len(seen)} | Errors: {errors} | Time: {elapsed:.0f}s")

        update_month_progress(month_str, pass_num, get_month_bid_count(month_str), newly_inserted)

        if len(seen) >= num_found:
            print(f"  *** 100% CAPTURED for {by_status} after {pass_num} passes ***")
            break

        if new_in_status == 0 and errors == 0:
            print(f"  No new bids in a clean pass — stopping {by_status} "
                  f"(the remaining {num_found - len(seen)} most likely moved to another status mid-crawl).")
            break

        time.sleep(DELAY_BETWEEN_PASSES)
    else:
        print(f"  Max passes reached for {by_status}: {len(seen)}/{num_found}")

    return info


def process_month(month_str, from_date=None, to_date=None):
    """
    Process a single month: list every status in BY_STATUSES, then prune bids
    already stored at the same stage (complete_month).

    from_date / to_date (GeM format "15-Jul-2025") narrow the window inside the
    month -- used for test runs only. Rows are still staged under month_str.
    """
    if from_date is None or to_date is None:
        from_date, to_date = month_to_date_range(month_str)

    print(f"\n{'=' * 70}")
    print(f"  PROCESSING: {month_str} ({from_date} to {to_date})")
    print(f"  Statuses  : {', '.join(BY_STATUSES)}")
    print(f"{'=' * 70}")

    start_month(month_str)

    status_counts = {}
    failed = []
    for by_status in BY_STATUSES:
        if _shutdown:
            break
        info = process_status(month_str, from_date, to_date, by_status)
        if info is None:
            failed.append(by_status)
        else:
            status_counts[by_status] = info

    if _shutdown:
        # Leave the month in_progress; pipeline_main resets it to pending on next start.
        print("  [!] Shutdown — month left unfinished.")
        return False

    total_found = sum(v["num_found"] for v in status_counts.values())
    total_staged = get_month_bid_count(month_str)
    max_passes = max((v["passes"] for v in status_counts.values()), default=0)

    print(f"\n  {month_str} listing summary:")
    for s, v in status_counts.items():
        print(f"    {s:15s} numFound={v['num_found']:>7}  seen={v['captured']:>7}  passes={v['passes']}")

    if failed:
        # Still prune, so pipeline3 doesn't re-scrape bids we already have.
        status_updated, pruned = prune_known_bid_ids(month_str)
        msg = f"numFound failed for: {', '.join(failed)}"
        print(f"  [!] {msg} — month marked failed (re-run it). Pruned {pruned}, status-updated {status_updated}.")
        fail_month(month_str, msg)
        return False

    complete_month(month_str, total_staged, max_passes,
                   status_counts=status_counts, total_tenders=total_found)
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
    print(f"  Sort             : {SORT_BY}")
    print(f"  Statuses         : {', '.join(BY_STATUSES)}")
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
