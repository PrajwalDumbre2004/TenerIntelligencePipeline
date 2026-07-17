"""
Worker for Pipeline3: Each browser worker processes a batch of tenders using
multiple concurrent pages (async playwright). Each page handles its subset
sequentially while all pages in a browser run in parallel.
"""
import os
import asyncio
from datetime import datetime, timezone
from playwright.async_api import async_playwright

from pipeline3.config import (
    BASE_URL, USER_AGENT, BROWSER_LOCALE, BROWSER_VIEWPORT,
    PAGES_PER_BROWSER, DELAY_ON_ERROR, MAX_RETRIES
)
from pipeline3.scraper import fetch_bid_results_async
from pipeline3.db import update_tender_results, mark_tender_failed

_LOG_DIR = os.environ.get("LOG_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs"))
os.makedirs(_LOG_DIR, exist_ok=True)
_LOG_FILE = os.path.join(_LOG_DIR, "error_logs.txt")

BROWSER_RESTART_EVERY = 500


def _log_to_file(worker_id, bid_id, error, context=""):
    """Append error to local log file."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [P3-W{worker_id}] bid_id={bid_id} | {error}"
    if context:
        line += f" | ctx={context}"
    try:
        with open(_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


async def _page_worker(page, tenders, stats, worker_id, page_id, month):
    """One page processes its assigned tenders sequentially."""
    for tender in tenders:
        bid_id = tender["bid_id"]
        retries = 0
        done = False

        while retries < MAX_RETRIES and not done:
            try:
                results = await fetch_bid_results_async(page, tender)

                has_data = (
                    results.get("bid_result") is not None or
                    results.get("ra_result") is not None or
                    results.get("buyer_details") is not None
                )

                if has_data:
                    update_tender_results(bid_id, month, results)
                    stats["success"] += 1
                else:
                    update_tender_results(bid_id, month, results)
                    stats["no_data"] += 1

                done = True

            except Exception as e:
                retries += 1
                if retries >= MAX_RETRIES:
                    print(f"    [W{worker_id}-P{page_id}] FAILED bid_id={bid_id}: {str(e)[:200]}")
                    _log_to_file(worker_id, bid_id, str(e)[:500], f"month={month}")
                    mark_tender_failed(bid_id, str(e)[:500], month=month, worker_id=worker_id)
                    stats["failed"] += 1
                    done = True
                else:
                    await asyncio.sleep(DELAY_ON_ERROR)

        stats["processed"] += 1
        if stats["processed"] % 100 == 0:
            print(
                f"    [W{worker_id}] {stats['processed']}/{stats['total']} | "
                f"success={stats['success']} no_data={stats['no_data']} failed={stats['failed']}"
            )


async def browser_worker(worker_id, tenders, pw, month=""):
    """
    One browser instance processing its batch with multiple concurrent pages.
    Returns stats dict.
    """
    total_batch = len(tenders)
    stats = {"processed": 0, "success": 0, "no_data": 0, "failed": 0, "total": total_batch}

    try:
        browser = await pw.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        chunks = [
            tenders[i: i + BROWSER_RESTART_EVERY]
            for i in range(0, total_batch, BROWSER_RESTART_EVERY)
        ]

        for chunk_idx, chunk in enumerate(chunks):
            context = await browser.new_context(
                user_agent=USER_AGENT,
                locale=BROWSER_LOCALE,
                viewport=BROWSER_VIEWPORT,
            )
            pages = [await context.new_page() for _ in range(PAGES_PER_BROWSER)]

            # Initialize all pages with GeM session
            await asyncio.gather(*[
                p.goto(f"{BASE_URL}/all-bids", wait_until="domcontentloaded", timeout=30000)
                for p in pages
            ])

            # Split chunk across pages
            page_batches = [[] for _ in range(PAGES_PER_BROWSER)]
            for i, tender in enumerate(chunk):
                page_batches[i % PAGES_PER_BROWSER].append(tender)

            # Run all pages concurrently
            tasks = [
                _page_worker(pages[p_idx], page_batches[p_idx], stats, worker_id, p_idx + 1, month)
                for p_idx in range(PAGES_PER_BROWSER)
                if page_batches[p_idx]
            ]
            await asyncio.gather(*tasks)

            await context.close()

            if chunk_idx < len(chunks) - 1:
                print(f"    [W{worker_id}] Restarting context at {stats['processed']}/{total_batch}")

        await browser.close()

        print(
            f"    [W{worker_id}] DONE — processed={stats['processed']} "
            f"success={stats['success']} no_data={stats['no_data']} failed={stats['failed']}"
        )
        return {
            "worker_id": worker_id,
            "processed": stats["processed"],
            "success": stats["success"],
            "no_data": stats["no_data"],
            "failed": stats["failed"],
            "error": None,
        }

    except Exception as e:
        print(f"    [W{worker_id}] FATAL CRASH: {str(e)[:300]}")
        _log_to_file(worker_id, 0, str(e)[:500], "FATAL CRASH")
        return {
            "worker_id": worker_id,
            "processed": stats["processed"],
            "success": stats["success"],
            "no_data": stats["no_data"],
            "failed": stats["failed"],
            "error": str(e),
        }
