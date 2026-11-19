"""
Worker for Pipeline2: Each worker fetches a range of pages for a given month.
Runs in its own process with its own browser instance.
"""
import json
import os
import time
import urllib.parse
import multiprocessing
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright

from pipeline2.config import (
    BASE_URL, USER_AGENT, BROWSER_LOCALE, BROWSER_VIEWPORT,
    SORT_BY, DELAY_BETWEEN_PAGES, PAGE_SIZE, STAGE_OF, CANCELLED_BID_STATUSES
)
from pipeline2.db import log_failed_page, log_error, bulk_upsert_tenders

_LOG_DIR = os.environ.get("LOG_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs"))
os.makedirs(_LOG_DIR, exist_ok=True)
_LOG_FILE = os.path.join(_LOG_DIR, "error_logs.txt")


def _log_to_file(worker_id, month, page_no, error, context=""):
    """Append error to local log file."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [P2-W{worker_id}] month={month} page={page_no} | {error}"
    if context:
        line += f" | ctx={context}"
    try:
        with open(_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# Shared progress counter (set by initializer)
_progress = None


def _init_worker(progress_array):
    """Initialize shared progress in each worker process."""
    global _progress
    _progress = progress_array


def capture_csrf(page):
    """Navigate to all-bids page and capture CSRF token from auto-fired XHR."""
    captured = {"value": None}

    def on_response(response):
        if "all-bids-data" in response.url and response.status == 200:
            post_data = response.request.post_data
            if post_data and "csrf_bd_gem_nk" in post_data:
                params = urllib.parse.parse_qs(post_data)
                if "csrf_bd_gem_nk" in params:
                    captured["value"] = params["csrf_bd_gem_nk"][0]

    page.on("response", on_response)
    page.goto(f"{BASE_URL}/all-bids", wait_until="domcontentloaded", timeout=30000)
    page.wait_for_load_state("networkidle", timeout=15000)
    time.sleep(3)
    page.remove_listener("response", on_response)

    if not captured["value"]:
        raise RuntimeError("Failed to capture CSRF token")
    return captured["value"]


def fetch_page(page, csrf, from_date, to_date, page_no, by_status):
    """
    Fetch a single page from the GeM API. Returns (docs, numFound).
    by_status: "tech_evaluated" | "fin_evaluated" | "bid_awarded". With
    bidStatusType "all_bids" GeM returns live AND cancelled bids at that stage.
    """
    payload = {
        "page": page_no,
        "param": {"searchBid": "", "searchType": "fullText"},
        "filter": {
            "bidStatusType": "all_bids",
            "byType": "all",
            "highBidValue": "",
            "byEndDate": {"from": from_date, "to": to_date},
            "sort": SORT_BY,
            "byStatus": by_status
        }
    }
    payload_json = json.dumps(payload)

    result = page.evaluate("""async (args) => {
        const [url, payload, csrf] = args;
        const body = 'payload=' + encodeURIComponent(payload) + '&csrf_bd_gem_nk=' + encodeURIComponent(csrf);
        try {
            const resp = await fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                body,
                credentials: 'include'
            });
            const text = await resp.text();
            let data;
            try { data = JSON.parse(text); } catch(_) { data = null; }
            if (!resp.ok) {
                if (data && data.code === 404 && data.message && data.message.includes('No data')) {
                    return {status: 1, response: {response: {docs: [], numFound: 0}}};
                }
                return {error: 'HTTP ' + resp.status};
            }
            if (!data) return {error: 'Invalid JSON'};
            return data;
        } catch(e) {
            return {error: 'Fetch exception: ' + e.message};
        }
    }""", [f"{BASE_URL}/all-bids-data", payload_json, csrf])

    if "error" in result:
        raise RuntimeError(result["error"])
    if result.get("status") != 1:
        raise RuntimeError(f"Unexpected status={result.get('status')}")

    docs = result["response"]["response"]["docs"]
    total = result["response"]["response"]["numFound"]
    return docs, total


def extract_bid_id(doc):
    """Extract bid_id from a raw API document."""
    b_id = doc.get("b_id")
    return b_id[0] if isinstance(b_id, list) else int(doc.get("id", 0))


def parse_doc_to_tender(doc, month: str, stage: int = None) -> dict:
    """
    Parse a raw API document into the tenderGemAll schema.
    Only listing fields — no results/details yet.
    Field names match the Solr schema used by GeM's API.

    stage: 1/2/3 from the byStatus list this doc came from (tech/fin/awarded).
    """
    bid_id = extract_bid_id(doc)

    def get_first(val):
        return val[0] if isinstance(val, list) and val else val

    raw_status = get_first(doc.get("b_status", [None]))
    try:
        bid_status = int(raw_status) if raw_status is not None else None
    except (TypeError, ValueError):
        bid_status = None

    return {
        "stage": stage,
        "bid_status": bid_status,
        # None when GeM didn't send b_status (unknown, not "not cancelled")
        "is_cancelled": (bid_status in CANCELLED_BID_STATUSES) if bid_status is not None else None,
        "bid_id": bid_id,
        "bid_number": get_first(doc.get("b_bid_number", [""])),
        "bid_number_parent": get_first(doc.get("b_bid_number_parent", [""])),
        "bid_id_parent": get_first(doc.get("b_id_parent", [0])),
        "ra_number": "",
        "bid_type": get_first(doc.get("b_bid_type", [0])),
        "eval_type": get_first(doc.get("b_eval_type", [0])),
        "single_packet": get_first(doc.get("ba_is_single_packet", [0])),
        "buyer_status": get_first(doc.get("b_buyer_status", [0])),
        "bid_to_ra": get_first(doc.get("b_bid_to_ra", [0])),
        "ra_to_bid": get_first(doc.get("b_ra_to_bid", [0])),
        "category": get_first(doc.get("b_category_name", [""])),
        "items": get_first(doc.get("bd_category_name", [""])),
        "category_id": get_first(doc.get("b_cat_id", [""])),
        "bid_title": get_first(doc.get("bbt_title", [""])),
        "department": get_first(doc.get("ba_official_details_deptName", [""])),
        "ministry": get_first(doc.get("ba_official_details_minName", [""])),
        "start_date": get_first(doc.get("final_start_date_sort", [""])),
        "end_date": get_first(doc.get("final_end_date_sort", [""])),
        "total_quantity": get_first(doc.get("b_total_quantity", [0])),
        "is_high_value": bool(get_first(doc.get("is_high_value", [False]))),
        "is_rc_bid": get_first(doc.get("is_rc_bid", [0])),
        "is_global_tendering": get_first(doc.get("ba_is_global_tendering", [0])),
        "is_bunch": get_first(doc.get("b_is_bunch", [0])),
        "is_custom_item": get_first(doc.get("b_is_custom_item", [0])),
        "has_boq": bool(get_first(doc.get("bd_details_is_boq", [False]))),
        "tender_link": f"https://bidplus.gem.gov.in/bidlists/{bid_id}",
        "month": month,
        "buyer_details": None,
        "contract_details": None,
        "bid_result": None,
        "ra_result": None,
        "scraped_at": None,  # Will be set when results are fetched later
        "derived_at": None,  # Will be set when derived collections are updated
        "listing_scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def worker_fetch_pages(worker_id, from_date, to_date, month, page_list, by_status):
    """
    Worker process: launches browser, fetches pages in page_list (round-robin assigned)
    of the `by_status` list, and saves each page to staging as soon as it is
    fetched. Returns dict with the bid_ids saved and stats.

    A bid_id is reported only after its page was saved, so the orchestrator
    never counts a bid as captured that isn't in the DB. A page whose save
    fails counts as an error; the next pass fetches it again.
    """
    global _progress
    stage = STAGE_OF[by_status]
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = browser.new_context(
                user_agent=USER_AGENT,
                locale=BROWSER_LOCALE,
                viewport=BROWSER_VIEWPORT
            )
            page = context.new_page()
            csrf = capture_csrf(page)

            bid_ids_found = set()
            num_found = None
            errors = 0
            pages_done = 0
            failed_pages_log = []

            for pg in page_list:
                try:
                    docs, total = fetch_page(page, csrf, from_date, to_date, pg, by_status)
                except RuntimeError:
                    # Retry once with fresh CSRF
                    try:
                        time.sleep(2)
                        csrf = capture_csrf(page)
                        docs, total = fetch_page(page, csrf, from_date, to_date, pg, by_status)
                    except Exception as e:
                        errors += 1
                        err_msg = str(e)
                        failed_pages_log.append({"page": pg, "error": err_msg})
                        # Log to file
                        _log_to_file(worker_id, month, pg, err_msg, f"{by_status} retry failed")
                        # Log to DB
                        try:
                            log_failed_page(month, pg, worker_id, err_msg, pass_num=0)
                            log_error("pipeline2", month, err_msg,
                                      context=f"{by_status} page={pg}, retry failed",
                                      worker_id=worker_id, page_no=pg)
                        except Exception:
                            pass
                        if errors > 10:
                            break
                        continue

                if num_found is None:
                    num_found = total

                if not docs:
                    continue

                page_docs, page_ids = [], set()
                for doc in docs:
                    bid_id = extract_bid_id(doc)
                    if bid_id not in bid_ids_found and bid_id not in page_ids:
                        page_ids.add(bid_id)
                        page_docs.append(parse_doc_to_tender(doc, month, stage))

                try:
                    bulk_upsert_tenders(page_docs, month)
                except Exception as e:
                    errors += 1
                    err_msg = f"db save failed: {e}"
                    failed_pages_log.append({"page": pg, "error": err_msg})
                    _log_to_file(worker_id, month, pg, err_msg[:500], f"{by_status} save failed")
                    try:
                        log_failed_page(month, pg, worker_id, err_msg, pass_num=0)
                    except Exception:
                        pass
                    if errors > 10:
                        break
                    continue
                bid_ids_found |= page_ids

                pages_done += 1

                # Update shared progress
                if _progress is not None:
                    with _progress[worker_id - 1].get_lock():
                        _progress[worker_id - 1].value = pages_done

                time.sleep(DELAY_BETWEEN_PAGES)

            browser.close()

        return {
            "worker_id": worker_id,
            "bid_ids": list(bid_ids_found),
            "num_found": num_found or 0,
            "pages": pages_done,
            "errors": errors,
            "failed_pages": failed_pages_log,
            "error": None,
        }
    except Exception as e:
        # Log fatal worker error to file
        _log_to_file(worker_id, month, 0, str(e)[:500], "FATAL CRASH")
        # Log fatal worker error to DB
        try:
            log_error("pipeline2", month, str(e),
                      context="worker fatal crash", worker_id=worker_id)
        except Exception:
            pass
        return {
            "worker_id": worker_id,
            "bid_ids": [],
            "num_found": 0,
            "pages": 0,
            "errors": 0,
            "failed_pages": [],
            "error": str(e),
        }
