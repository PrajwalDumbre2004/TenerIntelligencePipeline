"""
Database layer for Pipeline3: Read tenders from DB, update with results.

Uses the same tenderGemAll collection as pipeline2.
"""
import certifi
from datetime import datetime, timezone

from pymongo import MongoClient, ASCENDING

from pipeline3.config import MONGO_URI, MONGO_DB_NAME

_client: MongoClient = None
_db = None


def _get_client_kwargs() -> dict:
    """Pass tlsCAFile only when TLS is required (mongodb+srv://)."""
    kwargs = {}
    if MONGO_URI and MONGO_URI.startswith("mongodb+srv://"):
        kwargs["tlsCAFile"] = certifi.where()
    return kwargs


def get_db():
    """Get MongoDB database instance (lazy singleton)."""
    global _client, _db
    if _db is None:
        _client = MongoClient(MONGO_URI, **_get_client_kwargs())
        _db = _client[MONGO_DB_NAME]
    return _db


def get_tender_collection(month: str):
    """Get year-based tender collection (e.g. tenders_2025) for a given month string like '2025-06'."""
    year = month[:4]
    db = get_db()
    return db[f"tenders_{year}"]


def _now():
    return datetime.now(timezone.utc)


# ═══════════════════════════════════════════════════════════════
# Pipeline3 Status Tracking (stored in pipeline2_status collection)
# ═══════════════════════════════════════════════════════════════

def update_p3_status(month: str, status: str, pending: int = 0, done: int = 0,
                     total: int = 0, passes: int = 0, error: str = ""):
    """Update pipeline3 status fields in pipeline2_status collection."""
    db = get_db()
    update = {
        "$set": {
            "p3_status": status,
            "p3_pending": pending,
            "p3_done": done,
            "p3_total": total,
            "p3_passes": passes,
            "p3_updated_at": _now(),
        }
    }
    if status == "completed":
        update["$set"]["p3_completed_at"] = _now()
    if error:
        update["$set"]["p3_error"] = error
    db.pipeline2_status.update_one({"month": month}, update, upsert=True)


def get_p3_status(month: str) -> dict:
    """Get pipeline3 status for a month."""
    db = get_db()
    doc = db.pipeline2_status.find_one({"month": month}, {"_id": 0})
    if not doc:
        return {}
    return {
        "status": doc.get("p3_status", "pending"),
        "pending": doc.get("p3_pending", 0),
        "done": doc.get("p3_done", 0),
        "total": doc.get("p3_total", 0),
        "passes": doc.get("p3_passes", 0),
    }


def get_tenders_needing_results(month: str) -> list:
    """
    Get all tenders for a month that don't have results yet.
    Returns list of dicts with fields needed to determine result URLs.
    """
    query = {
        "month": month,
        "scraped_at": None  # Not yet processed by pipeline3
    }
    projection = {
        "_id": 0,
        "bid_id": 1,
        "bid_number": 1,
        "bid_id_parent": 1,
        "bid_type": 1,
        "eval_type": 1,
        "single_packet": 1,
        "bid_to_ra": 1,
        "ra_to_bid": 1,
        "is_bunch": 1,
        "has_boq": 1,
    }
    return list(get_tender_collection(month).find(query, projection).sort("bid_id", ASCENDING))


def get_month_result_stats(month: str) -> dict:
    """Get counts of processed vs unprocessed tenders for a month."""
    col = get_tender_collection(month)
    total = col.count_documents({"month": month})
    done = col.count_documents({"month": month, "scraped_at": {"$ne": None}})
    pending = total - done
    return {"total": total, "done": done, "pending": pending}


def update_tender_results(bid_id: int, month: str, results: dict) -> bool:
    """
    Update a tender document with scraped results.
    Sets buyer_details, bid_result, ra_result, ra_number, contract_details, and scraped_at.
    """
    update = {
        "$set": {
            "buyer_details": results.get("buyer_details"),
            "bid_result": results.get("bid_result"),
            "ra_result": results.get("ra_result"),
            "ra_number": results.get("ra_number", ""),
            "contract_details": results.get("contract_details"),
            "scraped_at": _now(),
        }
    }
    result = get_tender_collection(month).update_one({"bid_id": bid_id}, update)
    return result.modified_count > 0


def mark_tender_failed(bid_id: int, error: str, month: str = "", worker_id: int = 0):
    """Mark a tender as failed (set scraped_at to flag it, store error).
    Also logs to failed_tenders and error_logs collections."""
    db = get_db()
    if month:
        get_tender_collection(month).update_one(
            {"bid_id": bid_id},
            {"$set": {
                "scraped_at": _now(),
                "scrape_error": error,
            }}
        )
    # Log to failed_tenders collection
    db.failed_tenders.insert_one({
        "pipeline": "pipeline3",
        "bid_id": bid_id,
        "month": month,
        "worker_id": worker_id,
        "error": error[:1000],
        "retry_count": 0,
        "resolved": False,
        "timestamp": _now(),
    })
    # Log to error_logs collection
    db.error_logs.insert_one({
        "pipeline": "pipeline3",
        "month": month,
        "worker_id": worker_id,
        "bid_id": bid_id,
        "page_no": 0,
        "error": error[:2000],
        "context": "tender result extraction failed",
        "timestamp": _now(),
    })
