"""
Database layer for Pipeline2: MongoDB connection, tender storage, month status.

Collections:
  - tenderGemAll:      All scraped tender documents
  - pipeline2_status:  Per-month multi-pass progress tracking
  - failed_pages:      Pages that failed to fetch (for retry/debugging)
  - error_logs:        All error events from pipeline2 and pipeline3
"""
import certifi
from datetime import datetime, timezone
from typing import Optional

from pymongo import MongoClient, ASCENDING, UpdateOne
from pymongo.errors import BulkWriteError

from pipeline2.config import MONGO_URI, MONGO_DB_NAME


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


def create_indexes_for_year(year: str):
    """Ensure indexes on a year-based tender collection."""
    db = get_db()
    col = db[f"tenders_{year}"]
    col.create_index("bid_id", unique=True, sparse=True)
    col.create_index("bid_number")
    col.create_index("department")
    col.create_index("end_date")
    col.create_index("month")


def create_indexes():
    """Ensure indexes on status/error collections."""
    db = get_db()

    # pipeline2_status
    db.pipeline2_status.create_index("month", unique=True)

    # failed_pages
    db.failed_pages.create_index("month")
    db.failed_pages.create_index("page_no")
    db.failed_pages.create_index("timestamp")

    # failed_tenders
    db.failed_tenders.create_index("bid_id")
    db.failed_tenders.create_index("month")
    db.failed_tenders.create_index("pipeline")
    db.failed_tenders.create_index("timestamp")

    # error_logs
    db.error_logs.create_index("pipeline")
    db.error_logs.create_index("month")
    db.error_logs.create_index("timestamp")


def _now():
    return datetime.now(timezone.utc)


# ═══════════════════════════════════════════════════════════════
# Tender Storage (bulk upsert with dedup)
# ═══════════════════════════════════════════════════════════════

def bulk_upsert_tenders(docs: list, month: str, chunk_size: int = 5000) -> int:
    """
    Bulk upsert tender documents in chunks. Uses bid_id as unique key.
    Only inserts new documents, skips existing ones.
    Processes in chunks to avoid holding all UpdateOne objects in memory.
    Returns count of newly inserted documents.
    """
    if not docs:
        return 0

    col = get_tender_collection(month)
    total_upserted = 0

    for i in range(0, len(docs), chunk_size):
        chunk = docs[i:i + chunk_size]
        operations = [
            UpdateOne(
                {"bid_id": doc["bid_id"]},
                {"$setOnInsert": doc},
                upsert=True
            )
            for doc in chunk
        ]
        try:
            result = col.bulk_write(operations, ordered=False)
            total_upserted += result.upserted_count
        except BulkWriteError as e:
            total_upserted += e.details.get("nUpserted", 0)
        del operations  # Free chunk memory immediately

    return total_upserted


def get_month_bid_count(month: str) -> int:
    """Get count of tenders already stored for a given month."""
    col = get_tender_collection(month)
    return col.count_documents({"month": month})


# ═══════════════════════════════════════════════════════════════
# Error & Failure Logging
# ═══════════════════════════════════════════════════════════════

def log_failed_page(month: str, page_no: int, worker_id: int, error: str, pass_num: int = 0):
    """Log a failed page fetch to the failed_pages collection."""
    db = get_db()
    db.failed_pages.insert_one({
        "pipeline": "pipeline2",
        "month": month,
        "page_no": page_no,
        "worker_id": worker_id,
        "pass_num": pass_num,
        "error": error[:1000],
        "timestamp": _now(),
    })


def log_failed_tender(bid_id: int, month: str, worker_id: int, error: str, pipeline: str = "pipeline3"):
    """Log a failed tender result fetch to the failed_tenders collection."""
    db = get_db()
    db.failed_tenders.insert_one({
        "pipeline": pipeline,
        "bid_id": bid_id,
        "month": month,
        "worker_id": worker_id,
        "error": error[:1000],
        "retry_count": 0,
        "resolved": False,
        "timestamp": _now(),
    })


def log_error(pipeline: str, month: str, error: str, context: str = "",
              worker_id: int = 0, bid_id: int = 0, page_no: int = 0):
    """Log any error event to the error_logs collection."""
    db = get_db()
    db.error_logs.insert_one({
        "pipeline": pipeline,
        "month": month,
        "worker_id": worker_id,
        "bid_id": bid_id,
        "page_no": page_no,
        "error": error[:2000],
        "context": context[:500],
        "timestamp": _now(),
    })


# ═══════════════════════════════════════════════════════════════
# Month Status Tracking
# ═══════════════════════════════════════════════════════════════

def init_month_status(month: str):
    """Initialize a month in pipeline2_status if not exists."""
    db = get_db()
    db.pipeline2_status.update_one(
        {"month": month},
        {"$setOnInsert": {
            "month": month,
            "status": "pending",
            "total_tenders": 0,
            "unique_captured": 0,
            "passes_done": 0,
            "last_pass_new": 0,
            "error_message": None,
            "started_at": None,
            "completed_at": None,
            "updated_at": _now(),
        }},
        upsert=True
    )


def start_month(month: str):
    """Mark month as in_progress."""
    db = get_db()
    db.pipeline2_status.update_one(
        {"month": month},
        {"$set": {
            "status": "in_progress",
            "started_at": _now(),
            "updated_at": _now(),
        }}
    )


def update_month_progress(month: str, pass_num: int, unique_captured: int,
                          last_pass_new: int, total_tenders: int = 0):
    """Update progress after each pass."""
    db = get_db()
    update = {
        "$set": {
            "passes_done": pass_num,
            "unique_captured": unique_captured,
            "last_pass_new": last_pass_new,
            "updated_at": _now(),
        }
    }
    if total_tenders > 0:
        update["$set"]["total_tenders"] = total_tenders
    db.pipeline2_status.update_one({"month": month}, update)


def complete_month(month: str, unique_captured: int, passes_done: int):
    """Mark month as completed."""
    db = get_db()
    db.pipeline2_status.update_one(
        {"month": month},
        {"$set": {
            "status": "completed",
            "unique_captured": unique_captured,
            "passes_done": passes_done,
            "completed_at": _now(),
            "updated_at": _now(),
        }}
    )


def fail_month(month: str, error: str):
    """Mark month as failed."""
    db = get_db()
    db.pipeline2_status.update_one(
        {"month": month},
        {"$set": {
            "status": "failed",
            "error_message": error,
            "updated_at": _now(),
        }}
    )


def get_month_status(month: str) -> Optional[dict]:
    """Get current status for a month."""
    db = get_db()
    return db.pipeline2_status.find_one({"month": month}, {"_id": 0})


def reset_in_progress_months():
    """
    Reset any months stuck in 'in_progress' back to 'pending'.
    Call this at startup to recover from mid-run crashes.
    """
    db = get_db()
    result = db.pipeline2_status.update_many(
        {"status": "in_progress"},
        {"$set": {"status": "pending", "updated_at": _now()}}
    )
    return result.modified_count


def get_next_pending_month() -> Optional[str]:
    """Get the next month that needs processing (oldest pending/failed first)."""
    db = get_db()
    doc = db.pipeline2_status.find_one(
        {"status": {"$in": ["pending", "failed"]}},
        {"month": 1},
        sort=[("month", ASCENDING)]
    )
    return doc["month"] if doc else None


def get_all_month_statuses() -> list:
    """Get all month statuses for summary display."""
    db = get_db()
    return list(db.pipeline2_status.find({}, {"_id": 0}).sort("month", ASCENDING))
