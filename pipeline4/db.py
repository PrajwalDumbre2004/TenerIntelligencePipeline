"""
Database layer for Pipeline4: reads from TenderIntelligence, writes to TenderAnalytics.

Responsibilities:
  - Discover year-based tender collections
  - Fetch tenders where derived_at is None (newly scraped, not yet derived)
  - Mark tenders as derived after processing
  - Read/write seller analytics collections in TenderAnalytics
"""
import certifi
import re
from datetime import datetime, timezone
from typing import Optional

from pymongo import MongoClient, ASCENDING, DESCENDING, ReplaceOne
from pymongo.errors import BulkWriteError

from pipeline4.config import (
    MONGO_URI, SOURCE_DB, TARGET_DB,
    TENDER_COLLECTION_PATTERN, MONGO_WRITE_BATCH,
    COLL_SELLERS, COLL_SELLER_TENDERS, COLL_SELLER_BUYERS,
    COLL_SELLER_CATEGORIES, COLL_SELLER_COMPETITORS,
    BATCH_SIZE,
)

_client: Optional[MongoClient] = None
_source_db = None
_target_db = None


def _get_client_kwargs() -> dict:
    """Pass tlsCAFile only when TLS is required (mongodb+srv://)."""
    kwargs = {}
    if MONGO_URI and MONGO_URI.startswith("mongodb+srv://"):
        kwargs["tlsCAFile"] = certifi.where()
    return kwargs


def get_client():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, **_get_client_kwargs())
    return _client


def get_source_db():
    global _source_db
    if _source_db is None:
        _source_db = get_client()[SOURCE_DB]
    return _source_db


def get_target_db():
    global _target_db
    if _target_db is None:
        _target_db = get_client()[TARGET_DB]
    return _target_db


def _now():
    return datetime.now(timezone.utc)


def _apply_month_filter(query: dict, month_filter: str | list[str] | None):
    """Apply month filter to a query. Supports one month or multiple months."""
    if not month_filter:
        return
    if isinstance(month_filter, list):
        months = [m for m in month_filter if m]
        if months:
            query["month"] = {"$in": months}
    else:
        query["month"] = month_filter


# ═══════════════════════════════════════════════════════════════
# Source: TenderIntelligence
# ═══════════════════════════════════════════════════════════════

def discover_tender_collections() -> list[str]:
    """Find all tenders_YYYY collections, sorted by year."""
    db = get_source_db()
    collections = []
    for name in db.list_collection_names():
        if TENDER_COLLECTION_PATTERN.match(name):
            collections.append(name)
    return sorted(collections)


def get_underived_tenders(
    collection_name: str,
    batch_size: int = BATCH_SIZE,
    month_filter: str | list[str] | None = None,
) -> list[dict]:
    """
    Fetch tenders where derived_at is None AND scraped_at is not None.
    (Only process tenders that pipeline3 has already completed.)
    """
    db = get_source_db()
    query = {
        "derived_at": None,
        "scraped_at": {"$ne": None},
    }
    _apply_month_filter(query, month_filter)
    return list(
        db[collection_name]
        .find(query)
        .sort("bid_id", ASCENDING)
        .limit(batch_size)
    )


def get_underived_count(collection_name: str, month_filter: str | list[str] | None = None) -> int:
    """Count tenders not yet derived in a collection."""
    db = get_source_db()
    query = {
        "derived_at": None,
        "scraped_at": {"$ne": None},
    }
    _apply_month_filter(query, month_filter)
    return db[collection_name].count_documents(query)


def mark_tenders_derived(
    collection_name: str,
    bid_ids: list[int],
    month_filter: str | list[str] | None = None,
):
    """Mark a batch of tenders as derived."""
    if not bid_ids:
        return
    db = get_source_db()
    query = {"bid_id": {"$in": bid_ids}}
    _apply_month_filter(query, month_filter)
    db[collection_name].update_many(
        query,
        {"$set": {"derived_at": _now()}}
    )


# ═══════════════════════════════════════════════════════════════
# Target: TenderAnalytics — Read existing data for recomputation
# ═══════════════════════════════════════════════════════════════

def get_all_seller_tenders(seller_id: str) -> list[dict]:
    """
    Get ALL participation records for a seller from seller_tenders.
    Used for full recomputation of aggregates.
    """
    db = get_target_db()
    return list(
        db[COLL_SELLER_TENDERS]
        .find({"seller_id": seller_id}, {"_id": 0})
        .sort("bid_id", ASCENDING)
    )


def get_seller_bids_for_competitors(seller_id: str) -> list[int]:
    """Get all bid_ids for a seller (for competitor computation)."""
    db = get_target_db()
    docs = db[COLL_SELLER_TENDERS].find(
        {"seller_id": seller_id},
        {"_id": 0, "bid_id": 1}
    )
    return [d["bid_id"] for d in docs if d.get("bid_id")]


def get_co_participants_for_bids(bid_ids: list[int], exclude_seller: str) -> dict[str, int]:
    """
    For a set of bid_ids, count how many bids each other seller shares.
    Returns {competitor_id: shared_count}.
    """
    db = get_target_db()
    competitor_counts: dict[str, int] = {}

    # Process in chunks to avoid excessive $in queries
    chunk_size = 1000
    for i in range(0, len(bid_ids), chunk_size):
        chunk = bid_ids[i:i + chunk_size]
        cursor = db[COLL_SELLER_TENDERS].find(
            {"bid_id": {"$in": chunk}},
            {"_id": 0, "seller_id": 1}
        )
        for rec in cursor:
            sid = rec.get("seller_id")
            if not sid or sid == exclude_seller:
                continue
            competitor_counts[sid] = competitor_counts.get(sid, 0) + 1

    return competitor_counts


# ═══════════════════════════════════════════════════════════════
# Target: TenderAnalytics — Write operations
# ═══════════════════════════════════════════════════════════════

def upsert_seller_tenders(seller_id: str, docs: list[dict]):
    """
    Upsert seller_tenders using ReplaceOne(upsert=True) keyed on (seller_id, bid_id).
    Then clean stale docs.
    """
    db = get_target_db()
    collection = db[COLL_SELLER_TENDERS]

    if not docs:
        collection.delete_many({"seller_id": seller_id})
        return

    ops = []
    written_bid_ids: set = set()
    for doc in docs:
        bid_id = doc["bid_id"]
        written_bid_ids.add(bid_id)
        ops.append(ReplaceOne(
            {"seller_id": seller_id, "bid_id": bid_id},
            doc,
            upsert=True,
        ))

    for i in range(0, len(ops), MONGO_WRITE_BATCH):
        try:
            collection.bulk_write(ops[i:i + MONGO_WRITE_BATCH], ordered=False)
        except BulkWriteError:
            pass

    # Remove stale docs no longer in source
    if written_bid_ids:
        collection.delete_many({
            "seller_id": seller_id,
            "bid_id": {"$nin": list(written_bid_ids)},
        })


def upsert_seller_profile(seller_id: str, profile: dict):
    """Upsert the seller profile document."""
    db = get_target_db()
    db[COLL_SELLERS].replace_one({"seller_id": seller_id}, profile, upsert=True)


def upsert_seller_buyers(seller_id: str, docs: list[dict]):
    """Upsert seller_buyers keyed on (seller_id, buyer_id)."""
    db = get_target_db()
    collection = db[COLL_SELLER_BUYERS]

    if not docs:
        collection.delete_many({"seller_id": seller_id})
        return

    ops = []
    written_keys: set = set()
    for doc in docs:
        key = doc["buyer_id"]
        written_keys.add(key)
        ops.append(ReplaceOne(
            {"seller_id": seller_id, "buyer_id": key},
            doc,
            upsert=True,
        ))

    for i in range(0, len(ops), MONGO_WRITE_BATCH):
        try:
            collection.bulk_write(ops[i:i + MONGO_WRITE_BATCH], ordered=False)
        except BulkWriteError:
            pass

    if written_keys:
        collection.delete_many({
            "seller_id": seller_id,
            "buyer_id": {"$nin": list(written_keys)},
        })


def upsert_seller_categories(seller_id: str, docs: list[dict]):
    """Upsert seller_categories keyed on (seller_id, category)."""
    db = get_target_db()
    collection = db[COLL_SELLER_CATEGORIES]

    if not docs:
        collection.delete_many({"seller_id": seller_id})
        return

    ops = []
    written_keys: set = set()
    for doc in docs:
        key = doc["category"]
        written_keys.add(key)
        ops.append(ReplaceOne(
            {"seller_id": seller_id, "category": key},
            doc,
            upsert=True,
        ))

    for i in range(0, len(ops), MONGO_WRITE_BATCH):
        try:
            collection.bulk_write(ops[i:i + MONGO_WRITE_BATCH], ordered=False)
        except BulkWriteError:
            pass

    if written_keys:
        collection.delete_many({
            "seller_id": seller_id,
            "category": {"$nin": list(written_keys)},
        })


def upsert_seller_competitors(seller_id: str, competitors: list[dict]):
    """Upsert the seller_competitors document (one doc per seller)."""
    db = get_target_db()
    db[COLL_SELLER_COMPETITORS].replace_one(
        {"seller_id": seller_id},
        {"seller_id": seller_id, "competitors": competitors},
        upsert=True,
    )


# ═══════════════════════════════════════════════════════════════
# Index creation
# ═══════════════════════════════════════════════════════════════

def ensure_indexes():
    """Ensure all required indexes exist in both source and target DBs."""
    source = get_source_db()
    target = get_target_db()

    # Source: ensure derived_at index for fast querying
    for coll_name in discover_tender_collections():
        source[coll_name].create_index("derived_at")

    # Target: analytics indexes
    target[COLL_SELLERS].create_index("seller_id", unique=True)
    target[COLL_SELLERS].create_index([("seller_name_norm", ASCENDING)])

    target[COLL_SELLER_TENDERS].create_index(
        [("seller_id", ASCENDING), ("bid_id", ASCENDING)], unique=True
    )
    target[COLL_SELLER_TENDERS].create_index([("seller_id", ASCENDING), ("end_date", DESCENDING)])
    target[COLL_SELLER_TENDERS].create_index([("seller_id", ASCENDING), ("is_winner", ASCENDING)])
    target[COLL_SELLER_TENDERS].create_index([("seller_id", ASCENDING), ("category", ASCENDING)])
    target[COLL_SELLER_TENDERS].create_index([("seller_id", ASCENDING), ("buyer_name", ASCENDING)])
    target[COLL_SELLER_TENDERS].create_index([("seller_id", ASCENDING), ("month", ASCENDING)])
    target[COLL_SELLER_TENDERS].create_index([("bid_id", ASCENDING)])
    target[COLL_SELLER_TENDERS].create_index([("bid_id", ASCENDING), ("seller_id", ASCENDING)])

    target[COLL_SELLER_BUYERS].create_index(
        [("seller_id", ASCENDING), ("buyer_id", ASCENDING)], unique=True
    )
    target[COLL_SELLER_BUYERS].create_index(
        [("seller_id", ASCENDING), ("tenders_participated", DESCENDING)]
    )

    target[COLL_SELLER_CATEGORIES].create_index(
        [("seller_id", ASCENDING), ("category", ASCENDING)], unique=True
    )
    target[COLL_SELLER_CATEGORIES].create_index(
        [("seller_id", ASCENDING), ("tenders_participated", DESCENDING)]
    )

    target[COLL_SELLER_COMPETITORS].create_index("seller_id", unique=True)
