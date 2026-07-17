"""
Reusable pipeline to build derived analytics collections from raw year-wise tender data.

Source: TenderIntelligence.tenders_YYYY (auto-discovers all year collections)
Target: TenderAnalytics (derived collections in same DB)

Derived collections:
  - sellers              : one doc per seller (profile card)
  - seller_tenders       : one doc per seller×tender participation
  - seller_buyers        : one doc per seller→buyer relationship
  - seller_categories    : one doc per seller→category performance

HOW IT WORKS (SQLite disk-spill):
  Instead of holding all seller participation data in RAM simultaneously, this
  pipeline streams all tenders ONCE from Atlas into a local SQLite buffer, then
  processes each seller one-at-a-time from that local buffer.

  Atlas load  : one sequential read scan (same as before)
  RAM usage   : ~constant regardless of seller count (one seller in memory at a time)
  Disk usage  : ~1-2 GB local SQLite buffer (auto-deleted after run)

─── WHY THE ORIGINAL WAS SLOW (and got slower over time) ───────────────────────

  Problem 1 — DeleteMany + ordered InsertOne[]:
    The original _bulk_replace_collection() issued ONE DeleteMany followed by N
    InsertOne ops in a single ordered bulk_write.  ordered=True forces pymongo to
    wait for the DeleteMany acknowledgement before sending the first InsertOne.
    At 300 000 sellers × up to thousands of docs each, this means hundreds of
    millions of sequential round-trips and delete+index-cleanup cycles.

  Problem 2 — Index cost grows with collection size (the "gets slower" part):
    Every InsertOne must update ALL indexes on that collection.  seller_tenders
    has 6 compound indexes.  Updating a B-tree index on a 10M-row collection
    requires many more disk I/Os than on a 10k-row collection (the tree is much
    deeper).  So write throughput degrades approximately as O(log N) where N is
    the total document count — the pipeline slows noticeably after the first few
    hundred thousand sellers are written.

  Problem 3 — 24 outer × 4 inner = 96 simultaneous MongoDB write operations:
    All 96 threads hammer the same index B-trees simultaneously, causing heavy
    write-lock contention inside WiredTiger.  This compounds Problem 2.

─── THE FIX ────────────────────────────────────────────────────────────────────

  ReplaceOne(upsert=True) keyed on the natural compound unique key:
    seller_tenders    → (seller_id, bid_id)
    seller_buyers     → (seller_id, buyer_id)
    seller_categories → (seller_id, category)

  Benefits:
    • No DeleteMany — no delete-side index churn, no ordering constraint.
    • ordered=False — pymongo can pipeline all ops in a single network burst
      rather than waiting for each acknowledgement before sending the next.
    • Idempotent — re-running on the same seller just overwrites in place;
      no risk of partial deletes leaving the collection in a torn state.
    • Constant write cost — each ReplaceOne touches only the doc's own index
      entries, regardless of how large the overall collection has grown.
    • Zero data loss — a crash mid-batch leaves old data intact (not deleted);
      the next run simply overwrites those docs again.

  Stale-doc cleanup:
    With upsert, docs for bids that no longer exist in the source (e.g. a
    tender was retracted) would persist indefinitely.  To handle this we track
    the set of bid_ids written for the seller in this run and issue a single
    targeted DeleteMany(seller_id=X, bid_id NOT IN written_set) AFTER the
    upserts succeed.  Because the upserts have already landed, the delete is
    safe: if it's lost on a primary failover the worst case is one extra stale
    doc, not a missing doc.  On the next run the stale doc will be cleaned again.

  Thread reduction:
    PHASE2_WORKERS reduced to 12 (from 24).  Network I/O throughput per-worker
    is not the bottleneck — index lock contention is.  Fewer concurrent writers
    reduces lock pressure substantially with no throughput loss.

Performance tuning applied (2025-06, rev2):
  MONGO_WRITE_BATCH  : 5_000 → 10_000  fewer round-trips per seller
  PHASE2_WORKERS     : 24    → 12      less index lock contention (key change)
  maxPoolSize        : 8     (unchanged from rev1)
  ordered            : True  → False   bulk_write can now pipeline all ReplaceOnes
  DeleteMany timing  : pre-insert → post-upsert  eliminates torn-state risk
  write concern      : default (majority) — kept; w=1 risks lost writes on failover

Usage:
  # Full rebuild for specific sellers
  python build_derived_collections.py --sellers "IMPERIAL SURGICALS" "MERCURY LAB PLANNERS"

  # Full rebuild for all sellers in watched_sellers collection
  python build_derived_collections.py --watched

  # Full rebuild for ALL sellers encountered in source data
  python build_derived_collections.py --all

  # Incremental update: only process tenders added/modified since last run
  python build_derived_collections.py --watched --incremental

  # Process a specific year only (useful for newly added year collections)
  python build_derived_collections.py --watched --years 2026

  # Discover all unique sellers (writes to watched_sellers)
  python build_derived_collections.py --discover --min-tenders 50

  # Keep the SQLite buffer after run (for debugging)
  python build_derived_collections.py --watched --keep-buffer
"""

import argparse
import re
import sqlite3
import tempfile
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient, ReplaceOne
from pymongo import DeleteMany, InsertOne

load_dotenv(Path(__file__).parent / ".env")

# ─── Configuration ─────────────────────────────────────────────────────────────

MONGO_URI = "mongodb://localhost:27017"
SOURCE_DB = "TenderIntelligence"
TARGET_DB = "TenderAnalytics"
TENDER_COLLECTION_PATTERN = re.compile(r"^tenders_(\d{4})$")

# Derived collection names
COLL_SELLERS = "sellers"
COLL_SELLER_TENDERS = "seller_tenders"
COLL_SELLER_BUYERS = "seller_buyers"
COLL_SELLER_CATEGORIES = "seller_categories"
COLL_WATCHED_SELLERS = "watched_sellers"
COLL_PIPELINE_STATE = "pipeline_refresh_state"

# SQLite buffer settings
SQLITE_FLUSH_EVERY = 100_000  # rows buffered before writing to SQLite
SQLITE_BATCH_SIZE = 5_000     # rows per cursor batch from Atlas

MONGO_WRITE_BATCH = 10_000    # ReplaceOne ops per bulk_write call

# ─── CHANGED: reduced from 24 → 12 ─────────────────────────────────────────────
# The original bottleneck was NOT network I/O throughput — it was index lock
# contention inside WiredTiger.  24 concurrent writers all hammering the same
# B-tree indexes serialise on internal latches.  12 workers halves the contention
# with negligible throughput loss because each worker spends most of its time
# waiting on network RTT anyway.
PHASE2_WORKERS = 12

# Progress-print throttle
PRINT_EVERY_N = 100

# Failed log file
FAILED_LOG_FILE = Path(__file__).parent / "failed_logs.txt"


# ─── Seller name normalization ─────────────────────────────────────────────────

_MSE_SUFFIX_RE = re.compile(
    r"\s*\(\s*MSE\s+Social\s+Category\s*:\s*\w+\s*\)\s*$", re.IGNORECASE
)
_MII_SUFFIX_RE = re.compile(r"\s*\(\s*MII\s*\)\s*$", re.IGNORECASE)
_PARENS_SUFFIX_RE = re.compile(r"\s*\([^)]*\)\s*$")


def normalize_seller_name(raw: str | None) -> str:
    """Normalize raw seller name to a consistent uppercase key."""
    if not raw:
        return ""
    name = raw.strip()
    name = _MSE_SUFFIX_RE.sub("", name)
    name = _MII_SUFFIX_RE.sub("", name)
    name = _PARENS_SUFFIX_RE.sub("", name)
    name = name.strip()
    return name.upper()


def strip_mse_suffix(raw: str | None) -> str:
    """
    Strip ONLY a trailing '(MSE Social Category: <value>)' suffix from a raw
    seller name — e.g. 'ACME PVT LTD ( MSE Social Category:General )' becomes
    'ACME PVT LTD'. Nothing else is touched (no MII suffix, no other
    parenthetical content) — this is the initial cleanup applied to every
    seller name before any further processing.
    """
    if not raw:
        return ""
    return _MSE_SUFFIX_RE.sub("", raw.strip()).strip()


def extract_mse_status(raw_name: str | None, mse_field: str | None) -> str:
    """Extract MSE status from either the field or the seller name suffix."""
    if mse_field and mse_field.strip() and mse_field.strip().upper() != "N/A":
        return mse_field.strip()
    if raw_name:
        match = _MSE_SUFFIX_RE.search(raw_name)
        if match:
            return match.group(0).strip().strip("()")
    return ""


# ─── Tender participant extraction ────────────────────────────────────────────

def _extract_rank_num(rank: str | None) -> int | None:
    """Convert rank string like 'L1', 'L2' to integer."""
    if not rank:
        return None
    rank = rank.strip().upper()
    if rank.startswith("L") and rank[1:].isdigit():
        return int(rank[1:])
    return None


def extract_participants(tender: dict) -> list[dict]:
    """
    Extract all participants from a single tender document.
    Returns a flat list of participation records.
    """
    bid_id = tender.get("bid_id")
    bid_id_parent = tender.get("bid_id_parent")
    bid_number = tender.get("bid_number") or ""
    bid_number_parent = tender.get("bid_number_parent") or ""
    ra_number = tender.get("ra_number") or ""
    category = (tender.get("category") or "").strip()
    month = (tender.get("month") or "").strip()
    end_date = tender.get("end_date") or ""
    department = (tender.get("department") or "").strip()
    ministry = (tender.get("ministry") or "").strip()

    buyer_details = tender.get("buyer_details") or {}
    buyer_name = (buyer_details.get("buyer_name") or "").strip()
    buyer_department = (buyer_details.get("buyer_department") or department).strip()

    bid_result = tender.get("bid_result") or {}
    ra_result = tender.get("ra_result") if isinstance(tender.get("ra_result"), dict) else {}

    # ── RA precedence ──────────────────────────────────────────────────────
    # bid_number contains "/R/" or "/RA/"  -> this tender IS an RA round.
    # Use RA results ONLY — even if ra_result is empty/null, do NOT fall
    # back to bid_result.
    # Otherwise                     -> this is a plain bid (no RA). Use
    # bid_result ONLY — ra_result is ignored even if present.
    is_ra_bid = bool(re.search(r"/(?:R|RA)/", bid_number))
    active_result = ra_result if is_ra_bid else bid_result
    active_source = "ra" if is_ra_bid else "bid"

    participants: dict[str, dict] = {}

    for source_name, result in [(active_source, active_result)]:
        for item in result.get("technical_evaluation") or []:
            raw_name_full = (item.get("seller_name") or "").strip()
            # Capture MSE status from the suffix BEFORE stripping it, then
            # strip the trailing '(MSE Social Category: ...)' suffix from the
            # name initially, before any further processing.
            mse = extract_mse_status(raw_name_full, item.get("mse_status"))
            raw_name = strip_mse_suffix(raw_name_full)
            seller_id = normalize_seller_name(raw_name)
            if not seller_id:
                continue

            entry = participants.setdefault(seller_id, {
                "seller_id": seller_id,
                "seller_name": raw_name,
                "mse_status": "",
                "technical_status": "",
                "rank": None,
                "rank_num": None,
                "total_price": None,
                "is_winner": False,
            })

            if raw_name and (not entry["seller_name"] or len(raw_name) < len(entry["seller_name"])):
                entry["seller_name"] = raw_name

            if mse and not entry["mse_status"]:
                entry["mse_status"] = mse

            status = (item.get("status") or "").strip()
            if status:
                entry["technical_status"] = status

    for source_name, result in [(active_source, active_result)]:
        for item in result.get("financial_evaluation") or []:
            raw_name_full = (item.get("seller_name") or "").strip()
            mse = extract_mse_status(raw_name_full, item.get("mse_status"))
            raw_name = strip_mse_suffix(raw_name_full)
            seller_id = normalize_seller_name(raw_name)
            if not seller_id:
                continue

            entry = participants.setdefault(seller_id, {
                "seller_id": seller_id,
                "seller_name": raw_name,
                "mse_status": "",
                "technical_status": "",
                "rank": None,
                "rank_num": None,
                "total_price": None,
                "is_winner": False,
            })

            if raw_name and (not entry["seller_name"] or len(raw_name) < len(entry["seller_name"])):
                entry["seller_name"] = raw_name

            if mse and not entry["mse_status"]:
                entry["mse_status"] = mse

            rank = (item.get("rank") or "").strip()
            rank_num = _extract_rank_num(rank)
            total_price = item.get("total_price")

            if rank_num is not None:
                if entry["rank_num"] is None or rank_num < entry["rank_num"]:
                    entry["rank"] = rank
                    entry["rank_num"] = rank_num
                    entry["is_winner"] = (rank_num == 1)

            if isinstance(total_price, (int, float)) and total_price > 0:
                if entry["total_price"] is None or total_price < entry["total_price"]:
                    entry["total_price"] = total_price

    if not participants:
        return []

    winner_id = ""
    winner_name = ""
    winner_price = None
    for p in participants.values():
        if p["is_winner"]:
            winner_id = p["seller_id"]
            winner_name = p["seller_name"]
            winner_price = p["total_price"]
            break

    records = []
    for p in participants.values():
        outcome = "no_result"
        if p["is_winner"]:
            outcome = "won"
        elif "DISQUALIF" in (p.get("technical_status") or "").upper():
            outcome = "disqualified"
        elif p["rank_num"] is not None and p["rank_num"] > 1:
            outcome = "lost"
        elif winner_id and p["seller_id"] != winner_id:
            outcome = "lost"

        records.append({
            "seller_id": p["seller_id"],
            "seller_name": p["seller_name"],
            "mse_status": p["mse_status"],
            "bid_id": bid_id,
            "bid_id_parent": bid_id_parent,
            "bid_number": bid_number,
            "bid_number_parent": bid_number_parent,
            "ra_number": ra_number,
            "category": category,
            "buyer_name": buyer_name,
            "buyer_department": buyer_department,
            "ministry": ministry,
            "month": month,
            "end_date": end_date,
            "rank": p["rank"] or "",
            "rank_num": p["rank_num"],
            "total_price": p["total_price"],
            "is_winner": p["is_winner"],
            "winner_id": winner_id,
            "winner_name": winner_name,
            "winner_price": winner_price,
            "technical_status": p.get("technical_status") or "",
            "outcome": outcome,
        })

    return records


# ─── Collection discovery ─────────────────────────────────────────────────────

def discover_tender_collections(db) -> list[str]:
    """Find all tenders_YYYY collections, sorted by year."""
    collections = []
    for name in db.list_collection_names():
        if TENDER_COLLECTION_PATTERN.match(name):
            collections.append(name)
    return sorted(collections)


def filter_collections_by_years(collections: list[str], years: list[int] | None) -> list[str]:
    """Filter to specific years if requested."""
    if not years:
        return collections
    year_set = set(years)
    return [c for c in collections if int(TENDER_COLLECTION_PATTERN.match(c).group(1)) in year_set]


# ─── Pipeline state management ─────────────────────────────────────────────────

def get_last_run_timestamp(db, collection_name: str) -> datetime | None:
    state = db[COLL_PIPELINE_STATE].find_one({"source_collection": collection_name})
    if state:
        return state.get("last_processed_at")
    return None


def set_last_run_timestamp(db, collection_name: str, timestamp: datetime):
    db[COLL_PIPELINE_STATE].update_one(
        {"source_collection": collection_name},
        {
            "$set": {
                "source_collection": collection_name,
                "last_processed_at": timestamp,
                "updated_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )


# ─── SQLite buffer ────────────────────────────────────────────────────────────

def create_sqlite_buffer(db_path: str) -> sqlite3.Connection:
    """
    Create and initialize the local SQLite participation buffer.

    Schema:
      participations — one row per seller×tender participation
      seller_meta    — best-known raw_name and mse_status per seller
    """
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")   # 64 MB page cache
    conn.execute("PRAGMA temp_store=MEMORY")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS participations (
            seller_id        TEXT NOT NULL,
            bid_id           INTEGER,
            bid_id_parent    INTEGER,
            bid_number       TEXT,
            bid_number_parent TEXT,
            ra_number        TEXT,
            category         TEXT,
            buyer_name       TEXT,
            buyer_department TEXT,
            ministry         TEXT,
            month            TEXT,
            end_date         TEXT,
            rank             TEXT,
            rank_num         INTEGER,
            total_price      REAL,
            is_winner        INTEGER,
            winner_id        TEXT,
            winner_name      TEXT,
            winner_price     REAL,
            technical_status TEXT,
            outcome          TEXT,
            seller_name      TEXT,
            mse_status       TEXT
        );

        CREATE TABLE IF NOT EXISTS seller_meta (
            seller_id   TEXT PRIMARY KEY,
            raw_name    TEXT,
            mse_status  TEXT
        );
    """)
    conn.commit()
    return conn


def create_sqlite_indexes(conn: sqlite3.Connection):
    """
    Create indexes after bulk insert for maximum insert speed.
    Only idx_part_seller is needed — all Phase 2 queries filter by seller_id.
    """
    print("  Creating SQLite indexes...")
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_part_seller ON participations(seller_id);
    """)
    conn.commit()
    print("  SQLite indexes created.")


def stream_atlas_to_sqlite(
    source_db,
    target_db,
    source_collections: list[str],
    filter_seller_ids: set[str] | None,
    conn: sqlite3.Connection,
    incremental: bool = False,
) -> set[str]:
    """
    Single sequential scan of Atlas source collections.
    Writes all participation records to local SQLite buffer.

    If filter_seller_ids is provided, only those sellers are written
    (for --sellers / --watched modes). Pass None to capture all sellers
    (for --all mode).

    Returns: set of all seller_ids written to SQLite.
    """
    print("\n" + "=" * 60)
    print("PHASE 1: Streaming Atlas -> SQLite buffer")
    print("=" * 60)

    all_seller_ids_seen: set[str] = set()
    insert_batch: list[tuple] = []
    meta_updates: dict[str, tuple] = {}   # seller_id -> (raw_name, mse_status)

    total_docs = 0
    total_rows = 0

    INSERT_SQL = """
        INSERT INTO participations (
            seller_id, bid_id, bid_id_parent, bid_number, bid_number_parent, ra_number, category,
            buyer_name, buyer_department, ministry, month, end_date,
            rank, rank_num, total_price, is_winner,
            winner_id, winner_name, winner_price,
            technical_status, outcome, seller_name, mse_status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    def flush_batch():
        nonlocal total_rows
        if insert_batch:
            conn.executemany(INSERT_SQL, insert_batch)
            conn.commit()
            total_rows += len(insert_batch)
            insert_batch.clear()
        if meta_updates:
            conn.executemany(
                """INSERT INTO seller_meta (seller_id, raw_name, mse_status)
                   VALUES (?, ?, ?)
                   ON CONFLICT(seller_id) DO UPDATE SET
                     raw_name   = CASE WHEN excluded.raw_name != '' THEN excluded.raw_name ELSE raw_name END,
                     mse_status = CASE WHEN excluded.mse_status != '' THEN excluded.mse_status ELSE mse_status END
                """,
                [(sid, raw, mse) for sid, (raw, mse) in meta_updates.items()],
            )
            conn.commit()
            meta_updates.clear()

    for coll_name in source_collections:
        collection = source_db[coll_name]
        query: dict[str, Any] = {}

        if incremental:
            last_ts = get_last_run_timestamp(target_db, coll_name)
            if last_ts:
                query["scraped_at"] = {"$gt": last_ts}

        doc_count = collection.count_documents(query)
        print(f"\n  {coll_name}: {doc_count:,} docs" + (" (incremental)" if query else ""))

        cursor = collection.find(query, batch_size=SQLITE_BATCH_SIZE)
        try:
            for i, tender in enumerate(cursor, 1):
                for record in extract_participants(tender):
                    sid = record["seller_id"]

                    if filter_seller_ids is not None and sid not in filter_seller_ids:
                        continue

                    all_seller_ids_seen.add(sid)

                    insert_batch.append((
                        sid,
                        record["bid_id"],
                        record["bid_id_parent"],
                        record["bid_number"],
                        record["bid_number_parent"],
                        record["ra_number"],
                        record["category"],
                        record["buyer_name"],
                        record["buyer_department"],
                        record["ministry"],
                        record["month"],
                        record["end_date"],
                        record["rank"],
                        record["rank_num"],
                        record["total_price"],
                        1 if record["is_winner"] else 0,
                        record["winner_id"],
                        record["winner_name"],
                        record["winner_price"],
                        record["technical_status"],
                        record["outcome"],
                        record["seller_name"],
                        record["mse_status"],
                    ))

                    existing = meta_updates.get(sid, ("", ""))
                    raw = record["seller_name"] if record["seller_name"] != sid else existing[0]
                    mse = record["mse_status"] or existing[1]
                    meta_updates[sid] = (raw or existing[0], mse)

                total_docs += 1
                if len(insert_batch) >= SQLITE_FLUSH_EVERY:
                    flush_batch()

                if i % 100_000 == 0:
                    flush_batch()
                    print(f"    ...{i:,} / {doc_count:,} docs | {total_rows:,} rows in SQLite")
        finally:
            cursor.close()

        flush_batch()
        set_last_run_timestamp(target_db, coll_name, datetime.now(timezone.utc))

    flush_batch()
    print(f"\n  Done: {total_docs:,} docs scanned | {total_rows:,} participation rows | {len(all_seller_ids_seen):,} sellers")
    return all_seller_ids_seen


def load_seller_from_sqlite(conn: sqlite3.Connection, seller_id: str) -> dict:
    """
    Stream participation records for one seller from local SQLite using a
    server-side cursor rather than fetchall().
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM participations
        WHERE seller_id = ?
        ORDER BY
            bid_id ASC,
            CASE WHEN rank_num IS NULL THEN 999999 ELSE rank_num END ASC,
            is_winner DESC
    """, (seller_id,))
    cols = [d[0] for d in cur.description]

    tenders = []
    while True:
        batch = cur.fetchmany(2_000)
        if not batch:
            break
        for row in batch:
            r = dict(zip(cols, row))
            r["is_winner"] = bool(r["is_winner"])
            tenders.append(r)
    cur.close()

    meta = conn.execute(
        "SELECT raw_name, mse_status FROM seller_meta WHERE seller_id = ?", (seller_id,)
    ).fetchone()

    return {
        "tenders": tenders,
        "raw_name": (meta[0] if meta else "") or seller_id,
        "mse_status": meta[1] if meta else "",
    }


def list_all_sellers_in_sqlite(conn: sqlite3.Connection) -> list[str]:
    """Return all distinct seller_ids stored in the buffer."""
    rows = conn.execute("SELECT seller_id FROM seller_meta ORDER BY seller_id").fetchall()
    return [r[0] for r in rows]


# ─── Builder functions ────────────────────────────────────────────────────────

def build_seller_profile(seller_id: str, data: dict) -> dict | None:
    tenders = data["tenders"]
    if not tenders:
        return None

    total_tenders = len(set(t["bid_id"] for t in tenders))
    wins = [t for t in tenders if t["is_winner"]]
    total_wins = len(set(t["bid_id"] for t in wins))
    win_rate = round(total_wins * 100 / total_tenders, 1) if total_tenders else 0

    win_prices = [t["total_price"] for t in wins if isinstance(t.get("total_price"), (int, float))]
    total_contract_value = sum(win_prices)
    avg_contract_value = round(total_contract_value / len(win_prices), 2) if win_prices else 0

    cat_counter: Counter = Counter()
    cat_wins: Counter = Counter()
    seen_cats: set = set()
    for t in tenders:
        if t["category"]:
            cat_key = (t["bid_id"], t["category"])
            if cat_key not in seen_cats:
                seen_cats.add(cat_key)
                cat_counter[t["category"]] += 1
                if t["is_winner"]:
                    cat_wins[t["category"]] += 1
    top_categories = [
        {"category": cat, "count": count, "wins": cat_wins.get(cat, 0)}
        for cat, count in cat_counter.most_common(10)
    ]

    buyer_counter: Counter = Counter()
    buyer_wins: Counter = Counter()
    seen_buyers: set = set()
    for t in tenders:
        if t["buyer_name"]:
            buyer_key = (t["bid_id"], t["buyer_name"])
            if buyer_key not in seen_buyers:
                seen_buyers.add(buyer_key)
                buyer_counter[t["buyer_name"]] += 1
                if t["is_winner"]:
                    buyer_wins[t["buyer_name"]] += 1
    top_buyers = [
        {"buyer_name": buyer, "count": count, "wins": buyer_wins.get(buyer, 0)}
        for buyer, count in buyer_counter.most_common(10)
    ]

    months = sorted(set(t["month"] for t in tenders if t["month"]))
    first_seen = months[0] if months else ""
    last_seen = months[-1] if months else ""

    return {
        "seller_id": seller_id,
        "seller_name": data["raw_name"] or seller_id,
        "seller_name_norm": seller_id,
        "mse_status": data["mse_status"],
        "total_tenders": total_tenders,
        "total_wins": total_wins,
        "win_rate": win_rate,
        "total_contract_value": total_contract_value,
        "avg_contract_value": avg_contract_value,
        "top_categories": top_categories,
        "top_buyers": top_buyers,
        "first_seen": first_seen,
        "last_seen": last_seen,
        "updated_at": datetime.now(timezone.utc),
    }


def build_seller_tenders(seller_id: str, data: dict) -> list[dict]:
    """
    Deduplicate participations to one record per bid_id, keeping the best rank.
    SQLite ORDER BY ensures first row per bid_id is already best-ranked.
    """
    seen_bid_ids: set = set()
    results = []

    for t in data["tenders"]:
        bid_id = t["bid_id"]
        if bid_id in seen_bid_ids:
            continue
        seen_bid_ids.add(bid_id)

        doc = dict(t)
        doc["ministry"] = t.get("ministry") or ""
        doc["participants"] = {}
        results.append(doc)

    return results


def build_seller_buyers(seller_id: str, data: dict) -> list[dict]:
    buyer_stats: dict[str, dict] = {}

    for t in data["tenders"]:
        buyer_name = t.get("buyer_name") or ""
        if not buyer_name:
            continue

        buyer_id = buyer_name.strip().upper()
        stats = buyer_stats.setdefault(buyer_id, {
            "seller_id": seller_id,
            "buyer_id": buyer_id,
            "buyer_name": buyer_name,
            "buyer_department": t.get("buyer_department") or "",
            "tenders_participated": 0,
            "tenders_won": 0,
            "total_value_won": 0,
            "categories": set(),
            "last_participated": "",
            "last_won": "",
        })

        stats["tenders_participated"] += 1
        if t.get("category"):
            stats["categories"].add(t["category"])
        if t.get("month") and t["month"] > stats["last_participated"]:
            stats["last_participated"] = t["month"]

        if t["is_winner"]:
            stats["tenders_won"] += 1
            if isinstance(t.get("total_price"), (int, float)):
                stats["total_value_won"] += t["total_price"]
            if t.get("month") and t["month"] > stats["last_won"]:
                stats["last_won"] = t["month"]

    results = []
    for stats in buyer_stats.values():
        participated = stats["tenders_participated"]
        won = stats["tenders_won"]
        stats["win_rate"] = round(won * 100 / participated, 1) if participated else 0
        stats["categories"] = sorted(stats["categories"])
        stats["updated_at"] = datetime.now(timezone.utc)
        results.append(stats)

    return results


def build_seller_categories(seller_id: str, data: dict) -> list[dict]:
    cat_stats: dict[str, dict] = {}

    for t in data["tenders"]:
        category = (t.get("category") or "").strip()
        if not category:
            continue

        stats = cat_stats.setdefault(category, {
            "seller_id": seller_id,
            "category": category,
            "tenders_participated": 0,
            "tenders_won": 0,
            "total_value": 0,
            "prices": [],
            "monthly": Counter(),
            "monthly_wins": Counter(),
        })

        stats["tenders_participated"] += 1
        month = t.get("month") or ""
        if month:
            stats["monthly"][month] += 1

        if isinstance(t.get("total_price"), (int, float)) and t["total_price"] > 0:
            stats["prices"].append(t["total_price"])

        if t["is_winner"]:
            stats["tenders_won"] += 1
            if isinstance(t.get("total_price"), (int, float)):
                stats["total_value"] += t["total_price"]
            if month:
                stats["monthly_wins"][month] += 1

    results = []
    for stats in cat_stats.values():
        participated = stats["tenders_participated"]
        won = stats["tenders_won"]
        prices = stats.pop("prices")
        monthly_counts = stats.pop("monthly")
        monthly_wins = stats.pop("monthly_wins")

        stats["win_rate"] = round(won * 100 / participated, 1) if participated else 0
        stats["avg_price"] = round(sum(prices) / len(prices), 2) if prices else None
        stats["monthly"] = [
            {"month": m, "participated": monthly_counts[m], "won": monthly_wins.get(m, 0)}
            for m in sorted(monthly_counts.keys())
        ]
        stats["updated_at"] = datetime.now(timezone.utc)
        results.append(stats)

    return results


# ─── Write to derived collections ─────────────────────────────────────────────

def _upsert_collection(collection, seller_id: str, docs: list[dict], key_field: str) -> None:
    """
    Upsert all docs for one seller using ReplaceOne(upsert=True), then clean up
    any stale docs that are no longer present in the current source data.

    WHY THIS IS FASTER THAN DeleteMany + InsertOne[] (the original approach):
    ──────────────────────────────────────────────────────────────────────────
    1. No ordered constraint — bulk_write(ordered=False) lets pymongo pipeline
       all ReplaceOne ops in a single network burst.  The original ordered=True
       forced a serial DeleteMany-ack → InsertOne → ack → InsertOne → ... cycle.

    2. No delete-side index churn — DeleteMany removes every document for the
       seller and then every InsertOne re-adds them, forcing the B-tree to update
       twice per document.  ReplaceOne only touches the index entries that
       actually changed (MongoDB WiredTiger does an in-place record update when
       the key is unchanged).

    3. Constant write cost — the original InsertOne cost grew as O(log N) because
       each insert had to navigate a deeper B-tree as the collection grew to
       millions of docs.  ReplaceOne on a pre-existing document navigates the
       B-tree once (to find the key) and updates in place, which is the same
       depth regardless of total collection size.

    4. Zero torn-state risk — if the process is killed after DeleteMany but
       before the InsertOnes complete, the seller's data is gone.  With upserts,
       a crash mid-batch leaves the old data intact; the next run overwrites
       those docs.

    Stale-doc cleanup (accuracy guarantee):
    ────────────────────────────────────────
    After all upserts succeed we delete any docs for this seller whose key_field
    value is NOT in the set we just wrote.  This handles the case where a bid/
    buyer/category disappears from the source data between runs.  Because the
    upserts landed first, the worst-case outcome of a crash during this delete
    is one stale doc surviving to the next run — not a missing doc.

    key_field must be a field that uniquely identifies a doc within the seller
    (e.g. "bid_id" for seller_tenders, "buyer_id" for seller_buyers, "category"
    for seller_categories).  The compound unique index on (seller_id, key_field)
    ensures ReplaceOne hits the index path, not a collection scan.
    """
    if not docs:
        # No docs written this run → delete everything for this seller.
        collection.delete_many({"seller_id": seller_id})
        return

    # ── Step 1: upsert all current docs ──────────────────────────────────────
    ops = []
    written_keys: set = set()
    for doc in docs:
        key_val = doc[key_field]
        written_keys.add(key_val)
        ops.append(
            ReplaceOne(
                {"seller_id": seller_id, key_field: key_val},
                doc,
                upsert=True,
            )
        )

    # Fire in batches with ordered=False — no sequential wait between ops.
    for i in range(0, len(ops), MONGO_WRITE_BATCH):
        collection.bulk_write(ops[i : i + MONGO_WRITE_BATCH], ordered=False)

    # ── Step 2: remove stale docs that no longer exist in source data ─────────
    # This is the safety net for the "accuracy" requirement.
    # We only issue this delete if there could plausibly be stale docs
    # (i.e. the collection already had data for this seller before this run).
    # Using $nin on a set is safe because written_keys is bounded by the number
    # of unique bids/buyers/categories per seller — never unboundedly large.
    if written_keys:
        collection.delete_many({
            "seller_id": seller_id,
            key_field: {"$nin": list(written_keys)},
        })


def _upsert_seller_tenders(collection, seller_id: str, docs: list[dict]) -> None:
    """Upsert seller_tenders — keyed on (seller_id, bid_id)."""
    _upsert_collection(collection, seller_id, docs, key_field="bid_id")


def _upsert_seller_buyers(collection, seller_id: str, docs: list[dict]) -> None:
    """Upsert seller_buyers — keyed on (seller_id, buyer_id)."""
    _upsert_collection(collection, seller_id, docs, key_field="buyer_id")


def _upsert_seller_categories(collection, seller_id: str, docs: list[dict]) -> None:
    """Upsert seller_categories — keyed on (seller_id, category)."""
    _upsert_collection(collection, seller_id, docs, key_field="category")


def write_derived_data(
    db,
    seller_id: str,
    profile: dict,
    tenders: list,
    buyers: list,
    categories: list,
) -> None:
    """
    Write all four derived collections for one seller in parallel.

    Each collection write is dispatched to its own thread so all four network
    round-trips to MongoDB happen concurrently rather than sequentially.
    The seller profile uses replace_one (upsert) since it is a single document.
    The three multi-doc collections use _upsert_collection which issues batched
    ReplaceOne(upsert=True) ops followed by a targeted stale-doc cleanup.
    """
    def write_profile():
        if profile:
            db[COLL_SELLERS].replace_one({"seller_id": seller_id}, profile, upsert=True)

    def write_tenders():
        _upsert_seller_tenders(db[COLL_SELLER_TENDERS], seller_id, tenders)

    def write_buyers():
        _upsert_seller_buyers(db[COLL_SELLER_BUYERS], seller_id, buyers)

    def write_categories():
        _upsert_seller_categories(db[COLL_SELLER_CATEGORIES], seller_id, categories)

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [
            pool.submit(write_profile),
            pool.submit(write_tenders),
            pool.submit(write_buyers),
            pool.submit(write_categories),
        ]
        for fut in as_completed(futures):
            fut.result()  # re-raises any exception from the worker thread


# ─── Index creation ───────────────────────────────────────────────────────────

def ensure_indexes(db):
    """
    Ensure all required indexes exist.

    IMPORTANT: The compound unique indexes on (seller_id, bid_id),
    (seller_id, buyer_id), and (seller_id, category) are load-bearing for
    the upsert strategy — ReplaceOne uses them to locate the target document
    in O(log N) time.  Without them, every ReplaceOne falls back to a full
    collection scan, which is catastrophically slow at 10M+ docs.

    These must exist BEFORE Phase 2 begins.  ensure_indexes() is called at the
    end of main() as a final safety net, but for a first-time run you should
    call it manually (or let Phase 3 handle it) and then re-run.
    """
    print("\nCreating MongoDB indexes...")

    db[COLL_SELLERS].create_index("seller_id", unique=True)
    db[COLL_SELLERS].create_index([("seller_name_norm", ASCENDING)])

    # ── seller_tenders ────────────────────────────────────────────────────────
    # The (seller_id, bid_id) unique index is the upsert key for this collection.
    # All other indexes support query patterns from the application layer.
    db[COLL_SELLER_TENDERS].create_index(
        [("seller_id", ASCENDING), ("bid_id", ASCENDING)], unique=True
    )
    db[COLL_SELLER_TENDERS].create_index([("seller_id", ASCENDING), ("end_date", DESCENDING)])
    db[COLL_SELLER_TENDERS].create_index([("seller_id", ASCENDING), ("is_winner", ASCENDING)])
    db[COLL_SELLER_TENDERS].create_index([("seller_id", ASCENDING), ("category", ASCENDING)])
    db[COLL_SELLER_TENDERS].create_index([("seller_id", ASCENDING), ("buyer_name", ASCENDING)])
    db[COLL_SELLER_TENDERS].create_index([("seller_id", ASCENDING), ("month", ASCENDING)])
    db[COLL_SELLER_TENDERS].create_index([("bid_id", ASCENDING)])
    db[COLL_SELLER_TENDERS].create_index([("bid_id", ASCENDING), ("seller_id", ASCENDING)])  # bid_id_1_seller_id_1

    # ── seller_buyers ─────────────────────────────────────────────────────────
    # (seller_id, buyer_id) unique index is the upsert key.
    db[COLL_SELLER_BUYERS].create_index(
        [("seller_id", ASCENDING), ("buyer_id", ASCENDING)], unique=True
    )
    db[COLL_SELLER_BUYERS].create_index(
        [("seller_id", ASCENDING), ("tenders_participated", DESCENDING)]
    )

    # ── seller_categories ─────────────────────────────────────────────────────
    # (seller_id, category) unique index is the upsert key.
    db[COLL_SELLER_CATEGORIES].create_index(
        [("seller_id", ASCENDING), ("category", ASCENDING)], unique=True
    )
    db[COLL_SELLER_CATEGORIES].create_index(
        [("seller_id", ASCENDING), ("tenders_participated", DESCENDING)]
    )

    db[COLL_WATCHED_SELLERS].create_index("seller_id", unique=True)

    print("  MongoDB indexes created.")


# ─── Seller discovery ─────────────────────────────────────────────────────────

def discover_sellers(source_db, target_db, min_tenders: int = 50):
    """
    Scan all tender collections to find sellers with >= min_tenders participations.
    Uses a lean projection to minimize Atlas data transfer.
    """
    print(f"\nDiscovering sellers with >= {min_tenders} tender participations...")

    seller_counts: Counter = Counter()
    seller_names: dict[str, str] = {}
    seller_mse: dict[str, str] = {}

    projection = {
        "bid_number": 1,
        "bid_result.technical_evaluation.seller_name": 1,
        "bid_result.financial_evaluation.seller_name": 1,
        "ra_result.technical_evaluation.seller_name": 1,
        "ra_result.financial_evaluation.seller_name": 1,
        "_id": 0,
    }

    tender_collections = discover_tender_collections(source_db)
    for coll_name in tender_collections:
        collection = source_db[coll_name]
        count = collection.estimated_document_count()
        print(f"  Scanning {coll_name} ({count:,} docs)...")

        for i, tender in enumerate(collection.find({}, projection, batch_size=2000), 1):
            bid_result = tender.get("bid_result") or {}
            ra_result = tender.get("ra_result") or {}
            bid_number = (tender.get("bid_number") or "")
            is_ra_bid = bool(re.search(r"/(?:R|RA)/", bid_number))
            active_result = ra_result if is_ra_bid else bid_result

            seen_in_tender: set[str] = set()
            for eval_key in ("technical_evaluation", "financial_evaluation"):
                for item in active_result.get(eval_key) or []:
                    raw = strip_mse_suffix((item.get("seller_name") or "").strip())
                    sid = normalize_seller_name(raw)
                    if not sid:
                        continue
                    if sid not in seen_in_tender:
                        seen_in_tender.add(sid)
                        seller_counts[sid] += 1
                    if raw and raw != sid and sid not in seller_names:
                        seller_names[sid] = raw

            if i % 100_000 == 0:
                print(f"    ...{i:,} docs | {len(seller_counts):,} unique sellers")

    qualified = {sid: cnt for sid, cnt in seller_counts.items() if cnt >= min_tenders}
    print(f"\n  Found {len(qualified):,} sellers with >= {min_tenders} participations")
    print(f"  (Total unique sellers: {len(seller_counts):,})")

    operations = [
        ReplaceOne(
            {"seller_id": sid},
            {
                "seller_id": sid,
                "seller_name": seller_names.get(sid, sid),
                "mse_status": seller_mse.get(sid, ""),
                "estimated_tenders": count,
                "added_at": datetime.now(timezone.utc),
                "is_active": True,
            },
            upsert=True,
        )
        for sid, count in qualified.items()
    ]

    if operations:
        target_db[COLL_WATCHED_SELLERS].bulk_write(operations, ordered=False)

    print(f"  Written {len(operations):,} sellers to {COLL_WATCHED_SELLERS}")
    return qualified


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build derived analytics collections from raw year-wise tender data.",
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--sellers",
        nargs="+",
        help="Specific seller names to process.",
    )
    source.add_argument(
        "--watched",
        action="store_true",
        help="Process all sellers in the watched_sellers collection.",
    )
    source.add_argument(
        "--all",
        action="store_true",
        help="Process ALL sellers encountered in source data (no pre-filter).",
    )
    source.add_argument(
        "--discover",
        action="store_true",
        help="Discover all sellers and populate watched_sellers (no derived data built).",
    )

    parser.add_argument("--years", nargs="+", type=int, help="Only process specific years.")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Only process tenders added since last run (based on scraped_at).",
    )
    parser.add_argument(
        "--min-tenders",
        type=int,
        default=50,
        help="Minimum tender count for --discover mode (default: 50).",
    )
    parser.add_argument(
        "--keep-buffer",
        action="store_true",
        help="Keep the SQLite buffer file after the run (for debugging).",
    )
    parser.add_argument(
        "--buffer-path",
        default=None,
        help="Path for the SQLite buffer file (default: auto temp file).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=PHASE2_WORKERS,
        help=f"Parallel seller-processing threads for Phase 2 (default: {PHASE2_WORKERS}).",
    )
    parser.add_argument("--mongo-uri", default=MONGO_URI)
    parser.add_argument("--source-db", default=SOURCE_DB)
    parser.add_argument("--target-db", default=TARGET_DB)

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    client = MongoClient(args.mongo_uri)
    source_db = client[args.source_db]
    target_db = client[args.target_db]

    try:
        client.admin.command("ping")
        print(f"Connected to MongoDB: {args.mongo_uri}")
        print(f"Source DB: {args.source_db}  ->  Target DB: {args.target_db}")
    except Exception as e:
        print(f"Connection failed: {e}")
        return 1

    # Discover mode — just find sellers, no derived data
    if args.discover:
        discover_sellers(source_db, target_db, min_tenders=args.min_tenders)
        ensure_indexes(target_db)
        return 0

    # ── Determine seller filter ───────────────────────────────────────────────
    if args.sellers:
        filter_seller_ids: set[str] | None = {normalize_seller_name(s) for s in args.sellers}
        filter_seller_ids.discard("")
    elif args.watched:
        watched_docs = list(target_db[COLL_WATCHED_SELLERS].find({"is_active": True}))
        if not watched_docs:
            print("No active sellers in watched_sellers. Run with --discover first.")
            return 1
        filter_seller_ids = {doc["seller_id"] for doc in watched_docs}
    else:  # --all
        filter_seller_ids = None

    if filter_seller_ids is not None:
        print(f"\nSeller filter: {len(filter_seller_ids)} sellers")
    else:
        print("\nSeller filter: ALL (no filter)")

    # ── Source collections ────────────────────────────────────────────────────
    all_collections = discover_tender_collections(source_db)
    source_collections = filter_collections_by_years(all_collections, args.years)
    print(f"Source collections: {source_collections}")

    # ── Ensure indexes BEFORE Phase 2 so upserts use the index path ──────────
    # This is critical: if the unique compound indexes don't exist yet, every
    # ReplaceOne(upsert=True) falls back to a full collection scan.  We create
    # them here (idempotent — create_index is a no-op if the index already
    # exists) so they're always in place before the first write.
    ensure_indexes(target_db)

    # ── SQLite buffer setup ───────────────────────────────────────────────────
    if args.buffer_path:
        buffer_path = args.buffer_path
        auto_temp = False
    else:
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False, prefix="tender_buffer_")
        buffer_path = tmp.name
        tmp.close()
        auto_temp = True

    print(f"\nSQLite buffer: {buffer_path}")

    conn = None
    try:
        conn = create_sqlite_buffer(buffer_path)

        # ── PHASE 1: Stream Atlas -> SQLite ───────────────────────────────────
        seller_ids_in_buffer = stream_atlas_to_sqlite(
            source_db,
            target_db,
            source_collections,
            filter_seller_ids,
            conn,
            incremental=args.incremental,
        )

        create_sqlite_indexes(conn)

        if not seller_ids_in_buffer:
            print("\nNo participation data found. Check source collections and seller filter.")
            return 0

        # ── PHASE 2: Build derived collections per seller ─────────────────────
        print("\n" + "=" * 60)
        print("PHASE 2: Building derived collections")
        print(f"         workers={args.workers}  batch={MONGO_WRITE_BATCH}  strategy=upsert")
        print("=" * 60)

        seller_list = sorted(seller_ids_in_buffer)
        total = len(seller_list)

        _done_lock = threading.Lock()
        done_count = 0

        def process_seller(seller_id: str, worker_conn: sqlite3.Connection, worker_db) -> str:
            data = load_seller_from_sqlite(worker_conn, seller_id)
            if not data["tenders"]:
                return f"{seller_id}: no tenders, skipped"

            tender_count = len(data["tenders"])
            profile    = build_seller_profile(seller_id, data)
            tenders    = build_seller_tenders(seller_id, data)
            buyers     = build_seller_buyers(seller_id, data)
            categories = build_seller_categories(seller_id, data)

            write_derived_data(worker_db, seller_id, profile, tenders, buyers, categories)

            return (
                f"{seller_id}: {tender_count} rows → "
                f"{len(tenders)} tenders, {len(buyers)} buyers, "
                f"{len(categories)} categories ✓"
            )

        def make_worker_resources():
            worker_conn = sqlite3.connect(buffer_path, check_same_thread=False)
            worker_conn.execute("PRAGMA query_only=ON")
            worker_client = MongoClient(
                args.mongo_uri,
                maxPoolSize=8,
                connectTimeoutMS=10_000,
                serverSelectionTimeoutMS=10_000,
                # write concern left at default (majority): w=1 saves ~30% latency
                # but risks losing acknowledged writes on primary failover.
            )
            worker_db = worker_client[args.target_db]
            return worker_conn, worker_client, worker_db

        _local = threading.local()

        def get_worker_resources():
            if not hasattr(_local, "conn"):
                _local.conn, _local.client, _local.db = make_worker_resources()
            return _local.conn, _local.db

        def task(seller_id: str) -> tuple[str, str]:
            worker_conn, worker_db = get_worker_resources()
            summary = process_seller(seller_id, worker_conn, worker_db)
            return seller_id, summary

        errors: list[str] = []

        run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        failed_log_handle = FAILED_LOG_FILE.open("a", encoding="utf-8")
        failed_log_handle.write(f"\n--- Run started {run_ts} ---\n")
        failed_log_lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures_map = {executor.submit(task, sid): sid for sid in seller_list}

            for fut in as_completed(futures_map):
                nonlocal_sid = futures_map[fut]
                try:
                    _, summary = fut.result()
                except Exception as exc:
                    error_msg = f"{nonlocal_sid}: {exc}"
                    errors.append(error_msg)
                    summary = f"{nonlocal_sid}: ERROR — {exc}"
                    with failed_log_lock:
                        failed_log_handle.write(f"{error_msg}\n")
                        failed_log_handle.flush()

                with _done_lock:
                    done_count += 1
                    current = done_count

                is_error = "ERROR" in summary
                if current == 1 or current == total or current % PRINT_EVERY_N == 0 or is_error:
                    print(f"  [{current}/{total}] {summary}")

        failed_log_handle.write(
            f"--- Run ended   {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} "
            f"| {len(errors)} error(s) ---\n"
        )
        failed_log_handle.close()

        if errors:
            print(f"\n  {len(errors)} seller(s) failed:")
            for e in errors:
                print(f"    ✗ {e}")
            print(f"  Failed logs written to: {FAILED_LOG_FILE}")

        conn.close()
        conn = None

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        if not args.keep_buffer and auto_temp and Path(buffer_path).exists():
            try:
                Path(buffer_path).unlink()
                print(f"\nSQLite buffer deleted: {buffer_path}")
            except Exception as e:
                print(f"\nWarning: could not delete SQLite buffer ({e}): {buffer_path}")
        elif args.keep_buffer:
            print(f"\nSQLite buffer kept at: {buffer_path}")

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())