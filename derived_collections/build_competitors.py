"""
Build seller_competitors collection from existing seller_tenders.

Schema:
  {
    seller_id: "ORG_A",
    competitors: [
      { competitor_id: "ORG_B", shared_tenders: 120 },
      { competitor_id: "ORG_C", shared_tenders: 80 }
    ]
  }
  One doc per seller with embedded competitors array.

How it works:
  For each seller in seller_tenders:
    1. Fetch all their bid_ids (uses (seller_id, bid_id) index — fast)
    2. For each batch of 1000 bids, query all participants (uses (bid_id, seller_id) index)
    3. Count occurrences of each other seller across all shared bids
    4. Upsert single seller document with embedded competitors array

  Parallelised across sellers with ThreadPoolExecutor (same pattern as derived_claude.py).

Usage:
  python build_competitors.py --all
  python build_competitors.py --sellers "ORG A" "ORG B"
  python build_competitors.py --watched
  python build_competitors.py --sellers "ORG A" --dry-run
"""

import argparse
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient

load_dotenv(Path(__file__).parent / ".env")

MONGO_URI = "mongodb://localhost:27017"
TARGET_DB = "TenderAnalytics"
COLL_SELLER_TENDERS = "seller_tenders"
COLL_SELLER_COMPETITORS = "seller_competitors"
COLL_WATCHED_SELLERS = "watched_sellers"

BATCH_SIZE = 1000
WORKERS = 12
PRINT_EVERY_N = 100


def normalize_name(raw: str) -> str:
    return raw.strip().upper()


def get_sellers_from_watched(db) -> list[str]:
    docs = list(db[COLL_WATCHED_SELLERS].find({"is_active": True}))
    return [doc["seller_id"] for doc in docs]


def get_all_sellers(db) -> list[str]:
    docs = db["sellers"].find({}, {"_id": 0, "seller_id": 1})
    return [doc["seller_id"] for doc in docs]


def ensure_indexes(db):
    db[COLL_SELLER_COMPETITORS].create_index(
        [("seller_id", ASCENDING)],
        unique=True,
    )


def _upsert_empty(seller_id: str, competitors_collection, dry_run: bool) -> None:
    if not dry_run:
        competitors_collection.replace_one(
            {"seller_id": seller_id},
            {"seller_id": seller_id, "competitors": []},
            upsert=True,
        )


def build_competitors_for_seller(
    seller_id: str,
    tenders_collection,
    competitors_collection,
    dry_run: bool = False,
) -> dict:
    """
    For one seller, scan all their bids, find all co-participants,
    count shared tenders, then upsert.
    """
    t_start = time.time()

    # Step 1: Get MY bid_ids
    my_bids = list(
        tenders_collection.find(
            {"seller_id": seller_id},
            {"_id": 0, "bid_id": 1},
        )
    )
    if not my_bids:
        _upsert_empty(seller_id, competitors_collection, dry_run)
        return {"seller_id": seller_id, "bids": 0, "competitors": 0, "time": 0}

    my_bid_ids = sorted({t["bid_id"] for t in my_bids if t.get("bid_id")})
    if not my_bid_ids:
        _upsert_empty(seller_id, competitors_collection, dry_run)
        return {"seller_id": seller_id, "bids": 0, "competitors": 0, "time": 0}

    # Step 2: For each batch of bid_ids, find all participants
    # competitor_counts[competitor_id] = shared_tenders count (int)
    competitor_counts: dict[str, int] = {}

    for batch_start in range(0, len(my_bid_ids), BATCH_SIZE):
        batch = my_bid_ids[batch_start:batch_start + BATCH_SIZE]
        cursor = tenders_collection.find(
            {"bid_id": {"$in": batch}},
            {"_id": 0, "seller_id": 1},
        )
        for rec in cursor:
            sid = rec.get("seller_id")
            if not sid or sid == seller_id:
                continue
            competitor_counts[sid] = competitor_counts.get(sid, 0) + 1

    if not competitor_counts:
        _upsert_empty(seller_id, competitors_collection, dry_run)
        return {"seller_id": seller_id, "bids": len(my_bid_ids), "competitors": 0, "time": time.time() - t_start}

    # Step 3: Build and upsert the seller document with embedded competitors
    competitors_list = [
        {"competitor_id": comp_id, "shared_tenders": count}
        for comp_id, count in sorted(competitor_counts.items(), key=lambda x: -x[1])
    ]

    if not dry_run:
        doc = {
            "seller_id": seller_id,
            "competitors": competitors_list,
        }
        competitors_collection.replace_one(
            {"seller_id": seller_id},
            doc,
            upsert=True,
        )

    elapsed = time.time() - t_start
    return {
        "seller_id": seller_id,
        "bids": len(my_bid_ids),
        "competitors": len(competitors_list),
        "time": round(elapsed, 2),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Build seller_competitors collection.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--sellers", nargs="+", help="Specific seller names to process.")
    source.add_argument("--watched", action="store_true", help="Process watched_sellers.")
    source.add_argument("--all", action="store_true", help="Process ALL sellers in seller_tenders.")
    parser.add_argument("--dry-run", action="store_true", help="Scan and count only, no writes.")
    parser.add_argument("--mongo-uri", default=MONGO_URI)
    parser.add_argument("--workers", type=int, default=WORKERS)
    return parser.parse_args()


def main():
    args = parse_args()
    client = MongoClient(args.mongo_uri)
    db = client[TARGET_DB]

    ensure_indexes(db)

    tenders_coll = db[COLL_SELLER_TENDERS]
    competitors_coll = db[COLL_SELLER_COMPETITORS]

    if args.sellers:
        seller_ids = [normalize_name(s) for s in args.sellers]
    elif args.watched:
        seller_ids = get_sellers_from_watched(db)
    elif args.all:
        seller_ids = get_all_sellers(db)

    print(f"Processing {len(seller_ids)} sellers...")

    if args.dry_run:
        print("  DRY RUN — no writes will be performed.")

    ERROR_LOG = Path(__file__).parent / "competitors_errors.txt"
    _lock = threading.Lock()
    done = 0
    total = len(seller_ids)
    errors = []

    with ERROR_LOG.open("a", encoding="utf-8") as err_log:
        err_log.write(f"\n--- Run started ---\n")

        def task(sid):
            nonlocal done
            try:
                result = build_competitors_for_seller(sid, tenders_coll, competitors_coll, dry_run=args.dry_run)
                with _lock:
                    done += 1
                    c = done
                if c == 1 or c == total or c % PRINT_EVERY_N == 0:
                    print(f"  [{c}/{total}] {result['seller_id']}: {result['bids']} bids, {result['competitors']} competitors in {result['time']}s")
                return result
            except Exception as e:
                with _lock:
                    errors.append(f"{sid}: {e}")
                    done += 1
                    c = done
                    err_log.write(f"{sid}: {e}\n")
                    err_log.flush()
                print(f"  [{c}/{total}] {sid}: ERROR — {e}")
                return None

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            list(pool.map(task, sorted(seller_ids)))

    print(f"\nDone. {total - len(errors)}/{total} sellers processed, {len(errors)} errors.")
    if errors:
        print(f"\nErrors written to: {ERROR_LOG}")


if __name__ == "__main__":
    main()
