"""
Backfill derived_at on ALL tenders in tenders_YYYY collections.

For each tenders_YYYY collection, sets derived_at timestamp on all documents
that don't have it yet.

Usage:
    python backfill_derived.py --dry-run  # Preview changes
    python backfill_derived.py            # Apply changes
"""
import sys
import os
import argparse
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone

import certifi
from pymongo import MongoClient
from pymongo.errors import AutoReconnect, NotPrimaryError, ServerSelectionTimeoutError

from pipeline4.config import MONGO_URI, SOURCE_DB


BACKFILL_TIMESTAMP = datetime(2024, 1, 1, tzinfo=timezone.utc)
MAX_RETRIES = 5
RETRY_DELAY = 2  # seconds


def _get_client_kwargs() -> dict:
    kwargs = {}
    if MONGO_URI and MONGO_URI.startswith("mongodb+srv://"):
        kwargs["tlsCAFile"] = certifi.where()
    return kwargs


def update_with_retry(collection, query, update_op, max_retries=MAX_RETRIES):
    """Execute update_many with retry logic for replica state changes."""
    for attempt in range(max_retries):
        try:
            result = collection.update_many(query, update_op)
            return result
        except (AutoReconnect, NotPrimaryError, ServerSelectionTimeoutError) as e:
            if attempt < max_retries - 1:
                delay = RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                print(f"    Retry {attempt + 1}/{max_retries} after {delay}s (error: {e.__class__.__name__})")
                time.sleep(delay)
            else:
                raise


def main():
    parser = argparse.ArgumentParser(
        description="Backfill derived_at on all tenders in tenders_YYYY collections."
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    client = MongoClient(MONGO_URI, **_get_client_kwargs())
    source_db = client[SOURCE_DB]

    print(f"Source: {SOURCE_DB}")
    print(f"Timestamp: {BACKFILL_TIMESTAMP}")
    if args.dry_run:
        print("  *** DRY RUN ***\n")

    total_marked = 0

    # Find all tenders_YYYY collections
    coll_names = sorted(
        n for n in source_db.list_collection_names()
        if n.startswith("tenders_") and n[8:].isdigit()
    )

    print(f"Found {len(coll_names)} tender collections: {', '.join(coll_names)}\n")

    for coll_name in coll_names:
        print(f"  {coll_name}: Updating all unmarked documents...")

        if not args.dry_run:
            # Directly set derived_at for all unmarked documents (with retry)
            result = update_with_retry(
                source_db[coll_name],
                {"derived_at": None},
                {"$set": {"derived_at": BACKFILL_TIMESTAMP}}
            )
            marked_here = result.modified_count
        else:
            # In dry-run, still need to count
            marked_here = source_db[coll_name].count_documents({"derived_at": None})

        total_marked += marked_here
        print(f"  {coll_name}: {'Would mark' if args.dry_run else 'Marked'} {marked_here:,} documents\n")

    print(f"{'=' * 70}")
    print(f"  Total {'would be marked' if args.dry_run else 'marked'}: {total_marked:,}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
