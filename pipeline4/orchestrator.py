"""
Pipeline4 Orchestrator: Incremental derived collection updater.

Flow:
  1. Discover all tenders_YYYY collections in TenderIntelligence
  2. Fetch tenders where derived_at=None AND scraped_at!=None
  3. Extract participants from each tender (same logic as derived_claude.py)
  4. Upsert participation records into seller_tenders (canonical source of truth)
  5. Build impacted_sellers set (all sellers found in new tenders)
  6. For each impacted seller, recompute ALL aggregates from full seller_tenders history:
     - sellers (profile card)
     - seller_buyers
     - seller_categories
  7. Recompute seller_competitors for impacted sellers + their co-participants
  8. Mark processed tenders with derived_at timestamp

This ensures aggregates are always correct because they are recomputed from
the full canonical history, not just the delta.
"""
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from pipeline4.config import BATCH_SIZE, WORKERS, PRINT_EVERY
from pipeline4.extract import extract_participants
from pipeline4.builders import (
    build_seller_profile,
    build_seller_tenders_docs,
    build_seller_buyers_docs,
    build_seller_categories_docs,
)
from pipeline4.db import (
    discover_tender_collections,
    get_underived_tenders,
    get_underived_count,
    mark_tenders_derived,
    get_all_seller_tenders,
    get_seller_bids_for_competitors,
    get_co_participants_for_bids,
    upsert_seller_tenders,
    upsert_seller_profile,
    upsert_seller_buyers,
    upsert_seller_categories,
    upsert_seller_competitors,
    ensure_indexes,
)


def process_new_tenders(month_filter: str | list[str] | None = None) -> dict:
    """
    Phase 1: Fetch all underived tenders, extract participants,
    upsert into seller_tenders, and collect impacted sellers.

    Returns:
        {
            "impacted_sellers": set of seller_ids that need recomputation,
            "tenders_processed": int,
            "participations_upserted": int,
        }
    """
    print("\n" + "=" * 70)
    print("  PHASE 1: Extract participants from new tenders")
    print("=" * 70)
    if month_filter:
        if isinstance(month_filter, list):
            print(f"  Month filter: {', '.join(month_filter)}")
        else:
            print(f"  Month filter: {month_filter}")

    collections = discover_tender_collections()
    if not collections:
        print("  No tender collections found.")
        return {"impacted_sellers": set(), "tenders_processed": 0, "participations_upserted": 0}

    # Count total underived
    total_underived = 0
    for coll_name in collections:
        count = get_underived_count(coll_name, month_filter=month_filter)
        if count > 0:
            print(f"  {coll_name}: {count:,} underived tenders")
            total_underived += count

    if total_underived == 0:
        print("  No new tenders to process.")
        return {"impacted_sellers": set(), "tenders_processed": 0, "participations_upserted": 0}

    print(f"\n  Total to process: {total_underived:,} tenders")

    impacted_sellers: set[str] = set()
    total_processed = 0
    total_participations = 0

    # Collect all new participation records grouped by seller
    # We process in batches per collection to avoid memory issues
    new_records_by_seller: dict[str, list[dict]] = {}

    for coll_name in collections:
        while True:
            batch = get_underived_tenders(
                coll_name,
                batch_size=BATCH_SIZE,
                month_filter=month_filter,
            )
            if not batch:
                break

            batch_bid_ids = []
            for tender in batch:
                bid_id = tender.get("bid_id")
                if bid_id is not None:
                    batch_bid_ids.append(bid_id)

                # Extract participants using same logic as derived_claude.py
                records = extract_participants(tender)
                for rec in records:
                    seller_id = rec["seller_id"]
                    impacted_sellers.add(seller_id)
                    new_records_by_seller.setdefault(seller_id, []).append(rec)
                    total_participations += 1

            total_processed += len(batch)

            # Mark this batch as derived immediately
            mark_tenders_derived(coll_name, batch_bid_ids, month_filter=month_filter)

            if total_processed % 1000 == 0 or total_processed == total_underived:
                print(f"    Processed: {total_processed:,}/{total_underived:,} | "
                      f"Sellers impacted: {len(impacted_sellers):,} | "
                      f"Participations: {total_participations:,}")

    print(f"\n  Phase 1 complete: {total_processed:,} tenders → "
          f"{total_participations:,} participations → "
          f"{len(impacted_sellers):,} impacted sellers")

    # Upsert new participation records into seller_tenders
    # For each seller, upsert their new records (existing ones stay intact)
    print("\n  Upserting new seller_tenders records...")
    from pipeline4.db import get_target_db
    from pymongo import ReplaceOne
    from pymongo.errors import BulkWriteError
    from pipeline4.config import MONGO_WRITE_BATCH, COLL_SELLER_TENDERS

    db = get_target_db()
    coll = db[COLL_SELLER_TENDERS]
    upsert_count = 0

    for seller_id, records in new_records_by_seller.items():
        ops = []
        for rec in records:
            bid_id = rec["bid_id"]
            if bid_id is None:
                continue
            doc = {
                "seller_id": seller_id,
                "seller_name": rec.get("seller_name", ""),
                "mse_status": rec.get("mse_status", ""),
                "bid_id": bid_id,
                "bid_id_parent": rec.get("bid_id_parent"),
                "bid_number": rec.get("bid_number", ""),
                "bid_number_parent": rec.get("bid_number_parent", ""),
                "ra_number": rec.get("ra_number", ""),
                "category": rec.get("category", ""),
                "buyer_name": rec.get("buyer_name", ""),
                "buyer_department": rec.get("buyer_department", ""),
                "ministry": rec.get("ministry", ""),
                "month": rec.get("month", ""),
                "end_date": rec.get("end_date", ""),
                "rank": rec.get("rank", ""),
                "rank_num": rec.get("rank_num"),
                "total_price": rec.get("total_price"),
                "is_winner": rec.get("is_winner", False),
                "winner_id": rec.get("winner_id", ""),
                "winner_name": rec.get("winner_name", ""),
                "winner_price": rec.get("winner_price"),
                "technical_status": rec.get("technical_status", ""),
                "outcome": rec.get("outcome", "no_result"),
            }
            ops.append(ReplaceOne(
                {"seller_id": seller_id, "bid_id": bid_id},
                doc,
                upsert=True,
            ))

        if ops:
            for i in range(0, len(ops), MONGO_WRITE_BATCH):
                try:
                    result = coll.bulk_write(ops[i:i + MONGO_WRITE_BATCH], ordered=False)
                    upsert_count += result.upserted_count + result.modified_count
                except BulkWriteError as e:
                    upsert_count += e.details.get("nUpserted", 0) + e.details.get("nModified", 0)

    print(f"  Upserted {upsert_count:,} seller_tender records")

    return {
        "impacted_sellers": impacted_sellers,
        "tenders_processed": total_processed,
        "participations_upserted": total_participations,
    }


def recompute_seller_aggregates(impacted_sellers: set[str]) -> dict:
    """
    Phase 2: For each impacted seller, read their FULL seller_tenders history
    and recompute sellers, seller_buyers, seller_categories.

    Returns stats dict.
    """
    if not impacted_sellers:
        return {"sellers_updated": 0, "errors": 0}

    print("\n" + "=" * 70)
    print(f"  PHASE 2: Recompute aggregates for {len(impacted_sellers):,} sellers")
    print(f"           Workers: {WORKERS}")
    print("=" * 70)

    seller_list = sorted(impacted_sellers)
    total = len(seller_list)
    done_count = 0
    error_count = 0
    _lock = threading.Lock()

    def process_one_seller(seller_id: str) -> str:
        nonlocal done_count, error_count
        try:
            # Read FULL history from canonical seller_tenders
            all_tenders = get_all_seller_tenders(seller_id)

            if not all_tenders:
                # Seller has no records — clean up
                upsert_seller_profile(seller_id, build_seller_profile(seller_id, []))
                upsert_seller_buyers(seller_id, [])
                upsert_seller_categories(seller_id, [])
                with _lock:
                    done_count += 1
                return f"{seller_id}: empty"

            # Recompute all aggregates from full history
            profile = build_seller_profile(seller_id, all_tenders)
            buyers = build_seller_buyers_docs(seller_id, all_tenders)
            categories = build_seller_categories_docs(seller_id, all_tenders)

            # Write to TenderAnalytics
            upsert_seller_profile(seller_id, profile)
            upsert_seller_buyers(seller_id, buyers)
            upsert_seller_categories(seller_id, categories)

            with _lock:
                done_count += 1
                c = done_count

            if c == 1 or c == total or c % PRINT_EVERY == 0:
                print(f"    [{c}/{total}] {seller_id}: "
                      f"{profile['total_tenders']} tenders, "
                      f"{len(buyers)} buyers, {len(categories)} categories")

            return f"{seller_id}: ok"

        except Exception as e:
            with _lock:
                error_count += 1
                done_count += 1
            print(f"    ERROR {seller_id}: {str(e)[:200]}")
            return f"{seller_id}: error"

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(process_one_seller, sid) for sid in seller_list]
        for fut in as_completed(futures):
            fut.result()

    print(f"\n  Phase 2 complete: {total - error_count}/{total} sellers updated, "
          f"{error_count} errors")

    return {"sellers_updated": total - error_count, "errors": error_count}


def recompute_competitors(impacted_sellers: set[str]) -> dict:
    """
    Phase 3: Recompute seller_competitors for impacted sellers.

    For each impacted seller:
      1. Get all their bid_ids from seller_tenders
      2. Find all co-participants across those bids
      3. Count shared tenders per competitor
      4. Upsert the competitors document

    Also recompute competitors for sellers who are co-participants of
    impacted sellers (they have a new competitor entry too).
    
    Optimized: Single-pass parallel processing like build_competitors.py.
    """
    if not impacted_sellers:
        return {"competitors_updated": 0}

    print("\n" + "=" * 70)
    print(f"  PHASE 3: Recompute competitors for impacted sellers")
    print(f"           Workers: {WORKERS}")
    print("=" * 70)

    # We'll discover co-participants during the main pass to avoid double work
    # First, seed with impacted sellers
    sellers_to_process = set(impacted_sellers)
    co_participants_found: set[str] = set()
    
    updated = 0
    _lock = threading.Lock()

    def compute_competitors_for_seller(seller_id: str) -> tuple[str, set[str]]:
        """
        Compute competitors for one seller and return discovered co-participants.
        Returns (seller_id, set of co-participant seller_ids).
        """
        nonlocal updated
        
        bid_ids = get_seller_bids_for_competitors(seller_id)
        if not bid_ids:
            upsert_seller_competitors(seller_id, [])
            with _lock:
                updated += 1
                c = updated
            if c % PRINT_EVERY == 0:
                print(f"    [COMP] processed {c:,} sellers")
            return (seller_id, set())

        competitor_counts = get_co_participants_for_bids(bid_ids, seller_id)
        co_parts = set(competitor_counts.keys())
        
        competitors_list = [
            {"competitor_id": comp_id, "shared_tenders": count}
            for comp_id, count in sorted(competitor_counts.items(), key=lambda x: -x[1])
        ]

        upsert_seller_competitors(seller_id, competitors_list)
        
        with _lock:
            updated += 1
            c = updated
        
        if c % PRINT_EVERY == 0:
            print(f"    [COMP] processed {c:,} sellers")
        
        return (seller_id, co_parts)

    # First pass: Process impacted sellers in parallel and collect co-participants
    print(f"  Processing {len(impacted_sellers):,} impacted sellers...")
    
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(compute_competitors_for_seller, sid) 
                   for sid in sorted(impacted_sellers)]
        
        for fut in as_completed(futures):
            seller_id, co_parts = fut.result()
            co_participants_found.update(co_parts)
    
    # Second pass: Process co-participants who weren't in the impacted set
    new_co_participants = co_participants_found - sellers_to_process
    
    if new_co_participants:
        print(f"\n  Processing {len(new_co_participants):,} co-participants...")
        sellers_to_process.update(new_co_participants)
        
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = [pool.submit(compute_competitors_for_seller, sid) 
                       for sid in sorted(new_co_participants)]
            
            for fut in as_completed(futures):
                fut.result()  # Just complete, no need to track more co-participants

    print(f"\n  Phase 3 complete: {updated:,} seller competitor docs updated")
    print(f"  ({len(impacted_sellers):,} impacted + {len(new_co_participants):,} co-participants)")

    return {"competitors_updated": updated}


def run_pipeline(
    skip_competitors: bool = False,
    month_filter: str | list[str] | None = None,
):
    """
    Full pipeline4 run:
      Phase 1: Extract participants from new tenders → upsert seller_tenders
      Phase 2: Recompute aggregates for impacted sellers
      Phase 3: Recompute competitors (optional, can be slow for many sellers)
    """
    t_start = time.time()

    print("=" * 70)
    print("  PIPELINE 4 — Incremental Derived Collection Updater")
    print("=" * 70)

    # Ensure indexes exist
    print("\n  Ensuring indexes...")
    ensure_indexes()

    # Phase 1: Process new tenders
    phase1 = process_new_tenders(month_filter=month_filter)
    impacted = phase1["impacted_sellers"]

    if not impacted:
        print("\n  Nothing to do — all tenders already derived.")
        return

    # Phase 2: Recompute aggregates
    phase2 = recompute_seller_aggregates(impacted)

    # Phase 3: Competitors (optional)
    if not skip_competitors:
        phase3 = recompute_competitors(impacted)
    else:
        print("\n  Skipping competitor computation (--skip-competitors)")
        phase3 = {"competitors_updated": 0}

    # Summary
    elapsed = time.time() - t_start
    print(f"\n{'=' * 70}")
    print("  PIPELINE 4 SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Tenders processed     : {phase1['tenders_processed']:,}")
    print(f"  Participations found  : {phase1['participations_upserted']:,}")
    print(f"  Sellers impacted      : {len(impacted):,}")
    print(f"  Sellers updated       : {phase2['sellers_updated']:,}")
    print(f"  Competitors updated   : {phase3['competitors_updated']:,}")
    print(f"  Errors                : {phase2['errors']}")
    print(f"  Total time            : {elapsed:.0f}s ({elapsed/60:.1f}min)")
    print(f"{'=' * 70}")
