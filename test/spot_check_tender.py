"""
Spot Check - Manual verification helper for specific tenders

Fetches a tender from raw collection and traces it through all derived collections,
displaying all related data for manual inspection.

Usage:
    python test/spot_check_tender.py 12345678
    python test/spot_check_tender.py 12345678 --verbose
"""
import sys
import os
import argparse
from pprint import pprint

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from validate_data_consistency import get_client
from pipeline4.config import SOURCE_DB, TARGET_DB
from pipeline4.extract import extract_participants


def spot_check_tender(bid_id: int, verbose: bool = False):
    """Trace a tender through all collections and display results."""
    client = get_client()
    source_db = client[SOURCE_DB]
    target_db = client[TARGET_DB]
    
    print("=" * 70)
    print(f"  SPOT CHECK: bid_id = {bid_id}")
    print("=" * 70)
    
    # Find tender in raw collections
    tender = None
    found_in = None
    
    for coll_name in source_db.list_collection_names():
        if coll_name.startswith("tenders_"):
            tender = source_db[coll_name].find_one({"bid_id": bid_id})
            if tender:
                found_in = coll_name
                break
    
    if not tender:
        print(f"  ✗ Tender {bid_id} not found in any tenders_YYYY collection")
        return 1
    
    print(f"\n[RAW TENDER] Found in {found_in}")
    print(f"  Month: {tender.get('month')}")
    print(f"  Bid Number: {tender.get('bid_number')}")
    print(f"  Category: {tender.get('category')}")
    print(f"  Department: {tender.get('department')}")
    print(f"  End Date: {tender.get('end_date')}")
    print(f"  Scraped At: {tender.get('scraped_at')}")
    print(f"  Derived At: {tender.get('derived_at')}")
    
    if verbose:
        print("\n  Full Raw Document:")
        pprint({k: v for k, v in tender.items() if k != "_id"})
    
    # Extract participants
    print("\n[PARTICIPANT EXTRACTION]")
    try:
        participants = extract_participants(tender)
        print(f"  Extracted {len(participants)} participants")
        for p in participants:
            print(f"    - {p['seller_id']}: rank={p.get('rank')}, "
                  f"winner={p['is_winner']}, outcome={p['outcome']}")
    except Exception as e:
        print(f"  ✗ Extraction failed: {e}")
        participants = []
    
    # Check seller_tenders
    print("\n[SELLER_TENDERS]")
    seller_tender_docs = list(target_db["seller_tenders"].find({"bid_id": bid_id}))
    
    if not seller_tender_docs:
        print(f"  ✗ No records found for bid_id={bid_id}")
    else:
        print(f"  Found {len(seller_tender_docs)} records")
        for doc in seller_tender_docs:
            print(f"    - {doc['seller_id']}: rank={doc.get('rank')}, "
                  f"winner={doc['is_winner']}, price={doc.get('total_price')}")
        
        if verbose:
            print("\n  Full seller_tenders Documents:")
            for doc in seller_tender_docs:
                pprint({k: v for k, v in doc.items() if k != "_id"})
    
    # Check sellers profile for participants
    if seller_tender_docs:
        print("\n[SELLERS PROFILE]")
        for doc in seller_tender_docs[:3]:  # Check first 3
            seller_id = doc['seller_id']
            profile = target_db["sellers"].find_one({"seller_id": seller_id})
            
            if not profile:
                print(f"  ✗ {seller_id}: No profile found")
            else:
                print(f"  ✓ {seller_id}: {profile['total_tenders']} tenders, "
                      f"{profile['total_wins']} wins, {profile['win_rate']}% win rate")
    
    # Check competitors
    if seller_tender_docs:
        print("\n[COMPETITORS]")
        seller_id = seller_tender_docs[0]['seller_id']
        comp_doc = target_db["seller_competitors"].find_one({"seller_id": seller_id})
        
        if not comp_doc:
            print(f"  ✗ {seller_id}: No competitor doc found")
        else:
            comp_count = len(comp_doc.get("competitors", []))
            print(f"  ✓ {seller_id}: {comp_count} competitors")
            if verbose and comp_count > 0:
                print("    Top competitors:")
                for comp in comp_doc["competitors"][:5]:
                    print(f"      - {comp['competitor_id']}: {comp['shared_tenders']} shared")
    
    print("\n" + "=" * 70)
    print("  SPOT CHECK COMPLETE")
    print("=" * 70)
    return 0


def main():
    parser = argparse.ArgumentParser(description="Spot check a specific tender")
    parser.add_argument("bid_id", type=int, help="Tender bid_id to check")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show full documents")
    args = parser.parse_args()
    
    return spot_check_tender(args.bid_id, args.verbose)


if __name__ == "__main__":
    sys.exit(main())
