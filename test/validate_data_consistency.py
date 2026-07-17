"""
Data Consistency Validation Script

Runs automated integrity checks across TenderIntelligence (raw) and 
TenderAnalytics (derived) databases to verify pipeline correctness.

Usage:
    python test/validate_data_consistency.py
    python test/validate_data_consistency.py --months 2026-01 2026-02
    python test/validate_data_consistency.py --sample-size 100
    python test/validate_data_consistency.py --skip-competitors
"""
import sys
import os
import argparse
import random
from datetime import datetime
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import certifi
from pymongo import MongoClient

from pipeline4.config import MONGO_URI, SOURCE_DB, TARGET_DB


# Validation thresholds
MAX_PRICE_SANITY = 10_000_00_00_000  # ₹10,000 crore
MAX_WIN_RATE_TOLERANCE = 0.2  # Allow 0.2% difference due to rounding
SAMPLE_SELLER_COUNT = 50  # Random sellers to deep-check


def get_client():
    kwargs = {}
    if MONGO_URI and MONGO_URI.startswith("mongodb+srv://"):
        kwargs["tlsCAFile"] = certifi.where()
    return MongoClient(MONGO_URI, **kwargs)


class ValidationReport:
    def __init__(self):
        self.passed = []
        self.warnings = []
        self.errors = []
        self.stats = {}
    
    def pass_test(self, test_name: str, detail: str = ""):
        self.passed.append((test_name, detail))
        print(f"  ✓ {test_name}" + (f": {detail}" if detail else ""))
    
    def warn(self, test_name: str, detail: str):
        self.warnings.append((test_name, detail))
        print(f"  ⚠ {test_name}: {detail}")
    
    def fail(self, test_name: str, detail: str):
        self.errors.append((test_name, detail))
        print(f"  ✗ {test_name}: {detail}")
    
    def summary(self):
        print("\n" + "=" * 70)
        print("  VALIDATION SUMMARY")
        print("=" * 70)
        print(f"  Passed  : {len(self.passed)}")
        print(f"  Warnings: {len(self.warnings)}")
        print(f"  Errors  : {len(self.errors)}")
        print("=" * 70)
        
        if self.errors:
            print("\n  ERRORS:")
            for test, detail in self.errors:
                print(f"    - {test}: {detail}")
        
        if self.warnings:
            print("\n  WARNINGS:")
            for test, detail in self.warnings:
                print(f"    - {test}: {detail}")
        
        return len(self.errors) == 0


def discover_tender_collections(db):
    """Find all tenders_YYYY collections."""
    collections = []
    for name in db.list_collection_names():
        if name.startswith("tenders_") and name[8:].isdigit():
            collections.append(name)
    return sorted(collections)


def validate_count_match(source_db, report: ValidationReport, months: list = None):
    """Check that tenders_YYYY counts match pipeline2_status.unique_captured."""
    print("\n[TEST 1] Count Match: tenders vs pipeline2_status")
    
    collections = discover_tender_collections(source_db)
    status_coll = source_db["pipeline2_status"]
    
    for coll_name in collections:
        year = coll_name.split("_")[1]
        
        # Get all months in this year collection
        pipeline = [
            {"$match": {"month": {"$regex": f"^{year}-"}}},
            {"$group": {"_id": "$month", "count": {"$sum": 1}}}
        ]
        
        if months:
            pipeline[0]["$match"]["month"] = {"$in": months}
        
        month_counts = {doc["_id"]: doc["count"] 
                       for doc in source_db[coll_name].aggregate(pipeline)}
        
        for month, actual_count in month_counts.items():
            status = status_coll.find_one({"month": month})
            if not status:
                report.warn(f"Count match {month}", f"No status record found")
                continue
            
            expected = status.get("unique_captured", 0)
            if actual_count == expected:
                report.pass_test(f"Count match {month}", f"{actual_count} tenders")
            else:
                report.fail(
                    f"Count match {month}",
                    f"Expected {expected}, got {actual_count} (diff: {actual_count - expected})"
                )


def validate_no_orphans(source_db, report: ValidationReport, months: list = None):
    """Verify all scraped tenders have derived_at set."""
    print("\n[TEST 2] No Orphans: scraped_at set → derived_at must be set")
    
    collections = discover_tender_collections(source_db)
    
    for coll_name in collections:
        query = {"scraped_at": {"$ne": None}, "derived_at": None}
        if months:
            query["month"] = {"$in": months}
        
        orphan_count = source_db[coll_name].count_documents(query)
        
        if orphan_count == 0:
            report.pass_test(f"No orphans {coll_name}", "All scraped tenders derived")
        else:
            report.fail(
                f"No orphans {coll_name}",
                f"{orphan_count} tenders scraped but not derived"
            )


def validate_seller_math(target_db, report: ValidationReport, sample_size: int = SAMPLE_SELLER_COUNT):
    """Verify seller aggregate math is correct."""
    print(f"\n[TEST 3] Seller Math: Validate aggregates for {sample_size} random sellers")
    
    sellers_coll = target_db["sellers"]
    seller_tenders_coll = target_db["seller_tenders"]
    
    # Get random sample of sellers
    sample = list(sellers_coll.aggregate([{"$sample": {"size": sample_size}}]))
    
    for seller_doc in sample:
        seller_id = seller_doc["seller_id"]
        
        # Count actual tenders
        actual_total = seller_tenders_coll.count_documents({"seller_id": seller_id})
        actual_wins = seller_tenders_coll.count_documents({
            "seller_id": seller_id,
            "is_winner": True
        })
        
        expected_total = seller_doc.get("total_tenders", 0)
        expected_wins = seller_doc.get("total_wins", 0)
        expected_win_rate = seller_doc.get("win_rate", 0)
        
        # Check total_tenders
        if actual_total != expected_total:
            report.fail(
                f"Seller math {seller_id} total_tenders",
                f"Expected {expected_total}, actual {actual_total}"
            )
            continue
        
        # Check total_wins
        if actual_wins != expected_wins:
            report.fail(
                f"Seller math {seller_id} total_wins",
                f"Expected {expected_wins}, actual {actual_wins}"
            )
            continue
        
        # Check win_rate
        if actual_total > 0:
            calculated_win_rate = round(actual_wins * 100 / actual_total, 1)
            diff = abs(calculated_win_rate - expected_win_rate)
            
            if diff > MAX_WIN_RATE_TOLERANCE:
                report.fail(
                    f"Seller math {seller_id} win_rate",
                    f"Expected {expected_win_rate}%, calculated {calculated_win_rate}%"
                )
                continue
        
        report.pass_test(f"Seller math {seller_id}", "All aggregates correct")


# def validate_no_duplicates(target_db, report: ValidationReport):
#     """Check for duplicate (seller_id, bid_id) in seller_tenders."""
#     print("\n[TEST 4] No Duplicates: seller_tenders (seller_id, bid_id) uniqueness")
    
#     pipeline = [
#         {"$group": {
#             "_id": {"seller_id": "$seller_id", "bid_id": "$bid_id"},
#             "count": {"$sum": 1}
#         }},
#         {"$match": {"count": {"$gt": 1}}},
#         {"$limit": 10}
#     ]
    
#     duplicates = list(target_db["seller_tenders"].aggregate(pipeline))
    
#     if not duplicates:
#         report.pass_test("No duplicates seller_tenders", "All (seller_id, bid_id) unique")
#     else:
#         for dup in duplicates:
#             report.fail(
#                 "Duplicate seller_tenders",
#                 f"seller_id={dup['_id']['seller_id']}, bid_id={dup['_id']['bid_id']}, count={dup['count']}"
#             )


def validate_competitor_symmetry(target_db, report: ValidationReport, sample_size: int = 20):
    """Verify competitor relationships are symmetric."""
    print(f"\n[TEST 5] Competitor Symmetry: Check {sample_size} random pairs")
    
    competitors_coll = target_db["seller_competitors"]
    
    # Get sellers with competitors
    sellers_with_comps = list(competitors_coll.aggregate([
        {"$match": {"competitors": {"$ne": []}}},
        {"$sample": {"size": sample_size}}
    ]))
    
    for seller_doc in sellers_with_comps:
        seller_id = seller_doc["seller_id"]
        
        for comp_entry in seller_doc.get("competitors", [])[:5]:  # Check first 5
            comp_id = comp_entry["competitor_id"]
            shared_count = comp_entry["shared_tenders"]
            
            # Check reverse relationship
            comp_doc = competitors_coll.find_one({"seller_id": comp_id})
            if not comp_doc:
                report.warn(
                    f"Competitor symmetry {seller_id}↔{comp_id}",
                    f"{comp_id} has no competitor doc"
                )
                continue
            
            reverse_entry = next(
                (c for c in comp_doc.get("competitors", []) 
                 if c["competitor_id"] == seller_id),
                None
            )
            
            if not reverse_entry:
                report.fail(
                    f"Competitor symmetry {seller_id}↔{comp_id}",
                    f"{seller_id} lists {comp_id}, but not vice versa"
                )
            elif reverse_entry["shared_tenders"] != shared_count:
                report.fail(
                    f"Competitor symmetry {seller_id}↔{comp_id}",
                    f"Shared count mismatch: {shared_count} vs {reverse_entry['shared_tenders']}"
                )
            else:
                report.pass_test(
                    f"Competitor symmetry {seller_id}↔{comp_id}",
                    f"Both sides have {shared_count} shared tenders"
                )


def validate_price_sanity(target_db, report: ValidationReport):
    """Check for invalid prices in seller_tenders."""
    print("\n[TEST 6] Price Sanity: No negative or absurdly large prices")
    
    # Check for negative prices
    negative_count = target_db["seller_tenders"].count_documents({
        "total_price": {"$lt": 0}
    })
    
    if negative_count > 0:
        report.fail("Price sanity negative", f"{negative_count} tenders with negative prices")
    else:
        report.pass_test("Price sanity negative", "No negative prices")
    
    # Check for absurdly large prices
    large_count = target_db["seller_tenders"].count_documents({
        "total_price": {"$gt": MAX_PRICE_SANITY}
    })
    
    if large_count > 0:
        report.warn("Price sanity large", f"{large_count} tenders > ₹10,000 crore")
    else:
        report.pass_test("Price sanity large", "All prices within reasonable range")
    
    # Check for null prices on winners
    null_winner_price = target_db["seller_tenders"].count_documents({
        "is_winner": True,
        "total_price": None
    })
    
    if null_winner_price > 0:
        report.warn(
            "Price sanity null winners",
            f"{null_winner_price} winning tenders have null price"
        )
    else:
        report.pass_test("Price sanity null winners", "All winners have prices")


def validate_tender_linkage(source_db, target_db, report: ValidationReport, sample_size: int = 100):
    """Verify tenders in seller_tenders exist in raw collections."""
    print(f"\n[TEST 7] Tender Linkage: Check {sample_size} random seller_tenders → raw tenders")
    
    # Sample random seller_tenders
    sample = list(target_db["seller_tenders"].aggregate([
        {"$sample": {"size": sample_size}},
        {"$project": {"bid_id": 1, "month": 1, "_id": 0}}
    ]))
    
    for rec in sample:
        bid_id = rec.get("bid_id")
        month = rec.get("month", "")
        
        if not month:
            report.warn(f"Tender linkage bid_id={bid_id}", "No month field")
            continue
        
        year = month.split("-")[0]
        coll_name = f"tenders_{year}"
        
        tender = source_db[coll_name].find_one({"bid_id": bid_id})
        
        if not tender:
            report.fail(
                f"Tender linkage bid_id={bid_id}",
                f"Not found in {coll_name}"
            )
        else:
            report.pass_test(f"Tender linkage bid_id={bid_id}", f"Found in {coll_name}")


def validate_winner_consistency(target_db, report: ValidationReport, sample_size: int = 50):
    """Check winner fields are consistent across a tender's participants."""
    print(f"\n[TEST 8] Winner Consistency: Check {sample_size} random tenders")
    
    # Sample random bid_ids
    pipeline = [
        {"$group": {"_id": "$bid_id"}},
        {"$sample": {"size": sample_size}}
    ]
    
    bid_ids_sample = [doc["_id"] for doc in target_db["seller_tenders"].aggregate(pipeline)]
    
    for bid_id in bid_ids_sample:
        participants = list(target_db["seller_tenders"].find({"bid_id": bid_id}))
        
        if not participants:
            continue
        
        # Find winners
        winners = [p for p in participants if p.get("is_winner")]
        
        # All should agree on winner_id
        winner_ids = set(p.get("winner_id", "") for p in participants)
        
        if len(winner_ids) > 1:
            report.fail(
                f"Winner consistency bid_id={bid_id}",
                f"Multiple winner_ids: {winner_ids}"
            )
        elif len(winners) > 1:
            report.fail(
                f"Winner consistency bid_id={bid_id}",
                f"{len(winners)} participants marked as winner"
            )
        elif len(winners) == 0:
            # No winner is okay (no results case)
            report.pass_test(f"Winner consistency bid_id={bid_id}", "No winner (expected)")
        else:
            winner = winners[0]
            expected_winner_id = winner.get("seller_id")
            
            if all(p.get("winner_id") == expected_winner_id for p in participants):
                report.pass_test(
                    f"Winner consistency bid_id={bid_id}",
                    f"Winner {expected_winner_id} consistent"
                )
            else:
                report.fail(
                    f"Winner consistency bid_id={bid_id}",
                    f"Winner_id mismatch for {expected_winner_id}"
                )


def validate_category_aggregates(target_db, report: ValidationReport, sample_size: int = 20):
    """Verify seller_categories aggregates match seller_tenders."""
    print(f"\n[TEST 9] Category Aggregates: Check {sample_size} random seller→category pairs")
    
    # Sample random seller_categories
    sample = list(target_db["seller_categories"].aggregate([
        {"$sample": {"size": sample_size}}
    ]))
    
    for cat_doc in sample:
        seller_id = cat_doc["seller_id"]
        category = cat_doc["category"]
        
        expected_participated = cat_doc.get("tenders_participated", 0)
        expected_won = cat_doc.get("tenders_won", 0)
        
        # Count actual from seller_tenders
        actual_participated = target_db["seller_tenders"].count_documents({
            "seller_id": seller_id,
            "category": category
        })
        actual_won = target_db["seller_tenders"].count_documents({
            "seller_id": seller_id,
            "category": category,
            "is_winner": True
        })
        
        if actual_participated != expected_participated:
            report.fail(
                f"Category aggregate {seller_id}→{category[:30]}",
                f"Participated: expected {expected_participated}, actual {actual_participated}"
            )
        elif actual_won != expected_won:
            report.fail(
                f"Category aggregate {seller_id}→{category[:30]}",
                f"Won: expected {expected_won}, actual {actual_won}"
            )
        else:
            report.pass_test(
                f"Category aggregate {seller_id}→{category[:30]}",
                f"{actual_participated} participated, {actual_won} won"
            )


def validate_buyer_aggregates(target_db, report: ValidationReport, sample_size: int = 20):
    """Verify seller_buyers aggregates match seller_tenders."""
    print(f"\n[TEST 10] Buyer Aggregates: Check {sample_size} random seller→buyer pairs")
    
    # Sample random seller_buyers
    sample = list(target_db["seller_buyers"].aggregate([
        {"$sample": {"size": sample_size}}
    ]))
    
    for buyer_doc in sample:
        seller_id = buyer_doc["seller_id"]
        buyer_name = buyer_doc["buyer_name"]
        
        expected_participated = buyer_doc.get("tenders_participated", 0)
        expected_won = buyer_doc.get("tenders_won", 0)
        
        # Count actual from seller_tenders
        actual_participated = target_db["seller_tenders"].count_documents({
            "seller_id": seller_id,
            "buyer_name": buyer_name
        })
        actual_won = target_db["seller_tenders"].count_documents({
            "seller_id": seller_id,
            "buyer_name": buyer_name,
            "is_winner": True
        })
        
        if actual_participated != expected_participated:
            report.fail(
                f"Buyer aggregate {seller_id}→{buyer_name[:30]}",
                f"Participated: expected {expected_participated}, actual {actual_participated}"
            )
        elif actual_won != expected_won:
            report.fail(
                f"Buyer aggregate {seller_id}→{buyer_name[:30]}",
                f"Won: expected {expected_won}, actual {actual_won}"
            )
        else:
            report.pass_test(
                f"Buyer aggregate {seller_id}→{buyer_name[:30]}",
                f"{actual_participated} participated, {actual_won} won"
            )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate data consistency across TenderIntelligence and TenderAnalytics."
    )
    parser.add_argument(
        "--months",
        nargs="+",
        help="Only validate specific months (format: YYYY-MM)"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=50,
        help="Number of random samples to check for seller/tender tests (default: 50)"
    )
    parser.add_argument(
        "--skip-competitors",
        action="store_true",
        help="Skip competitor symmetry check (slow for large datasets)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    
    print("=" * 70)
    print("  DATA CONSISTENCY VALIDATION")
    print("=" * 70)
    print(f"  Source DB: {SOURCE_DB}")
    print(f"  Target DB: {TARGET_DB}")
    if args.months:
        print(f"  Months: {', '.join(args.months)}")
    print(f"  Sample size: {args.sample_size}")
    print("=" * 70)
    
    client = get_client()
    source_db = client[SOURCE_DB]
    target_db = client[TARGET_DB]
    
    report = ValidationReport()
    
    try:
        # Run all validation tests
        validate_count_match(source_db, report, args.months)
        validate_no_orphans(source_db, report, args.months)
        validate_seller_math(target_db, report, args.sample_size)
        # validate_no_duplicates(target_db, report)
        
        if not args.skip_competitors:
            validate_competitor_symmetry(target_db, report, min(args.sample_size, 20))
        
        validate_price_sanity(target_db, report)
        validate_tender_linkage(source_db, target_db, report, args.sample_size)
        validate_winner_consistency(target_db, report, min(args.sample_size, 50))
        validate_category_aggregates(target_db, report, min(args.sample_size, 20))
        validate_buyer_aggregates(target_db, report, min(args.sample_size, 20))
        
    except Exception as e:
        print(f"\n  FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Print summary
    success = report.summary()
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
