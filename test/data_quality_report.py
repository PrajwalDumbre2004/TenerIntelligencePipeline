"""
Data Quality Report Generator

Generates statistics and quality metrics across both databases.
Useful for understanding data completeness and identifying patterns.

Usage:
    python test/data_quality_report.py
    python test/data_quality_report.py --months 2026-01 2026-02
    python test/data_quality_report.py --output report.txt
"""
import sys
import os
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from validate_data_consistency import get_client, discover_tender_collections
from pipeline4.config import SOURCE_DB, TARGET_DB


def generate_report(source_db, target_db, months=None, output_file=None):
    """Generate comprehensive data quality report."""
    
    lines = []
    
    def log(text):
        lines.append(text)
        print(text)
    
    log("=" * 70)
    log("  DATA QUALITY REPORT")
    log("=" * 70)
    log(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"  Source DB: {SOURCE_DB}")
    log(f"  Target DB: {TARGET_DB}")
    if months:
        log(f"  Scope: {', '.join(months)}")
    log("=" * 70)
    
    # Raw tender statistics
    log("\n[RAW TENDERS]")
    collections = discover_tender_collections(source_db)
    
    total_tenders = 0
    total_scraped = 0
    total_derived = 0
    
    for coll_name in collections:
        query = {}
        if months:
            query["month"] = {"$in": months}
        
        count = source_db[coll_name].count_documents(query)
        scraped = source_db[coll_name].count_documents({**query, "scraped_at": {"$ne": None}})
        derived = source_db[coll_name].count_documents({**query, "derived_at": {"$ne": None}})
        
        if count > 0:
            log(f"  {coll_name}:")
            log(f"    Total: {count:,}")
            log(f"    Scraped: {scraped:,} ({scraped*100/count:.1f}%)")
            log(f"    Derived: {derived:,} ({derived*100/count:.1f}%)")
            
            total_tenders += count
            total_scraped += scraped
            total_derived += derived
    
    log(f"\n  TOTAL ACROSS ALL COLLECTIONS:")
    log(f"    Tenders: {total_tenders:,}")
    log(f"    Scraped: {total_scraped:,} ({total_scraped*100/total_tenders:.1f}%)")
    log(f"    Derived: {total_derived:,} ({total_derived*100/total_tenders:.1f}%)")
    
    # Derived collection statistics
    log("\n[DERIVED ANALYTICS]")
    
    seller_count = target_db["sellers"].count_documents({})
    log(f"  Sellers: {seller_count:,}")
    
    seller_tenders_count = target_db["seller_tenders"].count_documents({})
    log(f"  Seller-Tender participations: {seller_tenders_count:,}")
    
    if seller_count > 0:
        avg_tenders_per_seller = seller_tenders_count / seller_count
        log(f"    Avg tenders per seller: {avg_tenders_per_seller:.1f}")
    
    seller_buyers_count = target_db["seller_buyers"].count_documents({})
    log(f"  Seller-Buyer relationships: {seller_buyers_count:,}")
    
    seller_categories_count = target_db["seller_categories"].count_documents({})
    log(f"  Seller-Category entries: {seller_categories_count:,}")
    
    seller_competitors_count = target_db["seller_competitors"].count_documents({"competitors": {"$ne": []}})
    log(f"  Sellers with competitors: {seller_competitors_count:,}")
    
    # Data completeness
    log("\n[DATA COMPLETENESS]")
    
    # Tenders with results
    tenders_with_results = 0
    for coll_name in collections:
        query = {}
        if months:
            query["month"] = {"$in": months}
        query["$or"] = [
            {"bid_result": {"$ne": None}},
            {"ra_result": {"$ne": None}}
        ]
        tenders_with_results += source_db[coll_name].count_documents(query)
    
    log(f"  Tenders with bid/RA results: {tenders_with_results:,} ({tenders_with_results*100/total_scraped:.1f}% of scraped)")
    
    # Tenders with buyer details
    tenders_with_buyers = 0
    for coll_name in collections:
        query = {}
        if months:
            query["month"] = {"$in": months}
        query["buyer_details"] = {"$ne": None}
        tenders_with_buyers += source_db[coll_name].count_documents(query)
    
    log(f"  Tenders with buyer details: {tenders_with_buyers:,} ({tenders_with_buyers*100/total_scraped:.1f}% of scraped)")
    
    # Tenders with contract details
    tenders_with_contracts = 0
    for coll_name in collections:
        query = {}
        if months:
            query["month"] = {"$in": months}
        query["contract_details"] = {"$ne": None}
        tenders_with_contracts += source_db[coll_name].count_documents(query)
    
    log(f"  Tenders with contract details: {tenders_with_contracts:,} ({tenders_with_contracts*100/total_scraped:.1f}% of scraped)")
    
    # Winners and prices
    log("\n[WINNER & PRICE DATA]")
    
    winners_count = target_db["seller_tenders"].count_documents({"is_winner": True})
    log(f"  Total winners: {winners_count:,}")
    
    winners_with_price = target_db["seller_tenders"].count_documents({
        "is_winner": True,
        "total_price": {"$ne": None}
    })
    log(f"  Winners with price: {winners_with_price:,} ({winners_with_price*100/winners_count:.1f}% of winners)")
    
    # Price statistics
    pipeline = [
        {"$match": {"is_winner": True, "total_price": {"$ne": None, "$gt": 0}}},
        {"$group": {
            "_id": None,
            "avg_price": {"$avg": "$total_price"},
            "min_price": {"$min": "$total_price"},
            "max_price": {"$max": "$total_price"}
        }}
    ]
    
    price_stats = list(target_db["seller_tenders"].aggregate(pipeline))
    if price_stats:
        stats = price_stats[0]
        log(f"\n  Price Statistics (winners with price > 0):")
        log(f"    Average: ₹{stats['avg_price']:,.0f}")
        log(f"    Min: ₹{stats['min_price']:,.0f}")
        log(f"    Max: ₹{stats['max_price']:,.0f}")
    
    # Top sellers
    log("\n[TOP SELLERS]")
    
    top_sellers = list(target_db["sellers"].find(
        {},
        {"seller_id": 1, "seller_name": 1, "total_tenders": 1, "total_wins": 1, "win_rate": 1, "_id": 0}
    ).sort("total_tenders", -1).limit(10))
    
    log("  By Total Tenders:")
    for i, seller in enumerate(top_sellers, 1):
        log(f"    {i}. {seller['seller_name'][:50]}: "
            f"{seller['total_tenders']} tenders, {seller['total_wins']} wins ({seller['win_rate']}%)")
    
    # Potential issues
    log("\n[POTENTIAL ISSUES]")
    
    issues_found = 0
    
    # Check for sellers with 0 tenders
    empty_sellers = target_db["sellers"].count_documents({"total_tenders": 0})
    if empty_sellers > 0:
        log(f"  ⚠ {empty_sellers} sellers with 0 tenders")
        issues_found += 1
    
    # Check for negative prices
    negative_prices = target_db["seller_tenders"].count_documents({"total_price": {"$lt": 0}})
    if negative_prices > 0:
        log(f"  ⚠ {negative_prices} tenders with negative prices")
        issues_found += 1
    
    # Check for winners without prices
    winners_no_price = target_db["seller_tenders"].count_documents({
        "is_winner": True,
        "total_price": None
    })
    if winners_no_price > 0:
        log(f"  ⚠ {winners_no_price} winners without prices")
        issues_found += 1
    
    # Check for orphaned tenders
    orphaned = 0
    for coll_name in collections:
        query = {"scraped_at": {"$ne": None}, "derived_at": None}
        if months:
            query["month"] = {"$in": months}
        orphaned += source_db[coll_name].count_documents(query)
    
    if orphaned > 0:
        log(f"  ⚠ {orphaned} orphaned tenders (scraped but not derived)")
        issues_found += 1
    
    if issues_found == 0:
        log("  ✓ No issues detected")
    
    log("\n" + "=" * 70)
    log("  REPORT COMPLETE")
    log("=" * 70)
    
    # Write to file if requested
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        print(f"\n  Report saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Generate data quality report")
    parser.add_argument("--months", nargs="+", help="Filter by months (YYYY-MM)")
    parser.add_argument("--output", "-o", help="Save report to file")
    args = parser.parse_args()
    
    client = get_client()
    source_db = client[SOURCE_DB]
    target_db = client[TARGET_DB]
    
    generate_report(source_db, target_db, args.months, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
