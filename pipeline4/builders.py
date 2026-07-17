"""
Builders for Pipeline4: Recompute seller aggregates from canonical seller_tenders.

Each builder takes the FULL list of seller_tenders for one seller and produces
the corresponding analytics document(s). This ensures aggregates are always
accurate regardless of how many incremental runs have occurred.

Mirrors the logic in derived_claude.py exactly.
"""
from collections import Counter
from datetime import datetime, timezone


def build_seller_profile(seller_id: str, tenders: list[dict], raw_name: str = "", mse_status: str = "") -> dict:
    """
    Build the sellers profile card from all participation records.

    Returns a single document for the sellers collection.
    """
    if not tenders:
        return {
            "seller_id": seller_id,
            "seller_name": raw_name or seller_id,
            "seller_name_norm": seller_id,
            "mse_status": mse_status,
            "total_tenders": 0,
            "total_wins": 0,
            "win_rate": 0,
            "total_contract_value": 0,
            "avg_contract_value": 0,
            "top_categories": [],
            "top_buyers": [],
            "first_seen": "",
            "last_seen": "",
            "updated_at": datetime.now(timezone.utc),
        }

    # Deduplicate by bid_id for counting
    unique_bids: dict[int, dict] = {}
    for t in tenders:
        bid_id = t.get("bid_id")
        if bid_id is not None and bid_id not in unique_bids:
            unique_bids[bid_id] = t

    total_tenders = len(unique_bids)
    wins = {bid: t for bid, t in unique_bids.items() if t.get("is_winner")}
    total_wins = len(wins)
    win_rate = round(total_wins * 100 / total_tenders, 1) if total_tenders else 0

    win_prices = [
        t["total_price"] for t in wins.values()
        if isinstance(t.get("total_price"), (int, float)) and t["total_price"] > 0
    ]
    total_contract_value = sum(win_prices)
    avg_contract_value = round(total_contract_value / len(win_prices), 2) if win_prices else 0

    # Top categories
    cat_counter: Counter = Counter()
    cat_wins: Counter = Counter()
    for t in unique_bids.values():
        cat = (t.get("category") or "").strip()
        if cat:
            cat_counter[cat] += 1
            if t.get("is_winner"):
                cat_wins[cat] += 1
    top_categories = [
        {"category": cat, "count": count, "wins": cat_wins.get(cat, 0)}
        for cat, count in cat_counter.most_common(10)
    ]

    # Top buyers
    buyer_counter: Counter = Counter()
    buyer_wins: Counter = Counter()
    for t in unique_bids.values():
        buyer = (t.get("buyer_name") or "").strip()
        if buyer:
            buyer_counter[buyer] += 1
            if t.get("is_winner"):
                buyer_wins[buyer] += 1
    top_buyers = [
        {"buyer_name": buyer, "count": count, "wins": buyer_wins.get(buyer, 0)}
        for buyer, count in buyer_counter.most_common(10)
    ]

    # Timeline
    months = sorted(set(t.get("month") or "" for t in unique_bids.values()) - {""})
    first_seen = months[0] if months else ""
    last_seen = months[-1] if months else ""

    # Best raw name (shortest non-empty)
    if not raw_name:
        names = [t.get("seller_name", "") for t in tenders if t.get("seller_name")]
        raw_name = min(names, key=len) if names else seller_id

    # Best MSE status
    if not mse_status:
        for t in tenders:
            if t.get("mse_status"):
                mse_status = t["mse_status"]
                break

    return {
        "seller_id": seller_id,
        "seller_name": raw_name or seller_id,
        "seller_name_norm": seller_id,
        "mse_status": mse_status,
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


def build_seller_tenders_docs(seller_id: str, tenders: list[dict]) -> list[dict]:
    """
    Deduplicate participation records to one doc per bid_id for seller_tenders.
    Keeps the best-ranked record per bid.
    """
    seen_bid_ids: set = set()
    results = []

    # Sort: best rank first per bid_id
    sorted_tenders = sorted(
        tenders,
        key=lambda t: (
            t.get("bid_id", 0),
            t.get("rank_num") if t.get("rank_num") is not None else 999999,
            -int(t.get("is_winner", False)),
        )
    )

    for t in sorted_tenders:
        bid_id = t.get("bid_id")
        if bid_id is None or bid_id in seen_bid_ids:
            continue
        seen_bid_ids.add(bid_id)

        doc = {
            "seller_id": seller_id,
            "seller_name": t.get("seller_name", ""),
            "mse_status": t.get("mse_status", ""),
            "bid_id": bid_id,
            "bid_id_parent": t.get("bid_id_parent"),
            "bid_number": t.get("bid_number", ""),
            "bid_number_parent": t.get("bid_number_parent", ""),
            "ra_number": t.get("ra_number", ""),
            "category": t.get("category", ""),
            "buyer_name": t.get("buyer_name", ""),
            "buyer_department": t.get("buyer_department", ""),
            "ministry": t.get("ministry", ""),
            "month": t.get("month", ""),
            "end_date": t.get("end_date", ""),
            "rank": t.get("rank", ""),
            "rank_num": t.get("rank_num"),
            "total_price": t.get("total_price"),
            "is_winner": t.get("is_winner", False),
            "winner_id": t.get("winner_id", ""),
            "winner_name": t.get("winner_name", ""),
            "winner_price": t.get("winner_price"),
            "technical_status": t.get("technical_status", ""),
            "outcome": t.get("outcome", "no_result"),
        }
        results.append(doc)

    return results


def build_seller_buyers_docs(seller_id: str, tenders: list[dict]) -> list[dict]:
    """
    Build seller_buyers aggregation: one doc per seller→buyer pair.
    Computes from full seller_tenders history.
    """
    buyer_stats: dict[str, dict] = {}

    for t in tenders:
        buyer_name = (t.get("buyer_name") or "").strip()
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

        if t.get("is_winner"):
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


def build_seller_categories_docs(seller_id: str, tenders: list[dict]) -> list[dict]:
    """
    Build seller_categories aggregation: one doc per seller→category.
    Computes from full seller_tenders history.
    """
    cat_stats: dict[str, dict] = {}

    for t in tenders:
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

        if t.get("is_winner"):
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
