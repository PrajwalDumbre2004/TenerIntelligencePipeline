"""
Participant extraction for Pipeline3.

This is a standalone copy of pipeline4/extract.py's logic, run here instead --
right after pipeline3 finishes scraping a tender's bid_result/ra_result, not
later inside pipeline4's merge step. Kept as a standalone copy (rather than
importing pipeline4.extract from pipeline3) for the same reason pipeline4's
copy stated: no import dependency between pipeline packages.

Logic:
  - Seller name normalization (MSE/MII suffix stripping, uppercase)
  - RA vs Bid precedence based on bid_number pattern
  - Technical + Financial evaluation parsing
  - Winner determination and outcome classification
"""
import re
from typing import Optional


# ═══════════════════════════════════════════════════════════════
# Seller name normalization
# ═══════════════════════════════════════════════════════════════

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
    """Strip trailing '(MSE Social Category: <value>)' suffix."""
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


# ═══════════════════════════════════════════════════════════════
# Rank parsing
# ═══════════════════════════════════════════════════════════════

def _extract_rank_num(rank: str | None) -> int | None:
    """Convert rank string like 'L1', 'L2' to integer."""
    if not rank:
        return None
    rank = rank.strip().upper()
    if rank.startswith("L") and rank[1:].isdigit():
        return int(rank[1:])
    return None


# ═══════════════════════════════════════════════════════════════
# Main extraction function
# ═══════════════════════════════════════════════════════════════

def extract_participants(tender: dict) -> list[dict]:
    """
    Extract all participants from a single tender's raw scraped result.
    Returns a flat list of participation records -- one per seller who took
    part in this tender.

      - RA precedence: if bid_number contains /R/ or /RA/, use ra_result only
      - Otherwise use bid_result only
      - Technical evaluation → seller_name, mse_status, technical_status
      - Financial evaluation → rank, total_price, is_winner
      - Winner = L1 ranked seller, but ONLY when the bid is awarded
        (tender["stage"] == 3, or None for pre-stage rows)
      - Outcome classification: won / lost / disqualified / no_result
        (bids not yet awarded: disqualified / no_result only)
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
    is_ra_bid = bool(re.search(r"/(?:R|RA)/", bid_number))
    active_result = ra_result if is_ra_bid else bid_result

    participants: dict[str, dict] = {}

    # Parse technical evaluation
    for item in active_result.get("technical_evaluation") or []:
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

        status = (item.get("status") or "").strip()
        if status:
            entry["technical_status"] = status

    # Parse financial evaluation
    for item in active_result.get("financial_evaluation") or []:
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

    # Winners only exist on awarded bids. A bid at technical (1) or financial (2)
    # evaluation already shows L1/L2 ranks, but nobody has won it yet (and it may
    # still be cancelled), so rank and price are kept but is_winner stays False.
    # stage None = a row from before stages existed, which were all awarded.
    # An awarded bid that was cancelled later is still stage 3 and keeps its winner.
    stage = tender.get("stage")
    is_awarded = stage is None or stage >= 3
    if not is_awarded:
        for p in participants.values():
            p["is_winner"] = False

    # Determine winner
    winner_id = ""
    winner_name = ""
    winner_price = None
    for p in participants.values():
        if p["is_winner"]:
            winner_id = p["seller_id"]
            winner_name = p["seller_name"]
            winner_price = p["total_price"]
            break

    # Build output records with outcome classification
    records = []
    for p in participants.values():
        outcome = "no_result"
        if p["is_winner"]:
            outcome = "won"
        elif "DISQUALIF" in (p.get("technical_status") or "").upper():
            outcome = "disqualified"
        elif not is_awarded:
            outcome = "no_result"        # not decided yet: no "lost" before an award
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
            "winner_id": p["seller_id"] if p["is_winner"] else winner_id,
            "winner_name": p["seller_name"] if p["is_winner"] else winner_name,
            "winner_price": p["total_price"] if p["is_winner"] else winner_price,
            "technical_status": p.get("technical_status") or "",
            "outcome": outcome,
        })

    return records
