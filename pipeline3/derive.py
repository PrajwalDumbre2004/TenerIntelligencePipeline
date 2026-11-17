"""
Pipeline3 Derivation: flatten scraped tender results into seller participation
records, right after pipeline3 finishes scraping -- not later inside
pipeline4's merge step.

This is what used to be pipeline4's job (extract_participants() run inside
its atomic merge transaction). Moving it here means pipeline4's merge becomes
a plain INSERT ... SELECT copy from staging.bid_participations_raw into the
production bid_participations table -- no Python-side transformation left
downstream.

Safe to call repeatedly / mid-scrape: only ever reads rows where
scraped_at IS NOT NULL AND derived_at IS NULL, so tenders that haven't been
scraped yet (or failed and will be retried) are simply left for next time.
"""
from pipeline3.config import DERIVE_BATCH_SIZE
from pipeline3.extract import extract_participants
from pipeline3.db import (
    get_tenders_needing_derivation,
    bulk_insert_participations,
    mark_tenders_derived,
)


def _row_to_tender_dict(row: dict) -> dict:
    """Adapt a staging.tenders_raw row into the shape extract_participants() expects."""
    return {
        "bid_id": row["bid_id"],
        "bid_id_parent": row["bid_id_parent"],
        "bid_number": row["bid_number"] or "",
        "bid_number_parent": row["bid_number_parent"] or "",
        "ra_number": row["ra_number"] or "",
        "category": row["category"] or "",
        "month": row["month"] or "",
        "end_date": row["end_date"] or "",
        "department": row["department"] or "",
        "ministry": row["ministry"] or "",
        "buyer_details": row["buyer_details_raw"],
        "bid_result": row["bid_result_raw"],
        "ra_result": row["ra_result_raw"],
        "stage": row.get("stage"),
    }


def derive_month(month: str, batch_size: int = DERIVE_BATCH_SIZE) -> dict:
    """
    Flatten every scraped-but-not-yet-derived tender for a month into
    staging.bid_participations_raw, then mark each source row derived_at.
    """
    total_tenders = 0
    total_participations = 0

    while True:
        batch = get_tenders_needing_derivation(month, batch_size=batch_size)
        if not batch:
            break

        records = []
        bid_ids = []
        for row in batch:
            bid_ids.append(row["bid_id"])
            tender = _row_to_tender_dict(row)
            records.extend(extract_participants(tender))

        bulk_insert_participations(month, records)
        mark_tenders_derived(month, bid_ids)

        total_tenders += len(batch)
        total_participations += len(records)
        print(f"    [derive] {month}: {total_tenders} tenders -> {total_participations} participations")

    if total_tenders:
        print(f"  Derive complete: {total_tenders} tenders -> {total_participations} participations")

    return {"tenders_derived": total_tenders, "participations_written": total_participations}
