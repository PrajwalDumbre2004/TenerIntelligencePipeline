"""
ONE-OFF cleanup: delete tenders that reached production `tenders` with no
results, together with everything else stored for them, so the normal
pipeline can fetch them again from scratch.

Background: pipeline3 used to set scraped_at on a FAILED scrape, so the tender
looked done, passed validation and merged into `tenders` empty. pipeline2's
prune step then dropped it on every later run as "already known", so it could
never be scraped again.

That is now prevented going forward (pipeline3/db.py mark_tender_failed keeps
scraped_at NULL; validation_sql/03_validate_tenders.sql rejects not_scraped /
scrape_failed tenders). This script removes the empty tenders that already got
through. Run it once.

What gets deleted, for each empty tender's bid_id (one transaction):
  - tenders                            the empty tender row itself
  - failed_tenders, error_logs         pipeline3's failure logs for it
  - staging.tenders_raw,
    staging.bid_participations_raw,
    staging.bid_participations_quarantine   any staging leftovers
Before committing, EVERY table in the database that has a bid_id column
(discovered from the catalog, not taken from the list above) is re-counted for
these bid_ids. If any row is left anywhere, the whole delete is rolled back.

What is NOT touched, deliberately:
  - bid_participations: an "empty" tender has no participation rows by
    definition (the DELETE re-checks it), so sellers, seller_competitors and
    the seller_categories / seller_buyers views contain nothing from these
    tenders and need no change.
  - Other tenders whose bid_id_parent points at a deleted tender (RA tenders
    created from that bid). They are separate, complete tenders; the parent
    link is just a number and resolves again once the parent is re-fetched.

After running it, re-run each affected month to fetch the tenders again:
    python pipeline_main/run.py <YYYY-MM>
    psql "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -f pipeline4/sql/00_run_all.sql

Usage:
    python recover_empty_tenders.py --dry-run   # only show what would be deleted
    python recover_empty_tenders.py             # delete it
"""
import os
import sys
import argparse
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common.db import get_connection

# A production tender with no bid results, no RA results and no participations.
_EMPTY_TENDERS_SQL = """
    SELECT t.bid_id, t.month FROM tenders t
    WHERE t.bid_result_raw IS NULL AND t.ra_result_raw IS NULL
      AND NOT EXISTS (SELECT 1 FROM bid_participations bp WHERE bp.bid_id = t.bid_id)
"""

# Tables this script deletes from, besides `tenders` (which is deleted
# separately because it re-checks the empty condition).
_RELATED_TABLES = [
    "failed_tenders",
    "error_logs",
    "staging.tenders_raw",
    "staging.bid_participations_raw",
    "staging.bid_participations_quarantine",
]
# Never deleted from (see docstring); an empty tender has 0 rows here by definition.
_NEVER_DELETE = {"public.bid_participations", "bid_participations"}

# Verification is deliberately NOT based on _RELATED_TABLES: it discovers every
# permanent table in the DB that has a bid_id column, so a table missing from
# the delete list (or added to the DB later) is caught instead of skipped.
_BID_ID_TABLES_SQL = """
    SELECT format('%I.%I', n.nspname, c.relname)
    FROM pg_attribute a
    JOIN pg_class c     ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE a.attname = 'bid_id' AND a.attnum > 0 AND NOT a.attisdropped
      AND c.relkind IN ('r', 'p')        -- tables + partitioned parents
      AND NOT c.relispartition           -- partitions are counted via their parent
      AND c.relpersistence <> 't'        -- skip temp tables
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY 1
"""


def _existing(cur, tables):
    """Skip tables that don't exist on this DB (e.g. quarantine before validation ever ran)."""
    out = []
    for t in tables:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (t,))
        if cur.fetchone()[0]:
            out.append(t)
    return out


def find_empty_tenders(cur) -> dict:
    """{bid_id: month} of empty production tenders."""
    cur.execute(_EMPTY_TENDERS_SQL)
    return dict(cur.fetchall())


def count_rows(cur, bid_ids) -> dict:
    """{table: rows for these bid_ids} across EVERY table in the DB with a bid_id column."""
    cur.execute(_BID_ID_TABLES_SQL)
    tables = [r[0] for r in cur.fetchall()]
    counts = {}
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM {t} WHERE bid_id = ANY(%s)", (bid_ids,))
        counts[t] = cur.fetchone()[0]
    return counts


def delete_empty_tenders(conn) -> tuple:
    """
    Delete the empty tenders and all their related rows in ONE transaction,
    verify nothing is left, then commit. Returns (months Counter, {table: deleted}).
    Raises (and rolls back everything) if any row for these bid_ids remains.
    """
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "DELETE FROM tenders t WHERE t.bid_result_raw IS NULL AND t.ra_result_raw IS NULL "
            "AND NOT EXISTS (SELECT 1 FROM bid_participations bp WHERE bp.bid_id = t.bid_id) "
            "RETURNING t.bid_id, t.month"
        )
        rows = cur.fetchall()
        bid_ids = [bid_id for bid_id, _ in rows]
        deleted = {"tenders": len(rows)}

        for t in _existing(cur, _RELATED_TABLES):
            cur.execute(f"DELETE FROM {t} WHERE bid_id = ANY(%s)", (bid_ids,))
            deleted[t] = cur.rowcount

        leftovers = {t: n for t, n in count_rows(cur, bid_ids).items() if n}
        if leftovers:
            raise RuntimeError(f"rows still present after delete, rolled back: {leftovers}")

    return Counter(month for _, month in rows), deleted


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="only show what would be deleted, change nothing")
    args = parser.parse_args()

    conn = get_connection()
    with conn.cursor() as cur:
        empty = find_empty_tenders(cur)
        counts = count_rows(cur, list(empty)) if empty else {}

    print("=" * 70)
    print("  DELETE EMPTY TENDERS (one-off)")
    print("=" * 70)
    print(f"  Production tenders with no results and no participations: {len(empty)}")
    for month, n in sorted(Counter(empty.values()).items()):
        print(f"    {month or '(empty month)'}: {n}")

    if not empty:
        print("  Nothing to delete.")
        return

    print("\n  Rows stored for these tenders (every table with a bid_id column):")
    for t, n in counts.items():
        note = "  (never deleted; must be 0)" if t in _NEVER_DELETE else ""
        print(f"    {t:<40} {n}{note}")

    if args.dry_run:
        print("\n  --dry-run: no changes made.")
        return

    months, deleted = delete_empty_tenders(conn)
    print("\n  Deleted (committed; verified nothing left for these bid_ids):")
    for t, n in deleted.items():
        print(f"    {t:<40} {n}")
    print("\n  Re-run these months to fetch the tenders again:")
    for month in sorted(months):
        print(f"    python pipeline_main/run.py {month}")
    print('  then: psql "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -f pipeline4/sql/00_run_all.sql')
    print("=" * 70)


if __name__ == "__main__":
    main()
