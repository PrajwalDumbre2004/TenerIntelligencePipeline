"""
Seed the TEST database with known bids for one day (default 15 Jul 2025), so
the pipeline run can be checked against expected outcomes.

Reads production (read-only) and GeM's listing; writes ONLY to the test DB.

Seeded cases (bid ids saved to tests/e2e/seed_manifest.json):
  (a) 5 awarded bids, copied from production as they are (stage 3).
      Expected: pruned as duplicates, NOT re-scraped, participants unchanged.
  (b) 5 other awarded bids, copied from production but stored as stage 2
      (financial evaluation) plus a fake seller "TEST STALE SELLER" on each.
      Expected: re-scraped as a stage advance, stage -> 3, the fake row deleted,
      the fake seller's profile zeroed and its competitor pairs removed.
  (c) 3 bids GeM lists as cancelled after financial evaluation, stored as stage 2,
      NOT cancelled (bid_status 1), with a fake seller "TEST KEEP SELLER".
      Expected: pruned (same stage), is_cancelled -> TRUE, bid_status -> 3/5,
      the fake participant row left untouched (no re-scrape).

Usage:
    python tests/e2e/seed.py                     # day 2025-07-15
    python tests/e2e/seed.py --day 2025-07-15
"""
import argparse
import asyncio
import json
from datetime import date, datetime, timezone

from e2e_common import MANIFEST, DEFAULT_TEST_DB, connect_prod_ro, connect_test
from psycopg.types.json import Jsonb

STALE = "TEST STALE SELLER"
KEEP = "TEST KEEP SELLER"


def _adapt(v):
    return Jsonb(v) if isinstance(v, (dict, list)) else v


def columns(conn, table, schema="public"):
    return [r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s "
        "ORDER BY ordinal_position", (schema, table)).fetchall()]


def copy_rows(src, dst, table, where_sql, params, skip=("id",)):
    """Copy rows of `table` matching where_sql from src to dst (shared columns only)."""
    cols = [c for c in columns(src, table) if c in set(columns(dst, table)) and c not in skip]
    rows = src.execute(f"SELECT {', '.join(cols)} FROM {table} WHERE {where_sql}", params).fetchall()
    if rows:
        with dst.cursor() as cur:
            cur.executemany(
                f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
                f"ON CONFLICT DO NOTHING",
                [[_adapt(v) for v in r] for r in rows])
    return len(rows)


async def gem_cancelled_fin(day: date, n: int):
    """Bids GeM lists as cancelled after financial evaluation, ending on `day`."""
    from gem_probe.client import GemPool, make_payload, gem_date
    from pipeline2.worker import parse_doc_to_tender
    d = gem_date(day.year, day.month, day.day)
    async with GemPool(n_sessions=1, conc=2) as pool:
        payloads = [make_payload(page=p, st="cancelled_bids", bs="fin_evaluated", frm=d, to=d) for p in (1, 2, 3)]
        res = await pool.run(payloads, keep_docs=True)
    docs = [doc for r in res if r and r.get("ok") for doc in r.get("docs", [])]
    return [parse_doc_to_tender(doc, day.strftime("%Y-%m"), 2) for doc in docs][: n * 4]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--day", default="2025-07-15")
    ap.add_argument("--test-db", default=DEFAULT_TEST_DB)
    args = ap.parse_args()
    day = date.fromisoformat(args.day)

    prod = connect_prod_ro()
    test = connect_test(args.test_db)

    if test.execute("SELECT count(*) FROM tenders").fetchone()[0]:
        raise SystemExit("Test DB `tenders` is not empty - recreate it with setup_test_db.py --create --drop-existing")

    # (a) + (b): awarded, non-RA bids on that day with >= 2 participants
    ids = [r[0] for r in prod.execute(
        """
        SELECT t.bid_id FROM tenders t
        WHERE t.tender_year = %s AND t.end_date = %s AND NOT t.is_ra_bid
          AND (SELECT count(*) FROM bid_participations bp
               WHERE bp.bid_id = t.bid_id AND bp.participation_year = %s) >= 2
        ORDER BY t.bid_id LIMIT 10
        """, (day.year, day, day.year)).fetchall()]
    if len(ids) < 10:
        raise SystemExit(f"Only {len(ids)} suitable awarded bids in production for {day}")
    a_ids, b_ids = ids[:5], ids[5:]

    n_t = copy_rows(prod, test, "tenders", "bid_id = ANY(%s)", (ids,))
    sellers = [r[0] for r in prod.execute(
        "SELECT DISTINCT seller_id FROM bid_participations WHERE bid_id = ANY(%s)", (ids,)).fetchall()]
    n_s = copy_rows(prod, test, "sellers", "seller_id = ANY(%s)", (sellers,), skip=())
    n_p = copy_rows(prod, test, "bid_participations", "bid_id = ANY(%s)", (ids,))
    test.execute("UPDATE tenders SET stage = 3 WHERE bid_id = ANY(%s)", (a_ids,))
    test.execute("UPDATE tenders SET stage = 2 WHERE bid_id = ANY(%s)", (b_ids,))
    print(f"(a)+(b): copied {n_t} tenders, {n_p} participations, {n_s} sellers")

    # (b) fake stale seller on each b bid + competitor pairs with a real seller
    test.execute("""INSERT INTO sellers (seller_id, seller_name, total_tenders)
                    VALUES (%s, %s, %s)""", (STALE, STALE, len(b_ids)))
    with test.cursor() as cur:
        for bid in b_ids:
            cur.execute("""INSERT INTO bid_participations (seller_id, bid_id, participation_year, end_date, seller_name)
                           VALUES (%s, %s, %s, %s, %s)""", (STALE, bid, day.year, day, STALE))
    partner = test.execute("SELECT seller_id FROM bid_participations WHERE bid_id = %s AND seller_id <> %s LIMIT 1",
                           (b_ids[0], STALE)).fetchone()[0]
    test.execute("""INSERT INTO seller_competitors (seller_id, competitor_id, shared_tenders)
                    VALUES (%s, %s, 1), (%s, %s, 1)""", (STALE, partner, partner, STALE))

    # (c) cancelled-after-financial-evaluation bids, stored as live stage 2
    cands = asyncio.run(gem_cancelled_fin(day, 3))
    in_prod = {r[0] for r in prod.execute("SELECT bid_id FROM tenders WHERE bid_id = ANY(%s)",
                                          ([c["bid_id"] for c in cands],)).fetchall()}
    c_rows = [c for c in cands if c["bid_id"] not in in_prod and c["bid_status"] in (3, 5)][:3]
    if len(c_rows) < 3:
        raise SystemExit(f"Only {len(c_rows)} cancelled fin bids usable for {day}")
    test.execute("INSERT INTO sellers (seller_id, seller_name) VALUES (%s, %s)", (KEEP, KEEP))
    with test.cursor() as cur:
        for c in c_rows:
            cur.execute(
                """INSERT INTO tenders (bid_id, bid_id_parent, bid_number, bid_number_parent, tender_year,
                                        category, department, ministry, month, end_date, is_ra_bid,
                                        stage, is_cancelled, bid_status, bid_result_raw)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 2, FALSE, 1, %s)""",
                (c["bid_id"], c["bid_id_parent"], c["bid_number"] or "", c["bid_number_parent"] or "",
                 day.year, c["category"] or "", c["department"] or "", c["ministry"] or "",
                 c["month"], day, "/R/" in (c["bid_number"] or ""), Jsonb({"seeded": True})))
            cur.execute("""INSERT INTO bid_participations (seller_id, bid_id, participation_year, end_date, seller_name)
                           VALUES (%s, %s, %s, %s, %s)""", (KEEP, c["bid_id"], day.year, day, KEEP))
    c_ids = [c["bid_id"] for c in c_rows]
    print(f"(c): seeded {len(c_ids)} cancelled fin bids as live stage 2")

    manifest = {
        "day": day.isoformat(), "test_db": args.test_db,
        "seeded_at": datetime.now(timezone.utc).isoformat(),
        "a_awarded_unchanged": a_ids, "b_stage_advance": b_ids, "c_newly_cancelled": c_ids,
        "stale_seller": STALE, "stale_partner": partner, "keep_seller": KEEP,
        "a_participations": {str(b): n for b, n in test.execute(
            "SELECT bid_id, count(*) FROM bid_participations WHERE bid_id = ANY(%s) GROUP BY 1", (a_ids,)).fetchall()},
    }
    MANIFEST.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Manifest written: {MANIFEST}")


if __name__ == "__main__":
    main()
