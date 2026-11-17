"""
End-to-end assertions against the TEST database (read-only), plus GeM counts.

Phases:
  staged  - after pipeline_main (pipeline2 + pipeline3), BEFORE pipeline4
  merged  - after pipeline4 (validate + merge + profiles + cleanup)
  rerun   - after running pipeline_main again for the same day, BEFORE pipeline4

Usage:
    python tests/e2e/checks.py staged
    python tests/e2e/checks.py merged
    python tests/e2e/checks.py rerun
Exit code 1 if any check FAILS (INFO lines are informational only).
"""
import asyncio
import sys
from datetime import date

from e2e_common import load_manifest, connect_test

STATUSES = {"tech_evaluated": 1, "fin_evaluated": 2, "bid_awarded": 3}
results = []


def check(name, ok, detail=""):
    results.append((name, bool(ok)))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))


def info(name, detail):
    print(f"  [INFO] {name}: {detail}")


def one(conn, q, p=()):
    return conn.execute(q, p).fetchone()[0]


async def gem_counts(day: date):
    from gem_probe.client import GemPool, gem_date
    d = gem_date(day.year, day.month, day.day)
    specs = {}
    for s in STATUSES:
        specs[("all", s)] = dict(st="all_bids", bs=s, frm=d, to=d)
        specs[("canc", s)] = dict(st="cancelled_bids", bs=s, frm=d, to=d)
    async with GemPool(n_sessions=1, conc=3) as pool:
        return await pool.counts(specs)


def month_run(conn, month):
    row = conn.execute("SELECT run_id, status_counts FROM pipeline2_status WHERE month = %s", (month,)).fetchone()
    if not row:
        sys.exit(f"No pipeline2_status row for {month} in the test DB - did pipeline_main run?")
    return row


def prune_correct(conn, run_id):
    n = one(conn, """
        SELECT count(*) FROM staging.tenders_raw sr JOIN tenders t USING (bid_id)
        WHERE sr.run_id = %s AND COALESCE(sr.stage, 3) <= COALESCE(t.stage, 3)""", (run_id,))
    check("No already-stored bid (same or lower stage) left in staging", n == 0, f"{n} rows")


def phase_staged(conn, m, gem):
    month = m["day"][:7]
    run_id, sc = month_run(conn, month)
    sc = sc or {}

    print("\n-- Listing vs GeM --")
    for s in STATUSES:
        g = gem[("all", s)]
        seen = (sc.get(s) or {}).get("captured")
        ok = g is not None and seen is not None and seen >= 0.99 * g
        check(f"{s}: bids seen >= 99% of GeM numFound", ok, f"seen={seen} gem_now={g} "
              f"gem_at_crawl={(sc.get(s) or {}).get('num_found')}")

    print("\n-- Duplicates / prune --")
    prune_correct(conn, run_id)
    a, b, c = m["a_awarded_unchanged"], m["b_stage_advance"], m["c_newly_cancelled"]
    n = one(conn, "SELECT count(*) FROM staging.tenders_raw WHERE run_id=%s AND bid_id = ANY(%s)", (run_id, a))
    check("(a) stored awarded bids were pruned, not staged", n == 0, f"{n} staged")
    rows = conn.execute("SELECT bid_id, stage FROM staging.tenders_raw WHERE run_id=%s AND bid_id = ANY(%s)",
                        (run_id, b)).fetchall()
    check("(b) stage-advance bids are staged at stage 3", len(rows) == len(b) and all(r[1] == 3 for r in rows),
          f"{rows}")
    n = one(conn, "SELECT count(*) FROM staging.tenders_raw WHERE run_id=%s AND bid_id = ANY(%s)", (run_id, c))
    check("(c) same-stage cancelled bids were pruned, not staged", n == 0, f"{n} staged")
    rows = conn.execute("SELECT bid_id, is_cancelled, bid_status FROM tenders WHERE bid_id = ANY(%s)", (c,)).fetchall()
    check("(c) tenders.is_cancelled flipped to TRUE with bid_status 3/5",
          len(rows) == len(c) and all(r[1] is True and r[2] in (3, 5) for r in rows), f"{rows}")
    n = one(conn, "SELECT count(*) FROM tenders WHERE bid_id = ANY(%s) AND is_cancelled IS NULL", (a,))
    check("(a) cancellation filled in on old awarded rows (NULL -> TRUE/FALSE)", n == 0, f"{n} still NULL")

    print("\n-- Cancelled counts per stage vs GeM cancelled_bids --")
    day = m["day"]
    for s, st in STATUSES.items():
        staged = one(conn, "SELECT count(*) FROM staging.tenders_raw WHERE run_id=%s AND stage=%s AND is_cancelled",
                     (run_id, st))
        stored = one(conn, """SELECT count(*) FROM tenders t WHERE t.end_date = %s AND t.stage = %s AND t.is_cancelled
                              AND NOT EXISTS (SELECT 1 FROM staging.tenders_raw sr
                                              WHERE sr.run_id=%s AND sr.bid_id=t.bid_id)""", (day, st, run_id))
        g = gem[("canc", s)]
        ok = g is not None and abs((staged + stored) - g) <= max(2, round(0.01 * g))
        check(f"{s}: cancelled found = GeM cancelled_bids", ok, f"ours={staged + stored} (staged {staged} + "
              f"stored {stored}) gem={g}")

    print("\n-- Scraping --")
    pend, failed = conn.execute("""SELECT count(*) FILTER (WHERE scraped_at IS NULL AND scrape_error IS NULL),
                                          count(*) FILTER (WHERE scrape_error IS NOT NULL)
                                   FROM staging.tenders_raw WHERE run_id=%s""", (run_id,)).fetchone()
    info("staged rows not scraped / failed", f"{pend} / {failed}")
    n = one(conn, """SELECT count(*) FROM staging.tenders_raw WHERE run_id=%s AND stage < 3
                     AND contract_details_raw IS NOT NULL""", (run_id,))
    check("No contract page fetched for stage 1/2 bids", n == 0, f"{n} rows")
    n = one(conn, """SELECT count(*) FROM staging.bid_participations_raw bp
                     JOIN staging.tenders_raw sr USING (run_id, bid_id)
                     WHERE sr.run_id=%s AND sr.stage < 3 AND bp.is_winner""", (run_id,))
    check("No winner on stage 1/2 bids (staging)", n == 0, f"{n} rows")
    by_stage = conn.execute("""SELECT stage, count(*),
                                      count(*) FILTER (WHERE EXISTS (SELECT 1 FROM staging.bid_participations_raw bp
                                            WHERE bp.run_id=sr.run_id AND bp.bid_id=sr.bid_id))
                               FROM staging.tenders_raw sr WHERE run_id=%s GROUP BY 1 ORDER BY 1""",
                            (run_id,)).fetchall()
    for st, n, with_p in by_stage:
        info(f"stage {st}", f"{n} staged, {with_p} with participants ({with_p * 100 // max(n, 1)}%)")


def phase_merged(conn, m, gem):
    a, b, c = m["a_awarded_unchanged"], m["b_stage_advance"], m["c_newly_cancelled"]
    stale, keep = m["stale_seller"], m["keep_seller"]

    print("\n-- Duplicates --")
    n = one(conn, "SELECT count(*) FROM (SELECT bid_id FROM tenders GROUP BY 1 HAVING count(*) > 1) x")
    check("No duplicate bid_id in tenders", n == 0, f"{n}")
    n = one(conn, "SELECT count(*) FROM (SELECT seller_id, bid_id FROM bid_participations GROUP BY 1, 2 "
                  "HAVING count(*) > 1) x")
    check("No duplicate (seller_id, bid_id) in bid_participations", n == 0, f"{n}")

    print("\n-- Only tenders with participants in the main DB --")
    n = one(conn, """SELECT count(*) FROM tenders t
                     WHERE NOT EXISTS (SELECT 1 FROM bid_participations bp WHERE bp.bid_id = t.bid_id)""")
    check("Every tender has >= 1 participant row", n == 0, f"{n} without")
    n = one(conn, """SELECT count(*) FROM staging.tenders_raw
                     WHERE validation_errors @> '[{"code": "no_valid_participants"}]'::jsonb""")
    check("Rejected no-participant rows removed from staging", n == 0, f"{n} left")
    n = one(conn, "SELECT count(*) FROM staging.tenders_raw WHERE merged_at IS NOT NULL")
    check("Merged rows cleaned out of staging", n == 0, f"{n} left")
    left = conn.execute("""SELECT validated, count(*) FROM staging.tenders_raw GROUP BY 1""").fetchall()
    info("staging rows left (failed scrapes wait here for a retry)", f"{left}")

    print("\n-- Winners --")
    n = one(conn, """SELECT count(*) FROM bid_participations bp JOIN tenders t USING (bid_id)
                     WHERE t.stage < 3 AND bp.is_winner""")
    check("No is_winner on stage 1/2 tenders", n == 0, f"{n}")
    n_ac = one(conn, "SELECT count(*) FROM tenders WHERE stage = 3 AND is_cancelled")
    n_ac_w = one(conn, """SELECT count(*) FROM tenders t WHERE t.stage = 3 AND t.is_cancelled AND EXISTS
                          (SELECT 1 FROM bid_participations bp WHERE bp.bid_id=t.bid_id AND bp.is_winner)""")
    info("awarded-then-cancelled tenders / with a winner row", f"{n_ac} / {n_ac_w}")

    print("\n-- Seed (a): awarded, unchanged --")
    rows = conn.execute("SELECT bid_id, count(*) FROM bid_participations WHERE bid_id = ANY(%s) GROUP BY 1",
                        (a,)).fetchall()
    same = {str(k): v for k, v in rows} == m["a_participations"]
    check("(a) participant rows unchanged", same, f"{dict(rows)}")
    n = one(conn, "SELECT count(*) FROM tenders WHERE bid_id = ANY(%s) AND stage = 3", (a,))
    check("(a) still stage 3", n == len(a), f"{n}/{len(a)}")

    print("\n-- Seed (b): stage moved up, participants replaced --")
    n = one(conn, "SELECT count(*) FROM tenders WHERE bid_id = ANY(%s) AND stage = 3", (b,))
    check("(b) stage updated to 3", n == len(b), f"{n}/{len(b)}")
    n = one(conn, "SELECT count(*) FROM bid_participations WHERE seller_id = %s", (stale,))
    check("(b) stale seller rows deleted", n == 0, f"{n} left")
    tt = one(conn, "SELECT total_tenders FROM sellers WHERE seller_id = %s", (stale,))
    check("(b) stale seller profile zeroed", tt == 0, f"total_tenders={tt}")
    n = one(conn, "SELECT count(*) FROM seller_competitors WHERE seller_id = %s OR competitor_id = %s", (stale, stale))
    check("(b) stale seller competitor pairs removed", n == 0, f"{n} left")
    n = one(conn, """SELECT count(DISTINCT bid_id) FROM bid_participations WHERE bid_id = ANY(%s) AND is_winner""", (b,))
    info("(b) bids with a winner after re-scrape", f"{n}/{len(b)}")

    print("\n-- Seed (c): newly cancelled, not re-scraped --")
    rows = conn.execute("SELECT is_cancelled, bid_result_raw FROM tenders WHERE bid_id = ANY(%s)", (c,)).fetchall()
    check("(c) is_cancelled TRUE and not re-scraped",
          len(rows) == len(c) and all(r[0] is True and r[1] == {"seeded": True} for r in rows))
    n = one(conn, "SELECT count(*) FROM bid_participations WHERE seller_id = %s", (keep,))
    check("(c) existing participant rows untouched", n == len(c), f"{n}/{len(c)}")

    print("\n-- Win rate = wins / awarded participations --")
    # Computed in Python on purpose: as one SQL join over the partitioned tables
    # this got a pathological plan (~18 min) on the small, freshly loaded test DB.
    from collections import defaultdict
    from decimal import Decimal, ROUND_HALF_UP
    stage = dict(conn.execute("SELECT bid_id, COALESCE(stage, 3) FROM tenders").fetchall())
    wins, awarded = defaultdict(set), defaultdict(set)
    for seller, bid, is_winner in conn.execute(
            "SELECT seller_id, bid_id, is_winner FROM bid_participations").fetchall():
        if is_winner:
            wins[seller].add(bid)
        if stage.get(bid, 3) == 3:
            awarded[seller].add(bid)
    bad = []
    for seller, rate in conn.execute("SELECT seller_id, win_rate FROM sellers WHERE updated_at >= %s::timestamptz",
                                     (m["seeded_at"],)).fetchall():
        a = len(awarded[seller])
        exp = (Decimal(len(wins[seller]) * 100) / a).quantize(Decimal("0.1"), ROUND_HALF_UP) if a else Decimal(0)
        if Decimal(rate) != exp:
            bad.append((seller, rate, exp))
    check("sellers.win_rate uses awarded bids only", not bad, f"{len(bad)} mismatches {bad[:3]}")
    for v in ("seller_categories", "seller_buyers"):
        n = one(conn, f"""SELECT count(*) FROM {v} WHERE win_rate <>
                          CASE WHEN awarded_participated > 0
                               THEN round(tenders_won * 100.0 / awarded_participated, 1) ELSE 0 END""")
        check(f"{v}.win_rate uses awarded participations only", n == 0, f"{n} mismatches")

    print("\n-- Coverage vs GeM (informational) --")
    for s, st in STATUSES.items():
        n = one(conn, "SELECT count(*) FROM tenders WHERE end_date = %s AND stage = %s", (m["day"], st))
        g = gem[("all", s)]
        info(f"{s}", f"{n} stored with participants of {g} listed ({n * 100 // max(g or 1, 1)}%)")


def phase_rerun(conn, m, gem):
    month = m["day"][:7]
    run_id, _ = month_run(conn, month)
    print("\n-- Second run of the same day --")
    prune_correct(conn, run_id)
    rows = conn.execute("SELECT stage, count(*) FROM staging.tenders_raw WHERE run_id=%s AND merged_at IS NULL "
                        "GROUP BY 1 ORDER BY 1", (run_id,)).fetchall()
    info("re-staged (only bids NOT in the main DB, e.g. no participants last time)", f"{rows}")


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("staged", "merged", "rerun"):
        sys.exit(__doc__)
    phase = sys.argv[1]
    m = load_manifest()
    gem = asyncio.run(gem_counts(date.fromisoformat(m["day"])))
    conn = connect_test(m["test_db"])
    conn.execute("SET default_transaction_read_only = on")
    print(f"== Phase: {phase}  (day {m['day']}, db {m['test_db']}) ==")
    {"staged": phase_staged, "merged": phase_merged, "rerun": phase_rerun}[phase](conn, m, gem)
    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
