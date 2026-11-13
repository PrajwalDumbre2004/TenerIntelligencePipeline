"""
Step 6 — Compare what the project's Postgres DB holds against GeM (READ-ONLY).

The DB session is forced read-only (default_transaction_read_only=on), so any write would error.
Every query's SQL text is saved with its result.

What it measures:
  - tables present + approx sizes; stored bids per month (staging.tenders_raw and final tenders);
  - pipeline2_status (GeM total at crawl time vs unique captured), failed pages / tenders;
  - stored bids with participant rows; seller names that are only '*' (masked);
  - GeM's current count per stored month (all_bids+bid_awarded = pipeline target; all_bids = everything);
  - for one month (--walk-month), the exact set difference stored-vs-GeM (true scraping loss);
  - a random sample of stored bids looked up on GeM now (exact search) -> how many are cancelled / changed.

Usage:  venv/Scripts/python.exe -X utf8 research/run_db_compare.py [--walk-month 2026-01] [--sample 300]
Output: research/data/db_comparison.json
"""
import argparse
import asyncio
import json
import os
import random
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from research.gem_probe import DATA_DIR
from research.gem_probe.client import GemPool, make_payload, month_range, first, now_iso

import psycopg
from psycopg.rows import dict_row
from common.config import POSTGRES_DSN

MASK_RE = r"^[* ]+$"


def log(*a):
    print(*a, flush=True)


def connect_ro():
    conn = psycopg.connect(POSTGRES_DSN, autocommit=True, row_factory=dict_row)
    with conn.cursor() as cur:
        cur.execute("SET default_transaction_read_only = on")
        cur.execute("SET statement_timeout = '300s'")
    return conn


class Q:
    """Runs queries, records SQL + rows (or error) for the report appendix."""

    def __init__(self, conn):
        self.conn = conn
        self.log = []

    def run(self, name, sql, params=None):
        t0 = time.time()
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall() if cur.description else []
            rows = [{k: (str(v) if not isinstance(v, (int, float, str, bool, type(None))) else v) for k, v in r.items()}
                    for r in rows]
            self.log.append({"name": name, "sql": sql.strip(), "rows": rows, "seconds": round(time.time() - t0, 2)})
            log(f"  [db] {name}: {len(rows)} rows ({time.time()-t0:.1f}s)")
            return rows
        except Exception as e:  # noqa: BLE001
            self.log.append({"name": name, "sql": sql.strip(), "error": str(e)[:500]})
            log(f"  [db] {name}: ERROR {str(e)[:200]}")
            return None


def db_part():
    conn = connect_ro()
    q = Q(conn)
    tables = q.run("tables", """
        SELECT n.nspname AS schema, c.relname AS table, c.reltuples::bigint AS approx_rows
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r','p') AND n.nspname IN ('public','staging')
        ORDER BY 1, 2""") or []
    have = {f"{t['schema']}.{t['table']}" for t in tables}
    cols = {}
    for r in q.run("columns", """
        SELECT table_schema||'.'||table_name AS t, column_name AS c FROM information_schema.columns
        WHERE table_schema IN ('public','staging')""") or []:
        cols.setdefault(r["t"], set()).add(r["c"])

    if "staging.tenders_raw" in have:
        q.run("staging_by_month", """
            SELECT month, count(*) AS rows, count(DISTINCT bid_id) AS bids, count(DISTINCT run_id) AS runs,
                   count(*) FILTER (WHERE scraped_at IS NOT NULL) AS scraped,
                   count(*) FILTER (WHERE bid_result_raw IS NOT NULL) AS with_bid_result,
                   count(*) FILTER (WHERE ra_result_raw IS NOT NULL) AS with_ra_result,
                   count(*) FILTER (WHERE scrape_error IS NOT NULL) AS scrape_errors,
                   count(*) FILTER (WHERE merged_at IS NOT NULL) AS merged
            FROM staging.tenders_raw GROUP BY month ORDER BY month""")
        q.run("staging_buyer_status", """
            SELECT month, buyer_status, count(DISTINCT bid_id) AS bids
            FROM staging.tenders_raw GROUP BY 1, 2 ORDER BY 1, 2""")
    if "public.pipeline2_status" in have:
        q.run("pipeline2_status", """
            SELECT month, status, total_tenders, unique_captured, passes_done, last_pass_new,
                   p3_status, p3_done, p3_total, completed_at
            FROM pipeline2_status ORDER BY month""")
    if "public.failed_pages" in have:
        q.run("failed_pages", """
            SELECT month, count(*) AS failures, count(DISTINCT page_no) AS distinct_pages
            FROM failed_pages GROUP BY month ORDER BY month""")
    if "public.failed_tenders" in have:
        q.run("failed_tenders", """
            SELECT month, pipeline, resolved, count(*) AS n
            FROM failed_tenders GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""")
    if "staging.bid_participations_raw" in have:
        q.run("staging_participations_by_month", f"""
            SELECT t.month, count(DISTINCT p.bid_id) AS bids_with_participants, count(*) AS participant_rows,
                   count(*) FILTER (WHERE p.seller_name ~ '{MASK_RE}') AS masked_rows,
                   count(DISTINCT p.bid_id) FILTER (WHERE p.seller_name ~ '{MASK_RE}') AS bids_with_masked
            FROM staging.bid_participations_raw p
            JOIN (SELECT DISTINCT bid_id, month FROM staging.tenders_raw) t USING (bid_id)
            GROUP BY t.month ORDER BY t.month""")
    if "public.tenders" in have:
        mcol = "month" if "month" in cols.get("public.tenders", set()) else None
        if mcol:
            q.run("tenders_by_month", """
                SELECT month, count(*) AS rows, count(DISTINCT bid_id) AS bids
                FROM tenders GROUP BY month ORDER BY month""")
    if "public.bid_participations" in have:
        q.run("participations_by_year", f"""
            SELECT participation_year, count(*) AS rows, count(DISTINCT bid_id) AS bids,
                   count(*) FILTER (WHERE seller_name ~ '{MASK_RE}') AS masked_rows,
                   count(DISTINCT bid_id) FILTER (WHERE seller_name ~ '{MASK_RE}') AS bids_with_masked
            FROM bid_participations GROUP BY 1 ORDER BY 1""")
        q.run("masked_seller_ids", f"""
            SELECT seller_id, seller_name, count(*) AS rows, count(DISTINCT bid_id) AS bids
            FROM bid_participations WHERE seller_name ~ '{MASK_RE}'
            GROUP BY 1, 2 ORDER BY rows DESC LIMIT 15""")
        if "public.tenders" in have:
            q.run("tenders_with_participants_by_month", """
                SELECT t.month, count(*) AS tenders,
                       count(*) FILTER (WHERE EXISTS (SELECT 1 FROM bid_participations p WHERE p.bid_id = t.bid_id))
                         AS with_participants
                FROM tenders t GROUP BY t.month ORDER BY t.month""")
    if "public.sellers" in have:
        q.run("masked_sellers_table", f"""
            SELECT seller_id, seller_name, total_tenders, total_wins
            FROM sellers WHERE seller_name ~ '{MASK_RE}' ORDER BY total_tenders DESC LIMIT 15""")

    # Stored bid sets we may need on the GeM side
    src = "public.tenders" if "public.tenders" in have else ("staging.tenders_raw" if "staging.tenders_raw" in have else None)
    months = []
    stored_sample = []
    if src:
        months = [r["month"] for r in (q.run("stored_months", f"SELECT DISTINCT month FROM {src} ORDER BY 1") or [])
                  if r["month"]]
        stored_sample = q.run("stored_sample", f"""
            SELECT bid_id, bid_number, month FROM {src} TABLESAMPLE SYSTEM (5) LIMIT 5000""")
        if not stored_sample:
            stored_sample = q.run("stored_sample_fallback", f"""
                SELECT bid_id, bid_number, month FROM {src} ORDER BY random() LIMIT 5000""") or []
    conn_info = {"source_table": src, "tables": tables}
    return q, conn_info, months, stored_sample, conn


async def gem_part(q, months, stored_sample, walk_month, sample_n, conn, src):
    out = {}
    async with GemPool(n_sessions=3, conc=8, log=log) as pool:
        # GeM counts for every stored month
        specs = {}
        for m in months:
            try:
                y, mm = int(m[:4]), int(m[5:7])
            except ValueError:
                continue
            frm, to = month_range(y, mm)
            specs[(m, "all_bids")] = dict(st="all_bids", bs="", frm=frm, to=to)
            specs[(m, "all_bids+bid_awarded")] = dict(st="all_bids", bs="bid_awarded", frm=frm, to=to)
            specs[(m, "all_bids+fin_evaluated")] = dict(st="all_bids", bs="fin_evaluated", frm=frm, to=to)
            specs[(m, "all_bids+tech_evaluated")] = dict(st="all_bids", bs="tech_evaluated", frm=frm, to=to)
        res = await pool.counts(specs) if specs else {}
        per_month = {}
        for (m, k), v in res.items():
            per_month.setdefault(m, {})[k] = v
        out["gem_counts_per_stored_month"] = per_month
        log(f"  GeM counts for {len(months)} stored months done")

        # Exact set difference for one month
        if walk_month and src:
            y, mm = int(walk_month[:4]), int(walk_month[5:7])
            frm, to = month_range(y, mm)
            n = await pool.count(st="all_bids", bs="bid_awarded", frm=frm, to=to)
            pages = list(range(1, (n + 9) // 10 + 1))
            gem_ids = {}
            # One pass is enough with an official sort (drift test: 0 duplicates / 0 missing in one pass).
            for ps in range(1):
                r = await pool.run([make_payload(page=p, st="all_bids", bs="bid_awarded", frm=frm, to=to) for p in pages])
                for x in r:
                    for d in (x.get("docs") or []):
                        gem_ids[first(d.get("b_id"))] = (first(d.get("b_bid_number")), first(d.get("b_status")))
                log(f"  walk {walk_month} pass {ps+1}: {len(gem_ids)}/{n} unique")
            stored = {r["bid_id"] for r in (q.run("walk_month_stored_ids",
                                                  f"SELECT DISTINCT bid_id FROM {src} WHERE month = %s", (walk_month,)) or [])}
            gem_set = set(gem_ids)
            missing = sorted(gem_set - stored)
            out["walk_month"] = {
                "month": walk_month, "gem_numFound": n, "gem_collected_1_pass_official_sort": len(gem_set),
                "stored": len(stored), "in_gem_not_stored": len(missing),
                "stored_not_in_gem_list": len(stored - gem_set),
                "in_gem_not_stored_sample": [gem_ids[b][0] for b in missing[:25]],
                "in_gem_not_stored_b_status": dict(Counter(str(gem_ids[b][1]) for b in missing)),
            }
            log(f"  walk {walk_month}: GeM={len(gem_set)} stored={len(stored)} missing={len(missing)}")

        # Stored bids looked up on GeM now
        rnd = random.Random(1)
        sample = rnd.sample(stored_sample, min(sample_n, len(stored_sample))) if stored_sample else []
        if sample:
            r = await pool.run([make_payload(page=1, st="all_bids", bs="", search=s["bid_number"], search_type="exact")
                                for s in sample])
            states = Counter()
            rows = []
            for s, x in zip(sample, r):
                docs = [d for d in (x.get("docs") or []) if first(d.get("b_id")) == s["bid_id"]] if x and x.get("ok") else []
                if not docs:
                    states["not_found_by_exact_search"] += 1
                    continue
                d = docs[0]
                key = f"b_status={first(d.get('b_status'))}|buyer_status={first(d.get('b_buyer_status'))}"
                states[key] += 1
                rows.append({"bid_number": s["bid_number"], "month": s["month"], "b_status": first(d.get("b_status")),
                             "buyer_status": first(d.get("b_buyer_status"))})
            out["stored_sample_now_on_gem"] = {"n": len(sample), "states": dict(states),
                                               "cancelled_now": sum(1 for x in rows if x["b_status"] in (3, 5)),
                                               "rows": rows[:50]}
            log(f"  stored sample status now: {dict(states)}")
        out["pool_stats"] = pool.stats
    return out


def main(walk_month, sample_n):
    t0 = time.time()
    q, info, months, stored_sample, conn = db_part()
    gem = asyncio.run(gem_part(q, months, stored_sample, walk_month, sample_n, conn, info["source_table"]))
    conn.close()
    out = {"generated_at": now_iso(), "read_only": True, **info, "stored_months": months,
           "queries": q.log, "gem": gem, "elapsed_s": round(time.time() - t0, 1)}
    path = os.path.join(DATA_DIR, "db_comparison.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=1, ensure_ascii=False, default=str)
    log(f"done in {out['elapsed_s']}s -> {path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--walk-month", default=None, help="YYYY-MM: exact stored-vs-GeM set difference for this month")
    ap.add_argument("--sample", type=int, default=300, help="stored bids to look up on GeM now")
    a = ap.parse_args()
    main(a.walk_month, a.sample)
