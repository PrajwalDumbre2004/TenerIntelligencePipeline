"""
Step 2 — API parameter audit for /all-bids-data.

Tests every filter parameter (accepted values, behaviour on unknown values, partition vs overlap),
date-filter semantics, sort stability / pagination drift, page cap, page size and search.

Usage:  venv/Scripts/python.exe -X utf8 research/run_param_audit.py [--year 2025]
Output: research/data/param_audit.json, research/data/drift_test.json
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
from research.gem_probe.client import GemPool, make_payload, year_range, gem_date, first, now_iso

STATUS_TYPES = ["ongoing_bids", "bidrastatus", "cancelled_bids", "all_bids", "bogus_value"]
BY_STATUS = ["", "tech_evaluated", "fin_evaluated", "bid_awarded", "bogus_value"]
BY_TYPES = ["all", "product", "service", "bidToRA", "custom", "boq", "rcbid", "gt", "lt", "st", "bogus_value"]
SORTS = ["Bid-End-Date-Oldest", "Bid-End-Date-Latest", "Bid-Start-Date-Oldest", "Bid-Start-Date-Latest",
         "bid_id", "bogus_value"]
TYPE_FIELDS = ["b_bid_type", "b_is_custom_item", "bd_details_is_boq", "is_rc_bid", "ba_is_global_tendering",
               "ba_is_limited_tendering", "ba_is_single_packet", "b_eval_type", "b_type", "is_high_value",
               "b_bid_to_ra", "b_ra_to_bid", "b_is_bunch"]


def log(*a):
    print(*a, flush=True)


def docs_of(results):
    return [d for r in results if r and r.get("ok") for d in r.get("docs", [])]


async def audit(year: int):
    frm, to = year_range(year)
    out = {"year": year, "generated_at": now_iso(), "range": [frm, to]}
    async with GemPool(n_sessions=3, conc=8, log=log) as pool:
        t0 = time.time()

        # ── A. bidStatusType × byStatus (incl. unknown values) ──────────────
        specs = {f"{st}|{bs}": dict(st=st, bs=bs, frm=frm, to=to) for st in STATUS_TYPES for bs in BY_STATUS}
        specs["(byStatus key omitted)|all_bids"] = dict(st="all_bids", bs=None, frm=frm, to=to)
        out["status_matrix"] = await pool.counts(specs)
        log(f"A status matrix done ({time.time()-t0:.0f}s)")

        # ── B. byType values, and their sum vs 'all' ─────────────────────────
        out["by_type"] = await pool.counts({t: dict(st="all_bids", bs="", by_type=t, frm=frm, to=to) for t in BY_TYPES})
        known = [t for t in BY_TYPES if t not in ("all", "bogus_value")]
        out["by_type_sum_of_specific"] = sum(out["by_type"][t] or 0 for t in known)
        # Field fingerprint of each type: sample random pages, tabulate characteristic fields.
        fp = {}
        for t in ["all"] + known:
            n = out["by_type"].get(t) or 0
            if not n:
                continue
            pages = random.Random(t).sample(range(1, (n + 9) // 10 + 1), min(15, (n + 9) // 10))
            res = await pool.run([make_payload(page=p, st="all_bids", bs="", by_type=t, frm=frm, to=to) for p in pages])
            ds = docs_of(res)
            fp[t] = {"n_docs": len(ds),
                     **{f: dict(Counter(str(first(d.get(f))) for d in ds)) for f in TYPE_FIELDS},
                     "bid_number_kind": dict(Counter(("RA" if "/R/" in str(first(d.get("b_bid_number"))) else "B") for d in ds))}
        out["by_type_fingerprint"] = fp
        log(f"B byType done ({time.time()-t0:.0f}s)")

        # ── C. highBidValue ──────────────────────────────────────────────────
        out["high_bid_value"] = await pool.counts({h or "(empty)": dict(st="all_bids", bs="", high=h, frm=frm, to=to)
                                                    for h in ["", "highValue", "bogus_value"]})

        # ── D. byEndDate semantics ───────────────────────────────────────────
        d1, d2 = gem_date(year, 1, 15), gem_date(year, 1, 16)
        date_specs = {
            "year (dd-Mon-yyyy)": (frm, to),
            "ISO yyyy-mm-dd": (f"{year}-01-01", f"{year}-12-31"),
            "dd/mm/yyyy": (f"01/01/{year}", f"31/12/{year}"),
            "from only": (frm, ""),
            "to only": ("", to),
            "no dates": ("", ""),
            f"single day {d1}": (d1, d1),
            f"single day {d2}": (d2, d2),
            f"two days {d1}..{d2}": (d1, d2),
            "reversed range": (to, frm),
        }
        out["end_date_filter"] = await pool.counts({k: dict(st="all_bids", bs="", frm=a, to=b) for k, (a, b) in date_specs.items()})
        # Which date field does byEndDate filter on? Look at docs from a single-day query.
        res = await pool.run([make_payload(page=p, st="all_bids", bs="", frm=d1, to=d1) for p in (1, 2, 3)])
        ds = docs_of(res)
        out["end_date_field_check"] = {
            "query_day": d1,
            "final_end_date_sort": dict(Counter(str(first(d.get("final_end_date_sort")))[:10] for d in ds)),
            "final_start_date_sort": dict(Counter(str(first(d.get("final_start_date_sort")))[:10] for d in ds)),
        }
        # Unknown filter keys (are they ignored?)
        out["unknown_filter_keys"] = await pool.counts({
            "baseline": dict(st="all_bids", bs="", frm=frm, to=to),
            "byStartDate=Jan only": dict(st="all_bids", bs="", frm=frm, to=to,
                                         extra_filter={"byStartDate": {"from": gem_date(year, 1, 1), "to": gem_date(year, 1, 31)}}),
            "byMinistry=bogus": dict(st="all_bids", bs="", frm=frm, to=to, extra_filter={"byMinistry": "bogus"}),
        })
        log(f"C/D high-value + dates done ({time.time()-t0:.0f}s)")

        # ── E. Page size / page cap ─────────────────────────────────────────
        total = out["status_matrix"]["all_bids|"]
        last_page = (total + 9) // 10
        probe_pages = [0, 1, 2, 1000, 5000, 10000, 20000, 50000, 100000, last_page, last_page + 1, last_page + 100]
        res = await pool.run([make_payload(page=p, st="all_bids", bs="", frm=frm, to=to) for p in probe_pages])
        out["page_cap"] = {"numFound": total, "last_page": last_page,
                           "pages": {str(p): {"ok": r.get("ok"), "ndocs": len(r.get("docs", [])) if r.get("ok") else None,
                                              "error": r.get("error"),
                                              "first_bid": first(r["docs"][0].get("b_bid_number")) if r.get("ok") and r.get("docs") else None}
                                     for p, r in zip(probe_pages, res)}}
        res = await pool.run([make_payload(page=1, st="all_bids", bs="", frm=frm, to=to, extra_top={"rows": 100}),
                              make_payload(page=1, st="all_bids", bs="", frm=frm, to=to, extra_filter={"pageSize": 100, "rows": 100})])
        out["page_size"] = {"default": out["page_cap"]["pages"]["1"]["ndocs"],
                            "top-level rows=100": len(res[0].get("docs", [])) if res[0].get("ok") else res[0].get("error"),
                            "filter pageSize/rows=100": len(res[1].get("docs", [])) if res[1].get("ok") else res[1].get("error")}
        log(f"E page cap/size done ({time.time()-t0:.0f}s)")

        # ── F. Sort: order + repeatability ───────────────────────────────────
        w_from, w_to = gem_date(year, 7, 15), gem_date(year, 7, 17)          # 3-day window
        sort_info = {}
        for s in SORTS:
            res = await pool.run([make_payload(page=p, st="all_bids", bs="bid_awarded", frm=w_from, to=w_to, sort=s)
                                  for p in (1, 1, 30, 30)])
            p1a, p1b, p30a, p30b = [[first(d.get("b_id")) for d in r.get("docs", [])] for r in res]
            first_docs = res[0].get("docs", [])
            sort_info[s] = {
                "numFound": res[0].get("numFound"),
                "page1_bid_ids": p1a,
                "page1_end_dates": [str(first(d.get("final_end_date_sort")))[:19] for d in first_docs],
                "page1_start_dates": [str(first(d.get("final_start_date_sort")))[:19] for d in first_docs],
                "page1_repeat_identical": p1a == p1b,
                "page30_repeat_identical": p30a == p30b,
            }
        out["sort"] = sort_info
        log(f"F sort checks done ({time.time()-t0:.0f}s)")

        # ── G. Drift test: walk the 3-day window fully with each sort, twice ──
        drift = {}
        for s in ["bid_id", "Bid-End-Date-Oldest", "Bid-Start-Date-Oldest"]:
            n = sort_info[s]["numFound"] or 0
            pages = list(range(1, (n + 9) // 10 + 1))
            passes = []
            seen_all = set()
            for ps in range(2):
                res = await pool.run([make_payload(page=p, st="all_bids", bs="bid_awarded", frm=w_from, to=w_to, sort=s)
                                      for p in pages])
                ids = [first(d.get("b_id")) for d in docs_of(res)]
                failed = sum(1 for r in res if not r.get("ok"))
                seen_all |= set(ids)
                passes.append({"rows": len(ids), "unique": len(set(ids)), "duplicates": len(ids) - len(set(ids)),
                               "failed_pages": failed, "cumulative_unique": len(seen_all)})
            drift[s] = {"numFound": n, "pages": len(pages), "passes": passes,
                        "missing_after_2_passes": n - len(seen_all), "ids": sorted(seen_all)}
            log(f"  drift {s}: numFound={n} passes={[(p['unique'], p['duplicates']) for p in passes]} cum={len(seen_all)}")
        with open(os.path.join(DATA_DIR, "drift_test.json"), "w", encoding="utf-8") as f:
            json.dump({"window": [w_from, w_to], "filter": "all_bids + bid_awarded", "results": drift}, f)
        out["drift"] = {"window": [w_from, w_to], "filter": "all_bids + bid_awarded",
                        "results": {s: {k: v for k, v in d.items() if k != "ids"} for s, d in drift.items()}}
        log(f"G drift test done ({time.time()-t0:.0f}s)")

        # ── H. Search (exact / fullText by bid number) ──────────────────────
        sample = docs_of(await pool.run([make_payload(page=7, st="all_bids", bs="", frm=frm, to=to)]))[:3]
        srch = {}
        for d in sample:
            bn = first(d.get("b_bid_number"))
            res = await pool.run([make_payload(page=1, st="all_bids", bs="", search=bn, search_type="exact"),
                                  make_payload(page=1, st="all_bids", bs="", search=bn, search_type="fullText"),
                                  make_payload(page=1, st="bidrastatus", bs="", search=bn, search_type="exact")])
            srch[bn] = {k: {"numFound": r.get("numFound"),
                            "top": first(r["docs"][0].get("b_bid_number")) if r.get("ok") and r.get("docs") else None}
                        for k, r in zip(["exact|all_bids", "fullText|all_bids", "exact|bidrastatus"], res)}
        out["search"] = srch
        out["pool_stats"] = pool.stats
        out["elapsed_s"] = round(time.time() - t0, 1)
        log(f"H search done ({time.time()-t0:.0f}s)  stats={pool.stats}")

    with open(os.path.join(DATA_DIR, "param_audit.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, indent=1, ensure_ascii=False)
    log("-> research/data/param_audit.json")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=2025)
    asyncio.run(audit(ap.parse_args().year))
