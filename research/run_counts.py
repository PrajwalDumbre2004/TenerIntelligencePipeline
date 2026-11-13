"""
Step 3 — Count matrix for one year (by bid END date): status group × byType, and status group × month.

Status groups (mutually exclusive, together = all bids):
  awarded_live        bidrastatus    + bid_awarded
  awarded_cancelled   cancelled_bids + bid_awarded
  fin_live            bidrastatus    + fin_evaluated
  fin_cancelled       cancelled_bids + fin_evaluated
  tech_live           bidrastatus    + tech_evaluated
  tech_cancelled      cancelled_bids + tech_evaluated
  noteval_live        bidrastatus (no status)    − the three live stages   (buyer_status 0)
  cancelled_stage0    cancelled_bids (no status) − the three cancelled stages
Also records the all_bids+stage counts (what pipeline2-style filters return) and ongoing_bids.

Usage:  venv/Scripts/python.exe -X utf8 research/run_counts.py [--year 2025]
Output: research/data/counts_<year>.json
"""
import argparse
import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from research.gem_probe import DATA_DIR
from research.gem_probe.client import GemPool, year_range, month_range, now_iso

BY_TYPES = ["all", "product", "service", "bidToRA", "custom", "boq", "rcbid", "gt", "lt", "st"]
RAW = {  # key -> (bidStatusType, byStatus)
    "all":           ("all_bids", ""),
    "all_awarded":   ("all_bids", "bid_awarded"),
    "all_fin":       ("all_bids", "fin_evaluated"),
    "all_tech":      ("all_bids", "tech_evaluated"),
    "live":          ("bidrastatus", ""),
    "live_awarded":  ("bidrastatus", "bid_awarded"),
    "live_fin":      ("bidrastatus", "fin_evaluated"),
    "live_tech":     ("bidrastatus", "tech_evaluated"),
    "canc":          ("cancelled_bids", ""),
    "canc_awarded":  ("cancelled_bids", "bid_awarded"),
    "canc_fin":      ("cancelled_bids", "fin_evaluated"),
    "canc_tech":     ("cancelled_bids", "tech_evaluated"),
    "ongoing":       ("ongoing_bids", ""),
}
GROUPS = ["awarded_live", "awarded_cancelled", "fin_live", "fin_cancelled",
          "tech_live", "tech_cancelled", "noteval_live", "cancelled_stage0"]


def groups_from_raw(r: dict) -> dict:
    g = {
        "awarded_live": r["live_awarded"], "awarded_cancelled": r["canc_awarded"],
        "fin_live": r["live_fin"], "fin_cancelled": r["canc_fin"],
        "tech_live": r["live_tech"], "tech_cancelled": r["canc_tech"],
        "noteval_live": r["live"] - r["live_awarded"] - r["live_fin"] - r["live_tech"],
        "cancelled_stage0": r["canc"] - r["canc_awarded"] - r["canc_fin"] - r["canc_tech"],
    }
    g["_sum_groups"] = sum(g[k] for k in GROUPS)
    g["_all_bids"] = r["all"]
    g["_ongoing"] = r["ongoing"]
    g["_reconcile_diff"] = r["all"] - g["_sum_groups"] - r["ongoing"]
    return g


async def main(year: int):
    frm, to = year_range(year)
    out = {"year": year, "generated_at": now_iso(), "groups": GROUPS, "by_type": {}, "by_month": {}}
    async with GemPool(n_sessions=3, conc=8) as pool:
        t0 = time.time()
        specs = {}
        for t in BY_TYPES:
            for k, (st, bs) in RAW.items():
                specs[("type", t, k)] = dict(st=st, bs=bs, by_type=t, frm=frm, to=to)
        for m in range(1, 13):
            mf, mt = month_range(year, m)
            for k, (st, bs) in RAW.items():
                specs[("month", f"{year}-{m:02d}", k)] = dict(st=st, bs=bs, frm=mf, to=mt)
        res = await pool.counts(specs)
        missing = [k for k, v in res.items() if v is None]
        if missing:
            print("retrying", len(missing), "failed counts", flush=True)
            res.update(await pool.counts({k: specs[k] for k in missing}))
        for (dim, key, rk), v in res.items():
            out["by_type" if dim == "type" else "by_month"].setdefault(key, {"raw": {}})["raw"][rk] = v
        for dim in ("by_type", "by_month"):
            for key, d in out[dim].items():
                if all(v is not None for v in d["raw"].values()):
                    d["groups"] = groups_from_raw(d["raw"])
        out["pool_stats"] = pool.stats
        out["elapsed_s"] = round(time.time() - t0, 1)

    # Reconciliation summaries
    yr = out["by_type"]["all"]["groups"]
    out["checks"] = {
        "year_groups_sum_equals_all_bids": yr["_reconcile_diff"] == 0,
        "months_sum_all_bids": sum(d["raw"]["all"] for d in out["by_month"].values()),
        "year_all_bids": out["by_type"]["all"]["raw"]["all"],
        "months_with_reconcile_diff": {k: d["groups"]["_reconcile_diff"] for k, d in out["by_month"].items()
                                       if d.get("groups") and d["groups"]["_reconcile_diff"] != 0},
        "types_sum_all_bids": sum(out["by_type"][t]["raw"]["all"] for t in BY_TYPES if t != "all"),
    }
    path = os.path.join(DATA_DIR, f"counts_{year}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=1)
    print(json.dumps(out["checks"], indent=1))
    print(f"{len(specs)} counts in {out['elapsed_s']}s -> {path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=2025)
    asyncio.run(main(ap.parse_args().year))
