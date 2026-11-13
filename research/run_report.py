"""
Step 5 — Estimates + report tables.

Reads counts_<year>.json and participant_samples_<year>.jsonl, computes per-cell participant rates
(Wilson 95% CI), projects them onto the cell counts, and rolls up into:
  (a) fetched by the current pipeline filter (all_bids + bid_awarded)
  (b) has participant data but missed (clean / partly masked / fully masked)
  (c) no participant data
by status group, by byType and by month.

Usage:  venv/Scripts/python.exe -X utf8 research/run_report.py [--year 2025]
Output: research/data/estimates_<year>.json, research/data/report_tables_<year>.md
"""
import argparse
import json
import math
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from research.gem_probe import DATA_DIR

GROUPS = ["awarded_live", "awarded_cancelled", "fin_live", "fin_cancelled",
          "tech_live", "tech_cancelled", "noteval_live", "cancelled_stage0"]
LABEL = {
    "awarded_live": "Awarded", "awarded_cancelled": "Awarded → cancelled",
    "fin_live": "Financially evaluated", "fin_cancelled": "Fin. evaluated → cancelled",
    "tech_live": "Technically evaluated", "tech_cancelled": "Tech. evaluated → cancelled",
    "noteval_live": "Closed, not evaluated", "cancelled_stage0": "Cancelled before evaluation",
}
FETCHED = {"awarded_live", "awarded_cancelled"}     # = all_bids + bid_awarded (current pipeline2 filter)
BY_TYPES = ["product", "service", "bidToRA", "custom", "boq", "rcbid", "gt", "lt", "st"]


def wilson(k, n, z=1.96):
    if n == 0:
        return (None, None, None)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (p, max(0.0, c - h), min(1.0, c + h))


def fmt_n(x):
    return "–" if x is None else f"{round(x):,}"


def fmt_p(x):
    return "–" if x is None else f"{100*x:.0f}%"


def load(year):
    counts = json.load(open(os.path.join(DATA_DIR, f"counts_{year}.json"), encoding="utf-8"))
    rows = []
    p = os.path.join(DATA_DIR, f"participant_samples_{year}.jsonl")
    with open(p, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    return counts, rows


def cell_stats(rows):
    ok = [r for r in rows if r["result"].get("ok")]
    n = len(ok)
    k = sum(1 for r in ok if r["result"]["has_participants"])
    full = sum(1 for r in ok if r["result"]["has_participants"] and r["result"]["all_masked"])
    part = sum(1 for r in ok if r["result"]["has_participants"] and r["result"]["any_masked"] and not r["result"]["all_masked"])
    return {"n": n, "errors": len(rows) - n, "k": k, "full_masked": full, "part_masked": part,
            "rate": wilson(k, n), "full_rate": wilson(full, n), "part_rate": wilson(part, n)}


def project(cell_n, st):
    p, lo, hi = st["rate"]
    if p is None:
        return None
    n = st["n"]
    return {"with": cell_n * p, "with_lo": cell_n * lo, "with_hi": cell_n * hi,
            "clean": cell_n * (st["k"] - st["full_masked"] - st["part_masked"]) / n,
            "part_masked": cell_n * st["part_masked"] / n,
            "full_masked": cell_n * st["full_masked"] / n,
            "without": cell_n * (1 - p)}


def main(year):
    counts, rows = load(year)
    by_cell = defaultdict(list)
    for r in rows:
        by_cell[r["cell"]].append(r)

    est = {"year": year, "cells": {}, "groups": {}, "types": {}, "months": {}, "extra": {}}
    # Per cell
    for g in GROUPS:
        for t in ["all"] + BY_TYPES:
            cn = counts["by_type"][t]["groups"][g]
            st = cell_stats(by_cell.get(f"{g}|{t}", []))
            est["cells"][f"{g}|{t}"] = {"count": cn, **{k: v for k, v in st.items()}, "proj": project(cn, st) if cn else None}

    # Group roll-up (byType = all)
    tot = defaultdict(float)
    for g in GROUPS:
        c = est["cells"][f"{g}|all"]
        pr = c["proj"] or {"with": 0, "with_lo": 0, "with_hi": 0, "clean": 0, "part_masked": 0, "full_masked": 0, "without": 0}
        bucket = "fetched" if g in FETCHED else "missed"
        est["groups"][g] = {"count": c["count"], "bucket": bucket, **pr}
        tot[f"{bucket}_count"] += c["count"]
        for k in ("with", "with_lo", "with_hi", "clean", "part_masked", "full_masked", "without"):
            tot[f"{bucket}_{k}"] += pr[k]
    est["totals"] = dict(tot)
    est["totals"]["all_bids"] = counts["by_type"]["all"]["raw"]["all"]

    # Per type roll-up (each type uses its own cells)
    for t in BY_TYPES:
        d = defaultdict(float)
        for g in GROUPS:
            c = est["cells"][f"{g}|{t}"]
            b = "fetched" if g in FETCHED else "missed"
            d[f"{b}_count"] += c["count"] or 0
            if c["proj"]:
                for k in ("with", "with_lo", "with_hi", "full_masked", "part_masked", "without"):
                    d[f"{b}_{k}"] += c["proj"][k]
            elif c["count"]:
                d["unsampled_count"] += c["count"]
        d["all_bids"] = counts["by_type"][t]["raw"]["all"]
        est["types"][t] = dict(d)

    # Per month: apply group-level (year) rates to monthly group counts
    for m, md in sorted(counts["by_month"].items()):
        d = defaultdict(float)
        for g in GROUPS:
            gc = md["groups"][g]
            c = est["cells"][f"{g}|all"]
            p = c["rate"][0] or 0
            b = "fetched" if g in FETCHED else "missed"
            d[f"{b}_count"] += gc
            d[f"{b}_with"] += gc * p
            d[f"{b}_without"] += gc * (1 - p)
        d["all_bids"] = md["raw"]["all"]
        est["months"][m] = dict(d)

    # Extra breakdowns from samples (deduplicated by bid within group)
    def breakdown(keyfn):
        out = {}
        for g in GROUPS:
            seen = {}
            for r in rows:
                if r["group"] == g and r["result"].get("ok"):
                    seen[r["bid_id"]] = r
            dd = defaultdict(list)
            for r in seen.values():
                dd[keyfn(r)].append(r)
            out[g] = {k: {"n": len(v), "k": sum(1 for r in v if r["result"]["has_participants"]),
                          "rate": wilson(sum(1 for r in v if r["result"]["has_participants"]), len(v))[0],
                          "full_masked": sum(1 for r in v if r["result"]["all_masked"])}
                      for k, v in sorted(dd.items(), key=lambda kv: str(kv[0]))}
        return out
    est["extra"]["by_quarter"] = breakdown(lambda r: f"Q{(int(r['month'][5:7]) - 1)//3 + 1}" if r.get("month") else "?")
    est["extra"]["ra_vs_bid"] = breakdown(lambda r: "RA (/R/)" if r["is_ra_number"] else "Bid (/B/)")
    est["extra"]["single_packet"] = breakdown(lambda r: f"single_packet={r['single_packet']}")
    est["extra"]["sample_sizes"] = {"rows": len(rows), "unique_bids": len({r['bid_id'] for r in rows}),
                                    "errors": sum(1 for r in rows if not r['result'].get('ok'))}

    # ── Duplicate adjustment (RA ↔ parent) ──────────────────────────────────
    # RA bids (bidToRA) show their PARENT's participant table; an awarded RA child already carries the
    # parent's participants into the pipeline. Split "missed with participants" into unique vs duplicate.
    child = {}
    cpath = os.path.join(DATA_DIR, f"ra_child_check_{year}.json")
    if os.path.exists(cpath):
        for x in json.load(open(cpath, encoding="utf-8")):
            if x.get("has_participants"):
                d = child.setdefault(x["group"], {"n": 0, "awarded_child": 0})
                d["n"] += 1
                d["awarded_child"] += x["state"].startswith("RA child AWARDED") or x["state"].startswith("RA child awarded")
    rb = est["extra"]["ra_vs_bid"]
    adj = {}
    for g in GROUPS:
        if g in FETCHED:
            continue
        n_all = counts["by_type"]["all"]["groups"][g]
        n_ra = counts["by_type"]["bidToRA"]["groups"][g]
        n_b = n_all - n_ra
        r_ra = (rb[g].get("RA (/R/)") or {}).get("rate") or 0
        r_b = (rb[g].get("Bid (/B/)") or {}).get("rate") or 0
        with_ra, with_b = n_ra * r_ra, n_b * r_b
        dup_parent = with_ra if g in ("noteval_live", "cancelled_stage0") else 0.0
        frac_child = (child[g]["awarded_child"] / child[g]["n"]) if g in child and child[g]["n"] else 0.0
        dup_child = with_b * frac_child
        adj[g] = {"n_all": n_all, "n_ra": n_ra, "n_b": n_b, "rate_ra": r_ra, "rate_b": r_b,
                  "with_total": with_ra + with_b, "dup_parent_page": dup_parent,
                  "dup_awarded_ra_child": dup_child, "frac_awarded_ra_child": frac_child,
                  "child_sample_n": (child.get(g) or {}).get("n", 0),
                  "unique_new": with_ra + with_b - dup_parent - dup_child}
    est["adjusted"] = {"groups": adj,
                       "unique_new_total": sum(a["unique_new"] for a in adj.values()),
                       "dup_parent_total": sum(a["dup_parent_page"] for a in adj.values()),
                       "dup_child_total": sum(a["dup_awarded_ra_child"] for a in adj.values()),
                       "with_total": sum(a["with_total"] for a in adj.values())}

    with open(os.path.join(DATA_DIR, f"estimates_{year}.json"), "w", encoding="utf-8") as f:
        json.dump(est, f, indent=1)

    # ── Markdown tables ─────────────────────────────────────────────────────
    L = []
    T = est["totals"]
    L.append(f"## Coverage summary {year}\n")
    L.append("| Bucket | Bids | % of all |")
    L.append("|---|---:|---:|")
    allb = T["all_bids"]
    L.append(f"| Fetched today (all_bids + bid_awarded) | {fmt_n(T['fetched_count'])} | {fmt_p(T['fetched_count']/allb)} |")
    L.append(f"| ↳ of which have participant data | {fmt_n(T['fetched_with'])} | |")
    L.append(f"| **Missed, has participant data** | **{fmt_n(T['missed_with'])}** ({fmt_n(T['missed_with_lo'])}–{fmt_n(T['missed_with_hi'])}) | {fmt_p(T['missed_with']/allb)} |")
    L.append(f"| ↳ clean names | {fmt_n(T['missed_clean'])} | |")
    L.append(f"| ↳ partly masked names | {fmt_n(T['missed_part_masked'])} | |")
    L.append(f"| ↳ all names masked | {fmt_n(T['missed_full_masked'])} | |")
    L.append(f"| Missed, no participant data | {fmt_n(T['missed_without'])} | {fmt_p(T['missed_without']/allb)} |")
    L.append(f"| Fetched but no participant data | {fmt_n(T['fetched_without'])} | |")
    L.append(f"| **All bids** | **{fmt_n(allb)}** | 100% |\n")

    A = est["adjusted"]
    L.append(f"## Missed participant data: unique vs duplicate ({year})\n")
    L.append("RA bids show their parent's participant table; an awarded RA child already carries its parent's "
             "participants into the pipeline. Rates split by RA (/R/) vs normal (/B/) bids.\n")
    L.append("| Status group | Bids (normal / RA) | Rate normal / RA | Est. with participants | Dup: parent page (RA) | Dup: awarded RA child | **Unique new** |")
    L.append("|---|---|---|---:|---:|---:|---:|")
    for g, a in A["groups"].items():
        L.append(f"| {LABEL[g]} | {fmt_n(a['n_b'])} / {fmt_n(a['n_ra'])} | {fmt_p(a['rate_b'])} / {fmt_p(a['rate_ra'])} | "
                 f"{fmt_n(a['with_total'])} | {fmt_n(a['dup_parent_page'])} | {fmt_n(a['dup_awarded_ra_child'])} "
                 f"({fmt_p(a['frac_awarded_ra_child'])} of n={a['child_sample_n']}) | **{fmt_n(a['unique_new'])}** |")
    L.append(f"| **Total** | | | {fmt_n(A['with_total'])} | {fmt_n(A['dup_parent_total'])} | {fmt_n(A['dup_child_total'])} | **{fmt_n(A['unique_new_total'])}** |\n")

    L.append(f"## By status group ({year}, byType=all)\n")
    L.append("| Status group | Bids | Sampled | Has participants (95% CI) | All masked | Partly masked | Est. bids with participants | Est. without |")
    L.append("|---|---:|---:|---|---:|---:|---:|---:|")
    for g in GROUPS:
        c = est["cells"][f"{g}|all"]
        p, lo, hi = c["rate"]
        pr = c["proj"] or {}
        L.append(f"| {LABEL[g]}{' *(fetched)*' if g in FETCHED else ''} | {fmt_n(c['count'])} | {c['n']} | "
                 f"{fmt_p(p)} ({fmt_p(lo)}–{fmt_p(hi)}) | {fmt_p(c['full_rate'][0])} | {fmt_p(c['part_rate'][0])} | "
                 f"{fmt_n(pr.get('with'))} | {fmt_n(pr.get('without'))} |")
    L.append("")

    L.append(f"## Participant rate by status group × byType ({year})\n")
    L.append("Cell = has-participants rate (sample size). Blank = no bids in cell.\n")
    L.append("| Status group | " + " | ".join(BY_TYPES) + " |")
    L.append("|---|" + "---|" * len(BY_TYPES))
    for g in GROUPS:
        cells = []
        for t in BY_TYPES:
            c = est["cells"][f"{g}|{t}"]
            cells.append("" if not c["count"] else (f"{fmt_p(c['rate'][0])} ({c['n']})" if c["n"] else f"n/s ({fmt_n(c['count'])})"))
        L.append(f"| {LABEL[g]} | " + " | ".join(cells) + " |")
    L.append("")

    L.append(f"## Bid counts by status group × byType ({year})\n")
    L.append("| Status group | " + " | ".join(BY_TYPES) + " | all |")
    L.append("|---|" + "---:|" * (len(BY_TYPES) + 1))
    for g in GROUPS:
        L.append(f"| {LABEL[g]} | " + " | ".join(fmt_n(counts['by_type'][t]['groups'][g]) for t in BY_TYPES)
                 + f" | {fmt_n(counts['by_type']['all']['groups'][g])} |")
    L.append("| **All bids** | " + " | ".join(fmt_n(counts['by_type'][t]['raw']['all']) for t in BY_TYPES)
             + f" | {fmt_n(counts['by_type']['all']['raw']['all'])} |\n")

    L.append(f"## Coverage by byType ({year})\n")
    L.append("| byType | All bids | Fetched today | Missed, has participants (est.) | Missed, no participants (est.) |")
    L.append("|---|---:|---:|---:|---:|")
    for t in BY_TYPES:
        d = est["types"][t]
        L.append(f"| {t} | {fmt_n(d['all_bids'])} | {fmt_n(d.get('fetched_count'))} | {fmt_n(d.get('missed_with'))} | {fmt_n(d.get('missed_without'))} |")
    L.append("")

    L.append(f"## Coverage by month ({year}; year-level rates applied to monthly counts)\n")
    L.append("| Month | All bids | Fetched today | Missed, has participants (est.) | Missed, no participants (est.) |")
    L.append("|---|---:|---:|---:|---:|")
    for m, d in est["months"].items():
        L.append(f"| {m} | {fmt_n(d['all_bids'])} | {fmt_n(d['fetched_count'])} | {fmt_n(d['missed_with'])} | {fmt_n(d['missed_without'])} |")
    L.append("")

    L.append(f"## Monthly counts by status group ({year})\n")
    L.append("| Month | " + " | ".join(LABEL[g] for g in GROUPS) + " | All |")
    L.append("|---|" + "---:|" * (len(GROUPS) + 1))
    for m, md in sorted(counts["by_month"].items()):
        L.append(f"| {m} | " + " | ".join(fmt_n(md['groups'][g]) for g in GROUPS) + f" | {fmt_n(md['raw']['all'])} |")
    L.append("")

    for name, title in (("by_quarter", "by quarter of end date"), ("ra_vs_bid", "RA vs normal bid"),
                        ("single_packet", "single vs double packet")):
        L.append(f"## Participant rate {title} (from samples)\n")
        keys = sorted({k for g in GROUPS for k in est["extra"][name][g]})
        L.append("| Status group | " + " | ".join(keys) + " |")
        L.append("|---|" + "---|" * len(keys))
        for g in GROUPS:
            e = est["extra"][name][g]
            L.append(f"| {LABEL[g]} | " + " | ".join(
                (f"{fmt_p(e[k]['rate'])} ({e[k]['n']})" if k in e else "") for k in keys) + " |")
        L.append("")

    path = os.path.join(DATA_DIR, f"report_tables_{year}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    print("\n".join(L[:14]))
    print(f"-> {path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=2025)
    main(ap.parse_args().year)
