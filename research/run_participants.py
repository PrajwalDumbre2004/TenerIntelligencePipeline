"""
Step 4 — Participant availability by stratified random sampling.

For every (status group × byType) cell with bids: draw random bids spread across the whole year,
run pipeline3's parser on each, and record whether participant data exists (and whether names are masked).

Early stop: a cell first gets MIN bids; only cells whose result is mixed (some with, some without
participants) are topped up to MAX. byType='all' cells use larger MIN/MAX (they drive the totals).

Resumable: results are appended to research/data/participant_samples_<year>.jsonl; candidates are
saved to research/data/participant_candidates_<year>.json and reused on re-run.

Usage:  venv/Scripts/python.exe -X utf8 research/run_participants.py [--year 2025] [--sessions 3] [--conc 6]
Requires: research/data/counts_<year>.json (run_counts.py)
"""
import argparse
import asyncio
import json
import os
import random
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from research.gem_probe import DATA_DIR
from research.gem_probe.client import GemPool, make_payload, year_range, first
from research.gem_probe.participants import doc_to_item, check_many

# group -> (parent raw key in counts file, bidStatusType, byStatus, required buyer_status, cancelled?)
GROUPS = {
    "awarded_live":      ("live_awarded", "bidrastatus",    "bid_awarded",    3, False),
    "awarded_cancelled": ("canc_awarded", "cancelled_bids", "bid_awarded",    3, True),
    "fin_live":          ("live_fin",     "bidrastatus",    "fin_evaluated",  2, False),
    "fin_cancelled":     ("canc_fin",     "cancelled_bids", "fin_evaluated",  2, True),
    "tech_live":         ("live_tech",    "bidrastatus",    "tech_evaluated", 1, False),
    "tech_cancelled":    ("canc_tech",    "cancelled_bids", "tech_evaluated", 1, True),
    "noteval_live":      ("live",         "bidrastatus",    "",               0, False),
    "cancelled_stage0":  ("canc",         "cancelled_bids", "",               0, True),
}
BY_TYPES = ["all", "product", "service", "bidToRA", "custom", "boq", "rcbid", "gt", "lt", "st"]
CANCELLED_B_STATUS = {3, 5}


def log(*a):
    print(*a, flush=True)


def limits(by_type):
    return (30, 60) if by_type == "all" else (15, 30)


def matches(doc, req_bs, cancelled):
    bs = first(doc.get("b_buyer_status"))
    st = first(doc.get("b_status"))
    return bs == req_bs and ((st in CANCELLED_B_STATUS) == cancelled)


async def draw_candidates(pool, year, counts, seed=2025, only=None):
    """Random pages per cell -> random matching docs per page (spreads the sample over the year)."""
    frm, to = year_range(year)
    rnd = random.Random(seed)
    cells = {}
    for g, (rk, st, bs, req, canc) in GROUPS.items():
        for t in BY_TYPES:
            if only is not None and f"{g}|{t}" not in only:
                continue
            parent_n = counts["by_type"][t]["raw"][rk]
            cell_n = counts["by_type"][t]["groups"][g]
            if not cell_n:
                continue
            _, mx = limits(t)
            n_pages = (parent_n + 9) // 10
            density = cell_n / parent_n if parent_n else 0
            take_all = cell_n <= mx and n_pages <= 100          # tiny cell: read every bid in it
            if take_all:
                want_pages = n_pages
            else:
                # P(page has >=1 match) ~ min(1, 10*density); oversample so we get ~MAX candidates.
                hit = max(0.05, min(1.0, 10 * density))
                want_pages = min(n_pages, 400, int(mx / hit * 1.3) + 5)
            pages = rnd.sample(range(1, n_pages + 1), want_pages)
            # Few pages available (small dense cell) -> take several docs per page to reach MAX.
            per_page = 1 if take_all else max(1, -(-int(mx * 1.2) // max(1, want_pages)))
            cells[f"{g}|{t}"] = {"group": g, "by_type": t, "cell_n": cell_n, "parent_n": parent_n,
                                 "take_all": take_all, "pages": pages, "per_page": per_page, "st": st, "bs": bs,
                                 "req": req, "canc": canc}
    # One big batched fetch of all listing pages for all cells.
    flat = [(ck, p) for ck, c in cells.items() for p in c["pages"]]
    log(f"  fetching {len(flat)} listing pages for {len(cells)} cells ...")
    res = await pool.run([make_payload(page=p, st=cells[ck]["st"], bs=cells[ck]["bs"], by_type=cells[ck]["by_type"],
                                       frm=frm, to=to) for ck, p in flat])
    per_cell = defaultdict(list)
    for (ck, p), r in zip(flat, res):
        if not r or not r.get("ok"):
            continue
        c = cells[ck]
        ok_docs = [d for d in r["docs"] if matches(d, c["req"], c["canc"])]
        if c["take_all"]:
            per_cell[ck].extend(ok_docs)
        elif ok_docs:
            per_cell[ck].extend(rnd.sample(ok_docs, min(c["per_page"], len(ok_docs))))
    out = {}
    for ck, c in cells.items():
        seen, docs = set(), []
        for d in per_cell.get(ck, []):
            bid = first(d.get("b_id"))
            if bid not in seen:
                seen.add(bid)
                docs.append(d)
        rnd.shuffle(docs)
        out[ck] = {k: v for k, v in c.items() if k != "pages"} | {"n_pages_drawn": len(c["pages"]), "docs": docs}
    return out


def load_jsonl(path):
    rows = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    return rows


async def main(year, n_sessions, conc, seed, redraw_short=False):
    counts_path = os.path.join(DATA_DIR, f"counts_{year}.json")
    cand_path = os.path.join(DATA_DIR, f"participant_candidates_{year}.json")
    out_path = os.path.join(DATA_DIR, f"participant_samples_{year}.jsonl")
    counts = json.load(open(counts_path, encoding="utf-8"))

    done_rows = load_jsonl(out_path)
    by_cell = defaultdict(list)
    cache = {}                              # bid_id -> summary (re-use across cells)
    for r in done_rows:
        by_cell[r["cell"]].append(r)
        cache[r["bid_id"]] = r["result"]
    log(f"resume: {len(done_rows)} results already saved")

    fout = open(out_path, "a", encoding="utf-8")

    def record(cell, t, summary):
        row = {"cell": cell, "group": cell.split("|")[0], "by_type": cell.split("|")[1],
               "bid_id": t["bid_id"], "bid_number": t["bid_number"], "end_date": t.get("end_date"),
               "month": str(t.get("end_date") or "")[:7], "b_status": t.get("b_status"),
               "buyer_status": t.get("buyer_status"), "bid_type": t.get("bid_type"),
               "single_packet": t.get("single_packet"), "eval_type": t.get("eval_type"),
               "is_ra_number": "/R/" in str(t.get("bid_number")), "n_schedules": t.get("n_schedules"),
               "result": summary}
        fout.write(json.dumps(row, ensure_ascii=False) + "\n")
        fout.flush()
        by_cell[cell].append(row)
        cache[t["bid_id"]] = summary

    t0 = time.time()
    async with GemPool(n_sessions=n_sessions, conc=conc, log=log) as pool:
        if os.path.exists(cand_path):
            cands = json.load(open(cand_path, encoding="utf-8"))
            log(f"reusing candidates from {cand_path}")
            if redraw_short:
                short = {ck for ck, c in cands.items()
                         if len(c["docs"]) < 0.9 * min(limits(c["by_type"])[1], c["cell_n"])}
                log(f"redrawing {len(short)} short cells: {sorted(short)}")
                if short:
                    fresh = await draw_candidates(pool, year, counts, seed + 1, only=short)
                    for ck, c in fresh.items():
                        ids = {first(d.get("b_id")) for d in cands[ck]["docs"]}
                        cands[ck]["docs"] += [d for d in c["docs"] if first(d.get("b_id")) not in ids]
                        cands[ck]["take_all"] = c["take_all"]
                        log(f"  {ck}: now {len(cands[ck]['docs'])} candidates (cell has {c['cell_n']})")
                    json.dump(cands, open(cand_path, "w", encoding="utf-8"))
        else:
            cands = await draw_candidates(pool, year, counts, seed)
            json.dump(cands, open(cand_path, "w", encoding="utf-8"))
        log(f"candidates ready ({time.time()-t0:.0f}s): " +
            ", ".join(f"{k}={len(v['docs'])}" for k, v in list(cands.items())[:6]) + " ...")

        async def run_phase(target_fn, label):
            todo = []                       # (cell, tender)
            for ck, c in cands.items():
                tgt = target_fn(ck, c)
                have = {r["bid_id"] for r in by_cell[ck]}
                need = tgt - len(have)
                for d in c["docs"]:
                    if need <= 0:
                        break
                    t = doc_to_item(d)
                    if t["bid_id"] in have:
                        continue
                    if t["bid_id"] in cache:          # already checked in another cell
                        record(ck, t, cache[t["bid_id"]])
                    else:
                        todo.append((ck, t))
                    need -= 1
            uniq = {}
            for ck, t in todo:
                uniq.setdefault(t["bid_id"], (t, []))[1].append(ck)
            log(f"{label}: {len(uniq)} bids to check ({len(todo)} cell slots)")
            done = [0]

            def on_result(t, summary):
                for ck in uniq[t["bid_id"]][1]:
                    record(ck, t, summary)
                done[0] += 1
                if done[0] % 200 == 0:
                    log(f"    {done[0]}/{len(uniq)} checked ({time.time()-t0:.0f}s, conc={pool.conc})")
            await check_many(pool, [t for t, _ in uniq.values()], on_result=on_result)

        # Phase 1: MIN per cell (or all of a small cell)
        await run_phase(lambda ck, c: len(c["docs"]) if c["take_all"] else limits(c["by_type"])[0], "phase 1")

        # Phase 2: top up mixed cells to MAX
        def tgt2(ck, c):
            rows = by_cell[ck]
            ok = [r for r in rows if r["result"].get("ok")]
            k = sum(1 for r in ok if r["result"]["has_participants"])
            mixed = 0 < k < len(ok)
            mn, mx = limits(c["by_type"])
            return (len(c["docs"]) if c["take_all"] else mx) if mixed else len(rows)
        await run_phase(tgt2, "phase 2")
        log(f"pool stats: {pool.stats}")
    fout.close()
    log(f"done in {time.time()-t0:.0f}s -> {out_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=2025)
    ap.add_argument("--sessions", type=int, default=3)
    ap.add_argument("--conc", type=int, default=6)
    ap.add_argument("--seed", type=int, default=2025)
    ap.add_argument("--redraw-short", action="store_true",
                    help="re-draw candidates for cells that got fewer candidates than their target")
    a = ap.parse_args()
    asyncio.run(main(a.year, a.sessions, a.conc, a.seed, a.redraw_short))
