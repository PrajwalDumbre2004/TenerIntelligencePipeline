import asyncio, json, sys, collections
sys.path.insert(0, r"c:\Prajwal Dumbre\TenerIntelligencePipeline")
from research.gem_probe.client import GemPool, make_payload, first
D = r"c:\Prajwal Dumbre\TenerIntelligencePipeline\research\data"
rows = [json.loads(l) for l in open(D + r"\participant_samples_2025.jsonl", encoding="utf-8") if l.strip()]
cands = json.load(open(D + r"\participant_candidates_2025.json", encoding="utf-8"))
docs = {first(d.get("b_id")): d for c in cands.values() for d in c["docs"]}
GROUPS = ["fin_live", "fin_cancelled", "tech_live", "tech_cancelled", "noteval_live", "awarded_live"]
sel = {}
for r in rows:
    if r["group"] in GROUPS and not r["is_ra_number"]:
        sel[r["bid_id"]] = r
async def main():
    items = list(sel.values())
    async with GemPool(n_sessions=1, conc=4) as pool:
        res = await pool.run([make_payload(page=1, st="all_bids", bs="", search=i["bid_number"], search_type="exact") for i in items])
    agg = collections.Counter(); b2r = collections.Counter(); out = []
    for i, x in zip(items, res):
        d = docs.get(i["bid_id"], {})
        kids = [k for k in (x.get("docs") or []) if first(k.get("b_id_parent")) == i["bid_id"] and first(k.get("b_id")) != i["bid_id"]]
        if not x.get("ok"):
            state = "lookup failed"
        elif not kids:
            state = "no RA child"
        else:
            sts = sorted({(first(k.get("b_buyer_status")), first(k.get("b_status"))) for k in kids})
            aw_live = any(a == 3 and s not in (3, 5) for a, s in sts)
            aw_any = any(a == 3 for a, s in sts)
            state = ("RA child AWARDED (fetched by pipeline)" if aw_live else
                     "RA child awarded->cancelled (fetched)" if aw_any else
                     f"RA child not awarded {sts}")
        agg[(i["group"], state)] += 1
        b2r[(i["group"], f"b_bid_to_ra={first(d.get('b_bid_to_ra'))}", "has_child" if kids else "no_child")] += 1
        out.append({"group": i["group"], "bid_number": i["bid_number"], "state": state, "n_children": len(kids),
                    "has_participants": i["result"].get("has_participants")})
    for g in GROUPS:
        tot = sum(v for (gg, _), v in agg.items() if gg == g)
        print(f"== {g} (n={tot})")
        for (gg, s), v in sorted(agg.items(), key=lambda kv: -kv[1]):
            if gg == g: print(f"   {v:>4}  {s}")
    print()
    for k, v in sorted(b2r.items()): print(v, k)
    json.dump(out, open(D + r"\ra_child_check_2025.json", "w", encoding="utf-8"), indent=1)
asyncio.run(main())
