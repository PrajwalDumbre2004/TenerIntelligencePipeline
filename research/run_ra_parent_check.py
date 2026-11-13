import asyncio, json, sys, collections
sys.path.insert(0, r"c:\Prajwal Dumbre\TenerIntelligencePipeline")
from research.gem_probe.client import GemPool, make_payload, first
D = r"c:\Prajwal Dumbre\TenerIntelligencePipeline\research\data"
rows = [json.loads(l) for l in open(D + r"\participant_samples_2025.jsonl", encoding="utf-8") if l.strip()]
cands = json.load(open(D + r"\participant_candidates_2025.json", encoding="utf-8"))
docs = {first(d.get("b_id")): d for c in cands.values() for d in c["docs"]}
ra = {}
for r in rows:
    if r["is_ra_number"]:
        ra[r["bid_id"]] = r
async def main():
    items = []
    for b, r in ra.items():
        d = docs.get(b, {})
        items.append((r["group"], r["bid_number"], first(d.get("b_bid_number_parent")), first(d.get("b_id_parent")),
                      first(d.get("b_buyer_status")), first(d.get("ra_b_buyer_status"))))
    async with GemPool(n_sessions=1, conc=4) as pool:
        res = await pool.run([make_payload(page=1, st="all_bids", bs="", search=i[2] or "x", search_type="exact") for i in items])
    agg = collections.Counter(); ex = []
    for i, x in zip(items, res):
        pd = [d for d in (x.get("docs") or []) if first(d.get("b_id")) == i[3]]
        if not pd:
            agg[(i[0], "parent not found")] += 1; continue
        p = pd[0]
        key = (i[0], f"RA b_buyer={i[4]} ra_b_buyer={i[5]}", f"parent buyer_status={first(p.get('b_buyer_status'))}",
               f"parent b_status={first(p.get('b_status'))}", f"parent end={str(first(p.get('final_end_date_sort')))[:4]}")
        agg[key] += 1
        if len(ex) < 4 and i[0] == "noteval_live": ex.append((i[1], i[2], key))
    for k, v in sorted(agg.items(), key=lambda kv: (kv[0][0], -kv[1])): print(v, k)
    print(ex)
asyncio.run(main())
