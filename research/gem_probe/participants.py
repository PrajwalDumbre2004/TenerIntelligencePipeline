"""
Participant-data check: runs pipeline3's OWN result-page parser (BID_RESULTS_JS, plus the
RA_SCHEDULE_JS fallback with the same rule as pipeline3.scraper.fetch_bid_results) on many bids
concurrently inside one page. Contract-detail navigation is skipped (not needed to know whether
participants exist).

Diagnostics (HTTP code, page size, number of <th>Seller headers) are captured in the same pass by a
fetch() wrapper, so each result page is downloaded once.
"""
import asyncio

from . import DATA_DIR  # noqa: F401
from pipeline2.config import BASE_URL
from pipeline2.worker import parse_doc_to_tender
from pipeline3.scraper import get_result_urls, BID_RESULTS_JS, RA_SCHEDULE_JS

from .client import first

PARTICIPANTS_BATCH_JS = """async ([items, conc]) => {
  const parse = __BID_RESULTS_JS__;
  const raParse = __RA_SCHEDULE_JS__;
  if (!window.__diagInstalled) {
    const orig = window.fetch.bind(window);
    window.__diag = {};
    window.fetch = async (u, o) => {
      const r = await orig(u, o);
      try {
        const us = String(u);
        if (us.includes('/bidding/bid/') || us.includes('list-ra-schedules') || us.includes('showradocumentPdf')) {
          const t = await r.clone().text();
          window.__diag[us] = {http: r.status, len: t.length,
                               sellerTh: (t.match(/<th[^>]*>[^<]*Seller/gi) || []).length};
        }
      } catch (e) {}
      return r;
    };
    window.__diagInstalled = true;
  }
  const names = (rows) => rows.map(x => (x.seller_name || '').replace(/\\s+/g, ' ').trim());
  const out = new Array(items.length);
  let next = 0;
  async function worker() {
    while (true) {
      const k = next++;
      if (k >= items.length) return;
      const it = items[k];
      try {
        const r = await parse(it.urls);
        let fb = null;
        const hasRaRows = r.ra_technical_evaluation.length > 0 || r.ra_financial_evaluation.length > 0;
        if (!hasRaRows && (it.has_ra || r.ra_number)) {
          fb = await raParse(it.base + '/list-ra-schedules/' + it.bid_id);
          if (!fb) fb = await raParse(it.base + '/showradocumentPdf/' + it.bid_id);
        }
        const all = [].concat(names(r.technical_evaluation), names(r.financial_evaluation),
                              names(r.ra_technical_evaluation), names(r.ra_financial_evaluation),
                              fb ? names(fb.technical_evaluation) : [], fb ? names(fb.financial_evaluation) : []);
        out[k] = {ok: true,
                  tech: r.technical_evaluation.length, fin: r.financial_evaluation.length,
                  ra_tech: r.ra_technical_evaluation.length, ra_fin: r.ra_financial_evaluation.length,
                  fb_tech: fb ? fb.technical_evaluation.length : 0, fb_fin: fb ? fb.financial_evaluation.length : 0,
                  ra_number: r.ra_number || '',
                  names: all,
                  diag: it.urls.map(u => window.__diag[u] || null)};
      } catch (e) {
        out[k] = {ok: false, error: String(e)};
      }
    }
  }
  await Promise.all(Array.from({length: Math.max(1, conc)}, worker));
  return out;
}""".replace("__BID_RESULTS_JS__", BID_RESULTS_JS).replace("__RA_SCHEDULE_JS__", RA_SCHEDULE_JS)


def is_masked(name: str) -> bool:
    s = (name or "").strip()
    return bool(s) and set(s) <= set("* ")


def doc_to_item(doc: dict) -> dict:
    """Listing doc -> tender dict (pipeline2's parser) + the fields this research needs."""
    t = parse_doc_to_tender(doc, "research")
    t["b_status"] = first(doc.get("b_status"))
    t["ra_b_status"] = first(doc.get("ra_b_status"))
    t["is_limited_tendering"] = first(doc.get("ba_is_limited_tendering"))
    t["is_inactive"] = first(doc.get("b_is_inactive"))
    t["n_schedules"] = len(doc.get("bid_schedule") or [])
    return t


def js_item(t: dict) -> dict:
    has_ra = (t.get("ra_to_bid", 0) == 1 or (t.get("bid_to_ra") or 0) >= 1 or t.get("bid_type", 0) in (2, 5))
    return {"bid_id": t["bid_id"], "urls": get_result_urls(t), "has_ra": bool(has_ra), "base": BASE_URL}


def summarize(raw: dict) -> dict:
    if not raw or not raw.get("ok"):
        return {"ok": False, "error": (raw or {}).get("error", "no result")}
    names = raw["names"]
    uniq = sorted(set(n for n in names if n))
    masked = [n for n in uniq if is_masked(n)]
    n_rows = raw["tech"] + raw["fin"] + raw["ra_tech"] + raw["ra_fin"] + raw["fb_tech"] + raw["fb_fin"]
    n_masked_rows = sum(1 for n in names if is_masked(n))
    return {
        "ok": True,
        "has_participants": n_rows > 0,
        "rows": {k: raw[k] for k in ("tech", "fin", "ra_tech", "ra_fin", "fb_tech", "fb_fin")},
        "n_rows": n_rows,
        "n_named_unique": len([n for n in uniq if not is_masked(n)]),
        "n_masked_rows": n_masked_rows,
        "any_masked": n_masked_rows > 0,
        "all_masked": n_rows > 0 and n_masked_rows == len([n for n in names if n]) and n_masked_rows > 0,
        "ra_number": raw.get("ra_number", ""),
        "sample_names": [n for n in uniq if not is_masked(n)][:5],
        "diag": raw.get("diag"),
    }


async def check_batch(session, tenders: list, conc: int):
    items = [js_item(t) for t in tenders]
    raw = await session.page.evaluate(PARTICIPANTS_BATCH_JS, [items, conc])
    return [summarize(r) for r in raw]


async def check_many(pool, tenders: list, chunk: int = 40, on_result=None):
    """
    Check participant availability for many tenders across all pool sessions.
    on_result(tender, summary) is called as results arrive (used to append to JSONL).
    Failed items are retried once with a CSRF refresh.
    """
    queue = asyncio.Queue()
    for i in range(0, len(tenders), chunk):
        queue.put_nowait(tenders[i:i + chunk])
    out = {}

    async def drain(sess):
        while not queue.empty():
            batch = queue.get_nowait()
            for attempt in range(2):
                try:
                    res = await check_batch(sess, batch, pool.conc)
                except Exception as e:  # noqa: BLE001
                    res = [{"ok": False, "error": f"evaluate: {e}"}] * len(batch)
                bad = [t for t, r in zip(batch, res) if not r.get("ok")]
                for t, r in zip(batch, res):
                    if r.get("ok") or attempt == 1:
                        out[t["bid_id"]] = r
                        if on_result:
                            on_result(t, r)
                pool._adapt(len(batch), len(bad))
                if not bad:
                    break
                try:
                    await sess.refresh_csrf()
                except Exception:  # noqa: BLE001
                    pass
                batch = bad

    await asyncio.gather(*(drain(s) for s in pool.sessions))
    return out
