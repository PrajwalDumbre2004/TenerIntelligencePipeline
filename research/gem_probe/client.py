"""
Async GeM client: a pool of headless browser sessions that send *batched* requests to
/all-bids-data from inside the page (so cookies + CSRF behave exactly like pipeline2).

Speed features:
  - each page.evaluate() runs many POSTs concurrently (Promise pool inside the page);
  - several browser contexts run in parallel;
  - adaptive concurrency: halves on errors, CSRF auto-refresh, retries of failed items;
  - images / CSS / fonts are blocked while loading the CSRF page.
"""
import asyncio
import calendar
import json
import time
import urllib.parse
from typing import Optional

from playwright.async_api import async_playwright

from . import DATA_DIR  # noqa: F401  (ensures sys.path setup)
from pipeline2.config import BASE_URL, USER_AGENT, BROWSER_LOCALE, BROWSER_VIEWPORT

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
DEFAULT_SORT = "Bid-End-Date-Oldest"   # an official GeM sort value
LIST_URL = f"{BASE_URL}/all-bids-data"


# ─────────────────────────────────────────────────────────────────────────────
# Date helpers (GeM format: 15-Jan-2025)
# ─────────────────────────────────────────────────────────────────────────────
def gem_date(y: int, m: int, d: int) -> str:
    return f"{d:02d}-{MONTHS[m - 1]}-{y}"


def year_range(y: int):
    return gem_date(y, 1, 1), gem_date(y, 12, 31)


def month_range(y: int, m: int):
    return gem_date(y, m, 1), gem_date(y, m, calendar.monthrange(y, m)[1])


def first(v):
    return v[0] if isinstance(v, list) and v else v


# ─────────────────────────────────────────────────────────────────────────────
# Payload builder — mirrors the shape pipeline2/worker.py sends
# ─────────────────────────────────────────────────────────────────────────────
def make_payload(page: int = 1, st: str = "all_bids", bs: Optional[str] = "", by_type: str = "all",
                 high: str = "", frm: str = "", to: str = "", sort: str = DEFAULT_SORT,
                 search: str = "", search_type: str = "fullText",
                 extra_filter: Optional[dict] = None, extra_top: Optional[dict] = None) -> str:
    filt = {"bidStatusType": st, "byType": by_type, "highBidValue": high,
            "byEndDate": {"from": frm, "to": to}, "sort": sort}
    if bs is not None:
        filt["byStatus"] = bs
    if extra_filter:
        filt.update(extra_filter)
    body = {"page": page, "param": {"searchBid": search, "searchType": search_type}, "filter": filt}
    if extra_top:
        body.update(extra_top)
    return json.dumps(body)


# Runs a list of POST payloads with a concurrency pool, inside the page.
LIST_BATCH_JS = """async ([url, csrf, payloads, conc, keepDocs]) => {
  const out = new Array(payloads.length);
  let next = 0;
  async function worker() {
    while (true) {
      const k = next++;
      if (k >= payloads.length) return;
      const body = 'payload=' + encodeURIComponent(payloads[k]) + '&csrf_bd_gem_nk=' + encodeURIComponent(csrf);
      try {
        const r = await fetch(url, {method: 'POST', credentials: 'include', body,
          headers: {'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
                    'X-Requested-With': 'XMLHttpRequest'}});
        const t = await r.text();
        let d = null; try { d = JSON.parse(t); } catch (e) {}
        if (!r.ok) {
          if (d && d.code === 404 && d.message && String(d.message).includes('No data')) {
            out[k] = {ok: true, numFound: 0, docs: [], nodata: true}; continue;
          }
          out[k] = {ok: false, error: 'HTTP ' + r.status, body: t.slice(0, 200)}; continue;
        }
        if (!d || d.status !== 1) { out[k] = {ok: false, error: 'bad payload', body: t.slice(0, 200)}; continue; }
        const resp = d.response.response;
        out[k] = {ok: true, numFound: resp.numFound, docs: keepDocs ? resp.docs : [], ndocs: resp.docs.length};
      } catch (e) {
        out[k] = {ok: false, error: 'exception: ' + String(e)};
      }
    }
  }
  await Promise.all(Array.from({length: Math.max(1, conc)}, worker));
  return out;
}"""


class GemSession:
    """One browser context + page with a captured CSRF token."""

    def __init__(self, browser, idx: int):
        self.browser = browser
        self.idx = idx
        self.context = None
        self.page = None
        self.csrf = None

    async def open(self):
        self.context = await self.browser.new_context(user_agent=USER_AGENT, locale=BROWSER_LOCALE,
                                                      viewport=BROWSER_VIEWPORT)
        self.page = await self.context.new_page()

        async def _block(route):
            if route.request.resource_type in ("image", "stylesheet", "font", "media"):
                await route.abort()
            else:
                await route.continue_()
        await self.page.route("**/*", _block)
        await self.refresh_csrf()

    async def refresh_csrf(self, attempts: int = 4):
        last = None
        for a in range(attempts):
            captured = {}

            def on_resp(resp):
                if "all-bids-data" in resp.url and resp.request.post_data:
                    q = urllib.parse.parse_qs(resp.request.post_data)
                    if "csrf_bd_gem_nk" in q:
                        captured["v"] = q["csrf_bd_gem_nk"][0]
            self.page.on("response", on_resp)
            try:
                await self.page.goto(f"{BASE_URL}/all-bids", wait_until="domcontentloaded", timeout=60000)
                for _ in range(40):           # wait up to ~20 s for the auto-fired XHR
                    if "v" in captured:
                        break
                    await asyncio.sleep(0.5)
            except Exception as e:  # noqa: BLE001
                last = e
            finally:
                self.page.remove_listener("response", on_resp)
            if "v" in captured:
                self.csrf = captured["v"]
                return
            await asyncio.sleep(2 * (a + 1))
        raise RuntimeError(f"session {self.idx}: could not capture CSRF ({last})")

    async def list_batch(self, payloads, conc: int, keep_docs: bool):
        return await self.page.evaluate(LIST_BATCH_JS, [LIST_URL, self.csrf, payloads, conc, keep_docs])

    async def close(self):
        try:
            await self.context.close()
        except Exception:  # noqa: BLE001
            pass


class GemPool:
    """
    Pool of sessions. `await pool.run(payloads)` returns one result dict per payload:
      {ok, numFound, docs?, error?}
    Failed items are retried (with a CSRF refresh) up to `retries` times.
    """

    def __init__(self, n_sessions: int = 3, conc: int = 8, min_conc: int = 1, retries: int = 3,
                 log=print):
        self.n = n_sessions
        self.conc = conc
        self.min_conc = min_conc
        self.retries = retries
        self.sessions = []
        self.log = log
        self._pw = None
        self._browser = None
        self.stats = {"requests": 0, "errors": 0, "retries": 0, "csrf_refresh": 0}

    async def __aenter__(self):
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(headless=True,
                                                       args=["--no-sandbox", "--disable-dev-shm-usage"])
        self.sessions = [GemSession(self._browser, i) for i in range(self.n)]
        await asyncio.gather(*(s.open() for s in self.sessions))
        return self

    async def __aexit__(self, *exc):
        for s in self.sessions:
            await s.close()
        await self._browser.close()
        await self._pw.stop()

    def _adapt(self, n, n_err):
        """Halve concurrency on >10% errors, creep back up when clean."""
        if n and n_err / n > 0.10 and self.conc > self.min_conc:
            self.conc = max(self.min_conc, self.conc // 2)
            self.log(f"    [pool] error rate {n_err}/{n} -> concurrency {self.conc}")
        elif n_err == 0 and self.conc < 12:
            self.conc += 1

    async def run(self, payloads, keep_docs: bool = True, chunk: int = 64):
        results = [None] * len(payloads)
        pending = list(range(len(payloads)))
        for attempt in range(self.retries + 1):
            if not pending:
                break
            if attempt:
                self.stats["retries"] += len(pending)
                await asyncio.sleep(2 * attempt)
            # Split pending into chunks, round-robin over sessions.
            chunks = [pending[i:i + chunk] for i in range(0, len(pending), chunk)]
            queue = asyncio.Queue()
            for c in chunks:
                queue.put_nowait(c)
            failed = []

            async def drain(sess):
                while not queue.empty():
                    idxs = queue.get_nowait()
                    try:
                        res = await sess.list_batch([payloads[i] for i in idxs], self.conc, keep_docs)
                    except Exception as e:  # noqa: BLE001  (page crashed / navigation)
                        res = [{"ok": False, "error": f"evaluate: {e}"}] * len(idxs)
                    n_err = 0
                    for i, r in zip(idxs, res):
                        self.stats["requests"] += 1
                        if r.get("ok"):
                            results[i] = r
                        else:
                            n_err += 1
                            self.stats["errors"] += 1
                            kinds = self.stats.setdefault("error_kinds", {})
                            kk = f"{r.get('error')} | {(r.get('body') or '')[:80]}"
                            kinds[kk] = kinds.get(kk, 0) + 1
                            results[i] = r
                            failed.append(i)
                    self._adapt(len(idxs), n_err)
                    if n_err > len(idxs) // 2:       # likely CSRF / session problem
                        try:
                            self.stats["csrf_refresh"] += 1
                            await sess.refresh_csrf()
                        except Exception as e:  # noqa: BLE001
                            self.log(f"    [pool] session {sess.idx} refresh failed: {e}")

            await asyncio.gather(*(drain(s) for s in self.sessions))
            pending = sorted(set(failed))
        return results

    async def count(self, **kw) -> Optional[int]:
        r = (await self.run([make_payload(page=1, **kw)], keep_docs=False))[0]
        return r["numFound"] if r and r.get("ok") else None

    async def counts(self, specs: dict) -> dict:
        """specs: {key: payload-kwargs} -> {key: numFound or None}"""
        keys = list(specs)
        res = await self.run([make_payload(page=1, **specs[k]) for k in keys], keep_docs=False)
        return {k: (r["numFound"] if r and r.get("ok") else None) for k, r in zip(keys, res)}


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S")
