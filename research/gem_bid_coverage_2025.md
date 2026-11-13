# GeM bid coverage, 2025: which bids have participant data, and what the pipeline misses

*Data collected on 2026-11-13 from GeM (bidplus.gem.gov.in) and the project's Postgres DB (read-only).
Reference year: **bids whose end date falls in 2025**. GeM data changes daily, so re-running the scripts
(see [§10](#10-how-to-re-run)) will give slightly different numbers.*

---

## 1. Summary

GeM lists **1,070,322 bids** that ended in 2025. The pipeline asks GeM only for `all_bids` + `bid_awarded`,
so its target is **595,330 bids (55.6%)**, and the database holds **589,054** of them.

| Bucket (2025) | Bids | % of all | Participant data? |
|---|---:|---:|---|
| **A. Fetched by the current filter** (awarded) | 595,330 | 55.6% | Yes, 100% of sampled bids |
| ↳ stored in DB | 589,054 | | 99.99% of stored bids have participant rows |
| ↳ in GeM's target but not in DB | 6,278 | | Mostly bids awarded after the crawl (§8) |
| **B. Missed, with participant data** | **≈ 243,400** | **23%** | Yes, but some names are masked |
| ↳ financially evaluated (not cancelled) | ≈ 180,100 | | ~100% have participants; 7% fully masked, 17% partly. Includes ≈50k parents whose RA child is awarded: the DB has only the RA (auction subset), not the parent's full bidder list |
| ↳ financially evaluated → cancelled | ≈ 47,300 | | 100% have participants; 27% fully masked |
| ↳ technically evaluated (live + cancelled) | ≈ 16,000 | | Only ~20% of these bids have any; technical table only, no prices |
| **C. No participant data** (as pipeline3 stores it) | ≈ 231,600 | 22% | No |
| ↳ cancelled before evaluation (normal bids) | ≈ 73,000 | | |
| ↳ technically evaluated, no table published yet | ≈ 72,600 | | |
| ↳ RA bids not evaluated or cancelled at stage 0 | ≈ 51,400 | | Page shows only the *parent's* table, which pipeline3 ignores for RA bids (§6) |
| ↳ closed, not evaluated (normal bids) | ≈ 33,300 | | |
| ↳ financially evaluated, no table on the page | ≈ 1,300 | | |

With these filters added, about **838,700 of 1,070,322 bids (78%)** would have participant data, up 41% from today.

**Answer to the original question.** The database has fewer bids than GeM for two reasons:
1. **The status filter (~97% of the gap).** Only awarded bids are requested. About **243k more 2025 bids
   expose participant lists** on GeM: mostly *financially evaluated* bids, including cancelled ones, plus a few
   *technically evaluated* ones.
2. **Late awards (~1%).** In 2025, 6,278 awarded bids aren't stored. This rises from 0.2% of a month's bids in
   Jan 2025 to 3.5% in Dec 2025, which fits bids that were awarded *after* that month was crawled.

To capture every 2025 bid that has participant data, the pipeline needs to add **`fin_evaluated`** (+≈227k bids)
and, optionally, **`tech_evaluated`** (+≈16k bids from about 89k listings). It also needs to store status fields
and handle masked names (§9).

---

## 2. How GeM bid statuses work

| `b_buyer_status` | GeM label (page source) | Meaning | Participant data |
|---|---|---|---|
| 0 | Not Evaluated | Bid closed; buyer hasn't started evaluating | None (normal bids) |
| 1 | Technical Evaluation | Buyer has marked bidders qualified/disqualified | Sometimes (~20%): names + technical status, **no prices** |
| 2 | Financial Evaluation | Price bids opened and ranked L1, L2, … | **Yes (~100%)**: names, prices, ranks |
| 3 | Bid Award | Buyer created the demand/order | **Yes (100%)** |

- **One-way and buyer-driven.** Stages only move forward, and each step is a buyer action on the portal. In a
  live re-check, bids moved 1→2 and 2→3 within 30 minutes; none moved backwards and none skipped a stage. Many
  bids never reach stage 3: about 26% of 2025 bids that weren't cancelled are still at stage 1 or 2.
- **Cancellation is a separate flag (`b_status`).** It freezes the stage the bid had reached. `b_status` = 3 or
  5 means cancelled (GeM page code); 1 = in evaluation and 2 = awarded (observed). GeM's manual: *"Buyer has an
  option to cancel the bid before award of bid."* In practice cancellation happens at every stage, including
  88 bids in 2025 that were cancelled after award.
- **RA (reverse auction) bids** (`/R/` numbers, `b_bid_type = 2`, byType `bidToRA`) are children of a normal
  bid. On an RA listing, `b_buyer_status`/`b_status` describe the RA itself, while **`ra_b_buyer_status`/`ra_b_status`
  describe the *parent* bid** (matched 239/239 parent look-ups). `b_bid_to_ra = 1` on a normal bid means it was
  converted to an RA (62/62 had an RA child).

Sources: [GeM Single Packet Bid user manual](https://assets-bg.gem.gov.in/resources/upload/shared_doc/User%20Manual%20Single%20Packet%20Bid%20(1).pdf),
[GeM all-bids page source](https://bidplus.gem.gov.in/all-bids) (`bidStatus = ["Not Evaluated","Technical Evaluation","Financial Evaluation","Bid Award"]`).

---

## 3. API parameter reference: `POST /all-bids-data`

Body: `payload=<json>&csrf_bd_gem_nk=<token>`. The token comes from loading `/all-bids` (an XHR the page fires
on load). JSON: `{"page": n, "param": {"searchBid", "searchType"}, "filter": {...}}`.

| Parameter | Accepted values | Behaviour (tested on 2025) | Gotchas |
|---|---|---|---|
| `filter.bidStatusType` | `ongoing_bids`, `bidrastatus`, `cancelled_bids` | `bidrastatus` = closed bids that aren't cancelled (893,726); `cancelled_bids` = cancelled (176,596); `ongoing_bids` = still open (0 for 2025) | **`all_bids` isn't a real value.** It, and any unknown value, means *no filter* (1,070,322). |
| `filter.byStatus` | `tech_evaluated`, `fin_evaluated`, `bid_awarded`, `""` | Filters on `b_buyer_status` = 1/2/3. Works with `bidrastatus`, `cancelled_bids` and `all_bids`. | Unknown values are ignored (no filter). With `all_bids`, the stage counts **include cancelled bids**. |
| `filter.byType` | `all`, `product`, `service`, `bidToRA`, `custom`, `boq`, `rcbid`, `gt`, `lt`, `st` | `product` (814,912) + `service` (255,410) = all bids exactly. The rest are **overlapping attributes**. | Unknown values are ignored. `gt` = 0 and `rcbid` = 10 in 2025. |
| `filter.highBidValue` | `""`, `highValue` | `highValue` = 42,087 bids | Unknown values are ignored. |
| `filter.byEndDate` | `{"from": "dd-Mon-yyyy", "to": "dd-Mon-yyyy"}` | Filters on **`final_end_date_sort`**, both ends inclusive (15-Jan 2,360 + 16-Jan 3,019 = 5,379 = the two-day range). ISO `yyyy-mm-dd` also works. | `dd/mm/yyyy` → 0. A missing `from` or `to` → **no date filter** (5.85M bids). A reversed range → 0. |
| `filter.sort` | `Bid-End-Date-Oldest`, `Bid-End-Date-Latest`, `Bid-Start-Date-Oldest`, `Bid-Start-Date-Latest` | Official sorts give a **stable order**: one pass over 5,519 bids returned all of them with 0 duplicates. | **`"bid_id"` (used by pipeline2) isn't valid and is ignored**, so GeM uses its default order, which shifts between pages: 10 duplicates and 10 missing bids per pass on the same 5,519 bids. |
| `page` | 1 … ⌈numFound/10⌉ | **No page cap**: page 100,000 of the 2025 range works. Pages beyond the last return 0 docs. Page 0 = page 1. | **The page size is fixed at 10.** `rows`/`pageSize` are ignored. |
| `param.searchBid` + `searchType` | `fullText`, `exact` | Searching a bid number returns the bid **plus its RA children** | Useful for re-checking one bid's current status. |
| Unknown filter keys | e.g. `byStartDate`, `byMinistry` | **Ignored silently** | There's no start-date or ministry filter. |
| Rate limit | – | **HTTP 429 "Too Many Requests"** when bursting (≈20–25% of requests at 3 sessions × 8 in flight) | All succeeded on retry with back-off. pipeline2's 10 workers need back-off on 429. |

Bid-type fingerprints, i.e. the listing field that defines each `byType`: `product`/`service` = `b_type` 0/1;
`bidToRA` = `b_bid_type` 2 (`/R/` numbers); `custom` = `b_is_custom_item` 1; `boq` = `bd_details_is_boq`;
`rcbid` = `is_rc_bid` 1; `lt`/`st` = `ba_is_limited_tendering` 1/2.

Listing fields that aren't stored today but should be: `b_status`, `ra_b_status`, `ra_b_buyer_status`,
`b_is_inactive`, `bid_schedule`/`parent_bid_schedule` (per-schedule award state).

---

## 4. 2025 counts by status group × bid type

Status groups are mutually exclusive and together equal all bids (checked: exact for the year and for every month).

| Status group | product | service | bidToRA | custom | boq | lt | st | **all** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Awarded | 438,412 | 156,830 | 108,070 | 138,949 | 119,140 | 3,049 | 1,766 | **595,242** |
| Awarded → cancelled | 86 | 2 | 36 | 2 | 4 | 0 | 0 | **88** |
| Financially evaluated | 152,195 | 29,185 | 5,838 | 53,282 | 47,191 | 588 | 242 | **181,380** |
| Fin. evaluated → cancelled | 31,465 | 15,844 | 5,332 | 9,140 | 4,928 | 168 | 53 | **47,309** |
| Technically evaluated | 26,747 | 6,665 | 0 | 8,929 | 4,376 | 53 | 18 | **33,412** |
| Tech. evaluated → cancelled | 39,656 | 15,597 | 0 | 11,148 | 6,286 | 90 | 17 | **55,253** |
| Closed, not evaluated | 71,780 | 11,912 | 50,437 | 22,355 | 11,719 | 268 | 153 | **83,692** |
| Cancelled before evaluation | 54,571 | 19,375 | 972 | 11,723 | 9,458 | 455 | 232 | **73,946** |
| **All bids** | 814,912 | 255,410 | 170,685 | 255,528 | 203,102 | 4,671 | 2,481 | **1,070,322** |

(`rcbid`: 10 bids in total, 7 financially evaluated and 3 not evaluated. `gt`: 0.)

### By month (2025)

| Month | Awarded | Aw.→canc. | Fin. eval | Fin.→canc. | Tech. eval | Tech.→canc. | Not eval | Canc. stage 0 | All |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2025-01 | 55,024 | 1 | 15,247 | 3,944 | 2,795 | 5,383 | 6,368 | 7,903 | 96,665 |
| 2025-02 | 55,594 | 1 | 15,442 | 4,034 | 2,899 | 4,928 | 6,343 | 8,645 | 97,886 |
| 2025-03 | 54,501 | 1 | 14,346 | 4,073 | 3,038 | 5,043 | 7,055 | 8,216 | 96,273 |
| 2025-04 | 32,582 | 1 | 11,025 | 2,967 | 2,027 | 3,059 | 4,826 | 3,780 | 60,267 |
| 2025-05 | 41,469 | 1 | 13,975 | 3,455 | 2,010 | 3,125 | 6,096 | 4,647 | 74,778 |
| 2025-06 | 46,812 | 2 | 14,737 | 3,706 | 2,145 | 3,602 | 6,556 | 4,983 | 82,543 |
| 2025-07 | 55,060 | 2 | 16,892 | 4,643 | 3,030 | 4,906 | 8,431 | 6,340 | 99,304 |
| 2025-08 | 50,836 | 3 | 16,197 | 4,236 | 3,061 | 5,191 | 7,737 | 6,282 | 93,543 |
| 2025-09 | 53,365 | 6 | 16,547 | 4,343 | 3,286 | 5,173 | 8,086 | 6,400 | 97,206 |
| 2025-10 | 48,769 | 15 | 14,923 | 3,859 | 2,855 | 4,928 | 7,359 | 5,462 | 88,170 |
| 2025-11 | 46,535 | 34 | 14,584 | 3,722 | 2,715 | 4,493 | 6,885 | 5,032 | 84,000 |
| 2025-12 | 54,695 | 21 | 17,465 | 4,327 | 3,551 | 5,422 | 7,950 | 6,256 | 99,687 |

---

## 5. Participant availability by status group and bid type

Method: stratified random sample. Random listing pages spread over all of 2025, one or more random bids per page.
Each bid went through **pipeline3's own parser** (`get_result_urls` + `BID_RESULTS_JS` + the `RA_SCHEDULE_JS`
fallback). 1,323 bid checks (1,303 unique bids) and **0 failed checks**. Rates come with Wilson 95% intervals.

### By status group (byType = all)

| Status group | Bids | Sampled | Has participants (95% CI) | All names masked | Partly masked |
|---|---:|---:|---|---:|---:|
| Awarded *(fetched)* | 595,242 | 30 | **100%** (89–100%) | 0% | 0% |
| Awarded → cancelled *(fetched)* | 88 | 30 | **100%** (89–100%) | 3% | **93%** |
| Financially evaluated | 181,380 | 30 | **100%** (89–100%) | 7% | 17% |
| Fin. evaluated → cancelled | 47,309 | 30 | **100%** (89–100%) | 27% | 0% |
| Technically evaluated | 33,412 | 60 | 23% (14–35%) | 10% | 0% |
| Tech. evaluated → cancelled | 55,253 | 60 | 22% (13–34%) | 15% | 0% |
| Closed, not evaluated | 83,692 | 60 | 47% (35–59%): **all of it RA bids showing the parent's table** | 25% | 0% |
| Cancelled before evaluation | 73,946 | 30 | 0% (0–11%) | – | – |

### By status group × bid type (rate, with sample size in brackets)

| Status group | product | service | bidToRA | custom | boq | lt | st |
|---|---|---|---|---|---|---|---|
| Awarded | 100% (15) | 100% (15) | 100% (15) | 100% (15) | 100% (15) | 100% (15) | 100% (15) |
| Awarded → cancelled | 100% (15) | 100% (2) | 100% (15) | 100% (2) | 100% (4) | – | – |
| Financially evaluated | 100% (15) | 100% (15) | 100% (15) | 100% (15) | 100% (15) | 97% (30) | 100% (15) |
| Fin. evaluated → cancelled | 100% (15) | 100% (15) | 100% (15) | 100% (15) | 100% (15) | 100% (15) | 100% (15) |
| Technically evaluated | 13% (30) | 33% (30) | – | 7% (30) | 7% (30) | 7% (30) | 11% (18) |
| Tech. evaluated → cancelled | 23% (30) | 30% (30) | – | 10% (30) | 33% (30) | 3% (30) | 6% (17) |
| Closed, not evaluated | 50% (30) | 63% (30) | 100% (15) | 57% (30) | 67% (27) | 32% (28) | 0% (15) |
| Cancelled before evaluation | 0% (15) | 0% (15) | 100% (15) | 0% (15) | 0% (15) | 0% (15) | 0% (15) |

The "not evaluated" and "cancelled before evaluation" rates above come **entirely from RA bids**: normal (`/B/`)
bids were 0/117 and 0/120. In every RA case the seller table came from the **parent bid's** result page, the
third URL `get_result_urls` adds for RA bids. The RA's own pages had none.

### Other cuts (samples deduplicated within each group)

| Status group | Q1 | Q2 | Q3 | Q4 | Normal bid (/B/) | RA (/R/) | Double packet | Single packet |
|---|---|---|---|---|---|---|---|---|
| Financially evaluated | 96% | 100% | 100% | 100% | 99% (140) | 100% (17) | 100% | 94% (16) |
| Fin. evaluated → cancelled | 100% | 100% | 100% | 100% | 100% (109) | 100% (26) | 100% | 100% |
| Technically evaluated | 16% | 19% | 18% | 13% | 16% (226) | – | 16% | – |
| Tech. evaluated → cancelled | 20% | 18% | 23% | 17% | 19% (227) | – | 19% | – |
| Closed, not evaluated | 78% | 60% | 41% | 42% | **0% (117)** | **100% (121)** | 56% | 0% |
| Cancelled before evaluation | 19% | 24% | 12% | 2% | **0% (120)** | **100% (15)** | 14% | 0% |

Availability doesn't fade with age: Q1 2025 financially evaluated bids are as complete as Q4 ones.

---

## 6. RA bids and their parents (no duplicates under the separate-tender model)

An **RA (reverse auction)** bid is a child of a normal bid: after evaluation, the qualified bidders re-bid in a
live auction. The project treats the RA and its parent as **separate tenders**, and pipeline3 enforces this
([pipeline3/extract.py:110-112](../pipeline3/extract.py#L110-L112)):
- an RA bid (`/R/`) stores **only its RA section**: auction participants, auction ranks and prices;
- a normal bid stores **only its own table**.

The scraper also fetches the parent's result page for RA bids (third URL of `get_result_urls`), but the parent's
table is ignored when the RA's participations are built.

**DB check (2025, read-only).** 107,798 RA tenders are stored. For 25,808 of them the parent is stored too.
Across those pairs:
- 167,392 parent rows and 83,835 RA rows;
- 82,535 sellers appear in both, which is expected (RA bidders are a subset of the parent's bidders);
- only **6** of those have the same price, and only **2** rows are identical.

The records are separate stages with different data, **not duplicates**. Example:
`GEM/2025/B/6342811` stores 10 bidders with no prices, while its RA `GEM/2025/R/553651` stores 4 of them with
auction ranks and prices (L1 ₹65,40,000 …).

What this means for the missed groups:

| Status group | Bids (normal / RA) | Rate normal / RA (as pipeline3 stores) | **With participant data** | Note |
|---|---|---|---:|---|
| Financially evaluated | 175,542 / 5,838 | 99% / 100% | **180,126** | 29% of normal bids (40/139) have an *awarded* RA child. The DB has the RA (auction subset) but **not** the parent's full list, so fetching the parent adds a new tender. |
| Fin. evaluated → cancelled | 41,977 / 5,332 | 100% / 100% | **47,309** | |
| Technically evaluated | 33,412 / 0 | 16% / – | **5,322** | |
| Tech. evaluated → cancelled | 55,253 / 0 | 19% / – | **10,710** | |
| Closed, not evaluated | 33,255 / 50,437 | 0% / **0%** | **0** | RA pages show only the parent's table (121/121 from the parent URL); pipeline3 ignores it, so nothing is stored. The parents are awarded (70/121) or fin-evaluated (51/121) and are fetched as their own tenders. |
| Cancelled before evaluation | 72,974 / 972 | 0% / **0%** | **0** | Same as above |
| **Total missed with participant data** | | | **≈ 243,467** | |

Using the lower confidence bounds (89% for the fin groups, ~10–12% for the tech groups), the figure is still
about **212k**.

**Skip rule:** don't scrape RA bids with `b_buyer_status = 0` (≈51k in 2025). They yield no stored participations.

---

## 7. Coverage by bid type and by month (2025)

Bid types overlap, so the rows don't add up to the total. "Missed, has participants" here uses each cell's raw rate,
before the RA stage-0 correction in §6.

| byType | All bids | Fetched today | Missed, has participants | Missed, no participants |
|---|---:|---:|---:|---:|
| product | 814,912 | 438,498 | 232,369 | 144,045 |
| service | 255,410 | 156,832 | 59,474 | 39,104 |
| bidToRA | 170,685 | 108,106 | 62,579 *(≈51k are stage-0 RA bids that only show the parent's table, so pipeline3 stores nothing; really ≈11k)* | 0 |
| custom | 255,528 | 138,951 | 76,800 | 39,777 |
| boq | 203,102 | 119,144 | 62,319 | 21,639 |
| lt (limited tender) | 4,671 | 3,049 | 829 | 793 |
| st (single tender) | 2,481 | 1,766 | 298 | 417 |
| rcbid | 10 | 0 | 7 | 3 |

| Month | All bids | Fetched today | Missed, has participants* | Missed, no participants* |
|---|---:|---:|---:|---:|
| 2025-01 | 96,665 | 55,025 | 23,981 | 17,659 |
| 2025-02 | 97,886 | 55,595 | 24,180 | 18,111 |
| 2025-03 | 96,273 | 54,502 | 23,513 | 18,258 |
| 2025-04 | 60,267 | 32,583 | 17,380 | 10,304 |
| 2025-05 | 74,778 | 41,470 | 21,421 | 11,887 |
| 2025-06 | 82,543 | 46,814 | 22,783 | 12,946 |
| 2025-07 | 99,304 | 55,062 | 27,239 | 17,003 |
| 2025-08 | 93,543 | 50,839 | 25,883 | 16,821 |
| 2025-09 | 97,206 | 53,371 | 26,551 | 17,284 |
| 2025-10 | 88,170 | 48,784 | 23,950 | 15,436 |
| 2025-11 | 84,000 | 46,569 | 23,126 | 14,305 |
| 2025-12 | 99,687 | 54,716 | 27,505 | 17,466 |

\*Year-level group rates applied to each month's group counts, before the RA stage-0 correction (§6).

---

## 8. Database compared with GeM

Read-only session (`default_transaction_read_only = on`). The DB holds the merged `tenders` table (3.10M bids,
2018-05 → 2026-03) and `bid_participations` (39.8M rows). The staging tables are empty (already merged).

### Stored vs GeM, 2025

| Month | Stored in DB | GeM: awarded (pipeline target) | Not stored | % | GeM: all bids |
|---|---:|---:|---:|---:|---:|
| 2025-01 | 54,920 | 55,025 | 105 | 0.19% | 96,665 |
| 2025-02 | 55,506 | 55,595 | 89 | 0.16% | 97,886 |
| 2025-03 | 54,353 | 54,503 | 150 | 0.28% | 96,273 |
| 2025-04 | 32,442 | 32,583 | 141 | 0.43% | 60,267 |
| 2025-05 | 41,311 | 41,470 | 159 | 0.38% | 74,778 |
| 2025-06 | 46,574 | 46,814 | 240 | 0.51% | 82,543 |
| 2025-07 | 54,669 | 55,062 | 393 | 0.71% | 99,304 |
| 2025-08 | 50,421 | 50,839 | 418 | 0.82% | 93,543 |
| 2025-09 | 52,679 | 53,371 | 692 | 1.30% | 97,206 |
| 2025-10 | 47,927 | 48,784 | 857 | 1.76% | 88,170 |
| 2025-11 | 45,471 | 46,570 | 1,099 | 2.36% | 84,000 |
| 2025-12 | 52,781 | 54,716 | 1,935 | 3.54% | 99,687 |
| **2025** | **589,054** | **595,332** | **6,278** | **1.05%** | **1,070,322** |

- **The pipeline captures its own target almost completely (98.9%).** The DB holds **55.0% of all 2025 bids**;
  the other 45% is the status filter.
- The missing share **grows toward recent months**, which fits bids awarded after the crawl. Completed months are
  never re-crawled: pipeline2 picks up only `pending`/`failed` months.
- **Exact check for July 2025:** walking GeM's awarded list (one pass, official sort) and comparing bid IDs found
  387 awarded bids missing from the DB, all `b_status = 2` (awarded, not cancelled). 229 stored bids weren't in the
  walk, about the 235 bids that pass couldn't collect under rate limiting. The scraping loss is therefore small.
- 300 randomly sampled stored bids, looked up on GeM now, were all still awarded and not cancelled (`b_status=2`, `buyer_status=3`).
- **Stored bids with participant rows:** 99.99% for 2025 (e.g. Jan: 54,917 of 54,920).

### Masked names already in the DB

`bid_participations` has **2,205 rows from 2,205 bids** whose seller name is only `*`. They're stored as two
"sellers" whose `seller_id` is literally the mask:

| seller_id / seller_name | Rows (bids) | total_wins (sellers table) |
|---|---:|---:|
| `**********` | 1,378 | 0 |
| `******` | 827 | **824** |

Masked rows per year: 2018: 34 · 2019: 1 · 2021: 974 · 2022: 418 · 2023: 91 · 2024: 504 · **2025: 173** · 2026: 10.
These fake sellers distort seller statistics and competitor views (e.g. "`******`" has 824 wins). Adding
financially evaluated bids would add many more (§5: 7–27% of those bids are fully masked).

---

## 9. Recommendations

### What to fetch, and what to skip

Listing call = pipeline2's existing request with `bidStatusType: "all_bids"` and the `byStatus` below.
Using `all_bids` (not `bidrastatus`) **includes the bids cancelled after reaching that stage**, which still have
participant lists.

| Status | `byStatus` | 2025 bids listed | With participant data | Decision |
|---|---|---:|---:|---|
| Awarded (incl. 88 cancelled after award) | `bid_awarded` | 595,330 | 595,330 | ✅ **Keep** (current filter) |
| Financially evaluated (incl. 47,309 cancelled) | `fin_evaluated` | 228,689 | ≈227,400 | ✅ **Add** |
| Technically evaluated (incl. 55,253 cancelled) | `tech_evaluated` | 88,665 | ≈16,000 | ⚪ **Optional**: low yield (~18%), names + technical status only |
| RA bids with `b_buyer_status = 0` | – (from the listing) | ≈51,400 | 0 stored | ❌ **Skip** scraping |
| Closed, not evaluated (normal bids) | not requested | 33,255 | 0 | ❌ Skip: no table published yet |
| Cancelled before evaluation | not requested | 73,946 | 0 | ❌ Skip: never evaluated |
| Ongoing | – | 0 (2025) | 0 | ❌ Skip: not closed yet |
| **Total fetched with participant data** | | | **≈838,700 of 1,070,322 (78%)** | was 595,330 (56%) |

pipeline3's result-page parser (`get_result_urls` + `BID_RESULTS_JS`) and `extract.py` work unchanged for
financially and technically evaluated bids: the §5 sampling ran exactly this code.

### Pipeline changes

| # | Change | Effect (2025) |
|---|---|---|
| 1 | **Also request `fin_evaluated`** (`all_bids` + `fin_evaluated`, which includes cancelled ones) | **+≈227k bids with participants** from 228,689 listings (~22.9k listing pages). Includes ≈50k parents whose RA child is already stored; under the separate-tender model, the parent is a new tender with its full bidder list. |
| 2 | **Optionally request `tech_evaluated`** | +≈16k bids with participants from 88,665 listings (yield ~18%). Technical table only (names + qualified/disqualified); no prices or ranks. |
| 3 | *Simpler alternative to 1–2:* **one `all_bids` crawl per month** (1.07M listings, 107k pages), then **pipeline3 only scrapes `b_buyer_status ≥ 1`, skipping RA bids at stage 0** | One crawl covers every status and makes it possible to watch status changes. It costs ~17% more listing pages than awarded + fin + tech (107,033 vs 91,269). If tech-stage bids are included, ~72.6k of the scrapes return no table. |
| 4 | **Store `b_status`, `b_buyer_status`, `ra_b_status`, `ra_b_buyer_status`** from the listing | Flags cancelled bids (the 88 cancelled-after-award bids are in the DB today, unflagged) and links RA ↔ parent. |
| 5 | **Treat names matching `^[* ]+$` as unknown sellers**: keep the row (price, rank) but don't create or attach a seller | Removes the two fake sellers (2,205 rows) and keeps them from multiplying. |
| 6 | **Fix `sort`**: use `Bid-End-Date-Oldest`, not `bid_id` | Stable pagination. One pass returns every bid (vs ~0.2% duplicates/misses per pass now), so fewer passes are needed. |
| 7 | **Back off on HTTP 429** | GeM rate-limits bursts. All requests succeeded with retries and back-off. |
| 8 | **Re-crawl recent months** (e.g. the last 6–12 months, monthly) and **re-scrape bids whose stage moved forward** | Catches late awards (1–3.5% of recent months) and upgrades stage 1/2 bids as their participant tables fill in. When a bid's stage advances, **replace** its participation rows (don't append), so a bid that moves from technical to financial to awarded keeps one up-to-date set. |
| 9 | **RA handling**: keep RA and parent as separate tenders (already how pipeline3 works, and the DB check in §6 found no duplicate rows), but **don't scrape RA bids with `b_buyer_status = 0`** | Saves ≈51k scrapes that would store nothing |

---

## 10. Method, limitations and how to re-run

**Method.**
- Counts are exact `numFound` values from GeM.
- Participant rates come from random samples: at least 15–30 bids per cell, topped up to 30–60 where results
  were mixed. Every bid used pipeline3's unchanged parser, with the contract-detail page skipped.
- RA checks: exact-search look-ups of the parents of all sampled RA bids (239), and of the RA children of
  all sampled normal bids (919).
- DB figures come from read-only `SELECT`s; the SQL is saved in `research/data/db_comparison.json`.

**Limitations.**
- Small cells (tech-stage subtypes, `lt`/`st`) have wide confidence intervals.
- The 29% awarded-RA-child share is measured on 139 financially evaluated bids. Masking rates come from 30–60 bids per group.
- Monthly estimates apply year-level rates.
- The causes of masking aren't documented by GeM. It's more frequent on cancelled bids, and every sampled
  cancelled-after-award bid was partly masked.
- `b_status` 1/2 meanings are inferred from data.
- GeM numbers change daily as buyers act on bids.

**Re-run** (from the repo root; outputs go to `research/data/`; `--year` works for any year):

```
venv\Scripts\python.exe -X utf8 research\run_param_audit.py --year 2025     # §3  (~5 min)
venv\Scripts\python.exe -X utf8 research\run_counts.py --year 2025          # §4  (~1 min)
venv\Scripts\python.exe -X utf8 research\run_participants.py --year 2025    # §5  (~11 min, resumable; add --redraw-short to top up)
venv\Scripts\python.exe -X utf8 research\run_db_compare.py --walk-month 2025-07   # §8 (~9 min, read-only)
venv\Scripts\python.exe -X utf8 research\run_report.py --year 2025          # §5–7 tables -> data\report_tables_2025.md
```

The RA checks for §6 run after `run_participants.py` and before `run_report.py`:
`research\run_ra_parent_check.py` (prints parent states) and `research\run_ra_child_check.py`
(writes `data\ra_child_check_2025.json`). Note: `run_report.py`'s "unique vs duplicate" table predates the
separate-tender correction in §6; §6 of this report is the authoritative version.

| File | Contents |
|---|---|
| `data/param_audit.json` | Every parameter test (§3) |
| `data/drift_test.json` | Bid IDs per sort pass (§3) |
| `data/counts_2025.json` | Status × type × month counts + reconciliation (§4) |
| `data/participant_candidates_2025.json` | Sampled listing docs |
| `data/participant_samples_2025.jsonl` | One line per bid check: URLs, HTTP codes, row counts, masked, sample names |
| `data/ra_child_check_2025.json` | RA child look-ups for sampled normal bids (§6) |
| `data/estimates_2025.json`, `data/report_tables_2025.md` | Computed estimates and tables |
| `data/db_comparison.json` | DB queries (SQL + results) and GeM comparison (§8) |
