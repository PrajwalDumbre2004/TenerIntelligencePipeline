# Pipeline Architecture — End-to-End Working

## Theoretical Foundation

### Problem Domain

The Indian Government e-Marketplace (GeM) portal publishes public tender data but provides limited analytical capabilities. The raw data is spread across listing pages (what tenders exist) and detail pages (who participated, who won). To answer business questions like *"Which sellers win most in a category?"* or *"Who are this seller's top competitors?"*, raw data must be extracted, joined, and aggregated into derived forms.

### Architectural Philosophy

The system follows a **staged ETL** (Extract-Transform-Load) pattern with three principles:

| Principle                                     | Meaning                                                                                                                                                                               |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Separation of concerns**              | Each pipeline handles one distinct transformation stage. Pipeline2 only lists, Pipeline3 only enriches, Pipeline4 only derives. They can be developed, tested, and run independently. |
| **Incremental processing**              | Each pipeline tracks its progress via database markers (`scraped_at`, `derived_at`). Re-runs only process unmarked data — no need to reprocess history.                          |
| **Full recompute from canonical state** | Pipeline4 reads only new tenders but recomputes all analytics from the complete`seller_tenders` history. This prevents aggregate drift across incremental runs.                     |

### Data Flow Concept

```
Raw ──────► Enriched ──────► Derived
 │              │                │
 │              │                └── Who won? What are the patterns?
 │              │
 │              └── Who participated? What were the prices?
 │
 └── What tenders exist? What are their basic attributes?
```

Each stage adds a layer of meaning:

- **Raw (Pipeline2)**: Captures existence — *"Tender #123 exists in category X"*
- **Enriched (Pipeline3)**: Captures detail — *"Tender #123 had sellers A, B, C with prices P1, P2, P3"*
- **Derived (Pipeline4)**: Captures insight — *"Seller A wins 70% in category X"*

### Why 3 Separate Pipelines?

| Reason                        | Explanation                                                                                                                                                |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Failure isolation**   | If result scraping fails, listings are still saved. If analytics fail, raw+enriched data is preserved.                                                     |
| **Independent scaling** | Pipeline2 uses synchronous multi-process workers, Pipeline3 uses async concurrency (144 pages parallel). Different bottlenecks → different architectures. |
| **Debugging clarity**   | Each stage has dedicated status tracking and error logging. A failure in Phase 3 of Pipeline4 doesn't block Pipeline2 from scraping the next month.        |

---

## Overview

The system scrapes tender data from the Indian Government e-Marketplace (GeM) portal and transforms it into derived analytics collections. It consists of 3 sequential pipelines orchestrated by `pipeline_main/run.py`, processing month-by-month.

```
Source: GeM Portal (bidplus.gem.gov.in)
  │
  ▼
Pipeline 2 ──► TenderIntelligence DB (tenders_YYYY)
  │
  ▼
Pipeline 3 ──► TenderIntelligence DB (tenders_YYYY — enriched with results)
  │
  ▼
Pipeline 4 ──► TenderAnalytics DB (sellers, seller_tenders, seller_buyers,
                                     seller_categories, seller_competitors)
```

---

## Step 1: Pipeline2 — Listing Extraction

**Folder:** `pipeline2/`
**Purpose:** Scrape all bid listing data from the GeM API for a given month range.

### Theoretical Context

Pipeline2 solves the **exhaustive enumeration** problem. The GeM portal exposes a **search API** (`/bidplus/searchbydate`) that returns paginated JSON results (10 items per page), and the total count (`numFound`) is known upfront from the API response. The challenge is that pagination is **eventually consistent** — newly added tenders can shift page boundaries between passes. The solution is a **multi-pass converge** strategy: repeatedly call the API for all pages until the cumulative unique count matches `numFound`. Each pass uses `$setOnInsert` so only new `bid_id` values are stored, making the process idempotent.

This is a **capture-the-full-set** pattern, common in systems where the source API does not guarantee a stable snapshot.

### API Used

| Detail               | Value                                                                  |
| -------------------- | ---------------------------------------------------------------------- |
| **Endpoint**   | `https://bidplus.gem.gov.in/bidplus/searchbydate`                    |
| **Method**     | POST with form data (CSRF token, date range, page number, sort order)  |
| **Response**   | JSON with`numFound` (total count) and `data` array of bid listings |
| **Client**     | Playwright (for CSRF token capture) with`fetch_page()` function      |
| **Pagination** | 10 results per page, sorted by`bid_id` ascending                     |

### How it works

1. **Month Selection** — Iterates through months from `START_YEAR-START_MONTH` to `END_YEAR-END_MONTH`. Each month is tracked in the `pipeline2_status` collection.
2. **numFound** — For each month, makes a single API call to get the total tender count (`numFound`). Computes expected pages (10 tenders per page, hard-locked by GeM).
3. **Multi-Pass API Calls** — Runs up to `MAX_PASSES` (default 15) passes per month:

   - Divides all pages among `NUM_WORKERS` (24) parallel browser workers via round-robin
   - Each worker uses Playwright to POST to the search API and parse the JSON response
   - Workers extract basic listing fields: `bid_id`, `bid_number`, `category`, `department`, `ministry`, `end_date`, etc.
4. **Deduplication via `$setOnInsert`** — Each pass bulk-upserts results into the year-based collection `TenderIntelligence.tenders_YYYY`. Uses `UpdateOne` with `{"$setOnInsert": doc}` keyed on `bid_id` — **only inserts new documents, never overwrites existing ones**. The unique+sparse index on `bid_id` enforces this at the DB level.
5. **Completion Check** — After each pass, queries the actual document count in the DB. If `total_inserted >= numFound`, the month is marked complete. Otherwise, another pass runs.
6. **Month Status** — `pipeline2_status` tracks: `pending → in_progress → completed`. Crashed runs reset `in_progress` months back to `pending` on startup.

### Key characteristics

- **Idempotent** — Re-running only inserts missing `bid_id` values
- **Multi-pass** — Compensates for pagination drift and transient failures
- **No results scraping** — Only captures listing-level data

---

## Step 2: Pipeline3 — Result Extraction

**Folder:** `pipeline3/`
**Purpose:** Fetch detailed bid results, RA results, buyer details, and contract details for tenders already stored by Pipeline2.

### Theoretical Context

Pipeline3 addresses the **one-to-many enrichment** problem. While Pipeline2 only gets the listing-level data (existence + basic attributes), Pipeline3 calls **backend JSON API endpoints** (maintaining session via Playwright) to extract structured result data — technical evaluations, financial bids, buyer info, and contract details.

Each tender's results are fetched via JavaScript `fetch()` calls executed inside a Playwright browser context (to preserve authentication/session cookies). The endpoints are independent per tender, making the problem **embarrassingly parallel** and well-suited to async concurrency.

The key design choice is **per-tender granularity**: each tender is fetched, parsed, and stored independently. Failed tenders are logged and retried in subsequent passes, while successful ones are marked with `scraped_at` so Pipeline4 knows they are ready. This avoids the need for complex checkpointing — the DB itself serves as the state store.

The RA vs Bid precedence logic handles a domain-specific nuance: revised/re-tendered awards (`/R/` or `/RA/` in bid number) supersede the original bid results.

### Endpoints Used

| Data                                  | Endpoint                                                             | Method          | Notes                                                        |
| ------------------------------------- | -------------------------------------------------------------------- | --------------- | ------------------------------------------------------------ |
| **Bid Results**                 | `{BASE_URL}/bidding/bid/getBidResultView/{bid_id}`                 | JS`fetch()`   | Main result view for technical + financial evaluation        |
| **Bid Results (schedule)**      | `{BASE_URL}/bidding/bid/getBidResultViewSchedule/{bid_id}`         | JS`fetch()`   | Used when`bid_type=2` or `eval_type>0`                   |
| **Bid Results (single packet)** | `{BASE_URL}/bidding/bid/getSinglePacketResultView/{bid_id}`        | JS`fetch()`   | Used when`single_packet=1`                                 |
| **Parent bid results**          | `{BASE_URL}/bidding/bid/getBidResultView/{bid_id_parent}`          | JS`fetch()`   | For RA-converted bids, parent's result is also fetched       |
| **RA Schedules**                | `{BASE_URL}/list-ra-schedules/{bid_id}`                            | JS`fetch()`   | Dedicated RA endpoint                                        |
| **RA Fallback**                 | `{BASE_URL}/showradocumentPdf/{bid_id}`                            | JS`fetch()`   | PDF-based fallback for RA data                               |
| **Contract Details**            | `https://gem.gov.in/view_contracts/bid_detail?bid_no={bid_number}` | `page.goto()` | Navigates to a different domain (gem.gov.in) and parses HTML |

### Data Extracted

| Section                    | Fields Extracted                                                                                                                  |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| **Bid Result**       | Technical evaluation (seller names, MSE Social Category, technical status), Financial evaluation (rank`L1/L2/...`, total price) |
| **RA Result**        | Same structure as Bid Result — used when bid_number contains`/R/` or `/RA/`                                                  |
| **Buyer Details**    | Buyer name, department, address, contact (embedded in bid result response)                                                        |
| **Contract Details** | Contract number, status, seller name, organization, ministry, department, contract date, total value, product line items          |

### How it works

1. **Find Pending Tenders** — Queries `TenderIntelligence.tenders_YYYY` for tenders where `scraped_at` is `None` (not yet processed by Pipeline3).
2. **Async Worker Pool** — Spawns `NUM_WORKERS` (24) Playwright browser instances, each with `PAGES_PER_BROWSER` (6) concurrent pages. Total concurrency: 24 × 6 = 144 concurrent requests.
3. **Per-Tender Fetching** — Each worker executes `BID_RESULTS_JS` (in-browser JavaScript) which calls the bid result endpoints via `fetch()` with session cookies, parses the returned HTML/JSON, and extracts structured data back to Python.
4. **RA vs Bid Logic** — If `bid_number` contains `/R/` or `/RA/`, the RA result takes precedence over the bid result during extraction.
5. **DB Update** — Updates the tender document in-place with all scraped fields and sets `scraped_at` timestamp.
6. **Retry** — Up to `MAX_P3_PASSES` (3) passes for failed/residual tenders. Failed tenders are logged in `failed_tenders` collection for manual retry.

### Key characteristics

- **Async-first** — Uses Playwright async API with high concurrency
- **Per-tender granularity** — Each tender's detail page is fetched independently
- **Sets `scraped_at`** — Marks completion so Pipeline4 knows which tenders are ready

---

## Step 3: Pipeline4 — Derived Analytics

**Folder:** `pipeline4/`
**Purpose:** Extract participants from scraped tenders and build derived analytics collections in the TenderAnalytics database.

### Theoretical Context

Pipeline4 solves the **data normalization and aggregation** problem. The raw tenders store participants embedded inside nested `bid_result` and `ra_result` objects. To answer analytical questions, this nested data must be **flattened** (one record per seller-per-tender) and then **aggregated** (per-seller profiles, buyer relationships, category performance, competitor networks).

The key architectural pattern is **incremental read + full recompute**:

- Extract is incremental (only new tenders)
- Aggregation is a full recompute from the canonical `seller_tenders` collection

This is a deliberate trade-off: recomputing aggregates from full history is more expensive per seller, but guarantees **correctness** — no risk of accumulating errors from incremental updates. The `seller_tenders` collection acts as a **single source of truth** that all derived views are rebuilt from.

The 3-phase design reflects the data's natural dependency chain:

1. **Extract** creates the canonical records
2. **Aggregates** summarize those records
3. **Competitors** are a secondary derived view over the same records

This separation allows Phase 3 to be skipped when speed is needed, while still keeping the core profile aggregates up to date.

### How it works — 3 Phases

#### Phase 1: Extract Participants (`process_new_tenders`)

1. **Discover Collections** — Finds all `tenders_YYYY` collections in `TenderIntelligence`.
2. **Fetch Underived Tenders** — Batches through tenders where `derived_at=None` AND `scraped_at!=None` (scraped by Pipeline3 but not yet processed).
3. **Participant Extraction** — For each tender, calls `extract_participants()` which:
   - Normalizes seller names (strips MSE/MII suffixes, uppercase)
   - Determines RA vs Bid precedence (same regex as Pipeline3)
   - Parses technical evaluation → seller name, MSE status, technical status
   - Parses financial evaluation → rank, total price
   - Detects winner (L1-ranked seller)
   - Classifies outcome: `won` | `lost` | `disqualified` | `no_result`
4. **Upsert to `seller_tenders`** — All participation records are bulk-upserted into `TenderAnalytics.seller_tenders` (canonical source of truth), keyed on `(seller_id, bid_id)`.
5. **Mark Derived** — Each batch is immediately stamped with `derived_at` timestamp so it won't be reprocessed.

#### Phase 2: Recompute Aggregates (`recompute_seller_aggregates`)

For each impacted seller (in parallel via 8 threads):

1. Reads their **full** history from `seller_tenders`
2. Recomputes from scratch:
   - **`sellers`** — Profile card: total tenders, wins, win rate, contract value, top categories, top buyers, first/last seen
   - **`seller_buyers`** — Per-buyer stats: tenders participated, won, value, categories, win rate
   - **`seller_categories`** — Per-category stats: tenders, wins, average price, monthly breakdown
3. Writes all three collections via upsert (stale docs are cleaned up)

#### Phase 3: Recompute Competitors (`recompute_competitors`)

1. For each impacted seller, finds all their `bid_id` values
2. Queries `seller_tenders` for co-participants (other sellers who bid on the same tenders)
3. Counts shared tenders per competitor
4. Also recomputes competitors for co-participants (they gained a new competitor too)
5. Upserts into `seller_competitors` as `{seller_id, competitors: [{competitor_id, shared_tenders}]}`

### Design Decisions

| Decision                                   | Rationale                                                                            |
| ------------------------------------------ | ------------------------------------------------------------------------------------ |
| **Incremental read, full recompute** | Reads only new tenders, but recomputes aggregates from ALL history — prevents drift |
| **Immediate marking**                | `derived_at` is set mid-Phase 1 so partial runs don't reprocess                    |
| **Standalone extract**               | `extract.py` is a self-contained copy — no dependency on `derived_collections/` |
| **Stale cleanup**                    | Upserts delete docs that no longer exist in the data                                 |

---

## Orchestration (`pipeline_main/run.py`)

Processes months sequentially:

```
for each month in [2018-06, 2018-07, ..., 2024-12]:
    Step 1: Pipeline2 — Scrape all listings for this month
    Step 2: Pipeline3 — Scrape results for all tenders in this month
    Step 3: Pipeline4 — Extract participants & update TenderAnalytics
```

### CLI Usage

```bash
# Full run (pre-configured date range)
python pipeline_main/run.py

# Custom date range
python pipeline_main/run.py 2024-01 2024-06

# Single month
python pipeline_main/run.py 2024-01
```

Pipeline4 can also be run independently:

```bash
python -m pipeline4.run                           # Full 3 phases
python -m pipeline4.run --skip-competitors         # Skip Phase 3
python -m pipeline4.run --phase1-only              # Extract only
python -m pipeline4.run --recompute-only SELLER_ID # Manual recompute
```

---

## Database Schema

### TenderIntelligence (Source)

| Collection           | Purpose                                                                                                                                                                                                               |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tenders_YYYY`     | Raw tender documents, partitioned by year. Fields:`bid_id`, `bid_number`, `category`, `month`, `department`, `ministry`, `bid_result`, `ra_result`, `buyer_details`, `scraped_at`, `derived_at` |
| `pipeline2_status` | Per-month progress tracking for Pipeline2                                                                                                                                                                             |
| `failed_pages`     | Pages that failed during Pipeline2 scraping                                                                                                                                                                           |
| `failed_tenders`   | Tenders that failed during Pipeline3 result scraping                                                                                                                                                                  |
| `error_logs`       | All error events across pipelines                                                                                                                                                                                     |

### TenderAnalytics (Target)

| Collection             | Key                       | Purpose                                                            |
| ---------------------- | ------------------------- | ------------------------------------------------------------------ |
| `seller_tenders`     | `(seller_id, bid_id)`   | Canonical flat list of all seller↔tender participation records    |
| `sellers`            | `seller_id`             | Per-seller profile card (tenders, wins, rates, categories, buyers) |
| `seller_buyers`      | `(seller_id, buyer_id)` | Per-seller↔buyer aggregation stats                                |
| `seller_categories`  | `(seller_id, category)` | Per-seller↔category aggregation with monthly breakdown            |
| `seller_competitors` | `seller_id`             | Per-seller competitor list with shared tender counts               |

---

## One-Time Backfill

If you have data in `TenderAnalytics` that was created before Pipeline4 existed (via scripts in `derived_collections/`), those tenders in `TenderIntelligence` won't have `derived_at` set. Run the backfill script to mark them:

```bash
# Preview
python -m pipeline4.backfill_derived --dry-run

# Apply
python -m pipeline4.backfill_derived
```

---

## Summary: End-to-End Conceptual Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        GeM Portal                                   │
│               (bidplus.gem.gov.in)                                   │
└──────────┬──────────────────────────────────────┬──────────────────┘
           │                                      │
           ▼                                      ▼
   ┌───────────────┐                    ┌───────────────────┐
   │  Listing API   │                    │  Detail Pages     │
   │  (paginated)   │                    │  (per-tender)     │
   └───────┬───────┘                    └────────┬──────────┘
           │                                     │
           ▼                                     ▼
   ┌───────────────────────────────────────────────────────────────┐
   │                 TenderIntelligence DB                          │
   │                                                               │
   │   tenders_YYYY  ────  Raw bid listings                        │
   │      │ bid_result    (nested technical/financial data)         │
   │      │ ra_result     (revised award data)                     │
   │      │ scraped_at    (Pipeline3 checkpoint)                    │
   │      │ derived_at    (Pipeline4 checkpoint)                    │
   └──────────────────────┬────────────────────────────────────────┘
                          │
                          ▼ extract_participants()
                          │
              ┌───────────┴───────────┐
              │                       │
              ▼                       ▼
   ┌──────────────────┐   ┌──────────────────────┐
   │ seller_tenders   │   │   Derived Views       │
   │ (canonical flat  │   │                       │
   │  records)        │──►│  sellers              │
   │                  │   │  seller_buyers        │
   │ (seller_id,      │   │  seller_categories    │
   │  bid_id, rank,   │   │  seller_competitors   │
   │  price, outcome) │   │                       │
   └──────────────────┘   └──────────────────────┘
        ▲                        ▲
        │                        │
        └─── full recompute ─────┘
             (from canonical)
```

### Key Design Trade-offs

| Trade-off                   | Chosen Approach                                    | Alternative                                                   |
| --------------------------- | -------------------------------------------------- | ------------------------------------------------------------- |
| **When to derive**    | On write (pipeline processes new data immediately) | On read (compute analytics at query time) — chosen for speed |
| **How to aggregate**  | Full recompute from canonical history              | Incremental update (delta merge) — chosen for correctness    |
| **Storage model**     | Separate DB (TenderAnalytics)                      | Same DB with materialized views — chosen for isolation       |
| **Processing order**  | Sequential month-by-month                          | Parallel per month — chosen for stability                    |
| **Scraping strategy** | Multi-pass until all captured                      | Single pass with error handling — chosen for completeness    |
