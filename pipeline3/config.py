"""
Pipeline3 Configuration

This pipeline fetches bid results, RA results, buyer details, and contract details
for tenders already staged in Postgres by pipeline2. It reads bid_ids from
staging.tenders_raw, scrapes the result pages, and updates the existing rows.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

from common.config import START_YEAR, START_MONTH, END_YEAR, END_MONTH

# Load .env from the root a100 folder (one level up)
load_dotenv(Path(__file__).parent.parent / ".env")

# ═══════════════════════════════════════════════════════════════
# GeM Portal
# ═══════════════════════════════════════════════════════════════
BASE_URL = "https://bidplus.gem.gov.in"

# Date range to process — see common/config.py (shared with pipeline2
# and pipeline_main/run.py so all three stay in sync)

# ═══════════════════════════════════════════════════════════════
# PostgreSQL Connection
# ═══════════════════════════════════════════════════════════════
POSTGRES_DSN = os.getenv("POSTGRES_DSN")

# ═══════════════════════════════════════════════════════════════
# Worker Settings
# ═══════════════════════════════════════════════════════════════
NUM_WORKERS = int(os.getenv("P3_NUM_WORKERS", "10"))              # Parallel browser instances
PAGES_PER_BROWSER = int(os.getenv("P3_PAGES_PER_BROWSER", "3"))   # Concurrent pages per browser (was 6: 60 parallel pages
                           # got throttled by GeM; 30 is being tested)

# Rate limiting
DELAY_BETWEEN_TENDERS = 0.2  # Seconds between result page fetches
DELAY_ON_ERROR = 5           # Seconds to wait after an error

# Browser settings
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
BROWSER_LOCALE = "en-IN"
BROWSER_VIEWPORT = {"width": 1280, "height": 800}

# Retry settings
MAX_RETRIES = 3

# ═══════════════════════════════════════════════════════════════
# Derivation (participant extraction, runs after scraping per month)
# ═══════════════════════════════════════════════════════════════
DERIVE_BATCH_SIZE = 2000   # Scraped tenders to flatten into participations per batch

