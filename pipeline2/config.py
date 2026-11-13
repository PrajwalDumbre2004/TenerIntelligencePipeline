"""
Pipeline2 Configuration

This pipeline focuses ONLY on extracting all unique bid listing data from the
GeM API using multi-pass approach. Results/details fetching comes later.
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

# Date range to scrape — see common/config.py (shared with pipeline3
# and pipeline_main/run.py so all three stay in sync)

# ═══════════════════════════════════════════════════════════════
# PostgreSQL Connection
# ═══════════════════════════════════════════════════════════════
POSTGRES_DSN = os.getenv("POSTGRES_DSN")

# ═══════════════════════════════════════════════════════════════
# Worker Settings
# ═══════════════════════════════════════════════════════════════
NUM_WORKERS = 10          # Parallel browser workers per pass

# Multi-pass settings
MAX_PASSES = 15             # Max passes per month before moving on
SORT_BY = "bid_id"          # Stable sort to reduce pagination drift

# Rate limiting
DELAY_BETWEEN_PAGES = 0.1   # Seconds between page requests
DELAY_BETWEEN_PASSES = 3    # Seconds between consecutive passes

# Browser settings
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
BROWSER_LOCALE = "en-IN"
BROWSER_VIEWPORT = {"width": 1280, "height": 800}

# Pagination
PAGE_SIZE = 10  # GeM hard-locks at 10 per page
