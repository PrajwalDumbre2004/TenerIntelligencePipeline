"""
Pipeline3 Configuration

This pipeline fetches bid results, RA results, buyer details, and contract details
for tenders already stored in DB by pipeline2. It reads bid_ids from MongoDB,
scrapes the result pages, and updates the existing records.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the root a100 folder (one level up)
load_dotenv(Path(__file__).parent.parent / ".env")

# ═══════════════════════════════════════════════════════════════
# GeM Portal
# ═══════════════════════════════════════════════════════════════
BASE_URL = "https://bidplus.gem.gov.in"

# Date range to process (same as pipeline2)
START_YEAR = 2018
START_MONTH = 6
END_YEAR = 2018
END_MONTH = 12

# ═══════════════════════════════════════════════════════════════
# MongoDB Connection (Atlas)
# ═══════════════════════════════════════════════════════════════
MONGO_URI = os.getenv(
    "MONGO_URI"
)
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "TenderIntelligence")

# ═══════════════════════════════════════════════════════════════
# Worker Settings
# ═══════════════════════════════════════════════════════════════
NUM_WORKERS = 24            # Parallel browser instances
PAGES_PER_BROWSER = 6      # Concurrent pages per browser

# Rate limiting
DELAY_BETWEEN_TENDERS = 0.2  # Seconds between result page fetches
DELAY_ON_ERROR = 5           # Seconds to wait after an error

# Browser settings
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
BROWSER_LOCALE = "en-IN"
BROWSER_VIEWPORT = {"width": 1280, "height": 800}

# Retry settings
MAX_RETRIES = 3

