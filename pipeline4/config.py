"""
Pipeline4 Configuration

This pipeline reads newly scraped tenders (derived_at=None) from
TenderIntelligence.tenders_YYYY, extracts participants, and incrementally
updates seller analytics in TenderAnalytics.
"""
import os
import re
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# ═══════════════════════════════════════════════════════════════
# MongoDB Connection
# ═══════════════════════════════════════════════════════════════
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
SOURCE_DB = os.getenv("MONGO_DB_NAME", "TenderIntelligence")
TARGET_DB = os.getenv("ANALYTICS_DB_NAME", "TenderAnalytics")

# ═══════════════════════════════════════════════════════════════
# Collection names
# ═══════════════════════════════════════════════════════════════
TENDER_COLLECTION_PATTERN = re.compile(r"^tenders_(\d{4})$")

COLL_SELLERS = "sellers"
COLL_SELLER_TENDERS = "seller_tenders"
COLL_SELLER_BUYERS = "seller_buyers"
COLL_SELLER_CATEGORIES = "seller_categories"
COLL_SELLER_COMPETITORS = "seller_competitors"

# ═══════════════════════════════════════════════════════════════
# Processing settings
# ═══════════════════════════════════════════════════════════════
BATCH_SIZE = 500          # Tenders to fetch per batch from source
MONGO_WRITE_BATCH = 5000  # Docs per bulk_write call
WORKERS = 8               # Threads for seller recomputation
PRINT_EVERY = 50          # Print progress every N sellers
