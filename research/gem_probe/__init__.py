"""
Research toolkit for probing GeM's /all-bids-data API and bid result pages.

Read-only against GeM and (optionally) against the project's Postgres database.
Used by research/run_*.py to produce research/gem_bid_coverage_2025.md.
"""
import os
import sys

# Make the project root importable (pipeline2 / pipeline3 / common) when scripts run from research/.
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

DATA_DIR = os.path.join(_ROOT, "research", "data")
os.makedirs(DATA_DIR, exist_ok=True)
