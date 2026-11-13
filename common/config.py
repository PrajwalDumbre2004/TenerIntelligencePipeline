"""
Shared configuration for the PostgreSQL-backed pipelines.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the repo root
load_dotenv(Path(__file__).parent.parent / ".env")

POSTGRES_DSN = os.getenv("POSTGRES_DSN")

# ═══════════════════════════════════════════════════════════════
# Date range — single source of truth for pipeline2 and pipeline3
# full-range runs. pipeline_main/run.py drives both pipelines using
# these same values (overridable there via CLI args).
# ═══════════════════════════════════════════════════════════════
START_YEAR = 2026
START_MONTH = 1
END_YEAR = 2026
END_MONTH = 1
