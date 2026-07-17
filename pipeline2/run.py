"""
Pipeline2 Entry Point.

Usage:
    python -m pipeline2.run              # Process all pending months
    python -m pipeline2.run 2026-02      # Process a specific month only
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline2.orchestrator import run_pipeline, process_month
from pipeline2.db import create_indexes, init_month_status


def main():
    if len(sys.argv) > 1:
        # Process a single specific month
        month_str = sys.argv[1]
        print(f"  Single-month mode: {month_str}")
        create_indexes()
        init_month_status(month_str)
        process_month(month_str)
    else:
        # Full pipeline run
        run_pipeline()


if __name__ == "__main__":
    main()
