"""
Pipeline3 Entry Point.

Usage:
    python -m pipeline3.run              # Process all months in config range
    python -m pipeline3.run 2024-01      # Process a specific month only
"""
import sys
import os
import asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline3.orchestrator import run_pipeline, process_month


def main():
    if len(sys.argv) > 1:
        month_str = sys.argv[1]
        print(f"  Single-month mode: {month_str}")
        asyncio.run(process_month(month_str))
    else:
        asyncio.run(run_pipeline())


if __name__ == "__main__":
    main()
