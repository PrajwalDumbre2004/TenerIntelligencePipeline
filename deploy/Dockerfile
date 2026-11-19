# Image for the scraping job: pipeline2 + pipeline3 (pipeline_main/run.py).
# Used for the local memory tests (deploy/run_memtest.py) and later for the
# Azure Container Apps Job. pipeline4 runs inside the database, so no psql here.
#
# Build from the repo root:  docker build -f deploy/Dockerfile -t tender-pipeline:local .
# The base image's Playwright version must match the playwright pin in requirements.
FROM mcr.microsoft.com/playwright/python:v1.59.0-noble

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_BREAK_SYSTEM_PACKAGES=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY deploy/requirements.txt deploy/requirements.txt
RUN python3 -m pip install -r deploy/requirements.txt

COPY common/ common/
COPY pipeline2/ pipeline2/
COPY pipeline3/ pipeline3/
COPY pipeline_main/ pipeline_main/
COPY deploy/memprobe.py deploy/memprobe.py

# Scheduled runs: status in ops.pipeline_runs, resumes after a restart.
# For a manual run, override the command: ... python3 pipeline_main/run.py 2026-03
CMD ["python3", "pipeline_main/spot_run.py"]
