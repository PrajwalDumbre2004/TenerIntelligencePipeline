#!/usr/bin/env bash
# deploy/gem-runner.sh -- runs the scraping container on the Spot VM (R4).
#
# Started once per boot by gem-runner.service. Before every attempt it reads the
# scale set's tag run_args:
#   empty / no tag   -> the image's default command: the monthly run
#   e.g. "--backfill --months 2025-01..2025-12"
#                    -> python3 pipeline_main/spot_run.py <run_args>: one backfill batch
# Then it decides what happens when the container exits:
#
#   exit 0      run finished (or nothing to do) -> tag the VMSS cycle_done=<YYYY-MM>
#               and scale it to 0, so billing stops and the Logic App leaves it alone
#               (until next month, or until the next backfill batch is queued).
#   exit 75     blocked until a merge finishes (see pipeline_main/spot_run.py) ->
#               scale to 0 WITHOUT the tag, so the hourly Logic App starts it again.
#               Not a crash.
#   other       crash -> wait 60 s and run again (it resumes from the DB). After
#               MAX_RETRIES retries in this boot: tag cycle_done=<YYYY-MM>-failed
#               (the Logic App stops restarting it) and scale to 0. Re-trigger by
#               hand once fixed: remove the tag (or set it to "backfill" during the
#               backfill), set capacity to 1.
#
# If run_args cannot be read (Azure API error), the attempt counts as a crash
# and is retried: it must never fall back to a monthly run in the middle of a
# backfill.
#
# An eviction is not an exit: the VM is deleted with this script, and a restored
# VM starts again at attempt 1. So evictions never count toward the limit.
#
# Config: /etc/gem/runner.conf (GEM_IMAGE), /etc/gem/env (container env:
# POSTGRES_DSN, P2_NUM_WORKERS, ...; plain KEY=value lines, no quotes).
# Needs docker and the az CLI; az logs in with the VMSS's managed identity
# (Virtual Machine Contributor on the scale set: read tags, set tags, scale).

set -u

MAX_RETRIES=3
RETRY_DELAY=${RETRY_DELAY:-60}
CONTAINER_ENV=/etc/gem/env
IMDS="http://169.254.169.254/metadata/instance/compute"

log() { echo "[gem-runner $(date -u '+%Y-%m-%d %H:%M:%S')] $*"; }

# Read one field of this VM's metadata (resource group, scale set name).
imds() { curl -sf --max-time 5 -H Metadata:true "$IMDS/$1?api-version=2021-02-01&format=text"; }

# Which scale set this VM belongs to; empty when not on an Azure VMSS (local test).
RG=$(imds resourceGroupName) && VMSS=$(imds vmScaleSetName) || { RG=""; VMSS=""; }
if [ -n "$VMSS" ]; then
    az login --identity --output none || log "az login --identity failed (will retry per call)"
else
    log "Not on an Azure VMSS (no instance metadata): no run_args, no tag, no scale-in."
fi

# The scale set's run_args tag ("" when the tag is absent). Fails if Azure
# can't be asked, so the caller can retry instead of guessing.
read_run_args() {
    [ -n "$VMSS" ] || return 0
    az vmss show -g "$RG" -n "$VMSS" --query "tags.run_args" -o tsv
}

# Tag the scale set (unless tag is empty) and scale it to 0 (this deletes this
# VM, so it is the last step).
finish() {
    local tag="$1"
    [ -n "$VMSS" ] || return 0
    if [ -n "$tag" ]; then
        log "Tagging $VMSS cycle_done=$tag"
        az vmss update -g "$RG" -n "$VMSS" --set "tags.cycle_done=$tag" --output none
    fi
    log "Scaling $VMSS to 0"
    az vmss scale -g "$RG" -n "$VMSS" --new-capacity 0 --no-wait --output none
}

attempt=0
while true; do
    attempt=$((attempt + 1))
    docker rm -f gem-run >/dev/null 2>&1

    if ! RUN_ARGS=$(read_run_args); then
        log "Could not read the run_args tag (attempt $attempt)."
        code=1
    elif [ -z "$RUN_ARGS" ]; then
        log "Starting monthly run (attempt $attempt, image ${GEM_IMAGE})"
        docker run --rm --init --name gem-run --env-file "$CONTAINER_ENV" "$GEM_IMAGE"
        code=$?
    else
        log "Starting run with: $RUN_ARGS (attempt $attempt, image ${GEM_IMAGE})"
        # $RUN_ARGS unquoted on purpose: split into separate arguments
        docker run --rm --init --name gem-run --env-file "$CONTAINER_ENV" "$GEM_IMAGE" \
            python3 pipeline_main/spot_run.py $RUN_ARGS
        code=$?
    fi
    month=$(date -u +%Y-%m)

    if [ "$code" -eq 0 ]; then
        log "Run finished (exit 0)."
        finish "$month"
        exit 0
    fi

    if [ "$code" -eq 75 ]; then
        log "Waiting for a merge to finish (exit 75); the Logic App restarts this later."
        finish ""
        exit 0
    fi

    log "Run crashed (exit $code)."
    if [ "$attempt" -gt "$MAX_RETRIES" ]; then
        log "Giving up after $MAX_RETRIES retries in this boot."
        finish "$month-failed"
        exit 1
    fi
    log "Retrying in ${RETRY_DELAY}s..."
    sleep "$RETRY_DELAY"
done
