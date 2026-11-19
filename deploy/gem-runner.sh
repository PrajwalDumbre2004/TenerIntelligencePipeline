#!/usr/bin/env bash
# deploy/gem-runner.sh -- runs the scraping container on the Spot VM (R4).
#
# Started once per boot by gem-runner.service. Runs the image's default command
# (pipeline2 + pipeline3) and decides what happens when it exits:
#
#   exit 0      run finished (or nothing to do) -> tag the VMSS cycle_done=<YYYY-MM>
#               and scale it to 0, so billing stops and the Logic App leaves it alone
#               until next month.
#   exit 75     blocked until a merge finishes (see pipeline_main/spot_run.py) ->
#               scale to 0 WITHOUT the tag, so the hourly Logic App starts it again.
#               Not a crash.
#   other       crash -> wait 60 s and run again (it resumes from the DB). After
#               MAX_RETRIES retries in this boot: tag cycle_done=<YYYY-MM>-failed
#               (the Logic App stops restarting it) and scale to 0. Re-trigger by
#               hand once fixed: remove the tag, set capacity to 1.
#
# An eviction is not an exit: the VM is deleted with this script, and a restored
# VM starts again at attempt 1. So evictions never count toward the limit.
#
# Config: /etc/gem/runner.conf (GEM_IMAGE), /etc/gem/env (container env:
# POSTGRES_DSN, P2_NUM_WORKERS, ...; plain KEY=value lines, no quotes).
# Needs docker and the az CLI; az logs in with the VMSS's managed identity.

set -u

MAX_RETRIES=3
RETRY_DELAY=${RETRY_DELAY:-60}
CONTAINER_ENV=/etc/gem/env
IMDS="http://169.254.169.254/metadata/instance/compute"

log() { echo "[gem-runner $(date -u '+%Y-%m-%d %H:%M:%S')] $*"; }

# Read one field of this VM's metadata (resource group, scale set name).
imds() { curl -sf --max-time 5 -H Metadata:true "$IMDS/$1?api-version=2021-02-01&format=text"; }

# Tag the scale set (unless tag is empty) and scale it to 0 (this deletes this
# VM, so it is the last step).
finish() {
    local tag="$1"
    local rg vmss
    rg=$(imds resourceGroupName) && vmss=$(imds vmScaleSetName) || {
        log "Not on an Azure VMSS (no instance metadata); skipping tag + scale-in."
        return
    }
    az login --identity --output none || { log "az login --identity failed"; return 1; }
    if [ -n "$tag" ]; then
        log "Tagging $vmss cycle_done=$tag"
        az vmss update -g "$rg" -n "$vmss" --set "tags.cycle_done=$tag" --output none
    fi
    log "Scaling $vmss to 0"
    az vmss scale -g "$rg" -n "$vmss" --new-capacity 0 --no-wait --output none
}

attempt=0
while true; do
    attempt=$((attempt + 1))
    log "Starting run (attempt $attempt, image ${GEM_IMAGE})"
    docker rm -f gem-run >/dev/null 2>&1
    docker run --rm --init --name gem-run --env-file "$CONTAINER_ENV" "$GEM_IMAGE"
    code=$?
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
