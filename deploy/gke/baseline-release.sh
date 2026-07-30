#!/usr/bin/env bash
# Baseline a released Dgraph across topologies on GKE, on one reused cluster.
#
# Runs `benchmark.sh baseline` for each of single / ha3 / shard3 / ha5 in turn,
# reusing a single cluster (the dedicated Dgraph node pool is resized to match
# each topology) and appending one ledger line per repeat to results/gke/runs.jsonl.
# Configs run in ascending node-count order (1, 3, 3, 5) so the pool only grows.
#
# Each config is independent: if one fails the sweep keeps going and the cluster
# is kept at the end for debugging. On full success the cluster is torn down
# (disks included) unless --keep-cluster is given.
#
# Usage:
#   ./baseline-release.sh v25.4.0
#   ./baseline-release.sh --version v25.4.0 -z us-east1-c -m n2-standard-8 --keep-cluster
#
# Note: ha5 needs 5 Dgraph nodes; with the argus + default pools that's ~48 vCPU
# in one region. If the ha5 step fails on QUOTA, the earlier baselines still land —
# request a bump or drop ha5 from --configs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LEDGER="$REPO_ROOT/results/gke/runs.jsonl"

log()  { echo "$@" >&2; }
step() { log ""; log "===== $* ====="; }
die()  { log "ERROR: $*"; exit 1; }

VERSION=""
CONFIGS=(single ha3 shard3 ha5)
PROJECT="${PROJECT:-dgraph-dev-sandbox}"
ZONE="us-east1-c"
MACHINE="n2-standard-8"
ALPHA_CPU="6"
ALPHA_MEM="24Gi"
CLUSTER="dgraph-bench"
IMAGE_REPO=""          # empty => benchmark.sh default (dgraph/dgraph)
KEEP_CLUSTER=false

usage() {
  cat <<'USAGE'
Usage:
  ./baseline-release.sh <version> [options]
  ./baseline-release.sh --version <version> [options]

Options:
      --version        Released Dgraph tag to baseline (e.g. v25.4.0)
      --configs        Space-separated topologies (default: "single ha3 shard3 ha5")
  -z, --zone           GCP zone (default: us-east1-c)
  -m, --machine-type   Dgraph node machine type (default: n2-standard-8)
      --alpha-cpu      Alpha CPU requests==limits (default: 6)
      --alpha-mem      Alpha memory requests==limits (default: 24Gi)
  -p, --project        GCP project (default: $PROJECT env or dgraph-dev-sandbox)
  -C, --cluster        GKE cluster name (default: dgraph-bench)
      --image-repo     Image repository (default: dgraph/dgraph; e.g. dgraph/dgraph-custom)
      --keep-cluster   Do not tear down at the end (default: tear down on success)
  -h, --help           Show help
USAGE
}

# First bare arg is the version, for the common `./baseline-release.sh v25.4.0`.
[[ $# -gt 0 && "$1" != -* ]] && { VERSION="$1"; shift; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)        VERSION="${2:?}"; shift 2;;
    --configs)        read -r -a CONFIGS <<< "${2:?}"; shift 2;;
    -z|--zone)        ZONE="${2:?}"; shift 2;;
    -m|--machine-type) MACHINE="${2:?}"; shift 2;;
    --alpha-cpu)      ALPHA_CPU="${2:?}"; shift 2;;
    --alpha-mem)      ALPHA_MEM="${2:?}"; shift 2;;
    -p|--project)     PROJECT="${2:?}"; shift 2;;
    -C|--cluster)     CLUSTER="${2:?}"; shift 2;;
    --image-repo)     IMAGE_REPO="${2:?}"; shift 2;;
    --keep-cluster)   KEEP_CLUSTER=true; shift;;
    -h|--help)        usage; exit 0;;
    *) die "Unknown argument: $1 (see --help)";;
  esac
done

[[ -n "$VERSION" ]] || { usage; die "missing <version> (e.g. v25.4.0)"; }

log "Baselining $VERSION on $CLUSTER ($ZONE, $MACHINE) — configs: ${CONFIGS[*]}"
log "Ledger: $LEDGER"

extra=(); [[ -n "$IMAGE_REPO" ]] && extra=(--image-repo "$IMAGE_REPO")

done_ok=()
failed=()
for cfg in "${CONFIGS[@]}"; do
  step "Baseline $VERSION / $cfg"
  if "$SCRIPT_DIR/benchmark.sh" baseline \
      --version "$VERSION" --config "$cfg" \
      -z "$ZONE" -m "$MACHINE" --alpha-cpu "$ALPHA_CPU" --alpha-mem "$ALPHA_MEM" \
      -p "$PROJECT" -C "$CLUSTER" "${extra[@]}"; then
    done_ok+=("$cfg")
  else
    log "!! baseline failed for config=$cfg — continuing with the rest"
    failed+=("$cfg")
  fi
done

step "Sweep complete"
log "Recorded: ${done_ok[*]:-none}"
[[ ${#failed[@]} -gt 0 ]] && log "Failed:   ${failed[*]}"

# Show the run_ids this sweep landed for the version.
if [[ -f "$LEDGER" ]]; then
  log ""
  log "Runs recorded for $VERSION:"
  python3 - "$LEDGER" "$VERSION" <<'PY' >&2 || true
import json, sys
ledger, version = sys.argv[1], sys.argv[2]
seen = {}
for line in open(ledger):
    try:
        r = json.loads(line)
    except ValueError:
        continue
    if r.get("env", {}).get("DgraphVersion") == version:
        rid = r.get("run_id", "")
        seen[rid] = r.get("env", {}).get("Labels", "")
for rid in sorted(seen):
    print(f"  {rid}   {seen[rid]}")
PY
fi

teardown_hint="./gke-provision.sh teardown -p $PROJECT -z $ZONE -C $CLUSTER"
if [[ ${#failed[@]} -gt 0 ]]; then
  step "Cluster KEPT (a config failed — inspect, then tear down)"
  log "  $teardown_hint"
  exit 1
elif $KEEP_CLUSTER; then
  step "Cluster kept (--keep-cluster). Tear down when done:"
  log "  $teardown_hint"
else
  step "All baselines recorded — tearing down $CLUSTER"
  "$SCRIPT_DIR/gke-provision.sh" teardown -p "$PROJECT" -z "$ZONE" -C "$CLUSTER"
fi
