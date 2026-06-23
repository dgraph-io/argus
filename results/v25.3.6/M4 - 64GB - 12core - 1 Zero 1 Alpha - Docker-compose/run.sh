#!/usr/bin/env bash
# Reproducible argus run for this config tier:
#   Dgraph v25.3.6 | M4, 64GB, 12 core | 1 Zero + 1 Alpha | docker-compose
#
# Convention: every results/<version>/<config>/ folder holds the docker-compose.yml
# that defines the cluster AND this run.sh with the exact argus command, so both
# the environment and the run are checked in. Results land in this folder.
#
#   ./run.sh           # bring cluster up, run argus, leave cluster running
#   ./run.sh --down    # also tear the cluster down afterwards
set -euo pipefail

cd "$(dirname "$0")"
ROOT="$(cd ../../.. && pwd)" # argus repo root

BRANCH="v25.3.6"
SCALE=0.5 # dataset scale (1.0 = full default); tune to hit your target wall-clock
LABEL="machine=M4,ram=64GB,core=12,config=1zero-1alpha,deploy=docker-compose,scale=${SCALE}"

echo "==> building argus"
(cd "$ROOT" && go build -o bin/argus ./cmd/argus)

echo "==> starting cluster"
docker compose up -d

echo "==> waiting for alpha health"
for _ in $(seq 1 60); do
  if curl -s -m 3 -X POST localhost:8080/admin -H 'Content-Type: application/json' \
       -d '{"query":"{ health { status } }"}' 2>/dev/null | grep -q healthy; then
    echo "    ready"
    break
  fi
  sleep 2
done

echo "==> running argus"
# Each run drops the cluster first (clean slate), so repeats are independent and
# the deterministic dataset is exactly what's validated. Tune SCALE (above) to
# hit your target wall-clock — calibration on this box: scale 0.15 ~ 30s/pass,
# scale 1.0 ~ 7.5min/pass (single alpha, Docker-on-Mac I/O bound).
"$ROOT/bin/argus" \
  -scale "$SCALE" -passes 3 -repeats 2 \
  -graphql http://localhost:8080 \
  -alphas http://localhost:8080 \
  -dgraph-branch "$BRANCH" \
  -label "$LABEL" \
  -summary-output ./summary.json \
  -ops-output ./ops.csv -runtime-output ./runtime.csv -cluster-output ./cluster.csv

if [ "${1:-}" = "--down" ]; then
  echo "==> stopping cluster"
  docker compose down
else
  echo "==> done. cluster left running; 'docker compose down' (from this folder) to stop."
fi
