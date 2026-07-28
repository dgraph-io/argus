#!/usr/bin/env bash
# Reproducible argus run for this config tier:
#   Dgraph v25.3.7 | M4, 64GB, 12 core | 1 Zero + 1 Alpha | docker-compose
# Pins the per-run knobs; the shared driver (results/run-common.sh) reads the
# actual branch/version from the running image via `dgraph version`, so the
# recorded identity is authoritative rather than hand-typed here.
set -euo pipefail
cd "$(dirname "$0")"

SCALE=0.5     # dataset scale (1.0 = full default)
WRITECONC=24  # concurrent write workers — match the candidate run for a fair comparison
REPEATS=5     # extra repeats to tighten the noise band for the latency decision

source ../../run-common.sh
