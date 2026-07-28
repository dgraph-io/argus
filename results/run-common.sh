# Shared argus run driver, sourced by each results/<version>/<config>/run.sh.
#
# Each run appends one JSONL line per repeat to the consolidated ledger
# (results/runs.jsonl) instead of writing a per-run summary folder. The heavy,
# ephemeral artifacts — pprof profiles + per-interval CSVs — go to a gitignored
# per-run-id dir (results/artifacts/<run-id>/); `argus compare` finds them there
# by run-id. The config folder keeps only docker-compose.yml + this thin run.sh as
# the reproducibility inputs.
#
# The branch/version are read from the running image (`dgraph version`), so a
# wrapper only pins knobs — optionally SCALE/WRITECONC/REPEATS/DESC and the
# config-class knobs CPU_CLASS/DISK_CLASS/DEPLOY/LABEL (BRANCH is an optional
# fallback used only if the version probe fails) — cds to its own directory, then
# `source`s this file. Positional args ("--down") pass through.
#
#   ./run.sh           # bring cluster up, run argus, leave cluster running
#   ./run.sh --down    # also tear the cluster down afterwards

: "${BRANCH:=}"         # optional fallback branch label; normally read from the running image (see below)
: "${SCALE:=0.5}"       # dataset scale (1.0 = full default); tune to hit target wall-clock
: "${WRITECONC:=24}"    # concurrent write workers
: "${PASSES:=3}"        # workload passes per repeat
: "${REPEATS:=5}"       # independent repeats (defaults to 5 to shrink the noise band)
# Config-class columns (cores/RAM/topology are auto-detected; these are not).
: "${CPU_CLASS:=Apple M4}"
: "${DISK_CLASS:=NVMe}"
: "${DEPLOY:=docker-compose}"
: "${LABEL:=${LABEL_BASE:-}}" # free-form extras; honors a legacy LABEL_BASE if set

ROOT="$(cd ../../.. && pwd)" # argus repo root (results/<version>/<config> -> repo)
LEDGER="$ROOT/results/runs.jsonl"
# RUN_ID/ARTIFACTS/DESC are set after the cluster is up and the branch is read
# from the running binary (below), so the run id reflects the actual branch.

# capture_heap <label> writes the alpha's live-heap pprof profile to
# ./heap-<label>.pb.gz. Two snapshots per run — "startup" (fresh boot, before any
# workload) and "end" (post-workload) — feed `argus compare`, which diffs the
# resident floor between builds and names the allocation sites that moved.
# Non-fatal: a build without the pprof endpoint just logs and continues.
capture_heap() {
  local label="$1" out="$ARTIFACTS/heap-${1}.pb.gz"
  if curl -s -m 10 -f "http://localhost:8080/debug/pprof/heap" -o "$out"; then
    echo "    heap profile -> $out"
  else
    echo "    WARN: could not fetch heap profile ($label); skipping"
    rm -f "$out"
  fi
}

# capture_cpu grabs a CPU profile over a window in the middle of the workload and
# writes ./cpu.pb.gz. Meant to run in the background alongside the argus run: it
# waits CPU_WARMUP seconds so the bulk-load/schema phase settles into the steady
# query+write mix, then samples for CPU_SECONDS. `argus compare` diffs these to
# localize a latency regression (share of CPU per function) the way it localizes
# a heap regression. Non-fatal: a build without the endpoint just logs and skips.
capture_cpu() {
  local secs="${CPU_SECONDS:-60}" out="$ARTIFACTS/cpu.pb.gz"
  sleep "${CPU_WARMUP:-45}"
  if curl -s -m "$((secs + 30))" -f "http://localhost:8080/debug/pprof/profile?seconds=${secs}" -o "$out"; then
    echo "    cpu profile -> $out (${secs}s window)"
  else
    echo "    WARN: could not fetch cpu profile; skipping"
    rm -f "$out"
  fi
}

# capture_contention pulls the alpha's mutex- and block-contention profiles
# (cumulative since process start) at end of run. These localize an OFF-CPU
# regression — latency lost to lock contention or blocking — the way capture_cpu
# localizes on-CPU cost, and `argus compare` diffs them share-normalized.
#
# Opt-in, because it needs the alpha side enabled: Go's runtime records these only
# when a sampling rate is set, and Dgraph sets it via `--profile_mode`. To use:
#   1. add `--profile_mode=mutex` (or `--profile_mode=block`) to the alpha command
#      in docker-compose.yml — that flips the runtime rate on for the whole run;
#   2. run with CONTENTION=1 ./run.sh
# Only one mode's rate can be on per run (mutex XOR block), so do a second run for
# the other. Rate is maximal (every event), which adds overhead — fine for a diff
# since both builds carry the same instrumentation.
capture_contention() {
  [ "${CONTENTION:-0}" = "1" ] || return 0
  local kind out
  for kind in mutex block; do
    out="$ARTIFACTS/${kind}.pb.gz"
    if curl -s -m 15 -f "http://localhost:8080/debug/pprof/${kind}" -o "$out"; then
      echo "    ${kind} profile -> $out (empty unless --profile_mode=${kind} is set on the alpha)"
    else
      echo "    WARN: could not fetch ${kind} profile; skipping"
      rm -f "$out"
    fi
  done
}

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

# Authoritative build identity from the running binary: the cluster API exposes
# only the commit hash, so the human branch name comes from `dgraph version` in
# the alpha container (stderr carries jemalloc notices; keep stdout). Falls back
# to the wrapper's BRANCH, then "unknown", if the exec fails.
echo "==> reading build info (dgraph version)"
build_info="$(docker compose exec -T alpha dgraph version 2>/dev/null || true)"
det_version="$(printf '%s\n' "$build_info" | awk -F': ' '/^Dgraph version[[:space:]]*:/{print $2}')"
det_branch="$(printf '%s\n' "$build_info" | awk -F': ' '/^Branch[[:space:]]*:/{print $2}')"
det_commit="$(printf '%s\n' "$build_info" | awk -F': ' '/^Commit SHA-1[[:space:]]*:/{print $2}')"

# A released image is built from a detached tag, so its branch is "HEAD" — not a
# useful identifier. Treat empty/HEAD as "no meaningful branch" and let the
# version tag carry the identity in that case; the commit SHA pins it exactly.
case "$det_branch" in
  "" | HEAD) meaningful_branch="" ;;
  *)         meaningful_branch="$det_branch" ;;
esac
DGRAPH_BRANCH="${meaningful_branch:-$BRANCH}"                        # recorded branch (empty for released)
label_token="${meaningful_branch:-${det_version:-${BRANCH:-unknown}}}" # run-id / banner identity
: "${DESC:=Dgraph ${det_version:-?} (branch ${det_branch:-?}, commit ${det_commit:-?})}"
echo "    version=${det_version:-?}  branch=${det_branch:-?}  commit=${det_commit:-?}"

RUN_ID="$(date +%Y%m%d-%H%M%S)-$(printf '%s' "$label_token" | tr -c 'A-Za-z0-9._-' '-')-$(printf '%04x%04x' "$RANDOM" "$RANDOM")"
ARTIFACTS="$ROOT/results/artifacts/$RUN_ID"
mkdir -p "$ARTIFACTS"

echo "==> ${DESC}"
echo "    run-id: ${RUN_ID}"
echo "    ledger: ${LEDGER}"

echo "==> capturing startup heap (resident floor, pre-workload)"
capture_heap startup

echo "==> running argus (mid-run CPU profile captured in the background)"
# Each run drops the cluster first (clean slate), so repeats are independent and
# the deterministic dataset is exactly what's validated. Tune SCALE (in the
# wrapper) to hit your target wall-clock — calibration on this box: scale 0.15 ~
# 30s/pass, scale 1.0 ~ 7.5min/pass (single alpha, Docker-on-Mac I/O bound).
"$ROOT/bin/argus" \
  -scale "$SCALE" -passes "$PASSES" -repeats "$REPEATS" \
  -write-concurrency "$WRITECONC" \
  -graphql http://localhost:8080 \
  -alphas http://localhost:8080 \
  -dgraph-branch "$DGRAPH_BRANCH" -dgraph-commit "$det_commit" \
  -cpu-class "$CPU_CLASS" -disk-class "$DISK_CLASS" -deploy "$DEPLOY" -label "$LABEL" \
  -run-id "$RUN_ID" -ledger "$LEDGER" -summary-output "" \
  -ops-output "$ARTIFACTS/ops.csv" -runtime-output "$ARTIFACTS/runtime.csv" -cluster-output "$ARTIFACTS/cluster.csv" &
argus_pid=$!

capture_cpu &
cpu_pid=$!

wait "$argus_pid"        # propagate argus's exit status (set -e aborts on failure)
wait "$cpu_pid" || true  # CPU window finished long before argus; never fail the run on it

echo "==> capturing end heap (post-workload)"
capture_heap end
capture_contention

echo "==> appended run ${RUN_ID} to ${LEDGER}"
echo "    profiles + interval CSVs (gitignored): ${ARTIFACTS}"
echo "    compare: argus compare -baseline-run <other> -candidate-run ${RUN_ID}"

if [ "${1:-}" = "--down" ]; then
  echo "==> stopping cluster"
  docker compose down
else
  echo "==> done. cluster left running; 'docker compose down' (from this folder) to stop."
fi
