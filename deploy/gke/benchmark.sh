#!/usr/bin/env bash
# GKE benchmark orchestrator for the argus baseline library.
#
# Modes:
#   baseline   Provision GKE + deploy a released Dgraph + run argus, appending
#              one ledger line per repeat to the committed baselines ledger.
#              Repeat across versions/configs to build a reusable library.
#   candidate  Build a candidate Dgraph image (Cloud Build, optional local
#              badger/ristretto), deploy it under a chosen baseline's config,
#              run argus, then `argus compare` against that stored baseline.
#   matrix     Sweep `baseline` over a list of "version config [machine]" specs.
#
# The baselines ledger + artifacts live next to this script (deploy/k8s/baselines/);
# argus (compare/ls/compare.html) reads them via -ledger. The candidate compares
# against a pre-recorded baseline, so only the candidate is run on demand.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  ./benchmark.sh baseline  --version vX.Y.Z --config <desc> [cluster/workload options]
  ./benchmark.sh candidate --dgraph DIR --config <desc> --baseline-run RUN_ID \
      [--badger DIR] [--ristretto DIR] ...
  ./benchmark.sh matrix    --matrix FILE [cluster/workload options]

Common options:
  -p, --project        GCP project ID (default: $PROJECT env, else dgraph-dev-sandbox)
      --argus-image    Artifact Registry ref of the argus load-generator image
                       (default: $ARGUS_IMAGE env, else the dgraph-dev-sandbox v1;
                       build once: deploy/build-image.sh IMG --push)
      --argus-repo     argus checkout path (default: this repo — for the Job
                       template + `argus compare`; only needed if run from elsewhere)
      --ledger         Baselines ledger path (default: results/gke/runs.jsonl)
  -c, --config         Topology: single|ha3|ha5|shard3|shard3-ha3
  -z, --zone           GCP zone (default: us-central1-a)
      --zones          Candidate zones to auto-select from by probing capacity
                       (dodges GCE stockouts), e.g. "us-central1-c us-east1-b"
  -C, --cluster        GKE cluster name (default: dgraph-bench)
  -m, --machine-type   Dgraph pool machine type (default: n2-standard-8)
  -N, --num-nodes      Dgraph pool node count (default: derived from --config —
                       single=1, ha3/shard3=3, ha5=5, shard3-ha3=9)
      --disk-type      PD type for the ssd StorageClass (default: pd-ssd)
      --alpha-cpu      Alpha CPU requests==limits (default: 6)
      --alpha-mem      Alpha memory requests==limits (default: 26Gi)
      --alpha-extra-flags  Extra flags appended to the alpha command after the argus
                       IP whitelist, e.g. '--feature-flags=name=val' (tokens space-free)
  -n, --namespace      Namespace (default: dgraph)
  -r, --release        Helm release (default: dgraph)
      --scale          argus dataset scale (default: 0.25 — ~10-12 min/repeat on
                       a single alpha; cost is ~superlinear in scale)
      --passes         argus write passes (default: 5)
      --repeats        argus repeats — noise floor (default: 3; ~5% central-metric
                       noise on GKE dedicated nodes makes 3 sufficient)
      --write-concurrency  argus write workers (default: 8)
      --job-timeout    Max seconds to wait for the argus Job before abandoning +
                       deleting it (default: 0 = auto, 30 min/pass × passes × repeats)
      --label          Extra free-form label appended to the recorded -label
      --no-provision   Skip cluster create (reuse the current kube context)
      --keep           Do not teardown the cluster afterwards (default: keep)

baseline:
      --version        Dgraph image tag (e.g. v25.3.7, v25.4.0-preview1)
      --image-repo     Image repository (default: dgraph/dgraph; e.g. dgraph/dgraph-custom)

candidate:
      --dgraph         Path to the dgraph working tree to build
      --badger         Local badger to fold in (optional)
      --ristretto      Local ristretto to fold in (optional)
      --baseline-run   run_id in the ledger to compare against (required)
      --region         Artifact Registry region for the candidate image (default: us-central1)
      --repo           Artifact Registry repo (default: argus)
      --fresh-baseline Also (re)run the baseline's version paired, for a same-session diff

matrix:
      --matrix FILE    Lines of: <version> <config> [machine-type]   (# comments ok)
  -h, --help           Show help
USAGE
}

log()  { echo "$@" >&2; }
die()  { log "ERROR: $*"; exit 1; }
step() { log ""; log "=== $* ==="; }

# ---- shared defaults ----------------------------------------------------------
# Project + image default to these but can be overridden by the environment
# (export PROJECT / ARGUS_IMAGE) or the --project / --argus-image flags.
PROJECT="${PROJECT:-dgraph-dev-sandbox}"
ARGUS_IMAGE="${ARGUS_IMAGE:-us-central1-docker.pkg.dev/dgraph-dev-sandbox/argus/argus:v1}"
ARGUS_REPO="${ARGUS_REPO:-}" LEDGER=""
CFG="" ZONE="us-central1-a" ZONES="" CLUSTER="dgraph-bench"
MACHINE="n2-standard-8" NUM_NODES="" DISK_TYPE="pd-ssd"  # NUM_NODES empty = derive from topology
ALPHA_CPU="6" ALPHA_MEM="26Gi"
NS="dgraph" REL="dgraph"
SCALE="0.25" PASSES="5" REPEATS="3" WRITE_CONC="8" EXTRA_LABEL="" JOB_TIMEOUT="0"
NO_PROVISION=false KEEP=true
VERSION="" IMAGE_REPO="dgraph/dgraph" ALPHA_EXTRA_FLAGS="" CHART_VERSION=""
DGRAPH="" BADGER="" RISTRETTO="" BASELINE_RUN="" FRESH_BASELINE=false
REGION="us-central1" REPO="argus" MATRIX_FILE=""

MODE="${1:-}"; shift || true
case "$MODE" in
  baseline|candidate|matrix) ;;
  -h|--help|"") usage; exit 0;;
  *) die "Unknown mode: $MODE (expected baseline|candidate|matrix)";;
esac

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--project) PROJECT="${2:?}"; shift 2;;
    --argus-image) ARGUS_IMAGE="${2:?}"; shift 2;;
    --argus-repo) ARGUS_REPO="${2:?}"; shift 2;;
    --ledger) LEDGER="${2:?}"; shift 2;;
    -c|--config) CFG="${2:?}"; shift 2;;
    -z|--zone) ZONE="${2:?}"; shift 2;;
    --zones) ZONES="${2:?}"; shift 2;;
    -C|--cluster) CLUSTER="${2:?}"; shift 2;;
    -m|--machine-type) MACHINE="${2:?}"; shift 2;;
    -N|--num-nodes) NUM_NODES="${2:?}"; shift 2;;
    --disk-type) DISK_TYPE="${2:?}"; shift 2;;
    --alpha-cpu) ALPHA_CPU="${2:?}"; shift 2;;
    --alpha-mem) ALPHA_MEM="${2:?}"; shift 2;;
    -n|--namespace) NS="${2:?}"; shift 2;;
    -r|--release) REL="${2:?}"; shift 2;;
    --scale) SCALE="${2:?}"; shift 2;;
    --passes) PASSES="${2:?}"; shift 2;;
    --repeats) REPEATS="${2:?}"; shift 2;;
    --write-concurrency) WRITE_CONC="${2:?}"; shift 2;;
    --job-timeout) JOB_TIMEOUT="${2:?}"; shift 2;;
    --label) EXTRA_LABEL="${2:?}"; shift 2;;
    --no-provision) NO_PROVISION=true; shift;;
    --keep) KEEP=true; shift;;
    --version) VERSION="${2:?}"; shift 2;;
    --image-repo) IMAGE_REPO="${2:?}"; shift 2;;
    --alpha-extra-flags) ALPHA_EXTRA_FLAGS="${2:?}"; shift 2;;
    --chart-version) CHART_VERSION="${2:?}"; shift 2;;
    --dgraph) DGRAPH="${2:?}"; shift 2;;
    --badger) BADGER="${2:?}"; shift 2;;
    --ristretto) RISTRETTO="${2:?}"; shift 2;;
    --baseline-run) BASELINE_RUN="${2:?}"; shift 2;;
    --fresh-baseline) FRESH_BASELINE=true; shift;;
    --region) REGION="${2:?}"; shift 2;;
    --repo) REPO="${2:?}"; shift 2;;
    --matrix) MATRIX_FILE="${2:?}"; shift 2;;
    -h|--help) usage; exit 0;;
    *) log "Unknown argument: $1"; usage; exit 2;;
  esac
done

[[ -n "$PROJECT"     ]] || die "Missing --project"
[[ -n "$ARGUS_IMAGE" ]] || die "Missing --argus-image"
# This script lives at <repo>/deploy/gke, so the argus checkout is two levels up.
: "${ARGUS_REPO:=$(cd "$SCRIPT_DIR/../.." && pwd)}"
[[ -d "$ARGUS_REPO"  ]] || die "--argus-repo not found: $ARGUS_REPO"
TMPL="$ARGUS_REPO/deploy/argus-job.tmpl.yaml"
[[ -f "$TMPL" ]] || die "argus Job template not found: $TMPL"
command -v kubectl >/dev/null || die "kubectl not found"
command -v envsubst >/dev/null || die "envsubst not found (brew install gettext)"

# The ledger + artifacts live together so argus's ArtifactsDir convention
# (<dir(ledger)>/artifacts/<run-id>) resolves for `argus compare`.
: "${LEDGER:=$ARGUS_REPO/results/gke/runs.jsonl}"
ARTIFACTS_ROOT="$(cd "$(dirname "$LEDGER")" && pwd)/artifacts"
mkdir -p "$(dirname "$LEDGER")" "$ARTIFACTS_ROOT"

ALPHA_SVC="${REL}-dgraph-alpha"

# argus CLI for `compare`: prefer an installed binary, else `go run` from the repo.
argus_cli() {
  if command -v argus >/dev/null; then argus "$@"; else ( cd "$ARGUS_REPO" && go run ./cmd/argus "$@" ); fi
}

gen_run_id() { # <token>
  local tok; tok="$(printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '-')"
  printf '%s-%s-%04x%04x\n' "$(date +%Y%m%d-%H%M%S)" "$tok" "$RANDOM" "$RANDOM"
}

# Provision (or reuse) the cluster and deploy Dgraph for a given image + config.
# Nodes needed for a topology: one alpha per node (Guaranteed-QoS alpha ~fills a
# node), and the small zero co-locates with an alpha. So nodes = alpha count.
nodes_for() {
  case "$1" in
    single)                echo 1;;
    ha3|shard3|sharded|nonsharded) echo 3;;
    ha5)                   echo 5;;
    shard3-ha3)            echo 9;;
    *)                     echo 3;;
  esac
}

provision() { # <image-repository> <image-tag> <config>
  local repo="$1" tag="$2" cfg="$3"
  # -N derives from the topology unless the operator pinned it explicitly.
  local nodes="${NUM_NODES:-$(nodes_for "$cfg")}"
  local extra=(); [[ -n "$ALPHA_EXTRA_FLAGS" ]] && extra=(--alpha-extra-flags "$ALPHA_EXTRA_FLAGS")
  [[ -n "$CHART_VERSION" ]] && extra+=(--chart-version "$CHART_VERSION")
  if $NO_PROVISION; then
    step "Reusing current kube context; deploying Dgraph ($repo:$tag, $cfg)"
    "$SCRIPT_DIR/local-provision.sh" -c "$cfg" -e gke -n "$NS" -r "$REL" --reset \
      --image-repository "$repo" --image-tag "$tag" \
      --alpha-cpu "$ALPHA_CPU" --alpha-mem "$ALPHA_MEM" "${extra[@]}"
  else
    step "Provisioning GKE ($MACHINE x $nodes) + deploying Dgraph ($repo:$tag, $cfg)"
    local zone_args=(-z "$ZONE")
    [[ -n "$ZONES" ]] && zone_args+=(--zones "$ZONES")
    "$SCRIPT_DIR/gke-provision.sh" create -p "$PROJECT" -c "$cfg" \
      "${zone_args[@]}" -C "$CLUSTER" -m "$MACHINE" -N "$nodes" --disk-type "$DISK_TYPE" \
      -n "$NS" -r "$REL" --reset "${extra[@]}" \
      --image-repository "$repo" --image-tag "$tag" \
      --alpha-cpu "$ALPHA_CPU" --alpha-mem "$ALPHA_MEM"
  fi
}

# Read the Dgraph build identity from the running alpha (branch "HEAD" => blank).
DET_VERSION="" DET_BRANCH="" DET_COMMIT="" MEANINGFUL_BRANCH=""
capture_identity() {
  step "Reading build identity (dgraph version) from ${REL}-dgraph-alpha-0"
  local info
  info="$(kubectl -n "$NS" exec "${REL}-dgraph-alpha-0" -- dgraph version 2>/dev/null || true)"
  DET_VERSION="$(printf '%s\n' "$info" | awk -F': ' '/^Dgraph version[[:space:]]*:/{print $2}')"
  DET_BRANCH="$(printf '%s\n' "$info" | awk -F': ' '/^Branch[[:space:]]*:/{print $2}')"
  DET_COMMIT="$(printf '%s\n' "$info" | awk -F': ' '/^Commit SHA-1[[:space:]]*:/{print $2}')"
  case "$DET_BRANCH" in ""|HEAD) MEANINGFUL_BRANCH="" ;; *) MEANINGFUL_BRANCH="$DET_BRANCH" ;; esac
  log "    version=${DET_VERSION:-?} branch=${DET_BRANCH:-?} commit=${DET_COMMIT:-?}"
}

# Render + run the argus Job, wait, copy out results, append the ledger line.
# Sets RUN_ID. Args: <label-token> <extra-label-kv>
run_argus() {
  local token="$1" label="$2"
  RUN_ID="$(gen_run_id "$token")"
  local job="argus-$(printf '%s' "$RUN_ID" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9-' '-' | cut -c1-52)"
  local artifacts="$ARTIFACTS_ROOT/$RUN_ID"
  mkdir -p "$artifacts"

  # Sharded topologies get deterministic predicate placement (argus -shard-balance),
  # so writes distribute instead of concentrating on one group. Tag the ledger row
  # so the baseline is self-describing. Non-sharded configs pass false (a no-op).
  local shard_balance=false
  case "$CFG" in shard3|shard3-ha3|sharded) shard_balance=true; label="${label},balanced=1";; esac
  local zero_url="http://${REL}-dgraph-zero:6080"

  step "Running argus Job $job (run-id $RUN_ID)"
  JOB_NAME="$job" NAMESPACE="$NS" ARGUS_IMAGE="$ARGUS_IMAGE" ALPHA_SVC="$ALPHA_SVC" \
    PASSES="$PASSES" REPEATS="$REPEATS" SCALE="$SCALE" WRITE_CONC="$WRITE_CONC" \
    RUN_ID="$RUN_ID" CPU_CLASS="$MACHINE" DISK_CLASS="$DISK_TYPE" DEPLOY="gke" \
    LABEL="$label" DGRAPH_BRANCH="$MEANINGFUL_BRANCH" DGRAPH_COMMIT="$DET_COMMIT" \
    SHARD_BALANCE="$shard_balance" ZERO_URL="$zero_url" \
    envsubst '$JOB_NAME $NAMESPACE $ARGUS_IMAGE $ALPHA_SVC $PASSES $REPEATS $SCALE $WRITE_CONC $RUN_ID $CPU_CLASS $DISK_CLASS $DEPLOY $LABEL $DGRAPH_BRANCH $DGRAPH_COMMIT $SHARD_BALANCE $ZERO_URL' \
    < "$TMPL" | kubectl -n "$NS" apply -f - 1>&2

  # Wait on the *argus container's* exit, not the Job/pod phase: the `sink` sidecar
  # keeps the pod Running after argus exits (so we can copy /out — kubectl cp needs
  # a live container to exec `tar` in, impossible on a completed pod). Attach to
  # argus's logs once it's up; `logs -f` errors during ContainerCreating (image pull
  # on a fresh node), so retry rather than swallow. Polling keeps this bash-3.2 safe.
  # Safety-net budget (loop exits the instant argus exits, so a generous cap is free);
  # passes slow down as data accumulates, so budget 30 min/pass.
  local to="$JOB_TIMEOUT"
  [[ "$to" -eq 0 ]] && to=$(( (PASSES>0?PASSES:30) * REPEATS * 1800 + 1800 ))
  local waited=0 streamed=false exitcode="" jsonpath
  jsonpath='{.items[0].status.containerStatuses[?(@.name=="argus")].state.terminated.exitCode}'
  while :; do
    exitcode="$(kubectl -n "$NS" get pod -l job-name="$job" -o jsonpath="$jsonpath" 2>/dev/null || true)"
    [[ -n "$exitcode" ]] && break
    if ! $streamed && kubectl -n "$NS" logs -f "job/$job" -c argus 1>&2 2>/dev/null; then streamed=true; fi
    if [[ $waited -ge $to ]]; then
      log "argus exceeded ${to}s — deleting the runaway Job."
      kubectl -n "$NS" delete "job/$job" --wait=false >/dev/null 2>&1 || true
      die "argus did not finish within ${to}s. Raise --job-timeout, or (better) reduce --scale/--passes/--repeats."
    fi
    sleep 5; waited=$((waited+5))
  done

  if [[ "$exitcode" != "0" ]]; then
    log "argus container exited $exitcode — pod state (tail) + logs:"
    kubectl -n "$NS" describe pod -l job-name="$job" 2>/dev/null | tail -40 1>&2 || true
    kubectl -n "$NS" logs "job/$job" -c argus --tail=100 1>&2 2>/dev/null || true
    kubectl -n "$NS" delete "job/$job" --wait=false >/dev/null 2>&1 || true
    die "argus container exited non-zero ($exitcode)"
  fi

  local pod; pod="$(kubectl -n "$NS" get pod -l job-name="$job" -o name 2>/dev/null | head -1 | cut -d/ -f2)"
  [[ -n "$pod" ]] || die "argus pod not found for job/$job"
  local tmpout; tmpout="$(mktemp -d)"
  # Copy from the live `sink` sidecar (argus's container is gone by now). cp into a
  # non-existent subdir so /out's contents land directly under it.
  kubectl -n "$NS" cp "$pod:/out" "$tmpout/out" -c sink 1>&2 || die "kubectl cp of /out failed"

  [[ -s "$tmpout/out/runs.jsonl" ]] || die "no ledger lines produced (Job likely failed — check logs above)"
  cat "$tmpout/out/runs.jsonl" >> "$LEDGER"
  cp "$tmpout/out"/*.csv "$artifacts"/ 2>/dev/null || true
  # Best-effort post-run heap profile so `argus compare` can attribute a regression.
  kubectl -n "$NS" exec "${REL}-dgraph-alpha-0" -- \
    sh -c 'command -v curl >/dev/null && curl -s localhost:8080/debug/pprof/heap' \
    > "$artifacts/heap-end.pprof" 2>/dev/null || true
  [[ -s "$artifacts/heap-end.pprof" ]] || rm -f "$artifacts/heap-end.pprof"
  rm -rf "$tmpout"

  # Results are copied — stop the sink sidecar (the Job won't self-complete with it
  # sleeping, and this frees the argus node).
  kubectl -n "$NS" delete "job/$job" --wait=false >/dev/null 2>&1 || true

  log "    appended run $RUN_ID to $LEDGER"
  log "    artifacts: $artifacts"
}

teardown_if_asked() {
  $KEEP || return
  # --zones may have landed the cluster in a zone this script didn't choose; ask
  # gcloud where it actually is so the teardown hint is correct.
  local actual_zone
  actual_zone="$(gcloud container clusters list --project "$PROJECT" \
    --filter="name=$CLUSTER" --format='value(location)' 2>/dev/null | head -1)"
  : "${actual_zone:=$ZONE}"
  log ""; log "Cluster kept (tear down when done):"
  log "  ./gke-provision.sh teardown -p $PROJECT -z $actual_zone -C $CLUSTER"
}

mode_baseline() {
  [[ -n "$VERSION" ]] || die "baseline needs --version (released Dgraph tag)"
  [[ -n "$CFG"     ]] || die "baseline needs --config"
  provision "$IMAGE_REPO" "$VERSION" "$CFG"
  capture_identity
  local label="config=$CFG,version=${DET_VERSION:-$VERSION}${EXTRA_LABEL:+,$EXTRA_LABEL}"
  run_argus "${DET_VERSION:-$VERSION}-$CFG" "$label"
  log ""; log "Baseline recorded: run_id=$RUN_ID"
  log "  argus ls -ledger $LEDGER"
  teardown_if_asked
}

mode_candidate() {
  [[ -n "$DGRAPH"       ]] || die "candidate needs --dgraph"
  [[ -n "$CFG"          ]] || die "candidate needs --config (match the baseline's)"
  [[ -n "$BASELINE_RUN" ]] || die "candidate needs --baseline-run (ledger run_id to compare against)"

  # Optionally re-run the baseline fresh in this same session for a paired diff.
  local fresh_baseline_run=""
  if $FRESH_BASELINE; then
    [[ -n "$VERSION" ]] || die "--fresh-baseline needs --version"
    provision "$IMAGE_REPO" "$VERSION" "$CFG"
    capture_identity
    run_argus "${DET_VERSION:-$VERSION}-$CFG-fresh" "config=$CFG,version=${DET_VERSION:-$VERSION},role=fresh-baseline${EXTRA_LABEL:+,$EXTRA_LABEL}"
    fresh_baseline_run="$RUN_ID"
  fi

  step "Building candidate image via Cloud Build"
  local build_args=(-p "$PROJECT" --dgraph "$DGRAPH" --region "$REGION" --repo "$REPO")
  [[ -n "$BADGER"    ]] && build_args+=(--badger "$BADGER")
  [[ -n "$RISTRETTO" ]] && build_args+=(--ristretto "$RISTRETTO")
  local image; image="$("$SCRIPT_DIR/build-candidate.sh" "${build_args[@]}")" || die "candidate build failed"
  local crepo="${image%:*}" ctag="${image##*:}"
  local folded=""
  [[ -n "$BADGER"    ]] && folded="${folded}badger,"
  [[ -n "$RISTRETTO" ]] && folded="${folded}ristretto,"

  provision "$crepo" "$ctag" "$CFG"
  capture_identity
  run_argus "cand-${ctag}-$CFG" "config=$CFG,role=candidate${folded:+,folds=${folded%,}}${EXTRA_LABEL:+,$EXTRA_LABEL}"
  local candidate_run="$RUN_ID"

  local base="${fresh_baseline_run:-$BASELINE_RUN}"
  step "Comparing candidate ($candidate_run) vs baseline ($base)"
  argus_cli compare -ledger "$LEDGER" -baseline-run "$base" -candidate-run "$candidate_run" || true
  log ""; log "Candidate run_id=$candidate_run  baseline run_id=$base"
  teardown_if_asked
}

mode_matrix() {
  [[ -n "$MATRIX_FILE" ]] || die "matrix needs --matrix FILE"
  [[ -f "$MATRIX_FILE"  ]] || die "matrix file not found: $MATRIX_FILE"
  local n=0
  while read -r version cfg machine _rest; do
    [[ -z "$version" || "$version" == \#* ]] && continue
    n=$((n+1))
    VERSION="$version"; CFG="$cfg"; [[ -n "${machine:-}" ]] && MACHINE="$machine"
    step "Matrix [$n]: version=$VERSION config=$CFG machine=$MACHINE"
    mode_baseline
  done < "$MATRIX_FILE"
  log ""; log "Matrix complete: $n baseline(s) recorded to $LEDGER"
}

case "$MODE" in
  baseline)  mode_baseline;;
  candidate) mode_candidate;;
  matrix)    mode_matrix;;
esac
