#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  ./local-provision.sh --config <topology> --expose local|gke [options]

Required:
  -c, --config         Topology descriptor (see below)
  -e, --expose         local|gke

Topology descriptors (map to alpha.replicaCount x zero.shardReplicaCount):
  single       1 group  x 1 replica   (1 alpha)   — smoke/iteration
  ha3          1 group  x 3 replicas  (3 alphas)  — HA, no sharding
  ha5          1 group  x 5 replicas  (5 alphas)  — HA, no sharding
  shard3       3 groups x 1 replica   (3 alphas)  — sharded, no HA
  shard3-ha3   3 groups x 3 replicas  (9 alphas)  — sharded + HA
  nonsharded   alias of ha3   (back-compat)
  sharded      alias of shard3 (back-compat)
  (Learner/read-replica nodes are not expressible via the chart yet — see README.)

Options:
  -n, --namespace      Kubernetes namespace (default: dgraph)
  -r, --release        Helm release name (default: dgraph)
      --image-repository  Override Dgraph image repository (e.g. dgraph/dgraph)
      --image-tag         Override Dgraph image tag (e.g. v25.3.7)
      --alpha-cpu       Alpha CPU requests==limits (Guaranteed QoS), e.g. 3
      --alpha-mem       Alpha memory requests==limits, e.g. 12Gi
      --alpha-extra-flags  Extra flags appended to the alpha command (after the
                        argus IP whitelist), e.g. '--feature-flags=name=val'.
                        Each token must be space-free (chart splits on spaces).
      --chart-version   dgraph Helm chart version to pull+pin (default: 24.1.4).
                        Pulled fresh each deploy and patched for the prerelease
                        semver bug (preview image tags -> correct zero flags).
      --zero-cpu        Zero CPU requests==limits (default 1)
      --zero-mem        Zero memory requests==limits (default 2Gi)
      --reset          Delete PVCs for this release before (re)deploying
      --ready-timeout   Max seconds to wait for Dgraph pods to be ready (default 900).
                        Fails sooner on conditions that never clear on their own
                        (disk quota, image pull, crash loop, 5m+ unschedulable).
      --lb-source-range CIDR to restrict LB traffic (e.g. 1.2.3.4/32) (gke only)
      --emit-env       Print export lines for DGRAPH_* vars (for eval)
      --shell          Shell syntax for --emit-env: bash|fish (default: bash)
  -h, --help           Show help

Examples:
  # Local (NodePort) fresh deploy
  ./local-provision.sh -c shard3 -e local --reset

  # Pin a specific Dgraph version, size alpha to the node (Guaranteed QoS)
  ./local-provision.sh -c ha3 -e gke --image-tag v25.3.7 --alpha-cpu 6 --alpha-mem 26Gi

  # Export env vars into your current shell (bash/zsh):
  eval "$(./local-provision.sh -c sharded -e local --emit-env)"

  # Export env vars into your current shell (fish):
  ./local-provision.sh -c sharded -e local --emit-env --shell fish | source

Note:
  For GKE deployments, use ./gke-provision.sh — it creates the cluster,
  sets the kubectl context, and invokes this script with -e gke.
USAGE
}

# Defaults
NS="dgraph"
REL="dgraph"
CFG=""
EXPOSE=""
RESET=false
EMIT_ENV=false
LB_SOURCE_RANGE=""
IMAGE_REPOSITORY=""
IMAGE_TAG=""
SHELL_SYNTAX="bash"
ALPHA_CPU=""
ALPHA_MEM=""
ZERO_CPU="1"
ZERO_MEM="2Gi"
ALPHA_EXTRA_FLAGS=""
READY_TIMEOUT="900"
UNSCHEDULABLE_GRACE="300"  # seconds a pod may sit Unschedulable (autoscaler headroom)
CHART_VERSION="24.1.4"  # pinned so a stale local cache can't silently change the deploy
# argus runs in-cluster and calls the whitelist-guarded /admin,/health,/debug,/state
# endpoints, so the alpha must allow the pod's source IP. GKE pod/node IPs live in
# 10.0.0.0/8; the other private ranges cover local (Docker Desktop / kind) clusters.
# This is the base of alpha.extraFlags; --alpha-extra-flags is appended to it.
WHITELIST="--security whitelist=10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"

# Logging helper (keep stdout clean if using --emit-env)
log() { echo "$@" >&2; }

# One snapshot of the release's pods/PVCs/events. Prints a readiness summary line,
# then (exit 0) a second line naming a stuck condition that won't clear on its own,
# or exits 1 if the rollout may still progress.
check_stuck() {
  local snap="$CHART_TMP/state.json"
  kubectl -n "$NS" get pods,pvc,events -o json > "$snap" 2>/dev/null || return 1
  python3 - "$snap" "$REL" "$UNSCHEDULABLE_GRACE" <<'PY'
import json, re, sys
from collections import Counter
from datetime import datetime, timezone

rel, grace = sys.argv[2], int(sys.argv[3])
items = json.load(open(sys.argv[1]))["items"]
mine = lambda o: o["metadata"].get("labels", {}).get("release") == rel
pods = [o for o in items if o["kind"] == "Pod" and mine(o)]
pending_pvcs = {o["metadata"]["uid"]: o["metadata"]["name"] for o in items
                if o["kind"] == "PersistentVolumeClaim" and mine(o)
                and o.get("status", {}).get("phase") == "Pending"}
now = datetime.now(timezone.utc)
age = lambda ts: (now - datetime.fromisoformat(ts.replace("Z", "+00:00"))).total_seconds()

total, ready = Counter(), Counter()
for p in pods:
    comp = p["metadata"]["labels"].get("component", "?")
    total[comp] += 1
    ready[comp] += any(c["type"] == "Ready" and c["status"] == "True"
                       for c in p.get("status", {}).get("conditions", []))
print(", ".join(f"{c} {ready[c]}/{total[c]}" for c in sorted(total)) + " ready")

def stuck(obj, why):
    print(f"{obj}: {why}")
    sys.exit(0)

# Quota exhaustion: the CSI provisioner retries forever, and nothing frees quota.
for e in items:
    if e["kind"] != "Event" or e["involvedObject"].get("uid") not in pending_pvcs:
        continue
    m = re.search(r"Quota '[^']+' exceeded\.\s+Limit: \S+ in region [\w-]+", e.get("message", ""))
    if e.get("reason") == "ProvisioningFailed" and m:
        stuck(f"pvc/{pending_pvcs[e['involvedObject']['uid']]}", m.group(0))

for p in pods:
    name, st = f"pod/{p['metadata']['name']}", p.get("status", {})
    for c in st.get("initContainerStatuses", []) + st.get("containerStatuses", []):
        w = c.get("state", {}).get("waiting") or {}
        r = w.get("reason", "")
        if r in ("ImagePullBackOff", "InvalidImageName", "CreateContainerConfigError"):
            stuck(name, f"{r}: {w.get('message', '')}")
        if r == "CrashLoopBackOff" and c.get("restartCount", 0) >= 3:
            t = c.get("lastState", {}).get("terminated", {})
            stuck(name, f"CrashLoopBackOff after {c['restartCount']} restarts "
                        f"(last exit {t.get('exitCode')} {t.get('reason', '')})")
    # Grace period lets the cluster autoscaler add a node before we give up.
    for cond in st.get("conditions", []):
        if (cond["type"] == "PodScheduled" and cond["status"] == "False"
                and cond.get("reason") == "Unschedulable"
                and age(cond["lastTransitionTime"]) >= grace):
            stuck(name, f"unschedulable for {grace}s+: {cond.get('message', '')}")
sys.exit(1)
PY
}

fail_ready() {
  log ""
  log "ERROR: Dgraph pods are stuck: $1"
  log ""
  kubectl -n "$NS" get pods,pvc -l release="$REL" -o wide 1>&2 || true
  log ""
  log "Recent warning events:"
  kubectl -n "$NS" get events --field-selector type=Warning --sort-by=.lastTimestamp 2>/dev/null | tail -n 10 1>&2 || true
  exit 1
}

# Replaces an unbounded `kubectl rollout status`, which waits forever on pods that
# can never start (e.g. PVCs blocked by a GCE disk quota).
wait_ready() {
  local deadline=$((SECONDS + READY_TIMEOUT)) out last=""
  log "Waiting for Dgraph pods to be ready (timeout ${READY_TIMEOUT}s)..."
  while :; do
    if kubectl -n "$NS" rollout status "statefulset/${REL}-dgraph-zero" --timeout=1s >/dev/null 2>&1 &&
       kubectl -n "$NS" rollout status "statefulset/${REL}-dgraph-alpha" --timeout=1s >/dev/null 2>&1; then
      log "All Dgraph pods ready."
      return
    fi
    if out="$(check_stuck)"; then
      fail_ready "${out#*$'\n'}"
    fi
    if [[ -n "$out" && "${out%%$'\n'*}" != "$last" ]]; then
      last="${out%%$'\n'*}"
      log "  $last"
    fi
    if (( SECONDS >= deadline )); then
      fail_ready "not ready after ${READY_TIMEOUT}s (raise --ready-timeout if pods are still progressing)"
    fi
    sleep 10
  done
}

# Parse args (supports short + long)
while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--config) CFG="${2:?missing value for $1}"; shift 2;;
    -e|--expose) EXPOSE="${2:?missing value for $1}"; shift 2;;
    -n|--namespace) NS="${2:?missing value for $1}"; shift 2;;
    -r|--release) REL="${2:?missing value for $1}"; shift 2;;
    --reset) RESET=true; shift;;
    --lb-source-range) LB_SOURCE_RANGE="${2:?missing value for $1}"; shift 2;;
    --image-repository) IMAGE_REPOSITORY="${2:?missing value for $1}"; shift 2;;
    --image-tag) IMAGE_TAG="${2:?missing value for $1}"; shift 2;;
    --alpha-cpu) ALPHA_CPU="${2:?missing value for $1}"; shift 2;;
    --alpha-mem) ALPHA_MEM="${2:?missing value for $1}"; shift 2;;
    --alpha-extra-flags) ALPHA_EXTRA_FLAGS="${2:?missing value for $1}"; shift 2;;
    --chart-version) CHART_VERSION="${2:?missing value for $1}"; shift 2;;
    --zero-cpu) ZERO_CPU="${2:?missing value for $1}"; shift 2;;
    --zero-mem) ZERO_MEM="${2:?missing value for $1}"; shift 2;;
    --ready-timeout) READY_TIMEOUT="${2:?missing value for $1}"; shift 2;;
    --shell) SHELL_SYNTAX="${2:?missing value for $1}"; shift 2;;
    --emit-env) EMIT_ENV=true; shift;;
    -h|--help) usage; exit 0;;
    *)
      log "Unknown argument: $1"
      usage
      exit 2
      ;;
  esac
done

# Validate required args
if [[ -z "$CFG" || -z "$EXPOSE" ]]; then
  log "Missing required flags."
  usage
  exit 2
fi

# Map a topology descriptor to (alpha.replicaCount, zero.shardReplicaCount).
# groups = ceil(alphaReplicas / shardReplicaCount); voting replicas = shardReplicaCount.
ALPHA_REPLICAS=""
SHARD_REPLICAS=""
case "$CFG" in
  single)                ALPHA_REPLICAS=1; SHARD_REPLICAS=1;;
  ha3|nonsharded)        ALPHA_REPLICAS=3; SHARD_REPLICAS=3;;
  ha5)                   ALPHA_REPLICAS=5; SHARD_REPLICAS=5;;
  shard3|sharded)        ALPHA_REPLICAS=3; SHARD_REPLICAS=1;;
  shard3-ha3)            ALPHA_REPLICAS=9; SHARD_REPLICAS=3;;
  *) log "Invalid --config: $CFG (see --help for descriptors)"; exit 2;;
esac

case "$EXPOSE" in
  local|gke) ;;
  *) log "Invalid --expose: $EXPOSE"; exit 2;;
esac

case "$SHELL_SYNTAX" in
  bash|fish) ;;
  *) log "Invalid --shell: $SHELL_SYNTAX (expected bash|fish)"; exit 2;;
esac

# One base values file holds the topology-agnostic reproducibility pinning
# (pd-ssd, dedicated-pool nodeSelector/tolerations); topology + resources come
# from the descriptor and the --alpha-*/--zero-* flags as --set overrides.
VALUES_FILE="$SCRIPT_DIR/values-base.yaml"
if [[ ! -f "$VALUES_FILE" ]]; then
  log "Values file not found: $VALUES_FILE"
  exit 1
fi

ALPHA_SVC="${REL}-dgraph-alpha"

if $EMIT_ENV; then
  # --emit-env is a pure lookup: the release must already be deployed.
  if ! kubectl -n "$NS" get svc "$ALPHA_SVC" >/dev/null 2>&1; then
    log "ERROR: service '$ALPHA_SVC' not found in namespace '$NS'."
    log "Deploy first with: ./local-provision.sh -c $CFG -e $EXPOSE"
    exit 1
  fi
else
  # Ensure namespace exists
  kubectl create ns "$NS" >/dev/null 2>&1 || true

  # Helm repo
  helm repo add dgraph https://charts.dgraph.io >/dev/null 2>&1 || true
  helm repo update >/dev/null

  # Optional: full teardown of the Helm release + PVCs for a clean slate.
  # This is needed because StatefulSet has immutable fields (e.g. volumeClaimTemplates,
  # selectors), so a plain `helm upgrade` after config changes will fail with
  # "Forbidden: updates to statefulset spec ... are forbidden".
  if $RESET; then
    if helm status "$REL" -n "$NS" >/dev/null 2>&1; then
      log "Reset enabled: uninstalling helm release '$REL' in namespace '$NS'"
      helm uninstall "$REL" -n "$NS" --wait 1>&2 || true
    else
      log "Reset enabled: no existing release '$REL' to uninstall"
    fi

    log "Reset enabled: deleting PVCs for release=$REL in namespace=$NS"
    kubectl -n "$NS" delete pvc -l release="$REL" --ignore-not-found 1>&2 || true
  fi

  # Decide exposure mode for Alpha and Zero services
  SVC_FLAGS=()
  case "$EXPOSE" in
    local)
      SVC_FLAGS+=(--set alpha.service.type=NodePort)
      # Expose Zero as NodePort too so admin (6080) and raft (5080) are reachable
      # from the host. On GKE we leave Zero as ClusterIP — the admin API is sensitive
      # and users can `kubectl port-forward` when needed.
      SVC_FLAGS+=(--set zero.service.type=NodePort)
      ;;
    gke)
      SVC_FLAGS+=(--set alpha.service.type=LoadBalancer)
      if [[ -n "$LB_SOURCE_RANGE" ]]; then
        SVC_FLAGS+=(--set alpha.service.loadBalancerSourceRanges[0]="$LB_SOURCE_RANGE")
      fi
      ;;
  esac

  # Optional image overrides. The chart builds the ref as
  # `printf "%s/%s:%s" image.registry image.repository image.tag`, so a
  # fully-qualified repository (e.g. an Artifact Registry path
  # `us-central1-docker.pkg.dev/proj/repo/dgraph`) would get `docker.io/`
  # prepended. Detect a registry host in the first path segment (contains a
  # `.` or `:`) and split it into image.registry; a bare repo like
  # `dgraph/dgraph` leaves registry at the chart default (docker.io).
  if [[ -n "$IMAGE_REPOSITORY" ]]; then
    first="${IMAGE_REPOSITORY%%/*}"
    if [[ "$first" == *.* || "$first" == *:* ]]; then
      SVC_FLAGS+=(--set image.registry="$first" --set image.repository="${IMAGE_REPOSITORY#*/}")
    else
      SVC_FLAGS+=(--set image.repository="$IMAGE_REPOSITORY")
    fi
  fi
  [[ -n "$IMAGE_TAG" ]] && SVC_FLAGS+=(--set image.tag="$IMAGE_TAG")

  # Topology: replicaCount (total alphas) x shardReplicaCount (voting replicas/group).
  SVC_FLAGS+=(--set alpha.replicaCount="$ALPHA_REPLICAS")
  SVC_FLAGS+=(--set zero.shardReplicaCount="$SHARD_REPLICAS")

  # Resources: requests==limits (Guaranteed QoS — no bursting/noisy-neighbor) so a
  # baseline and a candidate get identical CPU/memory. Alpha sizing is opt-in
  # (--alpha-cpu/--alpha-mem) because it must track the node's machine type; if
  # unset, the chart default requests apply (fine for smoke tests, not benchmarks).
  if [[ -n "$ALPHA_CPU" ]]; then
    SVC_FLAGS+=(--set alpha.resources.requests.cpu="$ALPHA_CPU" --set alpha.resources.limits.cpu="$ALPHA_CPU")
  fi
  if [[ -n "$ALPHA_MEM" ]]; then
    SVC_FLAGS+=(--set alpha.resources.requests.memory="$ALPHA_MEM" --set alpha.resources.limits.memory="$ALPHA_MEM")
  fi
  SVC_FLAGS+=(--set zero.resources.requests.cpu="$ZERO_CPU" --set zero.resources.limits.cpu="$ZERO_CPU")
  SVC_FLAGS+=(--set zero.resources.requests.memory="$ZERO_MEM" --set zero.resources.limits.memory="$ZERO_MEM")

  # Assemble alpha.extraFlags = whitelist + any operator-supplied --alpha-extra-flags
  # (e.g. --feature-flags=...). A values overlay avoids `--set` comma/space escaping;
  # the later -f wins over values-base for this scalar. Both flag values must be
  # space-free per token (the chart splits extraFlags on spaces into the command).
  ALPHA_FLAGS="$WHITELIST${ALPHA_EXTRA_FLAGS:+ $ALPHA_EXTRA_FLAGS}"
  OVERLAY="$(mktemp -t dgraph-overlay.XXXXXX)"
  printf 'alpha:\n  extraFlags: "%s"\n' "$ALPHA_FLAGS" > "$OVERLAY"

  # Install from a freshly-pulled, pinned, patched copy of the chart — NOT
  # `dgraph/dgraph` directly. Two reasons:
  #   1. `helm repo update` refreshes the index but `helm install` can reuse a stale
  #      cached tarball, silently changing the deployed command — a reproducibility
  #      hazard for baselines. A fresh `helm pull` of a pinned version avoids that.
  #   2. Chart bug: `dgraph.version` feeds image.tag verbatim into
  #      `semverCompare ">= 21.03.0"`, and Helm semver excludes prereleases — so a
  #      preview tag like v25.4.0-preview1 wrongly selects the legacy `--idx` zero
  #      flag (removed in v25.4+), crash-looping zero. We strip the prerelease from
  #      the version used in comparisons so v25.4.0-preview1 behaves as v25.4.0.
  CHART_TMP="$(mktemp -d)"
  trap 'rm -rf "$OVERLAY" "$CHART_TMP"' EXIT
  helm pull dgraph/dgraph --version "$CHART_VERSION" --untar --untardir "$CHART_TMP" 1>&2
  CHART_PATH="$CHART_TMP/dgraph"
  python3 - "$CHART_PATH/templates/_helpers.tpl" <<'PY'
import sys
p = sys.argv[1]; s = open(p).read()
old = '{{- $safeVersion := .Values.image.tag -}}'
new = '{{- $safeVersion := regexReplaceAll "-.*$" .Values.image.tag "" -}}'
if old in s:
    open(p, "w").write(s.replace(old, new, 1))
else:
    sys.stderr.write("WARN: dgraph.version helper not found to patch; chart layout changed?\n")
PY

  log "Deploying: cfg=$CFG (alphas=$ALPHA_REPLICAS, replicas/group=$SHARD_REPLICAS) chart=$CHART_VERSION expose=$EXPOSE ns=$NS release=$REL${IMAGE_REPOSITORY:+ image=$IMAGE_REPOSITORY}${IMAGE_TAG:+:$IMAGE_TAG}${ALPHA_EXTRA_FLAGS:+ extra-flags=[$ALPHA_EXTRA_FLAGS]}"
  helm upgrade --install "$REL" "$CHART_PATH" -n "$NS" -f "$VALUES_FILE" -f "$OVERLAY" "${SVC_FLAGS[@]}" 1>&2

  wait_ready
fi

# Determine endpoints and export env vars (but note: exports won't persist unless sourced/eval'd)
HOST=""
HTTP_PORT=""
GRPC_PORT=""
ZERO_HTTP_PORT=""
ZERO_GRPC_PORT=""
ZERO_SVC="${REL}-dgraph-zero"

if [[ "$EXPOSE" == "local" ]]; then
  # NodePort assigned on the Alpha service
  PORT_MAP="$(kubectl -n "$NS" get svc "$ALPHA_SVC" -o jsonpath='{range .spec.ports[*]}{.port}{" "}{.nodePort}{"\n"}{end}')"
  HTTP_PORT="$(echo "$PORT_MAP" | awk '$1==8080{print $2}')"
  GRPC_PORT="$(echo "$PORT_MAP" | awk '$1==9080{print $2}')"
  HOST="localhost"

  if [[ -z "${HTTP_PORT:-}" || -z "${GRPC_PORT:-}" ]]; then
    log "Could not determine NodePorts for $ALPHA_SVC. Got:"
    log "$PORT_MAP"
    exit 1
  fi

  # NodePorts on the Zero service (optional — only present when deployed with -e local)
  ZERO_PORT_MAP="$(kubectl -n "$NS" get svc "$ZERO_SVC" -o jsonpath='{range .spec.ports[*]}{.port}{" "}{.nodePort}{"\n"}{end}' 2>/dev/null || true)"
  ZERO_HTTP_PORT="$(echo "$ZERO_PORT_MAP" | awk '$1==6080{print $2}')"
  ZERO_GRPC_PORT="$(echo "$ZERO_PORT_MAP" | awk '$1==5080{print $2}')"
else
  # Wait for LoadBalancer IP/hostname
  for _ in $(seq 1 120); do
    IP="$(kubectl -n "$NS" get svc "$ALPHA_SVC" -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)"
    HN="$(kubectl -n "$NS" get svc "$ALPHA_SVC" -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)"
    HOST="${IP:-$HN}"
    [[ -n "$HOST" ]] && break
    sleep 2
  done

  if [[ -z "$HOST" ]]; then
    log "LoadBalancer external address is still empty for $ALPHA_SVC."
    log "Try: kubectl -n $NS get svc $ALPHA_SVC -w"
    exit 1
  fi

  HTTP_PORT="8080"
  GRPC_PORT="9080"
fi

DGRAPH_ALPHA_HTTP_URL="http://${HOST}:${HTTP_PORT}"
DGRAPH_ALPHA_GRPC_ADDR="${HOST}:${GRPC_PORT}"

DGRAPH_ZERO_HTTP_URL=""
DGRAPH_ZERO_GRPC_ADDR=""
if [[ -n "$ZERO_HTTP_PORT" ]]; then
  DGRAPH_ZERO_HTTP_URL="http://${HOST}:${ZERO_HTTP_PORT}"
fi
if [[ -n "$ZERO_GRPC_PORT" ]]; then
  DGRAPH_ZERO_GRPC_ADDR="${HOST}:${ZERO_GRPC_PORT}"
fi

if $EMIT_ENV; then
  # Print env-setting statements so the user can load them into their shell.
  case "$SHELL_SYNTAX" in
    bash)
      echo "export DGRAPH_ALPHA_HTTP_URL=\"${DGRAPH_ALPHA_HTTP_URL}\""
      echo "export DGRAPH_ALPHA_GRPC_ADDR=\"${DGRAPH_ALPHA_GRPC_ADDR}\""
      [[ -n "$DGRAPH_ZERO_HTTP_URL" ]] && echo "export DGRAPH_ZERO_HTTP_URL=\"${DGRAPH_ZERO_HTTP_URL}\""
      [[ -n "$DGRAPH_ZERO_GRPC_ADDR" ]] && echo "export DGRAPH_ZERO_GRPC_ADDR=\"${DGRAPH_ZERO_GRPC_ADDR}\""
      ;;
    fish)
      echo "set -gx DGRAPH_ALPHA_HTTP_URL \"${DGRAPH_ALPHA_HTTP_URL}\""
      echo "set -gx DGRAPH_ALPHA_GRPC_ADDR \"${DGRAPH_ALPHA_GRPC_ADDR}\""
      [[ -n "$DGRAPH_ZERO_HTTP_URL" ]] && echo "set -gx DGRAPH_ZERO_HTTP_URL \"${DGRAPH_ZERO_HTTP_URL}\""
      [[ -n "$DGRAPH_ZERO_GRPC_ADDR" ]] && echo "set -gx DGRAPH_ZERO_GRPC_ADDR \"${DGRAPH_ZERO_GRPC_ADDR}\""
      ;;
  esac
else
  log "Alpha endpoint:"
  log "  DGRAPH_ALPHA_HTTP_URL=${DGRAPH_ALPHA_HTTP_URL}"
  log "  DGRAPH_ALPHA_GRPC_ADDR=${DGRAPH_ALPHA_GRPC_ADDR}"
  if [[ -n "$DGRAPH_ZERO_HTTP_URL" || -n "$DGRAPH_ZERO_GRPC_ADDR" ]]; then
    log "Zero endpoint:"
    [[ -n "$DGRAPH_ZERO_HTTP_URL" ]] && log "  DGRAPH_ZERO_HTTP_URL=${DGRAPH_ZERO_HTTP_URL}"
    [[ -n "$DGRAPH_ZERO_GRPC_ADDR" ]] && log "  DGRAPH_ZERO_GRPC_ADDR=${DGRAPH_ZERO_GRPC_ADDR}"
  fi
  log ""
  log "Tip: to export into your current shell:"
  log "  bash/zsh: eval \"\$(./local-provision.sh -c $CFG -e $EXPOSE --emit-env)\""
  log "  fish:     ./local-provision.sh -c $CFG -e $EXPOSE --emit-env --shell fish | source"
fi
