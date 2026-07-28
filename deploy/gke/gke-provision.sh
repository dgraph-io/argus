#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  ./gke-provision.sh <command> [options]

Commands:
  create       Create GKE cluster and deploy Dgraph
  teardown     Delete GKE cluster and associated resources

Required for 'create':
  -p, --project        GCP project ID
  -c, --config         Topology descriptor: single|ha3|ha5|shard3|shard3-ha3
                       (sharded/nonsharded accepted as aliases)

The cluster is provisioned for reproducible benchmarking: a dedicated Dgraph node
pool (machine type -m, labeled+tainted workload=dgraph) holds only the system
under test, while a separate argus pool (--argus-machine-type, labeled
workload=argus) runs the load generator — so the generator never steals CPU from
Dgraph. Data PVs use a created pd-ssd StorageClass ("ssd").

Options:
  -z, --zone           GCP zone (default: us-central1-a)
      --zones          Space-separated candidate zones to auto-select from by
                       probing live capacity for the Dgraph machine type, e.g.
                       "us-central1-c us-east1-b us-west1-b". Dodges GCE stockouts:
                       the first zone with capacity (or an existing cluster of this
                       name) is used, overriding -z.
  -C, --cluster        GKE cluster name (default: dgraph-cluster)
  -m, --machine-type   Dgraph pool machine type (default: n2-standard-8)
  -N, --num-nodes      Dgraph pool node count (default: 3; must fit the topology)
      --argus-machine-type  argus/system pool machine type (default: e2-standard-4)
      --disk-type      Persistent-disk type for the ssd StorageClass (default: pd-ssd)
  -n, --namespace      Kubernetes namespace (default: dgraph)
  -r, --release        Helm release name (default: dgraph)
      --disk-size      Dgraph node boot disk size in GB (default: 100)
      --alpha-cpu      Alpha CPU requests==limits (Guaranteed QoS), e.g. 6
      --alpha-mem      Alpha memory requests==limits, e.g. 26Gi
      --zero-cpu       Zero CPU requests==limits (default 1)
      --zero-mem       Zero memory requests==limits (default 2Gi)
      --image-repository  Override Dgraph image repository (e.g. dgraph/dgraph)
      --image-tag         Override Dgraph image tag (e.g. v25.3.7)
      --reset          Delete PVCs before (re)deploying Dgraph
      --lb-source-range CIDR to restrict LB traffic (e.g. your IP/32)
      --emit-env       Print export lines for DGRAPH_* vars
      --shell          Shell syntax for --emit-env: bash|fish (default: bash)
      --skip-deploy    Only create GKE cluster, skip Dgraph deployment
      --keep-disks     (teardown) Keep orphaned PVs (default: reclaim them, prompted)
  -h, --help           Show help

Examples:
  # Create cluster and deploy an HA (1x3) Dgraph on an SSD-backed dedicated pool
  ./gke-provision.sh create -p my-gcp-project -c ha3 -m n2-standard-8 -N 4 \
    --alpha-cpu 6 --alpha-mem 26Gi

  # Sharded (3x1)
  ./gke-provision.sh create -p my-gcp-project -c shard3 -N 4

  # Auto-pick a zone with capacity (dodges GCE stockouts)
  ./gke-provision.sh create -p my-gcp-project -c single \
    --zones "us-central1-c us-east1-b us-west1-b us-east4-c"

  # Restrict LoadBalancer to your IP
  ./gke-provision.sh create -p my-gcp-project -c ha3 --lb-source-range "$(curl -s ifconfig.me)/32"

  # Teardown cluster
  ./gke-provision.sh teardown -p my-gcp-project

  # Teardown but KEEP the persistent disks (default reclaims them)
  ./gke-provision.sh teardown -p my-gcp-project --keep-disks

  # Export env vars into your current shell:
  eval "$(./gke-provision.sh create -p my-gcp-project -c sharded --emit-env)"
USAGE
}

# Defaults
PROJECT=""
ZONE="us-central1-a"
ZONES=""
CLUSTER_NAME="dgraph-cluster"
MACHINE_TYPE="n2-standard-8"
ARGUS_MACHINE_TYPE="e2-standard-4"
DISK_TYPE="pd-ssd"
NUM_NODES="3"
DISK_SIZE="100"
ALPHA_CPU=""
ALPHA_MEM=""
ALPHA_EXTRA_FLAGS=""
CHART_VERSION=""
ZERO_CPU=""
ZERO_MEM=""
NS="dgraph"
REL="dgraph"
CFG=""
RESET=false
EMIT_ENV=false
LB_SOURCE_RANGE=""
SKIP_DEPLOY=false
DELETE_DISKS=true   # teardown reclaims orphaned PVs by default (prompted); --keep-disks to skip
IMAGE_REPOSITORY=""
IMAGE_TAG=""
SHELL_SYNTAX=""
COMMAND=""

log() { echo "$@" >&2; }

die() { log "ERROR: $*"; exit 1; }

check_gcloud() {
  if ! command -v gcloud &>/dev/null; then
    die "gcloud CLI not found. Install: https://cloud.google.com/sdk/docs/install"
  fi
}

check_prerequisites() {
  check_gcloud
  if ! command -v kubectl &>/dev/null; then
    die "kubectl not found. Install: https://kubernetes.io/docs/tasks/tools/"
  fi
  if ! command -v helm &>/dev/null; then
    die "helm not found. Install: https://helm.sh/docs/intro/install/"
  fi
}

# Parse command
if [[ $# -lt 1 ]]; then
  usage
  exit 2
fi

COMMAND="$1"
shift

case "$COMMAND" in
  create|teardown) ;;
  -h|--help) usage; exit 0;;
  *) die "Unknown command: $COMMAND";;
esac

# Parse options
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--project) PROJECT="${2:?missing value for $1}"; shift 2;;
    -z|--zone) ZONE="${2:?missing value for $1}"; shift 2;;
    --zones) ZONES="${2:?missing value for $1}"; shift 2;;
    -C|--cluster) CLUSTER_NAME="${2:?missing value for $1}"; shift 2;;
    -m|--machine-type) MACHINE_TYPE="${2:?missing value for $1}"; shift 2;;
    --argus-machine-type) ARGUS_MACHINE_TYPE="${2:?missing value for $1}"; shift 2;;
    --disk-type) DISK_TYPE="${2:?missing value for $1}"; shift 2;;
    -N|--num-nodes) NUM_NODES="${2:?missing value for $1}"; shift 2;;
    --disk-size) DISK_SIZE="${2:?missing value for $1}"; shift 2;;
    --alpha-cpu) ALPHA_CPU="${2:?missing value for $1}"; shift 2;;
    --alpha-mem) ALPHA_MEM="${2:?missing value for $1}"; shift 2;;
    --alpha-extra-flags) ALPHA_EXTRA_FLAGS="${2:?missing value for $1}"; shift 2;;
    --chart-version) CHART_VERSION="${2:?missing value for $1}"; shift 2;;
    --zero-cpu) ZERO_CPU="${2:?missing value for $1}"; shift 2;;
    --zero-mem) ZERO_MEM="${2:?missing value for $1}"; shift 2;;
    -c|--config) CFG="${2:?missing value for $1}"; shift 2;;
    -n|--namespace) NS="${2:?missing value for $1}"; shift 2;;
    -r|--release) REL="${2:?missing value for $1}"; shift 2;;
    --reset) RESET=true; shift;;
    --lb-source-range) LB_SOURCE_RANGE="${2:?missing value for $1}"; shift 2;;
    --image-repository) IMAGE_REPOSITORY="${2:?missing value for $1}"; shift 2;;
    --image-tag) IMAGE_TAG="${2:?missing value for $1}"; shift 2;;
    --shell) SHELL_SYNTAX="${2:?missing value for $1}"; shift 2;;
    --emit-env) EMIT_ENV=true; shift;;
    --skip-deploy) SKIP_DEPLOY=true; shift;;
    --delete-disks) DELETE_DISKS=true; shift;;
    --keep-disks) DELETE_DISKS=false; shift;;
    -h|--help) usage; exit 0;;
    *)
      log "Unknown argument: $1"
      usage
      exit 2
      ;;
  esac
done

# Validate
if [[ -z "$PROJECT" ]]; then
  die "Missing required flag: --project"
fi

# Probe live capacity by actually creating (then deleting) a throwaway VM — the
# only definitive test, since GCE exposes no availability API. A bare instance
# hits the same capacity pool as a GKE node and fails fast on stockout.
PROBE_ERR=""
probe_capacity() { # <zone> <machine> -> 0 if capacity available; sets PROBE_ERR on failure
  local z="$1" m="$2" name="capacity-probe-$$-${z##*-}" out
  if out="$(gcloud compute instances create "$name" --zone "$z" --machine-type "$m" \
       --no-address --project "$PROJECT" --quiet 2>&1)"; then
    gcloud compute instances delete "$name" --zone "$z" --project "$PROJECT" --quiet >/dev/null 2>&1 || true
    PROBE_ERR=""; return 0
  fi
  PROBE_ERR="$out"; return 1
}

# Classify a failed probe so quota isn't misread as capacity.
probe_reason() {
  case "$PROBE_ERR" in
    *"does not have enough resources"*|*ZONE_RESOURCE_POOL_EXHAUSTED*|*STOCKOUT*) echo "stockout";;
    *[Qq]uota*|*QUOTA_EXCEEDED*) echo "QUOTA denied (bump N2_CPUS/CPUS in this region, or use fewer/smaller nodes)";;
    *) echo "failed: $(printf '%s' "$PROBE_ERR" | grep -iE 'error|denied|forbidden' | head -1)";;
  esac
}

# When --zones is set, pick the zone to use: reuse a zone that already hosts this
# cluster (resume), else the first candidate with Dgraph-machine capacity. Sets ZONE.
select_zone() {
  local z
  for z in $ZONES; do
    if gcloud container clusters describe "$CLUSTER_NAME" --zone "$z" --project "$PROJECT" &>/dev/null; then
      log "Existing cluster '$CLUSTER_NAME' found in $z — reusing that zone."
      ZONE="$z"; return 0
    fi
  done
  log "Probing candidate zones for $MACHINE_TYPE capacity: $ZONES"
  local quota_seen=false
  for z in $ZONES; do
    log -n "  $z ... "
    if probe_capacity "$z" "$MACHINE_TYPE"; then
      log "available — selecting it."
      ZONE="$z"; return 0
    fi
    local reason; reason="$(probe_reason)"
    case "$reason" in QUOTA*) quota_seen=true;; esac
    log "$reason"
  done
  if $quota_seen; then
    die "Probes were QUOTA-denied, not stocked out — a different zone/machine won't help. Check quota: gcloud compute regions describe <region> --project $PROJECT --format='flattened(quotas)' | grep -iE 'N2_CPUS|[^_]CPUS'  (request an increase, or use fewer/smaller nodes)."
  fi
  die "No candidate zone has $MACHINE_TYPE capacity ($ZONES). Try a different -m (e.g. n2d-standard-8, c3-standard-8), more --zones, or retry later."
}

dgraph_pool_status() {
  gcloud container node-pools describe dgraph-pool \
    --cluster "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT" \
    --format='value(status)' 2>/dev/null || echo ABSENT
}

# Create (or reuse/repair) the dedicated Dgraph node pool. Robust to the two ways
# this bites in practice: (1) gcloud's client-side wait timing out while the pool
# is merely slow — we submit with --async and poll the pool's own status instead
# of trusting gcloud's exit code; (2) a prior attempt left the pool in a non-RUNNING
# state (e.g. a zone stockout → ERROR) — we delete and recreate rather than skip.
ensure_dgraph_pool() {
  local st; st="$(dgraph_pool_status)"
  case "$st" in
    RUNNING)
      # Reuse the pool, but resize it to match this topology's -N. Reusing a
      # too-small pool (e.g. a 1-node `single` pool for a 3-alpha `ha3`) leaves
      # alpha pods Pending forever; a too-big one wastes money. Match exactly.
      local have; have="$(gcloud container node-pools describe dgraph-pool \
        --cluster "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT" \
        --format='value(initialNodeCount)' 2>/dev/null || echo '')"
      if [[ -n "$have" && "$have" != "$NUM_NODES" ]]; then
        log "Dgraph pool has $have node(s); resizing to $NUM_NODES for this topology..."
        gcloud container clusters resize "$CLUSTER_NAME" --node-pool dgraph-pool \
          --num-nodes "$NUM_NODES" --zone "$ZONE" --project "$PROJECT" --quiet 1>&2 \
          || die "dgraph-pool resize to $NUM_NODES failed"
      fi
      log "Dgraph pool 'dgraph-pool' RUNNING with $NUM_NODES node(s) — reusing."
      return 0
      ;;
    ABSENT)  ;; # fall through to create
    *)
      log "Dgraph pool 'dgraph-pool' exists but status=$st; deleting before recreate..."
      gcloud container node-pools delete dgraph-pool \
        --cluster "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT" --quiet 1>&2 || true
      ;;
  esac

  log "Creating dedicated Dgraph node pool 'dgraph-pool' ($MACHINE_TYPE x $NUM_NODES, $DISK_TYPE)..."
  # --async: submit and return immediately; we own the wait below. A synchronous
  # rejection (bad machine type, etc.) still fails here and aborts, as it should.
  gcloud container node-pools create dgraph-pool \
    --project "$PROJECT" --zone "$ZONE" --cluster "$CLUSTER_NAME" \
    --machine-type "$MACHINE_TYPE" --num-nodes "$NUM_NODES" \
    --disk-type "$DISK_TYPE" --disk-size "${DISK_SIZE}GB" \
    --node-labels workload=dgraph --node-taints workload=dgraph:NoSchedule \
    --metadata disable-legacy-endpoints=true \
    --scopes "https://www.googleapis.com/auth/cloud-platform" \
    --async 1>&2

  log "Waiting for 'dgraph-pool' to provision (can take several minutes)..."
  local waited=0 max=1800 seen=false st2
  while :; do
    st2="$(dgraph_pool_status)"
    case "$st2" in
      RUNNING) log "Dgraph pool is RUNNING."; return 0;;
      ERROR|RUNNING_WITH_ERROR|STOPPING)
        log "Dgraph pool entered status=$st2. Most recent CREATE_NODE_POOL error:"
        gcloud container operations list --zone "$ZONE" --project "$PROJECT" \
          --filter="operationType=CREATE_NODE_POOL AND targetLink~dgraph-pool" \
          --sort-by=~startTime --limit=1 --format='value(error.message)' 1>&2 || true
        die "Dgraph node pool provisioning failed (status=$st2). Common cause: zone stockout for $MACHINE_TYPE — retry with --zone <other> (stockouts are zone-specific) or a different -m (e.g. n2d-standard-8, c3-standard-8)."
        ;;
      ABSENT)
        # Tolerate a brief window before the pool becomes describable; after that,
        # ABSENT means the create was rejected or the pool was rolled back.
        if $seen; then die "Dgraph node pool vanished during provisioning — try --zone <other> or a different -m."; fi
        [[ $waited -ge 120 ]] && die "Dgraph node pool never appeared (create rejected?) — check: gcloud container operations list --zone $ZONE --project $PROJECT"
        ;;
      *) seen=true;; # PROVISIONING / RECONCILING
    esac
    [[ $waited -ge $max ]] && die "Dgraph node pool not RUNNING after ${max}s; inspect it in the GCP console."
    sleep 15; waited=$((waited+15))
  done
}

create_cluster() {
  check_prerequisites

  if [[ -z "$CFG" && "$SKIP_DEPLOY" == "false" ]]; then
    die "Missing required flag: --config (single|ha3|ha5|shard3|shard3-ha3)"
  fi

  # Auto-select a zone with capacity when a candidate list is given.
  [[ -n "$ZONES" ]] && select_zone

  log "=== GKE Cluster Provisioning ==="
  log "Project:            $PROJECT"
  log "Zone:               $ZONE"
  log "Cluster:            $CLUSTER_NAME"
  log "Dgraph pool:        $MACHINE_TYPE x $NUM_NODES (workload=dgraph, tainted)"
  log "argus/system pool:  $ARGUS_MACHINE_TYPE x 1 (workload=argus)"
  log "Disk type:          $DISK_TYPE (StorageClass 'ssd')"
  log "Boot disk:          ${DISK_SIZE}GB"
  log ""

  # Check if cluster already exists
  if gcloud container clusters describe "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT" &>/dev/null; then
    log "Cluster '$CLUSTER_NAME' already exists. Reusing."
  else
    # The default pool doubles as the argus/system pool (untainted so kube-system
    # schedules there); it's labeled workload=argus so the load generator lands here.
    log "Creating GKE cluster '$CLUSTER_NAME' (argus/system pool)..."
    gcloud container clusters create "$CLUSTER_NAME" \
      --project "$PROJECT" \
      --zone "$ZONE" \
      --machine-type "$ARGUS_MACHINE_TYPE" \
      --num-nodes 1 \
      --disk-size "${DISK_SIZE}GB" \
      --node-labels workload=argus \
      --enable-ip-alias \
      --no-enable-basic-auth \
      --metadata disable-legacy-endpoints=true \
      --scopes "https://www.googleapis.com/auth/cloud-platform"
  fi

  # Dedicated Dgraph pool — created idempotently and *independent* of whether the
  # cluster pre-existed, so a resume after a failed/slow pool can finish the job.
  ensure_dgraph_pool

  # Get credentials
  log "Fetching kubectl credentials..."
  gcloud container clusters get-credentials "$CLUSTER_NAME" \
    --zone "$ZONE" \
    --project "$PROJECT"

  log "kubectl context set to cluster '$CLUSTER_NAME'"
  kubectl cluster-info

  # Create the pd-ssd StorageClass the values reference. WaitForFirstConsumer binds
  # the PV in the same zone as the pod (correct for a zonal cluster + node pools).
  log "Applying 'ssd' StorageClass ($DISK_TYPE)..."
  kubectl apply -f - <<EOF 1>&2
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: ssd
provisioner: pd.csi.storage.gke.io
parameters:
  type: $DISK_TYPE
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
reclaimPolicy: Delete
EOF

  if $SKIP_DEPLOY; then
    log ""
    log "Skipping Dgraph deployment (--skip-deploy)."
    log "Run local-provision.sh manually to deploy:"
    log "  ./local-provision.sh -c <single|ha3|ha5|shard3|shard3-ha3> -e gke"
    return
  fi

  # Deploy Dgraph using local-provision.sh
  log ""
  log "=== Deploying Dgraph ==="

  CONFIG_ARGS=(-c "$CFG" -e gke -n "$NS" -r "$REL")
  $RESET && CONFIG_ARGS+=(--reset)
  [[ -n "$LB_SOURCE_RANGE" ]] && CONFIG_ARGS+=(--lb-source-range "$LB_SOURCE_RANGE")
  [[ -n "$IMAGE_REPOSITORY" ]] && CONFIG_ARGS+=(--image-repository "$IMAGE_REPOSITORY")
  [[ -n "$IMAGE_TAG" ]] && CONFIG_ARGS+=(--image-tag "$IMAGE_TAG")
  [[ -n "$ALPHA_CPU" ]] && CONFIG_ARGS+=(--alpha-cpu "$ALPHA_CPU")
  [[ -n "$ALPHA_MEM" ]] && CONFIG_ARGS+=(--alpha-mem "$ALPHA_MEM")
  [[ -n "$ALPHA_EXTRA_FLAGS" ]] && CONFIG_ARGS+=(--alpha-extra-flags "$ALPHA_EXTRA_FLAGS")
  [[ -n "$CHART_VERSION" ]] && CONFIG_ARGS+=(--chart-version "$CHART_VERSION")
  [[ -n "$ZERO_CPU" ]] && CONFIG_ARGS+=(--zero-cpu "$ZERO_CPU")
  [[ -n "$ZERO_MEM" ]] && CONFIG_ARGS+=(--zero-mem "$ZERO_MEM")
  [[ -n "$SHELL_SYNTAX" ]] && CONFIG_ARGS+=(--shell "$SHELL_SYNTAX")
  $EMIT_ENV && CONFIG_ARGS+=(--emit-env)

  "$SCRIPT_DIR/local-provision.sh" "${CONFIG_ARGS[@]}"
}

delete_orphaned_disks() {
  log "Searching for orphaned disks in zone '$ZONE'..."

  # Find disks that were created by GKE PVCs (they have goog-gke-volume prefix or pvc- in name)
  DISKS=$(gcloud compute disks list \
    --project="$PROJECT" \
    --filter="zone:$ZONE AND (name~'^gke-' OR name~'^pvc-')" \
    --format="value(name)" 2>/dev/null || true)

  if [[ -z "$DISKS" ]]; then
    log "No orphaned disks found."
    return
  fi

  log "Found disks:"
  echo "$DISKS" | while read -r disk; do
    log "  - $disk"
  done

  read -r -p "Delete these disks? [y/N] " confirm
  case "$confirm" in
    [yY][eE][sS]|[yY])
      echo "$DISKS" | while read -r disk; do
        log "Deleting disk '$disk'..."
        gcloud compute disks delete "$disk" \
          --project="$PROJECT" \
          --zone="$ZONE" \
          --quiet || log "  Failed to delete $disk (may already be deleted)"
      done
      log "Disk cleanup complete."
      ;;
    *)
      log "Skipped disk deletion."
      ;;
  esac
}

teardown_cluster() {
  check_gcloud

  log "=== GKE Cluster Teardown ==="
  log "Project:      $PROJECT"
  log "Zone:         $ZONE"
  log "Cluster:      $CLUSTER_NAME"
  log "Delete disks: $DELETE_DISKS"
  log ""

  if ! gcloud container clusters describe "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT" &>/dev/null; then
    log "Cluster '$CLUSTER_NAME' does not exist."
    # The cluster may have been deleted already while its PVs orphaned — clean those
    # up regardless (deleting a GKE cluster does NOT reclaim dynamically-provisioned
    # pd-ssd PVs, so they keep billing). Skip only with --keep-disks.
    if $DELETE_DISKS; then
      log ""
      delete_orphaned_disks
    else
      log "Nothing to delete. (Orphaned PVs, if any, are left — omit --keep-disks to clean them.)"
    fi
    return
  fi

  read -r -p "Delete cluster '$CLUSTER_NAME'? This cannot be undone. [y/N] " confirm
  case "$confirm" in
    [yY][eE][sS]|[yY])
      log "Deleting cluster '$CLUSTER_NAME'..."
      gcloud container clusters delete "$CLUSTER_NAME" \
        --zone "$ZONE" \
        --project "$PROJECT" \
        --quiet

      log "Cluster deleted."

      # Clean up kubectl context
      CONTEXT="gke_${PROJECT}_${ZONE}_${CLUSTER_NAME}"
      kubectl config delete-context "$CONTEXT" &>/dev/null || true
      kubectl config delete-cluster "$CONTEXT" &>/dev/null || true

      log "kubectl context cleaned up."

      # Delete orphaned disks if requested
      if $DELETE_DISKS; then
        log ""
        delete_orphaned_disks
      fi
      ;;
    *)
      log "Aborted."
      ;;
  esac
}

case "$COMMAND" in
  create) create_cluster;;
  teardown) teardown_cluster;;
esac
