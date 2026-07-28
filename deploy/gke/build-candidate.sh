#!/usr/bin/env bash
# Build a candidate Dgraph image via Cloud Build (native linux/amd64, jemalloc)
# and push it to Artifact Registry, optionally folding in local badger/ristretto
# builds via go.mod replace.
#
# Why Cloud Build: dgraph's in-container build (contrib/Dockerfile.build) does
# `COPY . .`, so it can't see sibling modules — folding local badger/ristretto
# needs a *host* build with filesystem replaces. Cloud Build's amd64 VM is that
# host, and it produces the amd64+jemalloc image GKE runs.
#
# On success the AR image ref is printed as the LAST line of stdout (everything
# else goes to stderr) so an orchestrator can capture it:
#   IMAGE="$(./build-candidate.sh ... 2>/tmp/log)"
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  ./build-candidate.sh --project PROJECT --dgraph PATH [options]

Required:
  -p, --project      GCP project ID (Artifact Registry + Cloud Build)
      --dgraph       Path to the dgraph working tree to build (uncommitted changes included)

Options:
      --badger       Path to a local badger checkout to fold in (go.mod replace)
      --ristretto    Path to a local ristretto checkout to fold in (go.mod replace)
      --region       Artifact Registry region (default: us-central1)
      --repo         Artifact Registry repository name (default: argus)
      --tag          Image tag (default: cand-<dgraph short SHA>[-dirty])
      --go-version   Go toolchain for the build (default: 1.26)
  -h, --help         Show help

Example:
  ./build-candidate.sh -p my-proj --dgraph ~/code/dgraph \
    --badger ~/code/badger --ristretto ~/code/ristretto
USAGE
}

log() { echo "$@" >&2; }
die() { log "ERROR: $*"; exit 1; }

PROJECT="" DGRAPH="" BADGER="" RISTRETTO=""
REGION="us-central1" REPO="argus" TAG="" GO_VERSION="1.26"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--project) PROJECT="${2:?}"; shift 2;;
    --dgraph) DGRAPH="${2:?}"; shift 2;;
    --badger) BADGER="${2:?}"; shift 2;;
    --ristretto) RISTRETTO="${2:?}"; shift 2;;
    --region) REGION="${2:?}"; shift 2;;
    --repo) REPO="${2:?}"; shift 2;;
    --tag) TAG="${2:?}"; shift 2;;
    --go-version) GO_VERSION="${2:?}"; shift 2;;
    -h|--help) usage; exit 0;;
    *) log "Unknown argument: $1"; usage; exit 2;;
  esac
done

[[ -n "$PROJECT" ]] || die "Missing --project"
[[ -n "$DGRAPH"  ]] || die "Missing --dgraph"
[[ -d "$DGRAPH/.git" ]] || die "--dgraph ($DGRAPH) is not a git working tree"
command -v gcloud >/dev/null || die "gcloud not found"
command -v go >/dev/null || die "go not found (needed for go.mod edit)"

# Default tag from the dgraph commit (+dirty when the tree has uncommitted edits).
if [[ -z "$TAG" ]]; then
  sha="$(git -C "$DGRAPH" rev-parse --short HEAD)"
  git -C "$DGRAPH" diff --quiet && dirty="" || dirty="-dirty"
  TAG="cand-${sha}${dirty}"
fi
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/dgraph:${TAG}"

# Stage the build context: dgraph (with .git for stamping) + optional siblings.
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
log "==> staging build context in $STAGE"
rsync -a --delete --exclude='/linux/' "$DGRAPH/" "$STAGE/dgraph/"

FOLDED=()
fold() { # <module-path> <src-dir> <dest-name>
  local mod="$1" src="$2" name="$3"
  [[ -d "$src" ]] || die "fold source not found: $src"
  grep -q "$mod " "$STAGE/dgraph/go.mod" || die "dgraph go.mod has no require for $mod"
  rsync -a --delete --exclude='/.git/' "$src/" "$STAGE/$name/"
  ( cd "$STAGE/dgraph" && go mod edit -replace "${mod}=../${name}" )
  FOLDED+=("${mod} -> ${src}")
  log "==> folding ${mod} from ${src}"
}
[[ -n "$BADGER"    ]] && fold "github.com/dgraph-io/badger/v4"    "$BADGER"    "badger"
[[ -n "$RISTRETTO" ]] && fold "github.com/dgraph-io/ristretto/v2" "$RISTRETTO" "ristretto"

log "==> submitting Cloud Build"
log "    image:  $IMAGE"
log "    go:     $GO_VERSION"
[[ ${#FOLDED[@]} -gt 0 ]] && log "    folded: ${FOLDED[*]}"

gcloud builds submit "$STAGE" \
  --project "$PROJECT" \
  --config "$SCRIPT_DIR/cloudbuild.yaml" \
  --substitutions "_IMAGE=${IMAGE},_GO_VERSION=${GO_VERSION},_DGRAPH_VERSION=${TAG}" \
  1>&2

log "==> built and pushed: $IMAGE"
# Machine-readable result: image ref on the last stdout line.
echo "$IMAGE"
