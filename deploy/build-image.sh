#!/usr/bin/env bash
# Build a static linux/amd64 argus binary on the host (so the local dgo `replace`
# resolves) and package it into a minimal image.
#
# Usage: deploy/build-image.sh [IMAGE] [--push]
#   IMAGE defaults to argus:latest. For GKE, tag for Artifact Registry and --push:
#     deploy/build-image.sh us-docker.pkg.dev/PROJECT/REPO/argus:v1 --push
set -euo pipefail

IMAGE="argus:latest"
PUSH=false
for arg in "$@"; do
  case "$arg" in
    --push) PUSH=true;;
    -*) echo "Unknown flag: $arg" >&2; exit 2;;
    *) IMAGE="$arg";;
  esac
done

cd "$(dirname "$0")/.."

echo "==> building static linux/amd64 binary"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -o bin/argus-linux-amd64 ./cmd/argus

echo "==> docker build -> ${IMAGE}"
# --platform linux/amd64 is required: the binary is amd64, and without it a build
# on an arm64 host (Apple Silicon) pulls the arm64 base and labels the image
# linux/arm64 — which then can't run on amd64 GKE nodes.
docker build --platform linux/amd64 -f deploy/Dockerfile -t "${IMAGE}" .

if $PUSH; then
  echo "==> docker push ${IMAGE}"
  docker push "${IMAGE}"
  echo "==> done (built + pushed): ${IMAGE}"
else
  echo "==> done: ${IMAGE}"
  echo "    push with: docker push ${IMAGE}   (or re-run with --push)"
fi
