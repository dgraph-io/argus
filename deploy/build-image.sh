#!/usr/bin/env bash
# Build a static linux/amd64 argus binary on the host (so the local dgo `replace`
# resolves) and package it into a minimal image.
#
# Usage: deploy/build-image.sh [IMAGE]
#   IMAGE defaults to argus:latest. For GKE, tag for Artifact Registry, e.g.
#   deploy/build-image.sh us-docker.pkg.dev/PROJECT/REPO/argus:v1 && docker push <same>
set -euo pipefail

IMAGE="${1:-argus:latest}"
cd "$(dirname "$0")/.."

echo "==> building static linux/amd64 binary"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -o bin/argus-linux-amd64 ./cmd/argus

echo "==> docker build -> ${IMAGE}"
docker build -f deploy/Dockerfile -t "${IMAGE}" .

echo "==> done: ${IMAGE}"
echo "    push with: docker push ${IMAGE}"
