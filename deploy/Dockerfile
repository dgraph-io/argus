# Minimal runtime image for argus.
#
# The binary is built on the host (see deploy/build-image.sh) because the module
# uses a local `replace` for the dgo fork, which is not reachable from inside a
# Docker build context. build-image.sh cross-compiles a static linux/amd64
# binary on the host (where the replace path exists) and then runs this build.
FROM gcr.io/distroless/static-debian12:nonroot

COPY bin/argus-linux-amd64 /argus

ENTRYPOINT ["/argus"]
