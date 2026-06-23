# Running argus on Kubernetes / GKE

argus is a load generator *and* a per-node metrics collector, so it needs to
reach **each alpha individually** — both to spread load deterministically (dgo
and GraphQL round-robin across all alphas) and to scrape each node's
`/debug/prometheus_metrics`. That requirement drives how it must be deployed.

## Run it in-cluster (recommended)

On GKE the alpha is fronted by a **single LoadBalancer VIP**, and the addresses
in the cluster `/state` are pod-internal (headless DNS / pod IPs). From outside
the cluster argus therefore cannot reach individual alphas: every request to the
VIP lands on a random alpha, which scrambles per-node scraping (consecutive
scrapes diff counters from different pods → garbage rates).

Running argus **as a Job in the same namespace** fixes this: `/state`-discovered
per-pod addresses resolve, each alpha's `:8080`/`:9080` is reachable, load
spreads evenly across pods, and the cluster CSV gets real per-node data.

```bash
# 1. Build and push the image (built on the host so the local dgo replace resolves).
deploy/build-image.sh us-docker.pkg.dev/PROJECT/REPO/argus:v1
docker push us-docker.pkg.dev/PROJECT/REPO/argus:v1

# 2. Edit deploy/argus-job.yaml: set the image, namespace, alpha Service name,
#    -label (machine type / node count / config / region), and -dgraph-branch.
kubectl apply -f deploy/argus-job.yaml

# 3. Watch it.
kubectl logs -f job/argus

# 4. Copy the CSVs out (the pod lingers ttlSecondsAfterFinished).
kubectl cp "$(kubectl get pod -l app=argus -o name | head -1 | cut -d/ -f2)":/out ./results
```

argus only needs the alpha **Service** as a discovery seed (`-graphql`/`-cluster`);
it finds every alpha from `/state` and talks to them directly. No need to enumerate
pods, and the zero Service can stay ClusterIP (argus reads `/state` from an alpha).

Schedule argus on a **separate node pool** from Dgraph (see the `nodeSelector` in
the Job) so the load generator doesn't compete with the system under test for CPU.

## Laptop access (smoke tests only)

`kubectl port-forward` tunnels everything through a single proxied TCP stream — fine
for a correctness check, **not** for throughput numbers. If you must, forward each
**pod** (not the Service, which randomizes) on distinct local ports and pass them
explicitly:

```bash
kubectl port-forward pod/dgraph-dgraph-alpha-0 8080:8080 9080:9080 &
kubectl port-forward pod/dgraph-dgraph-alpha-1 8082:8080 9082:9080 &
kubectl port-forward pod/dgraph-dgraph-alpha-2 8084:8080 9084:9080 &
argus -small -duration 2m \
  -graphql http://localhost:8080 \
  -alphas http://localhost:8080,http://localhost:8082,http://localhost:8084
```

## What the report does and doesn't capture

- **Per-node RSS/CPU/throughput** come from each alpha's Prometheus metrics — real,
  per-node, aggregated correctly in the `CLUSTER` row of the cluster CSV.
- **Host CPU/RAM** in the header is the **load generator** (the argus pod), not the
  alpha node hardware. The GKE machine type / node count aren't exposed by the
  cluster API, so record them with `-label` (e.g.
  `-label machine=e2-standard-4,nodes=3,region=us-central1,config=sharded`).
- **Topology** (`sharded` vs `HA`, group/replica counts) is read from `/state`.

## Caveats / not yet supported

- **No TLS or ACL client support.** argus dials `insecure` gRPC and plain `http://`.
  The default deploy has neither enabled; if you turn on TLS or ACL, argus needs new
  flags first.
- **`--security whitelist`** (if you add one) must include argus's source. In-cluster
  the pod CIDR is simplest.
- **Subscriptions** ride the L4 LoadBalancer fine; an L7 ingress may break the
  WebSocket upgrade or time out idle connections.
