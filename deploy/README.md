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

**For GKE benchmarking you normally don't touch this Job directly** — the
orchestrator [`deploy/gke/benchmark.sh`](gke/) drives it end to end (provision a
dedicated-pool cluster → deploy Dgraph → render + run this Job → copy the ledger
line into [`results/gke/runs.jsonl`](../results/gke) → `argus compare`). See
[deploy/gke/README.md](gke/README.md) for that workflow. This doc covers the Job
itself and the manual path for when you're not using the orchestrator:

```bash
# 1. Build and push the image (built on the host so the local dgo replace resolves).
deploy/build-image.sh us-docker.pkg.dev/PROJECT/REPO/argus:v1 --push

# 2. Render the Job template and apply it. Only the ${VARS} are substituted;
#    the container's $(DGRAPH_ALPHA) downward-ref is left alone. Workload shape is
#    the canonical library shape (scale 0.25 / passes 5 / repeats 3 / wc 8); the
#    candidate you compare against a baseline must match its shape.
export JOB_NAME=argus NAMESPACE=dgraph ALPHA_SVC=dgraph-dgraph-alpha \
  ARGUS_IMAGE=us-docker.pkg.dev/PROJECT/REPO/argus:v1 \
  RUN_ID=$(date +%Y%m%d-%H%M%S)-ha3 PASSES=5 REPEATS=3 SCALE=0.25 WRITE_CONC=8 \
  CPU_CLASS=n2-standard-8 DISK_CLASS=pd-ssd DEPLOY=gke LABEL=config=ha3 \
  SHARD_BALANCE=false ZERO_URL=http://dgraph-dgraph-zero:6080 \
  DGRAPH_BRANCH="" DGRAPH_COMMIT="$(kubectl -n dgraph exec dgraph-dgraph-alpha-0 -- dgraph version 2>/dev/null | awk -F': ' '/Commit SHA/{print $2}')"
envsubst < deploy/argus-job.tmpl.yaml | kubectl -n "$NAMESPACE" apply -f -

# 3. Watch it.
kubectl -n dgraph logs -f job/argus

# 4. Copy the ledger line + CSVs out (the pod lingers ttlSecondsAfterFinished).
pod="$(kubectl -n dgraph get pod -l app=argus -o name | head -1 | cut -d/ -f2)"
kubectl -n dgraph cp "$pod":/out ./out
cat ./out/runs.jsonl >> path/to/results/gke/runs.jsonl   # append to the durable ledger
```

For sharded configs, set `SHARD_BALANCE=true` so argus spreads predicates across
groups after applying the schema (needs `ZERO_URL`); it's a no-op for <2 groups.

argus only needs the alpha **Service** as a discovery seed (`-graphql`/`-cluster`);
it finds every alpha from `/state` and talks to them directly. No need to enumerate
pods, and the zero Service can stay ClusterIP (argus reads `/state` from an alpha).

The Job writes its ledger line to `/out/runs.jsonl` in an `emptyDir` (this run's
repeats only); copy it out and append to your durable baselines ledger. The Job
pins `nodeSelector: workload=argus` so the load generator lands on the argus/system
pool, never competing with the Dgraph nodes (labeled+tainted `workload=dgraph` by
`gke-provision.sh`) for CPU.

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
- **`--security whitelist`** must include argus's source, or the alpha rejects its
  `/admin`/`/health`/`/state` calls with "unauthorized ip address". The orchestrated
  path handles this — `local-provision.sh` assembles `alpha.extraFlags` with the
  private-range whitelist (and any `--alpha-extra-flags`) automatically. If you
  deploy Dgraph by hand, whitelist the pod CIDR yourself.
- **Subscriptions** ride the L4 LoadBalancer fine; an L7 ingress may break the
  WebSocket upgrade or time out idle connections.
