# GKE benchmarking for argus

Orchestration to run the argus workload against Dgraph on a dedicated GKE cluster
and record comparable results into the committed baseline library at
[`../../results/gke/runs.jsonl`](../../results/gke). The strategy is a **library**:
record baselines across Dgraph versions × topologies once, then benchmark a
**candidate** build (Dgraph, or local `badger`/`ristretto` folded in) against the
matching stored baseline — so only the candidate is run on demand.

`argus`'s `compare`/`ls` and [`../../results/compare.html`](../../results) read the
ledger via `-ledger`; the load-generator Job template is
[`../argus-job.tmpl.yaml`](../argus-job.tmpl.yaml).

## Scripts

| Script | Purpose |
|--------|---------|
| `benchmark.sh` | Orchestrator: `baseline` / `candidate` / `matrix` modes |
| `gke-provision.sh` | Create/teardown GKE (dedicated pools + pd-ssd), then deploy Dgraph |
| `local-provision.sh` | Helm-deploy Dgraph (invoked by the above; also usable for local smoke tests) |
| `gcp-project-setup.sh` | Create a GCP project with billing + APIs |
| `build-candidate.sh` | Cloud Build a candidate Dgraph image (folds local badger/ristretto) |
| `cloudbuild.yaml` | Cloud Build config used by `build-candidate.sh` |
| `values-base.yaml` | Chart values: pd-ssd, dedicated-pool scheduling, argus IP whitelist |

## Prerequisites

`gcloud`, `kubectl`, `helm`, `docker`, `envsubst` (gettext). Authenticate and
install the GKE auth plugin:

```bash
gcloud auth login
gcloud components install gke-gcloud-auth-plugin
```

## One-time setup

```bash
export PROJECT=my-dgraph-bench REGION=us-central1
./gcp-project-setup.sh create -p $PROJECT -b <BILLING_ACCOUNT_ID>

# Artifact Registry repo for the argus image + docker auth (not done by setup):
gcloud services enable artifactregistry.googleapis.com --project $PROJECT
gcloud artifacts repositories create argus \
  --repository-format=docker --location=$REGION --project $PROJECT
gcloud auth configure-docker $REGION-docker.pkg.dev

# Build + push the argus load-generator image ONCE (reused by every run):
export ARGUS_IMAGE=$REGION-docker.pkg.dev/$PROJECT/argus/argus:v1
../build-image.sh $ARGUS_IMAGE --push
```

## Record a baseline

Workload shape defaults to **scale 0.25 / passes 5 / repeats 3 / write-concurrency
8** — the canonical library shape (~10–12 min/repeat on a single alpha). Every
candidate and same-topology baseline must match it.

```bash
./benchmark.sh baseline \
  --project $PROJECT --argus-image $ARGUS_IMAGE \
  --version v25.3.8 --config single \
  --zones "us-central1-c us-east1-b us-west1-b us-east4-c" \
  -m n2-standard-8 -N 1 --alpha-cpu 6 --alpha-mem 24Gi

# Sweep a matrix (one line per "version config [machine]"):
printf 'v25.3.7 ha3\nv25.3.7 shard3\nv25.3.8 ha3\n' > matrix.txt
./benchmark.sh matrix --project $PROJECT --argus-image $ARGUS_IMAGE --matrix matrix.txt
```

The ledger + artifacts default to `results/gke/` (relative to the repo); the argus
checkout is auto-detected (this script lives in it), so `--argus-repo` is only
needed when running from elsewhere.

## Test a candidate

Build a candidate image (optionally folding local `badger`/`ristretto`), run it
under the **same config** as a stored baseline, and compare:

```bash
./benchmark.sh candidate \
  --project $PROJECT --argus-image $ARGUS_IMAGE \
  --dgraph ~/code/dgraph --badger ~/code/badger --ristretto ~/code/ristretto \
  --config single --baseline-run <baseline run_id from `argus ls`>
```

## Topology descriptors (`--config`)

One base values file ([`values-base.yaml`](values-base.yaml)) pins the
reproducibility knobs (pd-ssd, `workload=dgraph` scheduling, the argus IP
whitelist); the topology comes from the descriptor as `--set` overrides:

| Descriptor | Alphas × replicas/group | Topology |
|------------|-------------------------|----------|
| `single` | 1 × 1 | 1 group, no HA |
| `ha3` / `ha5` | 3 / 5 × 3 / 5 | 1 group, HA |
| `shard3` | 3 × 1 | 3 groups, sharded |
| `shard3-ha3` | 9 × 3 | 3 groups, sharded + HA |

**Sharded configs get deterministic predicate placement.** `benchmark.sh`
auto-enables argus `-shard-balance` for `shard3`/`shard3-ha3`: after the schema is
applied (tablets empty → instant moves), argus spreads predicates across groups so
writes distribute instead of concentrating on whichever group Zero picked — each
entity type's predicates co-located (single-group writes), largest families on
distinct groups. These rows are tagged `balanced=1` in the ledger. Note the
inherent limit (see the topology write-up): GraphQL type-scoped predicates
distribute, but argus's DQL entities share bare predicates (`xid`/`name`), so DQL
writes stay group-bound regardless.

## Reproducibility

`gke-provision.sh` creates a dedicated **Dgraph pool** (`-m`, tainted
`workload=dgraph`) holding only the system under test, and a separate **argus
pool** (`--argus-machine-type`, `workload=argus`) for the load generator, so the
generator never steals CPU from Dgraph. Data PVs use a pd-ssd StorageClass. Size
`--alpha-cpu`/`--alpha-mem` to the machine (leaving OS/kubelet headroom) so one
alpha lands per node with Guaranteed QoS — no bursting, so baseline and candidate
measure the same hardware.

## Trusting a reused baseline

3 repeats is the default and is enough on GKE — measured run-to-run noise on
dedicated nodes is ~5% on central metrics (throughput/avg/p50), so 3 resolves
~6–8% real regressions. The p99 tail is noisier (~14%); treat it as directional
and lean on avg/throughput. For a high-stakes call, re-run the baseline **fresh**
alongside the candidate (`--fresh-baseline --version vX.Y.Z`) for a paired
same-session diff. Use ≥5 repeats only on a laptop, where drift is far larger.

## Troubleshooting (things that actually bite)

- **`GCE_STOCKOUT` / "does not have enough resources"** — the benchmark machine
  isn't available in that zone. Pass `--zones "z1 z2 ..."`; the script probes live
  capacity (there's no availability API) and picks the first zone that has it. If
  probes report **QUOTA** instead, it's your project's regional `N2_CPUS` limit —
  request an increase or use fewer/smaller nodes. Genuine multi-zone stockout →
  try `-m n2d-standard-8` or `-m c3-standard-8` (other silicon).
- **argus pod dies immediately (`exec format error`)** — the argus image was built
  for the wrong arch. `build-image.sh` forces `--platform linux/amd64`; rebuild +
  `--push`.
- **`unauthorized ip address`** — Dgraph's security whitelist rejecting the argus
  pod. `values-base.yaml` whitelists the private ranges (`10/8`,`172.16/12`,
  `192.168/16`); tighten via `alpha.extraFlags` if needed.
- **Run takes hours** — cost is ~superlinear in `--scale` (`~scale^1.4`). Stick to
  the 0.25 default; `--scale 0.5` is ~1.5–2h on a single alpha.
- **`kubectl cp` fails on a completed pod** — handled: the Job has a `sink` sidecar
  that keeps the pod alive so results copy out. Don't remove it.

## Teardown

```bash
./gke-provision.sh teardown -p $PROJECT -z <zone> -C dgraph-bench
```
Benchmark clusters favor fidelity over cost (dedicated n2 + pd-ssd) — tear down
after recording. Record baselines once and reuse; only run candidates on demand.

**Teardown reclaims the pd-ssd PVs by default** (it lists them and prompts). This
matters because deleting a GKE cluster does *not* reclaim dynamically-provisioned
disks — they orphan and keep billing. The cleanup also runs when the cluster is
already gone, so a follow-up teardown mops up disks left by an earlier one. Pass
`--keep-disks` to preserve them. To audit for strays:

```bash
gcloud compute disks list --project $PROJECT \
  --filter="name~'^pvc-'" --format="table(name,zone.basename(),sizeGb,users.basename())"
```
