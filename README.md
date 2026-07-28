# Argus: Dgraph Testing & Benchmarking Harness

Argus exercises Dgraph for correctness while benchmarking write and query performance. It drives DQL and GraphQL against the same cluster simultaneously, captures Go runtime and server-side statistics to CSV, and ends every run in a deterministic graph state so results are comparable across runs and versions. The name comes from Argus Panoptes, the hundred-eyed giant of Greek mythology.

## What it does

- **Concurrent read/write load.** Writers and readers run at the same time over the whole budget, mixing DQL and GraphQL.
- **Multi-alpha aware.** argus discovers every alpha from the cluster `/state` (or takes an explicit `-alphas` list), load-balances DQL (dgo round-robin) and GraphQL (round-robin) across all of them, and scrapes each node's Prometheus metrics. So the workload exercises the whole cluster, not one entry point.
- **Single + bulk write mix.** DQL persons and posts are split 50/50 by record: half go through single-record upserts, half through bulk upsert transactions of ~120 edges each. Both paths are idempotent and carry distinct CSV labels (`person` vs `person-bulk`) so their throughput and latency are directly comparable.
- **Deterministic end state.** The corpus is a fixed, enumerable dataset keyed by stable external ids (DQL `xid`; GraphQL `@id`). All writes are idempotent upserts, so any order or repetition converges to the same logical graph. A serial finalize pass pins the end state.
- **Complete verification.** Because the dataset is enumerable, validation reads back every persistent node and compares it field-by-field to the generator — no spot-checking — and asserts the node set matches exactly.
- **Deletions.** A deterministic ~5% "ephemeral" subset in each domain is created and deleted as churn; the finalize pass removes it, so the final graph is the persistent set only.
- **Ledger fields.** Post `likes` (DQL) and Product `price` (GraphQL) are exercised as ledgers: a deterministic amount is incremented then decremented via read-modify-write throughout the run. The DQL path uses a real optimistic-concurrency transaction (read, write, commit) that retries on conflict. The +d/-d pairs net to zero, so each field settles to a deterministic canonical value pinned by finalize and verified by the complete check.
- **Subscription latency.** A WebSocket subscriber (`graphql-ws`) watches a `@withSubscription` Counter that a writer increments at ~1s cadence; the harness measures commit-to-push propagation latency under concurrent load. Because Dgraph subscriptions are poll-based (re-query every poll-interval, push on change), latency is bounded by that interval; percentiles land in the ops CSV under `gql-sub:counter-latency`.
- **Rich feature coverage.** DQL queries span the function surface: `eq`/`lt`/`le`/`gt`/`ge`/`between`, `has`, `uid_in`, `allofterms`/`anyofterms`/`alloftext`/`anyoftext`/`regexp`/`match` (fuzzy)/`ngram`, `near`/`within`/`intersects`/`contains` (geo), `similar_to` (HNSW vector), `shortest` path, `min`/`max`/`avg`/`sum` aggregation, `math`, `expand(_all_)`, `@normalize`, `@cascade`, `@recurse`, `@groupby`, `orderasc`/`orderdesc`, `AND`/`OR`/`NOT` filters, and variable blocks. GraphQL covers `eq`/`in`/`has`/`regexp`/range filters and `and`/`or`/`not` combinators, enums, ordering, pagination, aggregation (incl. `sum`), nested traversal and nested filters, `@cascade`, `@id` lookups, introspection (`__type`/`__typename`), deep nested mutations (inline creation) and `update` set/remove, interface types with inline fragments, and `@dgraph` interop (a type mapped onto the DQL Person predicates, reading DQL-written data through GraphQL). Functions with computable answers are additionally checked for exact results against the generator. (Not exercised, each needing external infrastructure or auth: `@auth`/`@secret`, `@custom(http)`, `@lambda`/`@lambdaOnMutate`, `@remote`/Apollo Federation, and `checkpwd`.)
- **Vector search caveat.** Persons carry an HNSW-indexed `embedding`; the vector *values* are verified to round-trip exactly, and `similar_to` is exercised under load. Exact nearest-neighbor recall is **not** asserted: under the harness's repeated idempotent upserts, the tested build's HNSW index returns poorly-ordered results that omit exact matches (consistent with the known vector-shadow-index-on-update issue). The harness surfaces this rather than gating on it.
- **Shortest path over a deep subgraph.** The friends graph is a `+1` ring with a `+7` shortcut, giving a connected subgraph with a diameter of roughly N/7. A BFS over that deterministic rule predicts the exact shortest-path length, which is asserted against Dgraph's `shortest` (e.g. ~24 hops across the small dataset).
- **Geo with hits and misses.** Persons are placed in four well-separated geographic clusters by index, so a tight `near`/`within` around an anchor returns exactly that cluster (a hit) and a query over empty ocean returns exactly zero (an expected miss). Both are asserted as exact counts in validation.
- **Three CSV time-series + pprof profiles.** Per-interval operation snapshots (throughput, latency percentiles, server-side latency, touched uids), load-generator Go runtime samples (heap, GC pauses, GC CPU, goroutines), and per-alpha cluster resource/throughput (RSS, CPU, Go heap, **jemalloc off-heap**, queries/commits/aborts) — each with a metadata header recording the Dgraph version. The `results/` run harness additionally captures Go pprof profiles (heap at startup + end, a mid-run CPU profile, optional mutex/block) that `argus compare` diffs to localize a regression to its allocation site, hot function, or contended lock.

## Building

```bash
go build -o bin/argus ./cmd/argus
```

## Usage

```bash
./bin/argus \
  -cluster localhost:9080 \
  -graphql http://localhost:8080 \
  -duration 30m \
  -ops-output argus-ops.csv \
  -runtime-output argus-runtime.csv
```

### Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `-cluster` | `localhost:9080` | Dgraph gRPC address (DQL); seed for discovery |
| `-graphql` | `http://localhost:8080` | Alpha HTTP base URL (GraphQL); seed for discovery |
| `-alphas` | `""` | Comma-separated alpha HTTP base URLs to load-balance over and scrape (default: discover from `/state`) |
| `-cluster-output` | `argus-cluster.csv` | CSV path for per-Dgraph-node resource/throughput snapshots |
| `-passes` | `10` | Fixed-work mode (primary): run exactly N write passes. Set `0` to use `-duration` |
| `-duration` | `0` | Duration mode: run for this long (used only when `-passes=0`; defaults to 30m if both are 0) |
| `-repeats` | `1` | Repeat the whole measurement N times, emitting one summary per repeat (for a noise floor) |
| `-ledger` | `""` | Append each repeat's summary as one JSONL line to this ledger (the consolidated results store read by `argus compare`/`argus ls`) |
| `-run-id` | `""` | Stable id shared across a run's repeats (default: generated); keys the ledger rows and the artifacts dir |
| `-summary-output` | `argus-summary.json` | Per-run summary JSON path; set `""` to skip (prefer `-ledger`) |
| `-flush-interval` | `5s` | Snapshot cadence |
| `-write-concurrency` | `8` | Concurrent write workers |
| `-read-concurrency` | `8` | Concurrent read workers |
| `-small` | `false` | Use the small dataset for smoke tests |
| `-scale` | `0` | Scale the default dataset by this factor (e.g. `0.5`); size a run to a target wall-clock. Overrides `-small` |
| `-no-drop` | `false` | Skip the clean-slate drop-all at the start of each run (default drops, so the graph is exactly the dataset) |
| `-bulk-load` | `true` | Run the blind bulk-load phase (live-loader-shaped large insert batches) before the timed workload |
| `-bulk-batch` / `-bulk-records` / `-bulk-edges` | `0` / `0` / `-1` | Bulk-load tuning: records/mutation, total record count, and brhub fan-in edges per record (defaults derive from the dataset/`-scale`) |
| `-shard-balance` | `false` | On sharded clusters, spread predicates across groups after schema (before load) so writes distribute; needs `-zero`. No-op for <2 groups |
| `-zero` | `""` | Zero admin HTTP base URL (e.g. `http://dgraph-dgraph-zero:6080`) for `-shard-balance` moveTablet calls |
| `-strict` | `false` | Fail fast on the first error; default logs and continues |
| `-vector-strict` | `false` | Also assert exact `similar_to` nearest-neighbor recall (regression gate; fails on builds with the HNSW upsert issue) |
| `-dgraph-branch` | `""` | Dgraph build branch under test, recorded in the report (from `dgraph version`; the cluster API exposes only the commit hash) |
| `-dgraph-commit` | `""` | Dgraph commit SHA-1 (from `dgraph version`); the one field that uniquely pins any build, released or feature |
| `-cpu-class` | `""` | CPU hardware class column (e.g. `Apple M4`, `e2-standard-4`); not auto-detectable |
| `-disk-class` | `""` | Disk/storage class column (e.g. `NVMe`, `pd-ssd`, `local-ssd`); not auto-detectable |
| `-deploy` | `""` | Deployment mechanism column (e.g. `docker-compose`, `gke`) |
| `-label` | `""` | Free-form run annotation for anything not covered by `-cpu-class`/`-disk-class`/`-deploy` |
| `-seed` | `1` | Dataset seed (reserved; the dataset is index-deterministic) |
| `-v` | `false` | Verbose logging |

## How a run proceeds

1. **Setup** — drop all data (clean slate, unless `-no-drop`) so the graph ends up as exactly the deterministic dataset, then apply the GraphQL schema (`updateGQLSchema`) and the DQL schema (`Alter`).
2. **Mixed workload** — readers loop the query catalog while writers apply the dataset in idempotent upsert passes, repeating until the deadline. Snapshots flush every interval. A write pass always runs to completion, so the run may slightly exceed the target duration: completeness wins over the deadline.
3. **Finalize** — a serial (conflict-free) pass re-applies every persistent record and deletes every ephemeral record, guaranteeing the deterministic end state.
4. **Validation** — complete, not sampled. Every persistent node is read back (paginated) and compared field-by-field to the generator: values, edges, geo coordinates, and timestamps. The node set must equal the expected set exactly, so any missing, extra (e.g. a leaked ephemeral), or mismatched record fails. Geo-index (`near`/`within`) and `shortest`-path queries are additionally checked for functional correctness.

## Determinism

"Same" means logical identity: same nodes (by external key), edges, values, and counts. Raw Dgraph-assigned UIDs may differ on a fresh cluster. Concurrent upserts that share keys (a person writes its friends' keys) conflict and are retried during the timed phase; the serial finalize pass commits the full set without conflicts, which is what makes the end state reproducible.

## Output

`*-ops.csv` columns: `timestamp, elapsed_s, op, count, errors, ops_per_s, avg_ms, min_ms, max_ms, p50_ms, p99_ms, avg_server_ms, touched_total`. `op` is `{domain}-{kind}:{label}`, e.g. `dql-query:recurse-friends-depth3` or `gql-write:product`.

`*-runtime.csv` columns: `timestamp, elapsed_s, goroutines, alloc_mb, heap_alloc_mb, heap_sys_mb, heap_objects, num_gc, gc_pause_ms, gc_cpu_pct, next_gc_mb`. This is the **load generator's** (argus's) own Go runtime — not the cluster.

`*-cluster.csv` columns: `timestamp, elapsed_s, node, leader, rss_mb, cpu_pct, goroutines, heap_inuse_mb, jemalloc_mb, queries_per_s, commits_per_s, aborts_per_s, pending_queries, active_mutations, server_avg_ms`. One row **per alpha** scraped from its `/debug/prometheus_metrics`, plus a `node=CLUSTER` aggregate row. `jemalloc_mb` is the off-heap allocation (`dgraph_memory_alloc_bytes`); together with `heap_inuse_mb` it decomposes RSS into its Go-heap and off-heap halves, which a Go heap profile alone can't see. Aggregation respects metric type: gauges (RAM, heap, jemalloc, goroutines) and rates (queries/s, CPU) are summed across nodes; the server latency average merges the histogram sum/count (not a mean-of-means). Counters are turned into per-interval rates via deltas. The per-node breakdown is kept so imbalance (a hot shard, a busy leader) is visible.

Both files begin with a `#` comment header recording the run environment so results are comparable across hardware and cluster configurations:

```
# argus run | started=... | seed=1
# dgraph | version=v25.3.4-7-g1577f25c1 | branch=... | cluster=localhost:9080 | http=... | cid=...
# topology | sharded+HA (3 group(s) x 3 replica(s); 9 alphas, 3 zeros) | groups=3 | replicas=3 | alphas=9 | learners=0 | zeros=3
# host | name=... | os=linux/amd64 | cpu=32 | gomaxprocs=32 | ram_gb=128.00 | go=go1.26.4
```

Topology is read from the alpha `/state`: `groups` is the shard count, `replicas` the per-group HA factor (**voting** members only), and `learners` the count of non-voting read-replica alphas. The `single` / `HA` / `sharded` / `sharded+HA` class is *derived* from `groups` and `replicas` (learners don't change it), not stored — so it can't drift from the counts. The Dgraph version comes from the cluster; the branch and commit are read from `dgraph version` (the API exposes only the version tag). Host CPU/RAM describe the load generator (which is also the cluster host for local runs).

## Architecture

```
cmd/argus/        CLI entry point
pkg/
  cluster/        dgo client: connect, health, schema, transactions
  graphql/        thin net/http GraphQL client (round-robin over alphas)
  subscription/   graphql-ws WebSocket client + latency benchmark
  envinfo/        host CPU/RAM + cluster topology/version + endpoint discovery
  clusterstats/   per-alpha Prometheus scrape + aggregation
  schema/         DQL and GraphQL test schemas
  testdata/       deterministic dataset + upsert/delete builders
  queries/        DQL and GraphQL query catalogs
  workload/       concurrent scheduler + finalize
  metrics/        windowed aggregation + runtime sampling
  output/         two-CSV reporter
  validate/       deterministic correctness checks
  report/         per-run summary JSON + regression compare
  shardbalance/   deterministic predicate placement across shard groups (Zero moveTablet)
  harness/        orchestrator: setup -> workload -> validate -> summary
```

The GraphQL client is a thin `net/http` wrapper rather than a community library: struct-based clients (shurcooL/graphql, hasura/go-graphql-client) need a Go type per query, which is a poor fit for a harness firing many dynamic queries, and machinebox/graphql is unmaintained. The native client also gives precise timing and direct access to the `touched_uids` extension.

## Regression testing (primary use)

The main job of argus is to smoke-test a candidate Dgraph build against a
released one and catch performance regressions. Each run **appends one JSONL line
per repeat** (a full `RunSummary`: per-op latency percentiles, throughput, and
steady-state per-cluster resource) to a consolidated ledger via `-ledger`;
`argus compare` selects two runs from it and exits non-zero on a regression, so it
drops into CI. `argus ls` lists/filters the ledger (see
[results/README.md](results/README.md)).

```bash
# Baseline: released version. Repeats establish the noise floor — 2 can't estimate
# the spread. On a laptop use >=5 (drift is large); on dedicated GKE nodes 3 is
# enough (measured run-to-run noise there is ~5% on central metrics).
argus -small -passes 10 -repeats 5 -dgraph-branch v25.3.7 \
  -ledger results/runs.jsonl -run-id baseline-v25.3.7

# Candidate: the build under test, same hardware + same workload.
argus -small -passes 10 -repeats 5 -dgraph-branch v25.3.8-preview1 \
  -ledger results/runs.jsonl -run-id cand-v25.3.8

# Compare; non-zero exit = regression. Selectors are a run_id or a k=v filter.
argus compare \
  -baseline-run baseline-v25.3.7 -candidate-run cand-v25.3.8 \
  -latency 0.10 -throughput 0.10 -resource 0.15 -noise-margin 1.5
```

(For ad-hoc comparisons outside the ledger, `-baseline`/`-candidate` still take
comma-separated summary JSON file lists.)

A metric is flagged only when the candidate is worse by more than the threshold
**and** by more than the measured run-to-run noise times `-noise-margin`
(default 1.5) — so a delta that merely edges past the noise band (typical of
jittery p99 tails) doesn't trip the gate. A per-op **p99** regression that the
same op's **avg** does not corroborate is demoted to `tail-only` and shown but
not gated: a genuine slowdown moves the mean, not just one tail bucket. `compare`
also warns when the two sides differ in anything but the Dgraph version (host,
topology, workload, concurrency, dataset), which would invalidate the comparison.

**Profile diffs.** When run through the `results/` harness (below), each run also
drops pprof profiles into its `results/artifacts/<run-id>/` dir, and `compare`
auto-discovers and diffs them (by run-id) to localize a regression — no extra
flags needed:

- **heap** (`heap-startup.pb.gz`, `heap-end.pb.gz`) — per-allocation-site
  `inuse_space` delta; the total also gates against `-resource`, explaining the
  Go-heap half of an RSS regression the CSV would otherwise only flag (the
  off-heap half is the `jemalloc_mb` cluster metric).
- **CPU** (`cpu.pb.gz`) — a mid-run profile diffed by each function's *share* of
  samples (self-normalizing across runs); localizes on-CPU cost. Diagnostic only.
- **mutex / block** (`mutex.pb.gz`, `block.pb.gz`, opt-in) — contention diffed by
  share of blocked time, attributed to the *lock holder* (not the `sync`
  primitive), to localize an off-CPU regression. Diagnostic only.

Explicit paths (`-baseline-heap`/`-candidate-heap`, `-baseline-cpu`/…) override
discovery; `-heap-top`/`-cpu-top`/`-contention-top` cap rows shown.

`-passes` (fixed work) is primary so both versions run the *identical* operation
set, making latency apples-to-apples; `-duration` (fixed time) is available for
a throughput-over-a-window comparison.

For a verdict you intend to trust, run each side on a **fresh GKE deployment**
(real cores + SSD, no drift, clean per-side state); the local docker-compose tier
is for iteration and smoke tests. See [Kubernetes / GKE](#kubernetes--gke).

Runs are recorded as lines in a committed ledger — one line per repeat — so the
whole run matrix lives in one file instead of a folder per scenario. There are two:
[results/runs.jsonl](results/runs.jsonl) for the laptop/docker-compose tier (each
`results/<version>/<config>/` folder keeps only the reproducibility inputs — a
`docker-compose.yml` + a thin `run.sh` that sources `results/run-common.sh`), and
[results/gke/runs.jsonl](results/gke) for the trusted **GKE baseline library**
(written by `deploy/gke/benchmark.sh`). Both share the same schema, so `argus
compare`/`ls` and the visual [results/compare.html](results/compare.html) read
either via `-ledger`. Heavy per-run artifacts (profiles + interval CSVs) are
ephemeral under a gitignored `artifacts/<run-id>/` beside each ledger. See
[results/README.md](results/README.md) for the convention and `argus ls`.

### Visual diff — `compare.html`

[results/compare.html](results/compare.html) is a dependency-free, single-file
page — the eyeball companion to `argus compare`. It loads a `runs.jsonl`, lets you
pick any two `run_id`s (with a **swap** button), and shows the gain/loss on **every
recorded metric** — per-op throughput/latency, cluster resource, bulk-load — as a
color-coded Δ% heatmap: green = better, red = worse, per each metric's direction
(latency/RSS lower-is-better, throughput higher-is-better).

It mirrors the CLI's discipline so it doesn't become a wall of red:

- **Noise-aware by default.** Compared values are the **median across a run's
  repeats** (so one outlier repeat — a lone GC/compaction stall — can't dominate
  the `p999`/`max` tails), and a delta is only colored when it clears
  `noise × 1.5` (rel-stddev across repeats); within-noise deltas dim to neutral —
  the same gate as `argus compare`, so drift doesn't paint everything red. A
  checkbox toggles the raw (ungated) view.
- **Per-alpha resource normalization.** Cluster resource metrics (`rss`, `jemalloc`,
  `cpu`, `goroutines`) are shown **per alpha** (sum ÷ alpha count), so a
  cross-topology diff (e.g. single vs ha3) reflects "does each node work harder?"
  instead of just node count. It's invariant for same-node-count runs, so the
  normal version-vs-version compare is unaffected.
- **`write_balance_cv`** (per-alpha mutation-load spread) renders **uncolored** —
  it's only meaningful for sharded configs, so it's recorded, not graded.
- **Same warnings** `argus compare` raises: same build on both sides, differing
  topology/host/workload, too few repeats.

Open it by serving the folder (auto-loads a sibling `runs.jsonl`):

```bash
cd results/gke && python3 -m http.server   # http://localhost:8000/../compare.html
```

Served over http it always auto-loads the sibling ledger; on a `file://`
double-click Firefox auto-loads it too, while Chrome/Safari sandbox `file://`
reads — there, use the file picker on the page. It's a quick companion; the CLI
stays the source of truth for the gated verdict.

## Kubernetes / GKE — the benchmark library

**GKE is the tier you trust.** The local docker-compose tier is single-alpha and
Docker-on-Mac I/O-bound (it masks anything CPU- or parallelism-related) and a
reused laptop drifts ±30–60% run-to-run with background load — that drift, not the
code, dominated our early v25.3.7↔v25.3.8 deltas. Dedicated GKE nodes with nothing
else scheduled measure ~5% run-to-run noise on central metrics, so they can
resolve the ~5–10% differences that matter. The local tier is for iteration and
smoke tests — deltas, not shippable verdicts.

The orchestration lives in **[deploy/gke/](deploy/gke/)** and the strategy is a
**committed library of baselines**: record baselines across Dgraph versions ×
topologies once into [results/gke/runs.jsonl](results/gke), then a developer only
runs their **candidate** and diffs it against the matching stored baseline.
`deploy/gke/benchmark.sh` has three modes:

```bash
cd deploy/gke
# baseline — provision a dedicated-pool cluster, deploy a released Dgraph, run argus
./benchmark.sh baseline --version v25.3.8 --config ha3 -z us-central1-a
# candidate — Cloud Build a Dgraph image (optionally folding local badger/ristretto
#   via go.mod replace), run it under a baseline's config, then `argus compare`
./benchmark.sh candidate --dgraph ~/code/dgraph --config ha3 --baseline-run <id>
# matrix — sweep baselines over versions × topologies
./benchmark.sh matrix --matrix matrix.txt
```

It provisions a **dedicated Dgraph node pool** (tainted `workload=dgraph`) plus a
separate **argus pool** so the load generator never steals CPU from the system
under test, pins pd-ssd + Guaranteed-QoS resources for reproducibility, and runs
argus **in-cluster** as a Job (required — argus must reach each alpha individually
to spread load and scrape per-node metrics; an external LoadBalancer VIP randomizes
which alpha each request hits). The canonical workload shape is **scale 0.25 /
passes 5 / repeats 3 / write-concurrency 8**; every candidate must match its
baseline's shape. Topologies (`single`/`ha3`/`ha5`/`shard3`/`shard3-ha3`) derive
their node count automatically, and sharded configs get **deterministic predicate
placement** (`-shard-balance`) so writes distribute instead of concentrating on
whichever group Zero picked.

Testing a **preview/custom image** or passing alpha flags is first-class:
`--image-repo dgraph/dgraph-custom --version v25.4.0-preview1`,
`--alpha-extra-flags '--feature-flags=name=val'`. The chart is pulled fresh and
pinned per deploy (a stale Helm cache can otherwise silently change the deployed
command) and patched for a chart bug that mishandles prerelease image tags.

See **[deploy/gke/README.md](deploy/gke/README.md)** for setup, the config matrix,
node pools, stockout/quota handling, and troubleshooting.

### What the GKE runs taught us (v25.3.8)

- **Topology.** `single` is the slow floor; **`ha3` and `shard3` are ~20% faster
  and roughly tied with each other**, and that win is almost entirely **read
  distribution** across 3 alphas — not writes. HA adds serving capacity at a Raft
  tail cost; sharding partitions at a cross-group coordination cost; they cancel to
  a wash on this workload.
- **Sharding can't help this workload's writes.** Dgraph shards by *predicate*, and
  argus's DQL entities **share** bare predicates (`xid`, `name`, …), so every DQL
  write funnels to one group regardless of placement — the heaviest write op
  (`dql-write:person`) is bound by this. Only GraphQL's type-scoped predicates
  distribute. Worse, *deliberately* balancing the shards made it **slower** (cross-
  group coordination overhead on the workload's cross-entity traversals) while
  using less CPU. Sharding pays off only when the *operations* also partition along
  the shard boundary, which argus's don't. `results/gke/` records the balanced
  sharded baseline as a reproducible control for future builds where sharding
  improves; the `write_balance_cv` cluster metric (per-alpha mutation-load spread)
  quantifies whether writes actually distributed.

For the laptop docker-compose tier, see
[results/README.md](results/README.md) and [deploy/README.md](deploy/README.md)
(image build + the in-cluster Job manifest).

## Local development

Start a single-node cluster, then run the smoke test:

```bash
dgraph zero --my=localhost:5080 &
dgraph alpha --my=localhost:7080 --zero=localhost:5080 --security whitelist=0.0.0.0/0 &

go run ./cmd/argus -small -duration 15s -flush-interval 3s
```

```bash
go test ./...
go vet ./...
```
