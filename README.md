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
- **Two CSV outputs.** Per-interval operation snapshots (throughput, latency percentiles, server-side latency, touched uids) and Go runtime samples (heap, GC pauses, GC CPU, goroutines), each with a metadata header recording the Dgraph version.

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
| `-duration` | `30m` | Target run duration; a write pass always completes |
| `-flush-interval` | `5s` | Snapshot cadence |
| `-write-concurrency` | `8` | Concurrent write workers |
| `-read-concurrency` | `8` | Concurrent read workers |
| `-small` | `false` | Use the small dataset for smoke tests |
| `-strict` | `false` | Fail fast on the first error; default logs and continues |
| `-vector-strict` | `false` | Also assert exact `similar_to` nearest-neighbor recall (regression gate; fails on builds with the HNSW upsert issue) |
| `-dgraph-branch` | `""` | Dgraph build branch under test, recorded in the report (from `dgraph version`; the cluster API exposes only the commit hash) |
| `-seed` | `1` | Dataset seed (reserved; the dataset is index-deterministic) |
| `-v` | `false` | Verbose logging |

## How a run proceeds

1. **Setup** — apply the DQL schema (`Alter`) and the GraphQL schema (`updateGQLSchema`).
2. **Mixed workload** — readers loop the query catalog while writers apply the dataset in idempotent upsert passes, repeating until the deadline. Snapshots flush every interval. A write pass always runs to completion, so the run may slightly exceed the target duration: completeness wins over the deadline.
3. **Finalize** — a serial (conflict-free) pass re-applies every persistent record and deletes every ephemeral record, guaranteeing the deterministic end state.
4. **Validation** — complete, not sampled. Every persistent node is read back (paginated) and compared field-by-field to the generator: values, edges, geo coordinates, and timestamps. The node set must equal the expected set exactly, so any missing, extra (e.g. a leaked ephemeral), or mismatched record fails. Geo-index (`near`/`within`) and `shortest`-path queries are additionally checked for functional correctness.

## Determinism

"Same" means logical identity: same nodes (by external key), edges, values, and counts. Raw Dgraph-assigned UIDs may differ on a fresh cluster. Concurrent upserts that share keys (a person writes its friends' keys) conflict and are retried during the timed phase; the serial finalize pass commits the full set without conflicts, which is what makes the end state reproducible.

## Output

`*-ops.csv` columns: `timestamp, elapsed_s, op, count, errors, ops_per_s, avg_ms, min_ms, max_ms, p50_ms, p99_ms, avg_server_ms, touched_total`. `op` is `{domain}-{kind}:{label}`, e.g. `dql-query:recurse-friends-depth3` or `gql-write:product`.

`*-runtime.csv` columns: `timestamp, elapsed_s, goroutines, alloc_mb, heap_alloc_mb, heap_sys_mb, heap_objects, num_gc, gc_pause_ms, gc_cpu_pct, next_gc_mb`. This is the **load generator's** (argus's) own Go runtime — not the cluster.

`*-cluster.csv` columns: `timestamp, elapsed_s, node, leader, rss_mb, cpu_pct, goroutines, heap_inuse_mb, queries_per_s, commits_per_s, aborts_per_s, pending_queries, active_mutations, server_avg_ms`. One row **per alpha** scraped from its `/debug/prometheus_metrics`, plus a `node=CLUSTER` aggregate row. Aggregation respects metric type: gauges (RAM, goroutines) and rates (queries/s, CPU) are summed across nodes; the server latency average merges the histogram sum/count (not a mean-of-means). Counters are turned into per-interval rates via deltas. The per-node breakdown is kept so imbalance (a hot shard, a busy leader) is visible.

Both files begin with a `#` comment header recording the run environment so results are comparable across hardware and cluster configurations:

```
# argus run | started=... | seed=1
# dgraph | version=v25.3.4-7-g1577f25c1 | branch=... | cluster=localhost:9080 | http=... | cid=...
# topology | sharded+HA (3 group(s) x 3 replica(s); 9 alphas, 3 zeros) | groups=3 | replicas=3 | alphas=9 | zeros=3
# host | name=... | os=linux/amd64 | cpu=32 | gomaxprocs=32 | ram_gb=128.00 | go=go1.26.4
```

Topology is read from the alpha `/state`: `groups` is the shard count and `replicas` the per-group HA factor, classified as `single` / `HA` / `sharded` / `sharded+HA`. The Dgraph version (with commit hash) comes from the cluster; the branch is operator-supplied via `-dgraph-branch`. Host CPU/RAM describe the load generator (which is also the cluster host for local runs).

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
  harness/        orchestrator: setup -> workload -> validate
```

The GraphQL client is a thin `net/http` wrapper rather than a community library: struct-based clients (shurcooL/graphql, hasura/go-graphql-client) need a Go type per query, which is a poor fit for a harness firing many dynamic queries, and machinebox/graphql is unmaintained. The native client also gives precise timing and direct access to the `touched_uids` extension.

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
