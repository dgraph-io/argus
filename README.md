# Argus: Dgraph Testing & Benchmarking Harness

Argus exercises Dgraph for correctness while benchmarking write and query performance. It drives DQL and GraphQL against the same cluster simultaneously, captures Go runtime and server-side statistics to CSV, and ends every run in a deterministic graph state so results are comparable across runs and versions. The name comes from Argus Panoptes, the hundred-eyed giant of Greek myth.

## What it does

- **Concurrent read/write load.** Writers and readers run at the same time over the whole budget, mixing DQL and GraphQL.
- **Single + bulk write mix.** DQL persons and posts are split 50/50 by record: half go through single-record upserts, half through bulk upsert transactions of ~120 edges each. Both paths are idempotent and carry distinct CSV labels (`person` vs `person-bulk`) so their throughput and latency are directly comparable.
- **Deterministic end state.** The corpus is a fixed, enumerable dataset keyed by stable external ids (DQL `xid`; GraphQL `@id`). All writes are idempotent upserts, so any order or repetition converges to the same logical graph. A serial finalize pass pins the end state.
- **Deletions.** A deterministic ~5% "ephemeral" subset in each domain is created and deleted as churn; the finalize pass removes it, so the final graph is the persistent set only.
- **Rich feature coverage.** DQL queries span filters, aggregation, ordering, pagination, recursion, reverse edges, full-text and regexp search, math, geo (`near`/`within`), `shortest` path, and variable blocks. GraphQL covers filters, enums, ordering, aggregation, nested traversal, and `@id` lookups.
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
| `-cluster` | `localhost:9080` | Dgraph gRPC address (DQL) |
| `-graphql` | `http://localhost:8080` | Alpha HTTP base URL (GraphQL) |
| `-duration` | `30m` | Target run duration; a write pass always completes |
| `-flush-interval` | `5s` | Snapshot cadence |
| `-write-concurrency` | `8` | Concurrent write workers |
| `-read-concurrency` | `8` | Concurrent read workers |
| `-small` | `false` | Use the small dataset for smoke tests |
| `-strict` | `false` | Fail fast on the first error; default logs and continues |
| `-seed` | `1` | Dataset seed (reserved; the dataset is index-deterministic) |
| `-v` | `false` | Verbose logging |

## How a run proceeds

1. **Setup** — apply the DQL schema (`Alter`) and the GraphQL schema (`updateGQLSchema`).
2. **Mixed workload** — readers loop the query catalog while writers apply the dataset in idempotent upsert passes, repeating until the deadline. Snapshots flush every interval. A write pass always runs to completion, so the run may slightly exceed the target duration: completeness wins over the deadline.
3. **Finalize** — a serial (conflict-free) pass re-applies every persistent record and deletes every ephemeral record, guaranteeing the deterministic end state.
4. **Validation** — exact type counts, keyed spot-checks of values and edges, and ephemeral-absent assertions across both domains.

## Determinism

"Same" means logical identity: same nodes (by external key), edges, values, and counts. Raw Dgraph-assigned UIDs may differ on a fresh cluster. Concurrent upserts that share keys (a person writes its friends' keys) conflict and are retried during the timed phase; the serial finalize pass commits the full set without conflicts, which is what makes the end state reproducible.

## Output

`*-ops.csv` columns: `timestamp, elapsed_s, op, count, errors, ops_per_s, avg_ms, min_ms, max_ms, p50_ms, p99_ms, avg_server_ms, touched_total`. `op` is `{domain}-{kind}:{label}`, e.g. `dql-query:recurse-friends-depth3` or `gql-write:product`.

`*-runtime.csv` columns: `timestamp, elapsed_s, goroutines, alloc_mb, heap_alloc_mb, heap_sys_mb, heap_objects, num_gc, gc_pause_ms, gc_cpu_pct, next_gc_mb`.

## Architecture

```
cmd/argus/        CLI entry point
pkg/
  cluster/        dgo client: connect, health, schema, transactions
  graphql/        thin net/http GraphQL client (query + admin schema)
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
