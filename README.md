# Argus: Dgraph Testing & Benchmarking Harness

Argus is a comprehensive testing and benchmarking tool for Dgraph that exercises nearly every feature for correctness while capturing detailed performance metrics. It supports both DQL and GraphQL queries against any Dgraph cluster (local or Kubernetes) and produces CSV output with statistics.

## Features

- **Correctness Testing**: Validates Dgraph features including types, predicates, indexes, and queries
- **Performance Benchmarking**: Measures write and query performance with detailed metrics
- **Version Tracking**: Captures Dgraph version and cluster information
- **Metrics Collection**: Go runtime stats (GC, heap usage) and system resource monitoring
- **CSV Output**: Structured metrics for easy analysis and visualization

## Building

```bash
go build -o bin/argus ./cmd/argus
```

## Usage

```bash
./bin/argus \
  -cluster localhost:9080 \
  -output metrics.csv \
  -duration 30m \
  -strict=false \
  -v
```

### Flags

- `-cluster`: Dgraph cluster gRPC address (default: `localhost:9080`)
- `-output`: Path to CSV output file (default: `argus-metrics.csv`)
- `-duration`: Target run duration (default: `30m`)
- `-strict`: Fail fast on errors; default logs and continues (default: `false`)
- `-v`: Verbose logging (default: `false`)

## Phases

Argus runs in four phases:

1. **Setup**: Applies test schema to cluster
2. **Data Generation**: Creates test data via mutations
3. **Benchmarks**: Executes DQL queries and measurements
4. **Validation**: Checks correctness of final graph state

## Output Format

CSV file with columns:
- `timestamp`: When the operation ran
- `operation_type`: "write", "query-dql", "query-graphql", "schema", "validate"
- `duration_ms`: Operation duration in milliseconds
- `success`: Boolean success/failure
- `error`: Error message if failed
- `item_count`: Number of items for batch operations
- `memory_alloc_mb`: Allocated memory in MB
- `heap_alloc_mb`: Heap allocated memory in MB
- `gc_pauses_us`: GC pause time in microseconds
- `gc_count`: Total GC runs

## Architecture

```
cmd/argus/          - CLI entry point
pkg/
  cluster/          - Dgraph cluster client and connection
  metrics/          - Performance metrics collection
  output/           - CSV metrics writer
  harness/          - Orchestrator managing phases
  schema/           - Test schema definition
  testdata/         - Test data generators
  queries/          - Query templates (DQL/GraphQL)
  validate/         - Correctness validation
  benchmarks/       - Benchmark suites
  workload/         - Workload scheduling
```

## Local Development

### Prerequisites

- Go 1.26.4+
- Running Dgraph cluster

### Running Against Local Cluster

```bash
# Start a local Dgraph cluster (requires Docker)
docker-compose up -d

# Run the harness
go run ./cmd/argus -cluster localhost:9080 -output test-metrics.csv -duration 5m
```

### Testing

```bash
go test ./...
```

## Next Steps

- [ ] Implement GraphQL query support and client
- [ ] Add comprehensive benchmark suite for writes/queries
- [ ] Implement workload scheduler for balanced execution
- [ ] Add integration tests with local Dgraph cluster
- [ ] Create visualization tools for CSV output
- [ ] Support for Kubernetes cluster endpoints
