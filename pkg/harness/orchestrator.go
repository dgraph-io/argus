package harness

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dgraph-io/argus/pkg/cluster"
	"github.com/dgraph-io/argus/pkg/metrics"
	"github.com/dgraph-io/argus/pkg/output"
	"github.com/dgraph-io/argus/pkg/schema"
	"github.com/dgraph-io/argus/pkg/testdata"
	"github.com/dgraph-io/argus/pkg/validate"
	"github.com/dgraph-io/dgo/v250/protos/api"
)

// Orchestrator manages the harness execution phases.
type Orchestrator struct {
	dgraph    *cluster.DgraphClient
	csvWriter *output.CSVWriter
	collector *metrics.Collector
	version   string
	strict    bool
}

// New creates a new harness orchestrator.
func New(dgraph *cluster.DgraphClient, csvWriter *output.CSVWriter, version string, strict bool) *Orchestrator {
	return &Orchestrator{
		dgraph:    dgraph,
		csvWriter: csvWriter,
		collector: metrics.NewCollector(),
		version:   version,
		strict:    strict,
	}
}

// Run executes the full harness benchmark and correctness suite.
func (o *Orchestrator) Run(ctx context.Context, duration time.Duration) error {
	slog.Info("starting harness", "duration", duration, "strict", o.strict)

	deadline := time.Now().Add(duration)

	// Phase 1: Setup (schema)
	if err := o.runSetup(ctx); err != nil {
		if o.strict {
			return fmt.Errorf("setup phase failed: %w", err)
		}
		slog.Error("setup phase had errors", "err", err)
	}

	// Phase 2: Data generation (writes)
	if err := o.runDataGeneration(ctx, deadline); err != nil {
		if o.strict {
			return fmt.Errorf("data generation failed: %w", err)
		}
		slog.Error("data generation had errors", "err", err)
	}

	// Phase 3: Benchmarks (queries and writes)
	if err := o.runBenchmarks(ctx, deadline); err != nil {
		if o.strict {
			return fmt.Errorf("benchmarks failed: %w", err)
		}
		slog.Error("benchmarks had errors", "err", err)
	}

	// Phase 4: Validation
	if err := o.runValidation(ctx); err != nil {
		if o.strict {
			return fmt.Errorf("validation failed: %w", err)
		}
		slog.Error("validation had errors", "err", err)
	}

	// Write summary
	o.writeSummary()

	return nil
}

func (o *Orchestrator) runSetup(ctx context.Context) error {
	slog.Info("phase: setup")
	start := time.Now()

	schemaStr := schema.GetTestSchema()
	slog.Debug("applying schema", "size", len(schemaStr))

	err := o.applySchema(ctx, schemaStr)

	rec := metrics.Record{
		Timestamp:     start,
		OperationType: "schema",
		Duration:      time.Since(start),
		Success:       err == nil,
		ItemCount:     1,
	}
	if err != nil {
		rec.Error = err.Error()
	}
	o.collector.Record(rec)
	o.csvWriter.WriteRecord(rec)

	return err
}

func (o *Orchestrator) runDataGeneration(ctx context.Context, deadline time.Time) error {
	slog.Info("phase: data generation")

	gen := testdata.NewGenerator()
	nodeCount := 0

	for time.Now().Before(deadline) && nodeCount < 10000 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		start := time.Now()
		mutation := gen.GenerateMutation()

		req := &api.Request{Mutations: []*api.Mutation{mutation}}
		txn := o.dgraph.Txn()
		_, err := txn.Do(ctx, req)
		txn.Discard(ctx)

		rec := metrics.Record{
			Timestamp:     start,
			OperationType: "write",
			Duration:      time.Since(start),
			Success:       err == nil,
			ItemCount:     1,
		}
		if err != nil {
			rec.Error = err.Error()
		}
		o.collector.Record(rec)
		o.csvWriter.WriteRecord(rec)

		if err == nil {
			nodeCount++
		}
		slog.Debug("mutation", "count", nodeCount, "err", err)
	}

	slog.Info("data generation complete", "nodes_created", nodeCount)
	return nil
}

func (o *Orchestrator) runBenchmarks(ctx context.Context, deadline time.Time) error {
	slog.Info("phase: benchmarks")

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Simple query benchmark
		query := `{ q(func: has(uid)) { uid } }`
		start := time.Now()

		txn := o.dgraph.ReadOnlyTxn()
		_, err := txn.Query(ctx, query)
		txn.Discard(ctx)

		rec := metrics.Record{
			Timestamp:     start,
			OperationType: "query-dql",
			Duration:      time.Since(start),
			Success:       err == nil,
		}
		if err != nil {
			rec.Error = err.Error()
		}
		o.collector.Record(rec)
		o.csvWriter.WriteRecord(rec)

		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	slog.Info("benchmarks complete")
	return nil
}

func (o *Orchestrator) runValidation(ctx context.Context) error {
	slog.Info("phase: validation")
	start := time.Now()

	checker := validate.NewChecker(o.dgraph)
	result, err := checker.CheckAll(ctx)

	rec := metrics.Record{
		Timestamp:     start,
		OperationType: "validate",
		Duration:      time.Since(start),
		Success:       err == nil && result.AllPassed,
	}
	if err != nil {
		rec.Error = err.Error()
	}
	o.collector.Record(rec)
	o.csvWriter.WriteRecord(rec)

	slog.Info("validation complete", "passed", result.AllPassed, "checks", result.CheckCount)
	return nil
}

func (o *Orchestrator) applySchema(ctx context.Context, schemaStr string) error {
	return o.dgraph.ApplySchema(ctx, schemaStr)
}

func (o *Orchestrator) writeSummary() {
	summary := o.collector.Summary()
	slog.Info("harness summary",
		"ops", summary["total_operations"],
		"success", summary["successful"],
		"fail", summary["failed"],
		"mem_mb", summary["memory_alloc_mb"],
		"gc_runs", summary["gc_runs"],
	)
}
