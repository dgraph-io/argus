// Package harness orchestrates a run: apply schemas, run the concurrent
// workload while periodically flushing snapshots, then validate the final
// graph against the deterministic dataset.
package harness

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dgraph-io/argus/pkg/cluster"
	"github.com/dgraph-io/argus/pkg/graphql"
	"github.com/dgraph-io/argus/pkg/metrics"
	"github.com/dgraph-io/argus/pkg/output"
	"github.com/dgraph-io/argus/pkg/schema"
	"github.com/dgraph-io/argus/pkg/testdata"
	"github.com/dgraph-io/argus/pkg/validate"
	"github.com/dgraph-io/argus/pkg/workload"
)

// Options configures a harness run.
type Options struct {
	Duration         time.Duration
	FlushInterval    time.Duration
	WriteConcurrency int
	ReadConcurrency  int
	Strict           bool
	Dataset          testdata.Dataset
}

// Orchestrator coordinates the phases of a run.
type Orchestrator struct {
	dgraph    *cluster.DgraphClient
	gql       *graphql.Client
	reporter  *output.Reporter
	collector *metrics.Collector
	opts      Options
}

// New creates an orchestrator.
func New(dgraph *cluster.DgraphClient, gql *graphql.Client, reporter *output.Reporter, opts Options) *Orchestrator {
	return &Orchestrator{
		dgraph:    dgraph,
		gql:       gql,
		reporter:  reporter,
		collector: metrics.NewCollector(),
		opts:      opts,
	}
}

// Run executes setup, the timed concurrent workload, and validation.
func (o *Orchestrator) Run(ctx context.Context) (*validate.Result, error) {
	if err := o.setup(ctx); err != nil {
		return nil, fmt.Errorf("setup: %w", err)
	}

	start := time.Now()
	deadline := start.Add(o.opts.Duration)

	// Periodic snapshot flushing runs for the whole workload.
	flushCtx, stopFlush := context.WithCancel(ctx)
	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		o.flushLoop(flushCtx, start)
	}()

	sched := workload.New(o.dgraph, o.gql, o.collector, workload.Config{
		WriteConcurrency: o.opts.WriteConcurrency,
		ReadConcurrency:  o.opts.ReadConcurrency,
		Dataset:          o.opts.Dataset,
		Strict:           o.opts.Strict,
	})
	slog.Info("workload starting", "duration", o.opts.Duration, "deadline", deadline.Format(time.RFC3339))
	workloadErr := sched.Run(ctx, deadline)

	stopFlush()
	<-flushDone
	o.flush(start) // final snapshot

	slog.Info("validating final graph")
	result := validate.NewChecker(o.dgraph, o.gql, o.opts.Dataset).CheckAll(ctx)
	o.logValidation(result)

	if workloadErr != nil {
		return result, fmt.Errorf("workload: %w", workloadErr)
	}
	return result, nil
}

func (o *Orchestrator) setup(ctx context.Context) error {
	slog.Info("applying DQL schema")
	if err := o.dgraph.ApplySchema(ctx, schema.DQLSchema()); err != nil {
		return fmt.Errorf("apply DQL schema: %w", err)
	}
	slog.Info("applying GraphQL schema")
	if err := o.gql.UpdateSchema(ctx, schema.GraphQLSchema()); err != nil {
		return fmt.Errorf("apply GraphQL schema: %w", err)
	}
	// Give the cluster a moment to propagate the GraphQL schema before writes.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
	}
	return nil
}

func (o *Orchestrator) flushLoop(ctx context.Context, start time.Time) {
	ticker := time.NewTicker(o.opts.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.flush(start)
		}
	}
}

func (o *Orchestrator) flush(start time.Time) {
	now := time.Now()
	elapsed := now.Sub(start)
	if err := o.reporter.WriteOps(now, elapsed, o.collector.Snapshot()); err != nil {
		slog.Warn("ops flush failed", "err", err)
	}
	if err := o.reporter.WriteRuntime(now, elapsed, metrics.SampleRuntime()); err != nil {
		slog.Warn("runtime flush failed", "err", err)
	}
}

func (o *Orchestrator) logValidation(r *validate.Result) {
	passed := 0
	for _, c := range r.Checks {
		if c.Passed {
			passed++
		} else {
			slog.Error("validation check failed", "check", c.Name, "detail", c.Detail)
		}
	}
	slog.Info("validation complete", "passed", passed, "total", len(r.Checks), "all_passed", r.AllPassed)
}
