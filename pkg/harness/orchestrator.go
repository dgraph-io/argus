// Package harness orchestrates a run: apply schemas, run the concurrent
// workload while periodically flushing snapshots, then validate the final
// graph against the deterministic dataset.
package harness

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/dgraph-io/argus/pkg/cluster"
	"github.com/dgraph-io/argus/pkg/clusterstats"
	"github.com/dgraph-io/argus/pkg/envinfo"
	"github.com/dgraph-io/argus/pkg/graphql"
	"github.com/dgraph-io/argus/pkg/metrics"
	"github.com/dgraph-io/argus/pkg/output"
	"github.com/dgraph-io/argus/pkg/report"
	"github.com/dgraph-io/argus/pkg/schema"
	"github.com/dgraph-io/argus/pkg/subscription"
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
	VectorStrict     bool
	Passes           int // >0: fixed-work mode
	Seed             int64
	Env              envinfo.Info
	Dataset          testdata.Dataset
}

// Orchestrator coordinates the phases of a run.
type Orchestrator struct {
	dgraph     *cluster.DgraphClient
	gql        *graphql.Client
	reporter   *output.Reporter
	scraper    *clusterstats.Scraper
	collector  *metrics.Collector
	opts       Options
	clusterAgg []clusterstats.NodeSample // CLUSTER aggregate per flush, for steady-state summary
}

// New creates an orchestrator. scraper may be nil to skip per-node cluster
// metrics.
func New(dgraph *cluster.DgraphClient, gql *graphql.Client, reporter *output.Reporter, scraper *clusterstats.Scraper, opts Options) *Orchestrator {
	return &Orchestrator{
		dgraph:    dgraph,
		gql:       gql,
		reporter:  reporter,
		scraper:   scraper,
		collector: metrics.NewCollector(),
		opts:      opts,
	}
}

// Run executes setup, the concurrent workload, and validation, returning the
// validation result and a machine-readable run summary for regression compare.
func (o *Orchestrator) Run(ctx context.Context) (*validate.Result, *report.RunSummary, error) {
	if err := o.setup(ctx); err != nil {
		return nil, nil, fmt.Errorf("setup: %w", err)
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

	// Subscription latency benchmark runs concurrently with the workload.
	subCtx, stopSub := context.WithCancel(ctx)
	subDone := make(chan struct{})
	go func() {
		defer close(subDone)
		// Cadence ~ the server poll-interval (default 1s) yields the cleanest
		// commit-to-push latency distribution.
		subscription.New(o.gql, o.collector, time.Second).Run(subCtx)
	}()

	sched := workload.New(o.dgraph, o.gql, o.collector, workload.Config{
		WriteConcurrency: o.opts.WriteConcurrency,
		ReadConcurrency:  o.opts.ReadConcurrency,
		Dataset:          o.opts.Dataset,
		Strict:           o.opts.Strict,
		Passes:           o.opts.Passes,
	})
	slog.Info("workload starting", "mode", o.mode(), "duration", o.opts.Duration, "passes", o.opts.Passes)
	workloadErr := sched.Run(ctx, deadline)
	measured := time.Since(start)

	stopSub()
	<-subDone
	stopFlush()
	<-flushDone
	// Final snapshot with a fresh context so it is captured even if the run
	// context was cancelled (e.g. SIGINT).
	finalCtx, cancelFinal := context.WithTimeout(context.Background(), 15*time.Second)
	o.flush(finalCtx, start)
	cancelFinal()

	slog.Info("validating final graph")
	result := validate.NewChecker(o.dgraph, o.gql, o.opts.Dataset, o.opts.VectorStrict).CheckAll(ctx)
	o.logValidation(result)

	summary := o.buildSummary(measured)

	if workloadErr != nil {
		return result, summary, fmt.Errorf("workload: %w", workloadErr)
	}
	return result, summary, nil
}

func (o *Orchestrator) mode() string {
	if o.opts.Passes > 0 {
		return "passes"
	}
	return "duration"
}

// buildSummary assembles the machine-readable run summary: full-run per-op
// stats (throughput from the measured wall-clock) and steady-state cluster
// resource (median of the CLUSTER aggregate samples).
func (o *Orchestrator) buildSummary(measured time.Duration) *report.RunSummary {
	secs := measured.Seconds()
	ops := o.collector.FinalSummary()
	if secs > 0 {
		for i := range ops {
			ops[i].ThroughputPerSec = float64(ops[i].Count) / secs
		}
	}
	return &report.RunSummary{
		Env:             o.opts.Env,
		Mode:            o.mode(),
		Passes:          o.opts.Passes,
		Seed:            o.opts.Seed,
		StartedAt:       time.Now().Add(-measured).Format(time.RFC3339),
		MeasuredSeconds: secs,
		Ops:             ops,
		Cluster:         o.clusterSummary(),
	}
}

// clusterSummary medians each field of the accumulated CLUSTER aggregate samples
// (steady state). The first sample is skipped as warm-up/baseline.
func (o *Orchestrator) clusterSummary() report.ClusterSummary {
	s := o.clusterAgg
	if len(s) > 2 {
		s = s[1:] // drop the first (cold / no-rate) sample
	}
	if len(s) == 0 {
		return report.ClusterSummary{Nodes: o.scraperNodes()}
	}
	med := func(get func(clusterstats.NodeSample) float64) float64 {
		xs := make([]float64, len(s))
		for i, n := range s {
			xs[i] = get(n)
		}
		return median(xs)
	}
	return report.ClusterSummary{
		Nodes:           o.scraperNodes(),
		RSSMBTotal:      med(func(n clusterstats.NodeSample) float64 { return n.RSSMB }),
		CPUPercentTotal: med(func(n clusterstats.NodeSample) float64 { return n.CPUPercent }),
		GoroutinesTotal: med(func(n clusterstats.NodeSample) float64 { return n.Goroutines }),
		QueriesPerSec:   med(func(n clusterstats.NodeSample) float64 { return n.QueriesPerSec }),
		CommitsPerSec:   med(func(n clusterstats.NodeSample) float64 { return n.CommitsPerSec }),
		AbortsPerSec:    med(func(n clusterstats.NodeSample) float64 { return n.AbortsPerSec }),
		ServerAvgMs:     med(func(n clusterstats.NodeSample) float64 { return n.ServerAvgMs }),
	}
}

func (o *Orchestrator) scraperNodes() int {
	if o.scraper == nil {
		return 0
	}
	return o.scraper.Nodes()
}

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sorted := append([]float64{}, xs...)
	sort.Float64s(sorted)
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

func (o *Orchestrator) setup(ctx context.Context) error {
	// GraphQL schema first, then DQL. The interop type PersonView maps onto the
	// DQL Person predicates (xid/name/age/bio); applying the DQL Alter last makes
	// it authoritative for those shared predicates, so updateGQLSchema cannot
	// strip their indexes / @upsert / @lang.
	slog.Info("applying GraphQL schema")
	if err := o.gql.UpdateSchema(ctx, schema.GraphQLSchema()); err != nil {
		return fmt.Errorf("apply GraphQL schema: %w", err)
	}
	slog.Info("applying DQL schema")
	if err := o.dgraph.ApplySchema(ctx, schema.DQLSchema()); err != nil {
		return fmt.Errorf("apply DQL schema: %w", err)
	}
	// Give the cluster a moment to propagate the schema before writes.
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
			o.flush(ctx, start)
		}
	}
}

func (o *Orchestrator) flush(ctx context.Context, start time.Time) {
	now := time.Now()
	elapsed := now.Sub(start)
	if err := o.reporter.WriteOps(now, elapsed, o.collector.Snapshot()); err != nil {
		slog.Warn("ops flush failed", "err", err)
	}
	if err := o.reporter.WriteRuntime(now, elapsed, metrics.SampleRuntime()); err != nil {
		slog.Warn("runtime flush failed", "err", err)
	}
	if o.scraper != nil {
		samples := o.scraper.Sample(ctx)
		if err := o.reporter.WriteCluster(now, elapsed, samples); err != nil {
			slog.Warn("cluster flush failed", "err", err)
		}
		if len(samples) > 0 {
			o.clusterAgg = append(o.clusterAgg, clusterstats.Aggregate(samples))
		}
	}
}

func (o *Orchestrator) logValidation(r *validate.Result) {
	passed := 0
	for _, c := range r.Checks {
		if c.Passed {
			passed++
			slog.Info("validation check passed", "check", c.Name, "detail", c.Detail)
		} else {
			slog.Error("validation check failed", "check", c.Name, "detail", c.Detail)
		}
	}
	slog.Info("validation complete", "passed", passed, "total", len(r.Checks), "all_passed", r.AllPassed)
}
