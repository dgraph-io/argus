// Package harness orchestrates a run: apply schemas, run the concurrent
// workload while periodically flushing snapshots, then validate the final
// graph against the deterministic dataset.
package harness

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/dgraph-io/argus/pkg/cluster"
	"github.com/dgraph-io/argus/pkg/clusterstats"
	"github.com/dgraph-io/argus/pkg/envinfo"
	"github.com/dgraph-io/argus/pkg/graphql"
	"github.com/dgraph-io/argus/pkg/metrics"
	"github.com/dgraph-io/argus/pkg/output"
	"github.com/dgraph-io/argus/pkg/report"
	"github.com/dgraph-io/argus/pkg/schema"
	"github.com/dgraph-io/argus/pkg/shardbalance"
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
	NoDrop           bool // skip the clean-slate DropAll at setup
	Passes           int  // >0: fixed-work mode
	Seed             int64
	Env              envinfo.Info
	Dataset          testdata.Dataset
	BulkLoad         bool   // run the blind bulk-load phase (live-loader-shaped insert)
	BulkGroupSize    int    // records per bulk-load mutation (0 => testdata.BulkGroupSize)
	ShardBalance     bool   // after schema, spread predicates across shard groups (sharded configs)
	ZeroURL          string // Zero admin HTTP base (e.g. http://dgraph-dgraph-zero:6080) for ShardBalance
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
	perNodeMut map[string][]float64      // node -> active-mutations per flush, for the write-balance CV
	bulkLoad   *report.BulkLoadSummary   // result of the bulk-load phase (nil if skipped)
}

// New creates an orchestrator. scraper may be nil to skip per-node cluster
// metrics.
func New(dgraph *cluster.DgraphClient, gql *graphql.Client, reporter *output.Reporter, scraper *clusterstats.Scraper, opts Options) *Orchestrator {
	return &Orchestrator{
		dgraph:     dgraph,
		gql:        gql,
		reporter:   reporter,
		scraper:    scraper,
		collector:  metrics.NewCollector(),
		opts:       opts,
		perNodeMut: map[string][]float64{},
	}
}

// Run executes setup, the concurrent workload, and validation, returning the
// validation result and a machine-readable run summary for regression compare.
func (o *Orchestrator) Run(ctx context.Context) (*validate.Result, *report.RunSummary, error) {
	if err := o.setup(ctx); err != nil {
		return nil, nil, fmt.Errorf("setup: %w", err)
	}

	// Blind bulk-load phase runs once on the clean slate, before the timed mixed
	// workload, so its large conflict-free batches are measured in isolation.
	o.bulkLoad = o.runBulkLoad(ctx)

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
	result := validate.NewChecker(o.dgraph, o.gql, o.opts.Dataset, o.opts.VectorStrict, o.bulkLoad != nil).CheckAll(ctx)
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
	d := o.opts.Dataset
	return &report.RunSummary{
		Env:              o.opts.Env,
		Mode:             o.mode(),
		Passes:           o.opts.Passes,
		Seed:             o.opts.Seed,
		WriteConcurrency: o.opts.WriteConcurrency,
		ReadConcurrency:  o.opts.ReadConcurrency,
		Dataset: report.DatasetInfo{
			Persons: d.NumPersons, Posts: d.NumPosts, Products: d.NumProducts,
			Reviews: d.NumProducts * d.ReviewsPerProduct, Companies: d.NumCompanies,
			Categories: d.NumCategories, Dogs: d.NumDogs, Cats: d.NumCats,
		},
		StartedAt:       time.Now().Add(-measured).Format(time.RFC3339),
		MeasuredSeconds: secs,
		Ops:             ops,
		Cluster:         o.clusterSummary(),
		BulkLoad:        o.bulkLoad,
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
		JemallocMBTotal: med(func(n clusterstats.NodeSample) float64 { return n.JemallocMB }),
		CPUPercentTotal: med(func(n clusterstats.NodeSample) float64 { return n.CPUPercent }),
		GoroutinesTotal: med(func(n clusterstats.NodeSample) float64 { return n.Goroutines }),
		QueriesPerSec:   med(func(n clusterstats.NodeSample) float64 { return n.QueriesPerSec }),
		CommitsPerSec:   med(func(n clusterstats.NodeSample) float64 { return n.CommitsPerSec }),
		AbortsPerSec:    med(func(n clusterstats.NodeSample) float64 { return n.AbortsPerSec }),
		ServerAvgMs:     med(func(n clusterstats.NodeSample) float64 { return n.ServerAvgMs }),
		WriteBalanceCV:  o.writeBalanceCV(),
	}
}

// writeBalanceCV is the coefficient of variation of per-alpha mean active
// mutations at steady state: ~0 when writes are evenly spread across alphas, high
// when concentrated on a few. 0 for <2 alphas. Drops each node's first sample as
// cold, matching clusterSummary's steady-state window.
func (o *Orchestrator) writeBalanceCV() float64 {
	means := make([]float64, 0, len(o.perNodeMut))
	for _, xs := range o.perNodeMut {
		s := xs
		if len(s) > 2 {
			s = s[1:]
		}
		if len(s) == 0 {
			continue
		}
		sum := 0.0
		for _, v := range s {
			sum += v
		}
		means = append(means, sum/float64(len(s)))
	}
	if len(means) < 2 {
		return 0
	}
	mu := 0.0
	for _, m := range means {
		mu += m
	}
	mu /= float64(len(means))
	if mu == 0 {
		return 0
	}
	varr := 0.0
	for _, m := range means {
		d := m - mu
		varr += d * d
	}
	varr /= float64(len(means)) // population variance across the N alphas
	return math.Sqrt(varr) / mu
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
	// Start from a clean slate so the graph ends up as exactly the deterministic
	// dataset; otherwise data left by a prior (e.g. larger-scale) run survives as
	// "extra" records and the complete validation fails.
	if !o.opts.NoDrop {
		slog.Info("dropping all data (clean slate)")
		if err := o.dgraph.DropAll(ctx); err != nil {
			return fmt.Errorf("drop all: %w", err)
		}
	}

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

	// Spread predicates across shard groups now, while the just-created tablets are
	// still empty (moves are instant), so writes distribute instead of concentrating
	// on whichever group Zero placed them on. No-op for non-sharded clusters.
	if o.opts.ShardBalance && o.opts.ZeroURL != "" {
		res, err := shardbalance.Rebalance(ctx, o.opts.ZeroURL)
		switch {
		case err != nil:
			slog.Warn("shard-balance failed; continuing with default placement", "err", err)
		case res.Groups >= 2:
			slog.Info("shard-balance applied", "groups", res.Groups,
				"moved", res.Moved, "skipped", res.Skipped, "failed", res.Failed)
		}
	}
	return nil
}

// runBulkLoad executes the blind bulk-load phase: large, conflict-free insert
// batches (the live-loader-shaped path) fanned out across the write workers.
// It returns nil when skipped. Blind inserts dedup nothing, so it is unsafe
// without the clean-slate drop — a second load would double every record — and
// is therefore skipped under -no-drop.
func (o *Orchestrator) runBulkLoad(ctx context.Context) *report.BulkLoadSummary {
	d := o.opts.Dataset
	switch {
	case !o.opts.BulkLoad:
		return nil
	case d.NumBulkRecords <= 0:
		return nil
	case o.opts.NoDrop:
		slog.Warn("skipping bulk-load phase: -no-drop set (blind inserts would duplicate without a clean slate)")
		return nil
	}

	group := o.opts.BulkGroupSize
	if group < 1 {
		group = testdata.BulkGroupSize
	}
	conc := o.opts.WriteConcurrency
	if conc < 1 {
		conc = 1
	}

	// Build the shared hub pool first and capture each hub's server-assigned uid,
	// so records can fan brhub edges into it by uid (still a blind insert, no
	// per-record upsert query). Falls back to scalar-only if hubs can't be made.
	hubUIDs, hubIndices, edgesPerRec := o.createBulkHubs(ctx, d, group)

	records := make([]testdata.BulkRecord, d.NumBulkRecords)
	for i := range records {
		records[i] = d.BulkRecord(i)
	}
	reqs := testdata.BulkRecordInserts(records, hubUIDs, hubIndices, group)
	nquads := d.NumBulkRecords * (testdata.NQuadsPerBulkRecord + edgesPerRec)
	slog.Info("bulk-load starting", "records", d.NumBulkRecords, "nquads", nquads, "batches", len(reqs),
		"batch_records", group, "hubs", len(hubUIDs), "edges_per_record", edgesPerRec, "write_concurrency", conc)

	var (
		hist    metrics.Histogram
		mu      sync.Mutex
		errs    int
		retries int
		failed  []*api.Request
		ch      = make(chan *api.Request)
		wg      sync.WaitGroup
	)
	// Fan-in edges make concurrent batches occasionally touch the same hub's
	// reverse list and abort; retry a few times so the load completes.
	const bulkAttempts = 10
	start := time.Now()
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := range ch {
				if ctx.Err() != nil {
					continue
				}
				t0 := time.Now()
				var err error
				local := 0
				for a := 0; a < bulkAttempts; a++ {
					txn := o.dgraph.Txn()
					_, err = txn.Do(ctx, req)
					txn.Discard(ctx)
					if err == nil || ctx.Err() != nil || !isConflict(err) {
						break
					}
					local++
					time.Sleep(time.Duration(a+1) * 3 * time.Millisecond)
				}
				ms := float64(time.Since(t0).Microseconds()) / 1000.0
				mu.Lock()
				hist.Observe(ms)
				retries += local
				// A batch that exhausts its concurrent retries is deferred, not
				// dropped: contention (not the data) is the failure, so a serial
				// mop-up below re-runs it with no competing writers.
				if err != nil {
					failed = append(failed, req)
				}
				mu.Unlock()
				if err != nil {
					slog.Debug("bulk-load batch error", "err", err)
				}
			}
		}()
	}
	for _, req := range reqs {
		if ctx.Err() != nil {
			break
		}
		ch <- req
	}
	close(ch)
	wg.Wait()

	// Serial mop-up: with a single writer there is no cross-batch conflict, so
	// any batch deferred by contention lands here. A batch that still fails is a
	// real error and stays counted, so validation fails loudly at the load step
	// rather than silently short-loading the graph.
	if len(failed) > 0 && ctx.Err() == nil {
		slog.Info("bulk-load mop-up", "deferred_batches", len(failed))
		for _, req := range failed {
			if ctx.Err() != nil {
				break
			}
			t0 := time.Now()
			var err error
			for a := 0; a < bulkAttempts; a++ {
				txn := o.dgraph.Txn()
				_, err = txn.Do(ctx, req)
				txn.Discard(ctx)
				if err == nil || ctx.Err() != nil || !isConflict(err) {
					break
				}
				retries++
				time.Sleep(time.Duration(a+1) * 3 * time.Millisecond)
			}
			hist.Observe(float64(time.Since(t0).Microseconds()) / 1000.0)
			if err != nil {
				errs++
				slog.Warn("bulk-load batch failed after mop-up", "err", err)
			}
		}
	}
	secs := time.Since(start).Seconds()

	bl := &report.BulkLoadSummary{
		Records:        d.NumBulkRecords,
		NQuads:         nquads,
		BatchRecords:   group,
		Hubs:           len(hubUIDs),
		EdgesPerRecord: edgesPerRec,
		Seconds:        secs,
		BatchP50Ms:     hist.Percentile(50),
		BatchP99Ms:     hist.Percentile(99),
		Retries:        retries,
		Errors:         errs,
	}
	if secs > 0 {
		bl.NQuadsPerSec = float64(nquads) / secs
		bl.RecordsPerSec = float64(d.NumBulkRecords) / secs
	}
	slog.Info("bulk-load complete", "seconds", fmt.Sprintf("%.1f", secs),
		"nquads_per_s", fmt.Sprintf("%.0f", bl.NQuadsPerSec),
		"records_per_s", fmt.Sprintf("%.0f", bl.RecordsPerSec),
		"batch_p99_ms", fmt.Sprintf("%.1f", bl.BatchP99Ms), "retries", retries, "errors", errs)
	return bl
}

// createBulkHubs creates the shared hub pool and returns the captured hub uids,
// the per-record hub-index function, and the effective edges-per-record. Returns
// (nil, nil, 0) for the scalar-only path or if hub creation fails (the load then
// degrades to scalar inserts rather than aborting the run).
func (o *Orchestrator) createBulkHubs(ctx context.Context, d testdata.Dataset, group int) ([]string, func(int) []int, int) {
	if d.BulkHubs <= 0 || d.BulkEdges <= 0 {
		return nil, nil, 0
	}
	slog.Info("bulk-load creating hub pool", "hubs", d.BulkHubs)
	hubUIDs := make([]string, d.BulkHubs)
	for _, req := range testdata.BulkHubInserts(d.BulkHubs, group) {
		txn := o.dgraph.Txn()
		resp, err := txn.Do(ctx, req)
		txn.Discard(ctx)
		if err != nil {
			slog.Warn("bulk-load hub creation failed; falling back to scalar-only", "err", err)
			return nil, nil, 0
		}
		for blank, uid := range resp.Uids {
			// Blank-node names are "h{index}".
			if len(blank) > 1 && blank[0] == 'h' {
				if h, perr := strconv.Atoi(blank[1:]); perr == nil && h >= 0 && h < len(hubUIDs) {
					hubUIDs[h] = uid
				}
			}
		}
	}
	for h, u := range hubUIDs {
		if u == "" {
			slog.Warn("bulk-load hub uid missing; falling back to scalar-only", "hub", h)
			return nil, nil, 0
		}
	}
	return hubUIDs, d.BulkHubIndices, d.BulkEdges
}

// isConflict reports whether err is a retryable transaction conflict/abort.
func isConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "aborted") ||
		strings.Contains(msg, "conflict") ||
		strings.Contains(msg, "please retry")
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
			for _, s := range samples {
				o.perNodeMut[s.Node] = append(o.perNodeMut[s.Node], s.ActiveMutations)
			}
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
