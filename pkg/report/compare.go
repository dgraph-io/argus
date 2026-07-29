package report

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/dgraph-io/argus/pkg/metrics"
)

// Thresholds are the minimum worsening (as a fraction, e.g. 0.10 = 10%) that
// counts as a regression for each dimension.
type Thresholds struct {
	Latency    float64 // p99/avg getting larger
	Throughput float64 // ops/sec getting smaller
	Resource   float64 // cluster RSS/CPU getting larger
	// NoiseMargin: the worsening must exceed the measured run-to-run noise by
	// this factor to count, not just exceed it. p99 is a single tail sample per
	// repeat (HDR-bucketed), so a delta that merely edges past noise is jitter;
	// requiring, say, 1.5x separates signal from that. <=0 falls back to 1.
	NoiseMargin float64
	// LoadFactor: when the whole run finished faster (measured wall-clock down
	// beyond noise), a per-op latency regression no larger than this multiple of
	// the global speedup is demoted to a non-gating "load-shift" — it tracks the
	// run shifting rather than a localized slowdown. Every op's throughput is
	// count/shared-window, so a faster run lifts all op rates uniformly and drags
	// per-op latencies with it; only a latency increase that dwarfs the speedup is
	// a real per-op regression. <=0 disables the demotion.
	LoadFactor float64
	// MinLatencyDeltaMs: a per-op latency regression must move at least this many
	// milliseconds in absolute terms to gate. A sub-millisecond move on a cheap op
	// (e.g. schema introspection) clears a relative threshold easily but is below
	// timing resolution and scheduler jitter. <=0 disables the floor.
	MinLatencyDeltaMs float64
	// MinSamples: a per-op latency regression must rest on at least this many
	// completed ops (candidate side) to gate. A mean drawn from a handful of
	// samples is jitter, not signal. Skipped when the count is unknown (0).
	// <=0 disables the floor.
	MinSamples int
}

// DefaultThresholds is a reasonable starting gate.
var DefaultThresholds = Thresholds{
	Latency: 0.10, Throughput: 0.10, Resource: 0.15, NoiseMargin: 1.5,
	LoadFactor: 2, MinLatencyDeltaMs: 1.0, MinSamples: 100,
}

// stat is a mean and relative standard deviation across repeats.
type stat struct {
	mean   float64
	relStd float64 // stddev/mean, the run-to-run noise for this metric
	n      int
}

func meanStat(xs []float64) stat {
	if len(xs) == 0 {
		return stat{}
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	m := sum / float64(len(xs))
	if len(xs) == 1 || m == 0 {
		return stat{mean: m, n: len(xs)}
	}
	var v float64
	for _, x := range xs {
		v += (x - m) * (x - m)
	}
	sd := math.Sqrt(v / float64(len(xs)-1))
	return stat{mean: m, relStd: sd / math.Abs(m), n: len(xs)}
}

// Finding is one compared metric.
type Finding struct {
	Op          string
	Metric      string // "throughput", "p99", "avg", "cluster:rss", ...
	Baseline    float64
	Candidate   float64
	DeltaPct    float64 // signed, candidate vs baseline
	Threshold   float64
	NoisePct    float64 // run-to-run noise (max rel stddev of the two sides)
	Regression  bool
	HigherWorse bool
	// TailOnly marks a p99 regression that the op's avg does not corroborate: the
	// tail moved but the mean did not, which is usually noise or a narrow tail
	// blip rather than a systemic slowdown. Shown, but does not gate the verdict.
	TailOnly bool
	// LoadShifted marks a per-op latency regression demoted because the whole run
	// finished faster and this op's latency rose no more than LoadFactor× that
	// speedup — the increase tracks the faster run, not a localized slowdown.
	// Shown, but does not gate the verdict.
	LoadShifted bool
	// LowConfidence marks a per-op latency regression demoted because it rests on
	// too few samples or too small an absolute move to trust. Shown, but does not
	// gate the verdict.
	LowConfidence bool
}

// Report is the full comparison outcome.
type Report struct {
	Findings    []Finding
	Warnings    []string
	HeapDiffs   []*HeapDiff
	CPU         *CPUDiff
	Contention  []*ContentionDiff
	Regressed   bool
	noiseMargin float64 // factor by which worsening must exceed noise (see Thresholds)
	// globalSpeedup is the fraction the whole run got faster (measured wall-clock),
	// positive = faster; set only when it clears the noise margin. Drives the
	// load-shift demotion and the rendered note.
	globalSpeedup float64
	loadFactor    float64 // multiple of globalSpeedup below which a latency rise is load-shift
}

// AddCPU attaches a CPU-profile comparison. It is diagnostic only — CPU totals
// aren't comparable across runs, so it localizes where time moved without gating
// the verdict.
func (r *Report) AddCPU(basePath, candPath string, topN int) error {
	d, err := DiffCPU(basePath, candPath, topN)
	if err != nil {
		return err
	}
	r.CPU = d
	return nil
}

// AddContention attaches a mutex- or block-contention comparison (kind is
// "mutex" or "block"). Diagnostic only, like AddCPU.
func (r *Report) AddContention(kind, basePath, candPath string, topN int) error {
	d, err := DiffContention(kind, basePath, candPath, topN)
	if err != nil {
		return err
	}
	r.Contention = append(r.Contention, d)
	return nil
}

// AddHeap diffs a pair of heap profiles for one phase, attaches the per-site
// breakdown, and gates the total inuse_space against the resource threshold (the
// same gate as cluster RSS). Heap is captured once per run, so there are no
// repeats to estimate noise from — the total is treated as a single sample.
func (r *Report) AddHeap(phase, basePath, candPath string, resourceTh float64, topN int) error {
	d, err := DiffHeap(phase, basePath, candPath, topN)
	if err != nil {
		return err
	}
	r.HeapDiffs = append(r.HeapDiffs, d)
	before := len(r.Findings)
	r.add("heap:"+phase, "inuse_mb",
		[]float64{float64(d.BaselineTotal) / (1 << 20)},
		[]float64{float64(d.CandidateTotal) / (1 << 20)},
		resourceTh, true)
	if len(r.Findings) > before && r.Findings[len(r.Findings)-1].Regression {
		r.Regressed = true
	}
	return nil
}

// Compare diffs candidate summaries against baseline summaries. Each side may
// contain several repeats; metrics are averaged and the run-to-run noise (rel
// stddev) gates regressions so noise alone isn't flagged.
func Compare(baseline, candidate []*RunSummary, th Thresholds) *Report {
	r := &Report{}
	r.noiseMargin = th.NoiseMargin
	if r.noiseMargin <= 0 {
		r.noiseMargin = 1
	}
	r.loadFactor = th.LoadFactor
	r.Warnings = envWarnings(baseline, candidate)

	// Union of op keys present on both sides.
	keys := map[string]bool{}
	for _, k := range opKeys(baseline) {
		keys[k] = true
	}
	bKeys := keys
	keys = map[string]bool{}
	for _, k := range opKeys(candidate) {
		if bKeys[k] {
			keys[k] = true
		}
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	for _, key := range sorted {
		r.add(key, "throughput", collect(baseline, key, tpOf), collect(candidate, key, tpOf), th.Throughput, false)
		r.add(key, "p99", collect(baseline, key, p99Of), collect(candidate, key, p99Of), th.Latency, true)
		r.add(key, "avg", collect(baseline, key, avgOf), collect(candidate, key, avgOf), th.Latency, true)
	}

	// Whole-run wall-clock: the headline aggregate. Higher is worse, and it gates
	// like any other finding — a globally slower candidate flags here even if no
	// single op stands out. It also sets the global-speedup context the load-shift
	// demotion uses below.
	msBase := runVals(baseline, func(r *RunSummary) float64 { return r.MeasuredSeconds })
	msCand := runVals(candidate, func(r *RunSummary) float64 { return r.MeasuredSeconds })
	r.add("workload", "measured_s", msBase, msCand, th.Latency, true)
	if b, c := meanStat(msBase), meanStat(msCand); b.mean > 0 && c.mean > 0 {
		speedup := (b.mean - c.mean) / b.mean // positive = candidate faster
		noise := math.Max(b.relStd, c.relStd)
		if speedup > 0 && speedup > noise*r.noiseMargin {
			r.globalSpeedup = speedup
		}
	}

	// Cluster steady-state resource.
	r.add("cluster", "rss_mb_total", clusterVals(baseline, func(c ClusterSummary) float64 { return c.RSSMBTotal }),
		clusterVals(candidate, func(c ClusterSummary) float64 { return c.RSSMBTotal }), th.Resource, true)
	r.add("cluster", "jemalloc_mb_total", clusterVals(baseline, func(c ClusterSummary) float64 { return c.JemallocMBTotal }),
		clusterVals(candidate, func(c ClusterSummary) float64 { return c.JemallocMBTotal }), th.Resource, true)
	r.add("cluster", "cpu_pct_total", clusterVals(baseline, func(c ClusterSummary) float64 { return c.CPUPercentTotal }),
		clusterVals(candidate, func(c ClusterSummary) float64 { return c.CPUPercentTotal }), th.Resource, true)
	// Goroutines gated like memory: a steady-state count that climbs across
	// otherwise-similar runs is a leak signal RSS alone can lag behind.
	r.add("cluster", "goroutines_total", clusterVals(baseline, func(c ClusterSummary) float64 { return c.GoroutinesTotal }),
		clusterVals(candidate, func(c ClusterSummary) float64 { return c.GoroutinesTotal }), th.Resource, true)
	r.add("cluster", "queries_per_s", clusterVals(baseline, func(c ClusterSummary) float64 { return c.QueriesPerSec }),
		clusterVals(candidate, func(c ClusterSummary) float64 { return c.QueriesPerSec }), th.Throughput, false)

	// Bulk-load: the live-loader-shaped insert phase. nquads/s is where parallel
	// mutations should help; batch p99 guards tail latency under big batches.
	r.add("bulkload", "nquads_per_s", bulkVals(baseline, func(b *BulkLoadSummary) float64 { return b.NQuadsPerSec }),
		bulkVals(candidate, func(b *BulkLoadSummary) float64 { return b.NQuadsPerSec }), th.Throughput, false)
	r.add("bulkload", "batch_p99", bulkVals(baseline, func(b *BulkLoadSummary) float64 { return b.BatchP99Ms }),
		bulkVals(candidate, func(b *BulkLoadSummary) float64 { return b.BatchP99Ms }), th.Latency, true)

	r.demoteLoadShifted(th.LoadFactor)
	r.dampenLowConfidence(th.MinLatencyDeltaMs, th.MinSamples, candCounts(candidate))
	r.corroborateLatency()

	for _, f := range r.Findings {
		if f.Regression {
			r.Regressed = true
			break
		}
	}
	return r
}

// demoteLoadShifted demotes a per-op latency regression (p99 or avg) to a
// non-gating "load-shift" when the whole run finished faster (globalSpeedup set)
// and the latency rose no more than loadFactor× that speedup. Per-op throughput
// is count/shared-window, so a faster run lifts every op's rate uniformly and
// drags its latencies up in step; an increase within loadFactor× the speedup is
// that shift, not a localized slowdown. A latency increase that dwarfs the
// speedup is left as a regression. Runs before corroborateLatency so a demoted
// avg lets its p99 fall through to tail-only. No-op unless the run is globally
// faster, so a flat-or-slower candidate gates latency normally.
func (r *Report) demoteLoadShifted(loadFactor float64) {
	if loadFactor <= 0 || r.globalSpeedup <= 0 {
		return
	}
	ceiling := loadFactor * r.globalSpeedup * 100 // DeltaPct is in percent
	for i := range r.Findings {
		f := &r.Findings[i]
		if !f.Regression || (f.Metric != "p99" && f.Metric != "avg") {
			continue
		}
		if f.DeltaPct > 0 && f.DeltaPct <= ceiling {
			f.Regression = false
			f.LoadShifted = true
		}
	}
}

// dampenLowConfidence demotes a per-op latency regression to a non-gating
// "low-confidence" when the evidence is too thin to trust: the absolute move is
// below minMs (sub-millisecond jitter on a cheap op) or the candidate op ran
// fewer than minSamples times (a mean from a handful of samples). Runs before
// corroborateLatency so a demoted avg lets its p99 fall through to tail-only.
func (r *Report) dampenLowConfidence(minMs float64, minSamples int, count map[string]float64) {
	for i := range r.Findings {
		f := &r.Findings[i]
		if !f.Regression || (f.Metric != "p99" && f.Metric != "avg") {
			continue
		}
		subMs := minMs > 0 && math.Abs(f.Candidate-f.Baseline) < minMs
		lowN := minSamples > 0 && count[f.Op] > 0 && count[f.Op] < float64(minSamples)
		if subMs || lowN {
			f.Regression = false
			f.LowConfidence = true
		}
	}
}

// corroborateLatency demotes a per-op p99 regression to tail-only when the same
// op's avg did not also regress. A genuine slowdown moves the mean, not just one
// tail bucket, so an uncorroborated p99 is treated as jitter for the verdict
// (still shown). Only per-op p99 has an avg counterpart; cluster/bulkload latency
// findings are left alone.
func (r *Report) corroborateLatency() {
	avgRegressed := map[string]bool{}
	for _, f := range r.Findings {
		if f.Metric == "avg" && f.Regression {
			avgRegressed[f.Op] = true
		}
	}
	for i := range r.Findings {
		f := &r.Findings[i]
		if f.Metric == "p99" && f.Regression && !avgRegressed[f.Op] {
			f.Regression = false
			f.TailOnly = true
		}
	}
}

func (r *Report) add(op, metric string, base, cand []float64, threshold float64, higherWorse bool) {
	if len(base) == 0 || len(cand) == 0 {
		return
	}
	b, c := meanStat(base), meanStat(cand)
	if b.mean == 0 {
		return
	}
	delta := (c.mean - b.mean) / b.mean
	noise := math.Max(b.relStd, c.relStd)
	// "Worse" direction depends on the metric.
	worse := delta
	if !higherWorse {
		worse = -delta
	}
	// Regression when the worsening exceeds the threshold AND clears the measured
	// run-to-run noise by the configured margin (so a delta merely edging past
	// noise — typical of jittery p99 tails — isn't flagged).
	regression := worse > threshold && worse > noise*r.noiseMargin
	r.Findings = append(r.Findings, Finding{
		Op: op, Metric: metric, Baseline: b.mean, Candidate: c.mean,
		DeltaPct: delta * 100, Threshold: threshold * 100, NoisePct: noise * 100,
		Regression: regression, HigherWorse: higherWorse,
	})
}

// Render produces a human-readable report; onlyRegressions limits op rows to
// regressions (cluster + summary always shown).
func (r *Report) Render(onlyRegressions bool) string {
	var b strings.Builder
	for _, w := range r.Warnings {
		fmt.Fprintf(&b, "WARNING: %s\n", w)
	}
	if len(r.Warnings) > 0 {
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, "%-34s %-14s %12s %12s %9s %8s %8s  %s\n",
		"op", "metric", "baseline", "candidate", "delta%", "thr%", "noise%", "verdict")
	for _, f := range r.Findings {
		// The aggregate resource/throughput block (cluster RSS/jemalloc/CPU/
		// goroutines, whole-run wall-clock, bulk-load) is always shown — it's the
		// safety panel you check to confirm nothing regressed, so hiding a healthy
		// row would defeat the purpose. Only per-op rows collapse to regressions.
		if onlyRegressions && !isAggregate(f.Op) &&
			!f.Regression && !f.TailOnly && !f.LoadShifted && !f.LowConfidence {
			continue
		}
		verdict := "ok"
		switch {
		case f.Regression:
			verdict = "REGRESSION"
		case f.LoadShifted:
			verdict = "load-shift (run faster)"
		case f.LowConfidence:
			verdict = "low-confidence (thin sample)"
		case f.TailOnly:
			verdict = "tail-only (avg ok)"
		}
		fmt.Fprintf(&b, "%-34s %-14s %12.3f %12.3f %+9.1f %8.0f %8.1f  %s\n",
			f.Op, f.Metric, f.Baseline, f.Candidate, f.DeltaPct, f.Threshold, f.NoisePct, verdict)
	}
	b.WriteString("\n")
	for _, d := range r.HeapDiffs {
		b.WriteString(d.Render())
		b.WriteString("\n")
	}
	if r.CPU != nil {
		b.WriteString(r.CPU.Render())
		b.WriteString("\n")
	}
	for _, d := range r.Contention {
		b.WriteString(d.Render())
		b.WriteString("\n")
	}
	if r.globalSpeedup > 0 {
		fmt.Fprintf(&b, "NOTE: whole run %.1f%% faster (wall-clock); a per-op latency rise up to %.1f%% (%.0f× the speedup) tracks that and is marked load-shift, not a regression.\n",
			r.globalSpeedup*100, r.loadFactor*r.globalSpeedup*100, r.loadFactor)
	}
	if r.Regressed {
		b.WriteString("VERDICT: REGRESSION DETECTED\n")
	} else {
		b.WriteString("VERDICT: OK (no regression beyond thresholds + noise)\n")
	}
	return b.String()
}

// --- helpers ---

func tpOf(o *metrics.OpSummary) float64  { return o.ThroughputPerSec }
func p99Of(o *metrics.OpSummary) float64 { return o.P99Ms }
func avgOf(o *metrics.OpSummary) float64 { return o.AvgMs }

func collect(runs []*RunSummary, key string, f func(*metrics.OpSummary) float64) []float64 {
	var out []float64
	for _, r := range runs {
		if o := r.op(key); o != nil {
			out = append(out, f(o))
		}
	}
	return out
}

// candCounts is the median per-op completion count across the candidate repeats,
// keyed by op — the sample size behind each op's latency mean.
func candCounts(runs []*RunSummary) map[string]float64 {
	byOp := map[string][]float64{}
	for _, r := range runs {
		for i := range r.Ops {
			o := &r.Ops[i]
			byOp[o.Key] = append(byOp[o.Key], float64(o.Count))
		}
	}
	out := make(map[string]float64, len(byOp))
	for k, xs := range byOp {
		sort.Float64s(xs)
		out[k] = xs[len(xs)/2]
	}
	return out
}

// isAggregate reports whether an op key is a whole-run aggregate (resource,
// wall-clock, bulk-load) rather than a per-op workload row. These always render.
func isAggregate(op string) bool {
	return op == "cluster" || op == "workload" || op == "bulkload"
}

// runVals collects a per-run scalar across the repeats.
func runVals(runs []*RunSummary, f func(*RunSummary) float64) []float64 {
	out := make([]float64, 0, len(runs))
	for _, r := range runs {
		out = append(out, f(r))
	}
	return out
}

func clusterVals(runs []*RunSummary, f func(ClusterSummary) float64) []float64 {
	out := make([]float64, 0, len(runs))
	for _, r := range runs {
		out = append(out, f(r.Cluster))
	}
	return out
}

// bulkVals collects a bulk-load field across the runs that have one (runs
// without a bulk-load phase are skipped, so a missing phase reads as no data
// rather than a zero that would skew the mean).
func bulkVals(runs []*RunSummary, f func(*BulkLoadSummary) float64) []float64 {
	var out []float64
	for _, r := range runs {
		if r.BulkLoad != nil {
			out = append(out, f(r.BulkLoad))
		}
	}
	return out
}

func opKeys(runs []*RunSummary) []string {
	seen := map[string]bool{}
	for _, r := range runs {
		for _, o := range r.Ops {
			seen[o.Key] = true
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	return out
}

func envWarnings(baseline, candidate []*RunSummary) []string {
	if len(baseline) == 0 || len(candidate) == 0 {
		return nil
	}
	b, c := baseline[0].Env, candidate[0].Env
	var w []string
	if b.Topology() != c.Topology() {
		w = append(w, fmt.Sprintf("topology differs: baseline %q vs candidate %q — comparison may be invalid", b.Topology(), c.Topology()))
	}
	if b.Hostname != c.Hostname || b.OS != c.OS || b.NumCPU != c.NumCPU {
		w = append(w, fmt.Sprintf("host differs: baseline %s/%s/%dcpu vs candidate %s/%s/%dcpu", b.Hostname, b.OS, b.NumCPU, c.Hostname, c.OS, c.NumCPU))
	}
	if baseline[0].Mode != candidate[0].Mode || baseline[0].Passes != candidate[0].Passes {
		w = append(w, fmt.Sprintf("workload differs: baseline mode=%s passes=%d vs candidate mode=%s passes=%d", baseline[0].Mode, baseline[0].Passes, candidate[0].Mode, candidate[0].Passes))
	}
	if baseline[0].WriteConcurrency != candidate[0].WriteConcurrency || baseline[0].ReadConcurrency != candidate[0].ReadConcurrency {
		w = append(w, fmt.Sprintf("concurrency differs: baseline w=%d/r=%d vs candidate w=%d/r=%d — not comparable",
			baseline[0].WriteConcurrency, baseline[0].ReadConcurrency, candidate[0].WriteConcurrency, candidate[0].ReadConcurrency))
	}
	if baseline[0].Dataset != candidate[0].Dataset {
		w = append(w, fmt.Sprintf("dataset differs: baseline %+v vs candidate %+v — not comparable", baseline[0].Dataset, candidate[0].Dataset))
	}
	// A shared version tag is only "nothing to compare" if the builds are truly
	// identical. Two runs can share a tag (e.g. v25.3.6) yet be different builds —
	// released vs a patch — which the commit SHA distinguishes; don't cry wolf then.
	if b.DgraphVersion == c.DgraphVersion && b.DgraphCommit == c.DgraphCommit {
		id := b.DgraphVersion
		if b.DgraphCommit != "" {
			id += " @ " + b.DgraphCommit
		}
		w = append(w, fmt.Sprintf("both sides report the same Dgraph build (%s) — nothing to compare?", id))
	}
	return w
}
