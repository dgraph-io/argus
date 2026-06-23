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
}

// DefaultThresholds is a reasonable starting gate.
var DefaultThresholds = Thresholds{Latency: 0.10, Throughput: 0.10, Resource: 0.15}

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
}

// Report is the full comparison outcome.
type Report struct {
	Findings  []Finding
	Warnings  []string
	Regressed bool
}

// Compare diffs candidate summaries against baseline summaries. Each side may
// contain several repeats; metrics are averaged and the run-to-run noise (rel
// stddev) gates regressions so noise alone isn't flagged.
func Compare(baseline, candidate []*RunSummary, th Thresholds) *Report {
	r := &Report{}
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

	// Cluster steady-state resource.
	r.add("cluster", "rss_mb_total", clusterVals(baseline, func(c ClusterSummary) float64 { return c.RSSMBTotal }),
		clusterVals(candidate, func(c ClusterSummary) float64 { return c.RSSMBTotal }), th.Resource, true)
	r.add("cluster", "cpu_pct_total", clusterVals(baseline, func(c ClusterSummary) float64 { return c.CPUPercentTotal }),
		clusterVals(candidate, func(c ClusterSummary) float64 { return c.CPUPercentTotal }), th.Resource, true)
	r.add("cluster", "queries_per_s", clusterVals(baseline, func(c ClusterSummary) float64 { return c.QueriesPerSec }),
		clusterVals(candidate, func(c ClusterSummary) float64 { return c.QueriesPerSec }), th.Throughput, false)

	for _, f := range r.Findings {
		if f.Regression {
			r.Regressed = true
			break
		}
	}
	return r
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
	// Regression when the worsening exceeds the threshold AND exceeds the
	// measured run-to-run noise (so noise alone isn't flagged).
	regression := worse > threshold && worse > noise
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
		if onlyRegressions && !f.Regression {
			continue
		}
		verdict := "ok"
		if f.Regression {
			verdict = "REGRESSION"
		}
		fmt.Fprintf(&b, "%-34s %-14s %12.3f %12.3f %+9.1f %8.0f %8.1f  %s\n",
			f.Op, f.Metric, f.Baseline, f.Candidate, f.DeltaPct, f.Threshold, f.NoisePct, verdict)
	}
	b.WriteString("\n")
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

func clusterVals(runs []*RunSummary, f func(ClusterSummary) float64) []float64 {
	out := make([]float64, 0, len(runs))
	for _, r := range runs {
		out = append(out, f(r.Cluster))
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
	if b.Topology != c.Topology {
		w = append(w, fmt.Sprintf("topology differs: baseline %q vs candidate %q — comparison may be invalid", b.Topology, c.Topology))
	}
	if b.Hostname != c.Hostname || b.OS != c.OS || b.NumCPU != c.NumCPU {
		w = append(w, fmt.Sprintf("host differs: baseline %s/%s/%dcpu vs candidate %s/%s/%dcpu", b.Hostname, b.OS, b.NumCPU, c.Hostname, c.OS, c.NumCPU))
	}
	if baseline[0].Mode != candidate[0].Mode || baseline[0].Passes != candidate[0].Passes {
		w = append(w, fmt.Sprintf("workload differs: baseline mode=%s passes=%d vs candidate mode=%s passes=%d", baseline[0].Mode, baseline[0].Passes, candidate[0].Mode, candidate[0].Passes))
	}
	if baseline[0].Dataset != candidate[0].Dataset {
		w = append(w, fmt.Sprintf("dataset differs: baseline %+v vs candidate %+v — not comparable", baseline[0].Dataset, candidate[0].Dataset))
	}
	if b.DgraphVersion == c.DgraphVersion {
		w = append(w, fmt.Sprintf("both sides report the same Dgraph version (%s) — nothing to compare?", b.DgraphVersion))
	}
	return w
}
