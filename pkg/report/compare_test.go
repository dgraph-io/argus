package report

import (
	"testing"

	"github.com/dgraph-io/argus/pkg/metrics"
)

// run builds a RunSummary carrying one op with the given p99/avg (throughput
// fixed, so only latency is exercised).
func run(key string, p99, avg float64) *RunSummary {
	return &RunSummary{
		Ops: []metrics.OpSummary{{
			Key: key, ThroughputPerSec: 1, P99Ms: p99, AvgMs: avg,
		}},
	}
}

// runMS is run() with a whole-run wall-clock, so the global-speedup path is
// exercised.
func runMS(key string, p99, avg, measured float64) *RunSummary {
	r := run(key, p99, avg)
	r.MeasuredSeconds = measured
	return r
}

// runN is run() with a completion count, so the sample-size floor is exercised.
func runN(key string, p99, avg float64, count int) *RunSummary {
	r := run(key, p99, avg)
	r.Ops[0].Count = count
	return r
}

func finding(r *Report, op, metric string) *Finding {
	for i := range r.Findings {
		if r.Findings[i].Op == op && r.Findings[i].Metric == metric {
			return &r.Findings[i]
		}
	}
	return nil
}

// A p99 that jumps well past threshold but is not backed by an avg move is
// demoted to tail-only and does not gate the verdict.
func TestP99WithoutAvgIsTailOnly(t *testing.T) {
	base := []*RunSummary{run("q", 100, 50), run("q", 100, 50)}
	// p99 +60% (clears threshold + noise*margin), avg flat.
	cand := []*RunSummary{run("q", 160, 50), run("q", 160, 50)}

	r := Compare(base, cand, DefaultThresholds)

	p99 := finding(r, "q", "p99")
	if p99 == nil || p99.Regression {
		t.Fatalf("p99 should not be a gating regression, got %+v", p99)
	}
	if !p99.TailOnly {
		t.Errorf("p99 should be flagged TailOnly")
	}
	if r.Regressed {
		t.Errorf("verdict should be OK when only the tail moved")
	}
}

// When avg also regresses, the p99 regression stands and gates the verdict.
func TestP99WithAvgIsRealRegression(t *testing.T) {
	base := []*RunSummary{run("q", 100, 50), run("q", 100, 50)}
	cand := []*RunSummary{run("q", 160, 80), run("q", 160, 80)} // p99 +60%, avg +60%

	r := Compare(base, cand, DefaultThresholds)

	p99 := finding(r, "q", "p99")
	if p99 == nil || !p99.Regression || p99.TailOnly {
		t.Fatalf("p99 should be a real regression, got %+v", p99)
	}
	if !r.Regressed {
		t.Errorf("verdict should be REGRESSION when avg corroborates p99")
	}
}

// When the whole run finished faster, a per-op latency rise within LoadFactor×
// the speedup is demoted to load-shift and does not gate the verdict.
func TestLoadShiftDemotesLatencyWhenRunFaster(t *testing.T) {
	// Run 12% faster (1000s -> 880s); op latency up 12%, within 2x the speedup.
	base := []*RunSummary{runMS("q", 100, 50, 1000), runMS("q", 100, 50, 1000)}
	cand := []*RunSummary{runMS("q", 112, 56, 880), runMS("q", 112, 56, 880)}

	r := Compare(base, cand, DefaultThresholds)

	if r.globalSpeedup <= 0 {
		t.Fatalf("expected a global speedup to be recorded")
	}
	for _, m := range []string{"avg", "p99"} {
		f := finding(r, "q", m)
		if f == nil || f.Regression || !f.LoadShifted {
			t.Errorf("%s should be load-shifted (non-gating), got %+v", m, f)
		}
	}
	if ws := finding(r, "workload", "measured_s"); ws == nil || ws.Regression {
		t.Errorf("workload wall-clock should be present and not a regression, got %+v", ws)
	}
	if r.Regressed {
		t.Errorf("verdict should be OK when latency merely tracks a faster run")
	}
}

// A latency increase that dwarfs the speedup stays a real regression even when
// the run is globally faster — load-shift must not hide a localized slowdown.
func TestLoadShiftKeepsDisproportionateRegression(t *testing.T) {
	// Run 12% faster, but this op's avg+p99 jump 60% — far past 2x the speedup.
	base := []*RunSummary{runMS("q", 100, 50, 1000), runMS("q", 100, 50, 1000)}
	cand := []*RunSummary{runMS("q", 160, 80, 880), runMS("q", 160, 80, 880)}

	r := Compare(base, cand, DefaultThresholds)

	avg := finding(r, "q", "avg")
	if avg == nil || !avg.Regression || avg.LoadShifted {
		t.Fatalf("avg should stay a real regression, got %+v", avg)
	}
	if !r.Regressed {
		t.Errorf("a slowdown far beyond the speedup should still gate REGRESSION")
	}
}

// The demotion only applies when the run is actually faster; a flat wall-clock
// leaves latency regressions gating normally.
func TestLoadShiftInactiveWhenNotFaster(t *testing.T) {
	base := []*RunSummary{runMS("q", 100, 50, 1000), runMS("q", 100, 50, 1000)}
	cand := []*RunSummary{runMS("q", 160, 80, 1000), runMS("q", 160, 80, 1000)} // same wall-clock

	r := Compare(base, cand, DefaultThresholds)

	if r.globalSpeedup != 0 {
		t.Fatalf("no speedup expected, got %v", r.globalSpeedup)
	}
	avg := finding(r, "q", "avg")
	if avg == nil || !avg.Regression || avg.LoadShifted {
		t.Errorf("avg should gate normally with no speedup, got %+v", avg)
	}
	if !r.Regressed {
		t.Errorf("verdict should be REGRESSION when the run is not faster")
	}
}

// A latency regression resting on too few samples is demoted to low-confidence
// and does not gate, while the same delta on a well-sampled op does gate.
func TestLowConfidenceSampleFloor(t *testing.T) {
	// avg +60% (well past threshold+noise) but only 12 completions.
	thin := []*RunSummary{runN("q", 100, 50, 12), runN("q", 100, 50, 12)}
	thinC := []*RunSummary{runN("q", 160, 80, 12), runN("q", 160, 80, 12)}
	r := Compare(thin, thinC, DefaultThresholds)
	if avg := finding(r, "q", "avg"); avg == nil || avg.Regression || !avg.LowConfidence {
		t.Errorf("thin-sample avg should be low-confidence, got %+v", avg)
	}
	if r.Regressed {
		t.Errorf("verdict should be OK when the only regression is thin-sample")
	}

	// Same relative delta, but 2000 completions — trustworthy, so it gates.
	fat := []*RunSummary{runN("q", 100, 50, 2000), runN("q", 100, 50, 2000)}
	fatC := []*RunSummary{runN("q", 160, 80, 2000), runN("q", 160, 80, 2000)}
	if r := Compare(fat, fatC, DefaultThresholds); !r.Regressed {
		t.Errorf("a well-sampled +60%% avg should still gate REGRESSION")
	}
}

// A sub-millisecond absolute move is demoted even when well-sampled — below
// timing resolution, a large relative delta on a cheap op is not signal.
func TestLowConfidenceAbsoluteFloor(t *testing.T) {
	base := []*RunSummary{runN("q", 6.0, 2.4, 1000), runN("q", 6.0, 2.4, 1000)}
	cand := []*RunSummary{runN("q", 8.0, 2.8, 1000), runN("q", 8.0, 2.8, 1000)} // avg +0.4ms

	r := Compare(base, cand, DefaultThresholds)
	if avg := finding(r, "q", "avg"); avg == nil || avg.Regression || !avg.LowConfidence {
		t.Errorf("sub-ms avg move should be low-confidence, got %+v", avg)
	}
	if r.Regressed {
		t.Errorf("verdict should be OK when the avg move is below the ms floor")
	}
}

// The noise margin drops a worsening that merely edges past measured noise.
func TestNoiseMarginDampensJitter(t *testing.T) {
	// avg +18% mean, but the two repeats disagree enough that rel stddev ~13%.
	// worse=0.18 clears threshold(0.10) and noise(~0.13) but not noise*1.5(~0.20).
	base := []*RunSummary{run("q", 100, 100), run("q", 100, 100)}
	cand := []*RunSummary{run("q", 100, 105), run("q", 100, 131)} // avg mean 118, noisy

	strict := Compare(base, cand, DefaultThresholds) // NoiseMargin 1.5
	loose := Compare(base, cand, Thresholds{Latency: 0.10, NoiseMargin: 1})

	if finding(strict, "q", "avg").Regression {
		t.Errorf("with margin 1.5, a noisy +18%% avg should not be flagged")
	}
	if !finding(loose, "q", "avg").Regression {
		t.Errorf("with margin 1.0, the same delta clears bare noise and should flag")
	}
}
