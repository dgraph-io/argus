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
