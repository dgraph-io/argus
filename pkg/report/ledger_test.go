package report

import (
	"path/filepath"
	"testing"

	"github.com/dgraph-io/argus/pkg/envinfo"
	"github.com/dgraph-io/argus/pkg/metrics"
)

func summary(runID, version string, p99 float64) *RunSummary {
	return &RunSummary{
		RunID: runID,
		Env:   envinfo.Info{DgraphVersion: version, CPUClass: "Apple M4", Deploy: "docker-compose"},
		Ops:   []metrics.OpSummary{{Key: "q", ThroughputPerSec: 1, P99Ms: p99, AvgMs: p99 / 2}},
	}
}

func TestLedgerRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.jsonl")
	want := []*RunSummary{
		summary("run-a", "v25.3.7", 100),
		summary("run-a", "v25.3.7", 110), // repeat 2 of run-a
		summary("run-b", "v25.3.8-preview1", 120),
	}
	for _, s := range want {
		if err := AppendRun(path, s); err != nil {
			t.Fatalf("AppendRun: %v", err)
		}
	}
	got, err := LoadLedger(path)
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("loaded %d lines, want 3", len(got))
	}
	if got[0].RunID != "run-a" || got[0].Ops[0].P99Ms != 100 {
		t.Errorf("line 0 = %+v, want run-a p99=100", got[0])
	}
	if got[2].Env.DgraphVersion != "v25.3.8-preview1" {
		t.Errorf("line 2 version = %q", got[2].Env.DgraphVersion)
	}
}

func TestSelectByRun(t *testing.T) {
	runs := []*RunSummary{
		summary("20260708-run-a", "v25.3.7", 100),
		summary("20260708-run-a", "v25.3.7", 110),
		summary("20260709-run-b", "v25.3.8-preview1", 120),
	}

	// Exact run_id -> both repeat lines.
	got, err := SelectByRun(runs, "20260708-run-a")
	if err != nil || len(got) != 2 {
		t.Fatalf("exact select: got %d, err %v", len(got), err)
	}

	// Unique prefix.
	if got, err = SelectByRun(runs, "20260709"); err != nil || len(got) != 1 {
		t.Fatalf("prefix select: got %d, err %v", len(got), err)
	}

	// Filter resolving to one run.
	if got, err = SelectByRun(runs, "version=v25.3.8"); err != nil || len(got) != 1 {
		t.Fatalf("filter select: got %d, err %v", len(got), err)
	}

	// No match errors.
	if _, err = SelectByRun(runs, "nope"); err == nil {
		t.Error("expected error for no match")
	}

	// Ambiguous (both runs are v-something on the same box) errors and names them.
	if _, err = SelectByRun(runs, "cpu=Apple M4"); err == nil {
		t.Error("expected ambiguity error when a filter spans two runs")
	}
}

func TestMatchRunsEmptyIsAll(t *testing.T) {
	runs := []*RunSummary{summary("a", "v1", 1), summary("b", "v2", 2)}
	got, err := MatchRuns(runs, "")
	if err != nil || len(got) != 2 {
		t.Fatalf("empty selector should match all: got %d, err %v", len(got), err)
	}
}

func TestArtifactsDir(t *testing.T) {
	got := ArtifactsDir("results/runs.jsonl", "20260709-v25.3.8-abcd")
	want := filepath.FromSlash("results/artifacts/20260709-v25.3.8-abcd")
	if got != want {
		t.Errorf("ArtifactsDir = %q, want %q", got, want)
	}
}
