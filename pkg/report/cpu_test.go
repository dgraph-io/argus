package report

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/pprof/profile"
)

// writeCPUProfile synthesizes a gzipped CPU profile whose "cpu" samples are the
// given per-function nanosecond counts, and returns its path.
func writeCPUProfile(t *testing.T, dir, name string, sites map[string]int64) string {
	t.Helper()
	p := &profile.Profile{
		SampleType: []*profile.ValueType{
			{Type: "samples", Unit: "count"},
			{Type: "cpu", Unit: "nanoseconds"},
		},
		PeriodType: &profile.ValueType{Type: "cpu", Unit: "nanoseconds"},
		Period:     1,
	}
	var id uint64
	for fn, ns := range sites {
		id++
		f := &profile.Function{ID: id, Name: fn}
		loc := &profile.Location{ID: id, Line: []profile.Line{{Function: f}}}
		p.Function = append(p.Function, f)
		p.Location = append(p.Location, loc)
		p.Sample = append(p.Sample, &profile.Sample{
			Location: []*profile.Location{loc},
			Value:    []int64{1, ns}, // samples, cpu
		})
	}
	path := filepath.Join(dir, name)
	out, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer out.Close()
	if err := p.Write(out); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestDiffCPU(t *testing.T) {
	dir := t.TempDir()
	base := writeCPUProfile(t, dir, "cpu-base.pb.gz", map[string]int64{"a": 600, "b": 400})
	cand := writeCPUProfile(t, dir, "cpu-cand.pb.gz", map[string]int64{"a": 300, "b": 700})

	d, err := DiffCPU(base, cand, 0)
	if err != nil {
		t.Fatalf("DiffCPU: %v", err)
	}
	if d.BaseTotal != 1000 || d.CandTotal != 1000 {
		t.Fatalf("totals = %d/%d, want 1000/1000", d.BaseTotal, d.CandTotal)
	}
	// Shares: base a=60 b=40, cand a=30 b=70. |Δ|=30 for both; tie-break by name.
	if d.Sites[0].Func != "a" || d.Sites[1].Func != "b" {
		t.Fatalf("order = [%s %s], want [a b]", d.Sites[0].Func, d.Sites[1].Func)
	}
	approx(t, "a BasePct", d.Sites[0].BasePct, 60)
	approx(t, "a CandPct", d.Sites[0].CandPct, 30)
	approx(t, "a DeltaPP", d.Sites[0].DeltaPP, -30)
	approx(t, "b DeltaPP", d.Sites[1].DeltaPP, +30)
}

// A profile diff on share should be blind to absolute magnitude: same
// distribution at 10x the samples must produce zero deltas.
func TestDiffCPUNormalizesByShare(t *testing.T) {
	dir := t.TempDir()
	base := writeCPUProfile(t, dir, "b.pb.gz", map[string]int64{"a": 100, "b": 100})
	cand := writeCPUProfile(t, dir, "c.pb.gz", map[string]int64{"a": 1000, "b": 1000})
	d, err := DiffCPU(base, cand, 0)
	if err != nil {
		t.Fatalf("DiffCPU: %v", err)
	}
	for _, s := range d.Sites {
		if math.Abs(s.DeltaPP) > 1e-9 {
			t.Errorf("%s DeltaPP = %v, want 0 (same distribution)", s.Func, s.DeltaPP)
		}
	}
}

func TestDiffCPURejectsEmptyProfile(t *testing.T) {
	dir := t.TempDir()
	base := writeCPUProfile(t, dir, "b.pb.gz", map[string]int64{"a": 100})
	empty := writeCPUProfile(t, dir, "e.pb.gz", map[string]int64{"a": 0})
	if _, err := DiffCPU(base, empty, 0); err == nil {
		t.Fatal("expected error for zero-sample cpu profile, got nil")
	}
}

func approx(t *testing.T, what string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-6 {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}
