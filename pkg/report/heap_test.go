package report

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/pprof/profile"
)

const mbUnit = int64(1) << 20

// writeHeapProfile synthesizes a gzipped heap profile whose inuse_space samples
// are the given per-function byte counts, and returns its path. An extra
// inuse_objects sample type sits before inuse_space so the test also exercises
// picking the right value index.
func writeHeapProfile(t *testing.T, dir, name string, sites map[string]int64) string {
	t.Helper()
	p := &profile.Profile{
		SampleType: []*profile.ValueType{
			{Type: "inuse_objects", Unit: "count"},
			{Type: "inuse_space", Unit: "bytes"},
		},
		PeriodType: &profile.ValueType{Type: "space", Unit: "bytes"},
		Period:     1,
	}
	var id uint64
	for fn, bytes := range sites {
		id++
		f := &profile.Function{ID: id, Name: fn}
		loc := &profile.Location{ID: id, Line: []profile.Line{{Function: f}}}
		p.Function = append(p.Function, f)
		p.Location = append(p.Location, loc)
		p.Sample = append(p.Sample, &profile.Sample{
			Location: []*profile.Location{loc},
			Value:    []int64{1, bytes}, // inuse_objects, inuse_space
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

func TestDiffHeap(t *testing.T) {
	dir := t.TempDir()
	base := writeHeapProfile(t, dir, "heap-startup-base.pb.gz", map[string]int64{
		"a":      100 * mbUnit,
		"b":      50 * mbUnit,
		"shared": 30 * mbUnit,
	})
	cand := writeHeapProfile(t, dir, "heap-startup-cand.pb.gz", map[string]int64{
		"a":      700 * mbUnit,
		"c":      20 * mbUnit,
		"shared": 30 * mbUnit,
	})

	d, err := DiffHeap("startup", base, cand, 3)
	if err != nil {
		t.Fatalf("DiffHeap: %v", err)
	}

	if got, want := d.BaselineTotal, 180*mbUnit; got != want {
		t.Errorf("BaselineTotal = %d, want %d", got, want)
	}
	if got, want := d.CandidateTotal, 750*mbUnit; got != want {
		t.Errorf("CandidateTotal = %d, want %d", got, want)
	}

	// topN=3 trims the zero-delta "shared" site; the rest rank by |delta|.
	want := []HeapSite{
		{Func: "a", Baseline: 100 * mbUnit, Candidate: 700 * mbUnit, Delta: 600 * mbUnit},
		{Func: "b", Baseline: 50 * mbUnit, Candidate: 0, Delta: -50 * mbUnit},
		{Func: "c", Baseline: 0, Candidate: 20 * mbUnit, Delta: 20 * mbUnit},
	}
	if len(d.Sites) != len(want) {
		t.Fatalf("got %d sites, want %d: %+v", len(d.Sites), len(want), d.Sites)
	}
	for i, w := range want {
		if d.Sites[i] != w {
			t.Errorf("site[%d] = %+v, want %+v", i, d.Sites[i], w)
		}
	}
}

func TestDiffHeapTieBreakDeterministic(t *testing.T) {
	dir := t.TempDir()
	// Two sites with equal |delta| must order by function name, not map order.
	base := writeHeapProfile(t, dir, "b.pb.gz", map[string]int64{"zebra": 0, "alpha": 0})
	cand := writeHeapProfile(t, dir, "c.pb.gz", map[string]int64{"zebra": 10 * mbUnit, "alpha": 10 * mbUnit})
	d, err := DiffHeap("startup", base, cand, 0) // 0 = no trim
	if err != nil {
		t.Fatalf("DiffHeap: %v", err)
	}
	if d.Sites[0].Func != "alpha" || d.Sites[1].Func != "zebra" {
		t.Errorf("tie-break order = [%s %s], want [alpha zebra]", d.Sites[0].Func, d.Sites[1].Func)
	}
}

func TestDiffHeapRejectsNonHeapProfile(t *testing.T) {
	dir := t.TempDir()
	p := &profile.Profile{
		SampleType: []*profile.ValueType{{Type: "cpu", Unit: "nanoseconds"}},
		PeriodType: &profile.ValueType{Type: "cpu", Unit: "nanoseconds"},
		Period:     1,
	}
	path := filepath.Join(dir, "cpu.pb.gz")
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Write(out); err != nil {
		t.Fatal(err)
	}
	out.Close()

	if _, err := DiffHeap("startup", path, path, 5); err == nil {
		t.Fatal("expected error for profile without inuse_space, got nil")
	}
}

func TestAddHeapGatesOnTotal(t *testing.T) {
	dir := t.TempDir()
	base := writeHeapProfile(t, dir, "base.pb.gz", map[string]int64{"cache": 200 * mbUnit})
	big := writeHeapProfile(t, dir, "big.pb.gz", map[string]int64{"cache": 2000 * mbUnit})
	small := writeHeapProfile(t, dir, "small.pb.gz", map[string]int64{"cache": 210 * mbUnit})

	// A 10x jump trips the 15% resource gate.
	r := &Report{}
	if err := r.AddHeap("startup", base, big, 0.15, 5); err != nil {
		t.Fatalf("AddHeap: %v", err)
	}
	if !r.Regressed {
		t.Error("expected Regressed=true for 10x heap growth")
	}
	if len(r.HeapDiffs) != 1 {
		t.Fatalf("expected 1 HeapDiff, got %d", len(r.HeapDiffs))
	}

	// A 5% jump stays under the gate.
	r2 := &Report{}
	if err := r2.AddHeap("startup", base, small, 0.15, 5); err != nil {
		t.Fatalf("AddHeap: %v", err)
	}
	if r2.Regressed {
		t.Error("expected Regressed=false for 5% heap growth")
	}
}
