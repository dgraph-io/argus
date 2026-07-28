package report

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/pprof/profile"
)

// writeContentionProfile synthesizes a gzipped mutex/block-shaped profile whose
// "delay" samples are the given per-function nanosecond counts.
func writeContentionProfile(t *testing.T, dir, name string, sites map[string]int64) string {
	t.Helper()
	p := &profile.Profile{
		SampleType: []*profile.ValueType{
			{Type: "contentions", Unit: "count"},
			{Type: "delay", Unit: "nanoseconds"},
		},
		PeriodType: &profile.ValueType{Type: "contentions", Unit: "count"},
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
			Value:    []int64{1, ns}, // contentions, delay
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

func TestDiffContention(t *testing.T) {
	dir := t.TempDir()
	// Candidate spends a much larger share of blocked time in one lock.
	base := writeContentionProfile(t, dir, "mutex-base.pb.gz", map[string]int64{
		"posting.(*List).Lock": 10, "badger.(*DB).flush": 80, "raft.(*node).run": 10,
	})
	cand := writeContentionProfile(t, dir, "mutex-cand.pb.gz", map[string]int64{
		"posting.(*List).Lock": 70, "badger.(*DB).flush": 25, "raft.(*node).run": 5,
	})

	d, err := DiffContention("mutex", base, cand, 0)
	if err != nil {
		t.Fatalf("DiffContention: %v", err)
	}
	if d.Kind != "mutex" {
		t.Errorf("Kind = %q, want mutex", d.Kind)
	}
	if d.BaseTotal != 100 || d.CandTotal != 100 {
		t.Fatalf("totals = %d/%d, want 100/100", d.BaseTotal, d.CandTotal)
	}
	// Shares: base Lock=10 flush=80 raft=10; cand Lock=70 flush=25 raft=5.
	// Lock moves +60pp — the unambiguous top mover.
	if d.Sites[0].Func != "posting.(*List).Lock" {
		t.Fatalf("top mover = %q, want posting.(*List).Lock", d.Sites[0].Func)
	}
	approx(t, "Lock DeltaPP", d.Sites[0].DeltaPP, +60)
	if out := d.Render(); !strings.Contains(out, "mutex contention") {
		t.Errorf("Render() missing kind label:\n%s", out)
	}
}

// writeStackContentionProfile builds a profile where each sample carries a full
// stack (leaf-first) rather than a single frame, so attribution past the sync
// primitive can be exercised.
func writeStackContentionProfile(t *testing.T, dir, name string, samples map[int64][]string) string {
	t.Helper()
	p := &profile.Profile{
		SampleType: []*profile.ValueType{
			{Type: "contentions", Unit: "count"},
			{Type: "delay", Unit: "nanoseconds"},
		},
		PeriodType: &profile.ValueType{Type: "contentions", Unit: "count"},
		Period:     1,
	}
	var id uint64
	for ns, stack := range samples {
		locs := make([]*profile.Location, 0, len(stack))
		for _, fn := range stack { // stack[0] = leaf
			id++
			f := &profile.Function{ID: id, Name: fn}
			l := &profile.Location{ID: id, Line: []profile.Line{{Function: f}}}
			p.Function = append(p.Function, f)
			p.Location = append(p.Location, l)
			locs = append(locs, l)
		}
		p.Sample = append(p.Sample, &profile.Sample{Location: locs, Value: []int64{1, ns}})
	}
	path := filepath.Join(dir, name)
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if err := p.Write(out); err != nil {
		t.Fatal(err)
	}
	return path
}

// Contention must be attributed to the nearest non-sync/non-runtime caller (the
// lock holder), not the sync primitive that is the leaf of every mutex sample.
func TestDiffContentionAttributesToCaller(t *testing.T) {
	dir := t.TempDir()
	// Both samples share the same leaf primitive; only the holder differs.
	base := writeStackContentionProfile(t, dir, "b.pb.gz", map[int64][]string{
		30: {"sync.(*RWMutex).Unlock", "github.com/dgraph-io/dgraph/v25/posting.(*List).Iterate"},
		70: {"sync.(*Mutex).Unlock", "github.com/dgraph-io/badger/v4.(*DB).flush"},
	})
	cand := writeStackContentionProfile(t, dir, "c.pb.gz", map[int64][]string{
		80: {"sync.(*RWMutex).Unlock", "github.com/dgraph-io/dgraph/v25/posting.(*List).Iterate"},
		20: {"sync.(*Mutex).Unlock", "github.com/dgraph-io/badger/v4.(*DB).flush"},
	})
	d, err := DiffContention("mutex", base, cand, 0)
	if err != nil {
		t.Fatalf("DiffContention: %v", err)
	}
	// Attribution must skip sync.* and land on the Dgraph/badger holders.
	got := map[string]float64{}
	for _, s := range d.Sites {
		got[s.Func] = s.DeltaPP
	}
	if _, ok := got["github.com/dgraph-io/dgraph/v25/posting.(*List).Iterate"]; !ok {
		t.Fatalf("expected attribution to posting.(*List).Iterate, got sites: %v", got)
	}
	for fn := range got {
		if strings.HasPrefix(fn, "sync.") {
			t.Errorf("attribution leaked the sync primitive: %s", fn)
		}
	}
	// posting.Iterate share rose 30%%->80%% = +50pp.
	approx(t, "Iterate DeltaPP", got["github.com/dgraph-io/dgraph/v25/posting.(*List).Iterate"], +50)
}

func TestDiffContentionEmptyProfileErrors(t *testing.T) {
	dir := t.TempDir()
	base := writeContentionProfile(t, dir, "b.pb.gz", map[string]int64{"a": 10})
	empty := writeContentionProfile(t, dir, "e.pb.gz", map[string]int64{"a": 0})
	_, err := DiffContention("block", base, empty, 0)
	if err == nil {
		t.Fatal("expected error for zero-sample profile")
	}
	if !strings.Contains(err.Error(), "block") {
		t.Errorf("error should name the kind, got: %v", err)
	}
}
