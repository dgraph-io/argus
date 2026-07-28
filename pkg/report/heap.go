package report

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/google/pprof/profile"
)

// HeapSite is one function's live-heap (inuse_space) footprint on each side, with
// the signed candidate-minus-baseline delta.
type HeapSite struct {
	Func      string
	Baseline  int64 // bytes
	Candidate int64 // bytes
	Delta     int64 // bytes, candidate - baseline
}

// HeapDiff compares two heap profiles for one capture phase (e.g. "startup").
// It explains a resident-memory regression by naming the allocation sites that
// moved, and carries the totals that gate the verdict.
type HeapDiff struct {
	Phase          string
	BaselinePath   string
	CandidatePath  string
	BaselineTotal  int64      // bytes, sum of inuse_space
	CandidateTotal int64      // bytes
	Sites          []HeapSite // biggest |delta| first
}

// sumBy parses a pprof profile and sums the given sample type's values by
// function, where pick chooses which stack frame each sample is attributed to.
// leafFunc gives the flat attribution `go tool pprof -top` shows; contention
// profiles use a caller-aware picker instead (see attributedFunc).
func sumBy(path, sampleType string, pick func(*profile.Sample) string) (map[string]int64, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	p, err := profile.Parse(f) // transparently handles gzip
	if err != nil {
		return nil, 0, fmt.Errorf("%s: %w", path, err)
	}

	idx := -1
	for i, st := range p.SampleType {
		if st.Type == sampleType {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, 0, fmt.Errorf("%s: no %q sample type (wrong profile kind?)", path, sampleType)
	}

	byFn := map[string]int64{}
	var total int64
	for _, s := range p.Sample {
		v := s.Value[idx]
		if v == 0 {
			continue
		}
		byFn[pick(s)] += v
		total += v
	}
	return byFn, total, nil
}

// sumByFunc sums by leaf function — flat attribution, used for heap
// ("inuse_space") and CPU ("cpu").
func sumByFunc(path, sampleType string) (map[string]int64, int64, error) {
	return sumBy(path, sampleType, leafFunc)
}

// leafFunc names the innermost frame of a sample, or "(unknown)" if the profile
// carries no symbols there.
func leafFunc(s *profile.Sample) string {
	if len(s.Location) == 0 {
		return "(unknown)"
	}
	// Location[0] is the leaf; Line[0] is the innermost inlined frame at it.
	if lines := s.Location[0].Line; len(lines) > 0 && lines[0].Function != nil {
		return lines[0].Function.Name
	}
	return "(unknown)"
}

// DiffHeap diffs two heap profiles' inuse_space and returns the topN sites by
// absolute delta.
func DiffHeap(phase, basePath, candPath string, topN int) (*HeapDiff, error) {
	bMap, bTot, err := sumByFunc(basePath, "inuse_space")
	if err != nil {
		return nil, err
	}
	cMap, cTot, err := sumByFunc(candPath, "inuse_space")
	if err != nil {
		return nil, err
	}

	seen := map[string]bool{}
	var sites []HeapSite
	for fn := range bMap {
		seen[fn] = true
	}
	for fn := range cMap {
		seen[fn] = true
	}
	for fn := range seen {
		b, c := bMap[fn], cMap[fn]
		sites = append(sites, HeapSite{Func: fn, Baseline: b, Candidate: c, Delta: c - b})
	}
	sort.Slice(sites, func(i, j int) bool {
		ai, aj := abs64(sites[i].Delta), abs64(sites[j].Delta)
		if ai != aj {
			return ai > aj
		}
		return sites[i].Func < sites[j].Func // stable, deterministic tie-break
	})
	if topN > 0 && len(sites) > topN {
		sites = sites[:topN]
	}

	return &HeapDiff{
		Phase: phase, BaselinePath: basePath, CandidatePath: candPath,
		BaselineTotal: bTot, CandidateTotal: cTot, Sites: sites,
	}, nil
}

// Render draws the heap section: totals then the sites that moved most.
func (d *HeapDiff) Render() string {
	var b strings.Builder
	deltaTot := d.CandidateTotal - d.BaselineTotal
	var pct float64
	if d.BaselineTotal != 0 {
		pct = float64(deltaTot) / float64(d.BaselineTotal) * 100
	}
	fmt.Fprintf(&b, "HEAP inuse_space — %s\n", d.Phase)
	fmt.Fprintf(&b, "  baseline  %10s\n", mb(d.BaselineTotal))
	fmt.Fprintf(&b, "  candidate %10s   (%s, %+.1f%%)\n", mb(d.CandidateTotal), mbSigned(deltaTot), pct)
	fmt.Fprintf(&b, "  top sites by |delta| (candidate − baseline):\n")
	for _, s := range d.Sites {
		if s.Delta == 0 {
			continue
		}
		fmt.Fprintf(&b, "    %10s   %9s -> %-9s  %s\n", mbSigned(s.Delta), mb(s.Baseline), mb(s.Candidate), s.Func)
	}
	return b.String()
}

func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// mb formats a byte count as unsigned "N MB"; mbSigned prefixes an explicit sign
// so deltas read as "+N MB" / "-N MB".
func mb(bytes int64) string {
	return fmt.Sprintf("%.0f MB", math.Round(float64(bytes)/(1<<20)))
}

func mbSigned(bytes int64) string {
	sign := "+"
	if bytes < 0 {
		sign = "-"
	}
	return fmt.Sprintf("%s%.0f MB", sign, math.Round(math.Abs(float64(bytes))/(1<<20)))
}
