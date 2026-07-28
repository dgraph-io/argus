package report

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/google/pprof/profile"
)

// shareSite is one function's share of a profile's total on each side, with the
// signed candidate-minus-baseline change in percentage points. Used for any
// profile compared by distribution rather than absolute magnitude (CPU time,
// mutex/block contention).
type shareSite struct {
	Func    string
	BasePct float64 // % of baseline's total
	CandPct float64 // % of candidate's total
	DeltaPP float64 // candidate - baseline, percentage points
}

// diffShare sums sampleType by function on both sides, converts each function to
// its share of the side's total, and returns the topN functions by absolute
// change in share. Comparing shares (not absolute values) is what makes two runs
// with different sampling windows or background load comparable.
func diffShare(sampleType, basePath, candPath string, topN int, pick func(*profile.Sample) string) (sites []shareSite, baseTotal, candTotal int64, err error) {
	bMap, bTot, err := sumBy(basePath, sampleType, pick)
	if err != nil {
		return nil, 0, 0, err
	}
	cMap, cTot, err := sumBy(candPath, sampleType, pick)
	if err != nil {
		return nil, 0, 0, err
	}
	if bTot == 0 || cTot == 0 {
		return nil, bTot, cTot, fmt.Errorf("%q profile has zero samples (baseline=%d, candidate=%d) — not captured, or profiling not enabled?", sampleType, bTot, cTot)
	}

	seen := map[string]bool{}
	for fn := range bMap {
		seen[fn] = true
	}
	for fn := range cMap {
		seen[fn] = true
	}
	for fn := range seen {
		bp := float64(bMap[fn]) / float64(bTot) * 100
		cp := float64(cMap[fn]) / float64(cTot) * 100
		sites = append(sites, shareSite{Func: fn, BasePct: bp, CandPct: cp, DeltaPP: cp - bp})
	}
	sort.Slice(sites, func(i, j int) bool {
		ai, aj := math.Abs(sites[i].DeltaPP), math.Abs(sites[j].DeltaPP)
		if ai != aj {
			return ai > aj
		}
		return sites[i].Func < sites[j].Func // stable, deterministic tie-break
	})
	if topN > 0 && len(sites) > topN {
		sites = sites[:topN]
	}
	return sites, bTot, cTot, nil
}

// renderShare draws a share diff: a header line then the functions whose share
// moved most.
func renderShare(header string, sites []shareSite) string {
	var b strings.Builder
	b.WriteString(header)
	b.WriteString("\n  top sites by |Δshare| (candidate − baseline):\n")
	for _, s := range sites {
		if math.Abs(s.DeltaPP) < 0.05 { // rounds to 0.0pp — no signal to show
			continue
		}
		fmt.Fprintf(&b, "    %+6.1fpp   %5.1f%% -> %-5.1f%%  %s\n", s.DeltaPP, s.BasePct, s.CandPct, s.Func)
	}
	return b.String()
}

// CPUDiff compares two CPU profiles by share of sampled CPU. Diagnostic only —
// absolute CPU time isn't comparable across runs, so it never gates the verdict.
type CPUDiff struct {
	BaselinePath  string
	CandidatePath string
	BaseTotal     int64 // sampled CPU nanoseconds
	CandTotal     int64
	Sites         []shareSite // biggest |DeltaPP| first
}

// DiffCPU diffs two CPU profiles' "cpu" samples by share.
func DiffCPU(basePath, candPath string, topN int) (*CPUDiff, error) {
	sites, bTot, cTot, err := diffShare("cpu", basePath, candPath, topN, leafFunc)
	if err != nil {
		return nil, err
	}
	return &CPUDiff{BaselinePath: basePath, CandidatePath: candPath, BaseTotal: bTot, CandTotal: cTot, Sites: sites}, nil
}

// Render draws the CPU section with the sampling windows for context.
func (d *CPUDiff) Render() string {
	return renderShare(fmt.Sprintf("CPU profile (share of samples; %.1fs baseline / %.1fs candidate CPU sampled)",
		float64(d.BaseTotal)/1e9, float64(d.CandTotal)/1e9), d.Sites)
}
