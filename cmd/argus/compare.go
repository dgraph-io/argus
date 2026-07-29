package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dgraph-io/argus/pkg/report"
)

// runCompare implements `argus compare`: diff a candidate run against a baseline
// and exit non-zero if a regression is detected. Runs are selected either from
// the ledger (-baseline-run/-candidate-run) or as explicit summary file lists
// (-baseline/-candidate).
func runCompare(args []string) {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	ledger := fs.String("ledger", defaultLedger, "Results ledger (JSONL) to select runs from")
	baselineRun := fs.String("baseline-run", "", "Baseline run selector: a run_id (exact/prefix) or a k=v[,k=v] filter (version/cpu_class/deploy/...)")
	candidateRun := fs.String("candidate-run", "", "Candidate run selector (paired with -baseline-run)")
	baseline := fs.String("baseline", "", "Comma-separated baseline summary JSON files (alternative to -baseline-run)")
	candidate := fs.String("candidate", "", "Comma-separated candidate summary JSON files (alternative to -candidate-run)")
	latency := fs.Float64("latency", report.DefaultThresholds.Latency, "Latency regression threshold (fraction, e.g. 0.10)")
	throughput := fs.Float64("throughput", report.DefaultThresholds.Throughput, "Throughput regression threshold (fraction)")
	resource := fs.Float64("resource", report.DefaultThresholds.Resource, "Cluster resource (RAM/CPU/heap) regression threshold (fraction)")
	noiseMargin := fs.Float64("noise-margin", report.DefaultThresholds.NoiseMargin, "Worsening must exceed run-to-run noise by this factor to count (dampens jittery p99 tails)")
	loadFactor := fs.Float64("load-factor", report.DefaultThresholds.LoadFactor, "When the whole run is faster, a per-op latency rise up to this multiple of the speedup is load-shift, not a regression (0 disables)")
	minLatencyMs := fs.Float64("min-latency-ms", report.DefaultThresholds.MinLatencyDeltaMs, "A latency regression must move at least this many ms absolute to gate (0 disables)")
	minSamples := fs.Int("min-samples", report.DefaultThresholds.MinSamples, "A latency regression must rest on at least this many completed ops to gate (0 disables)")
	baseHeap := fs.String("baseline-heap", "", "Baseline heap pprof file (default: auto-discover heap-{startup,end}.pb.gz in the run's artifacts dir)")
	candHeap := fs.String("candidate-heap", "", "Candidate heap pprof file (paired with -baseline-heap)")
	heapTop := fs.Int("heap-top", 12, "Number of allocation sites to show per heap phase")
	baseCPU := fs.String("baseline-cpu", "", "Baseline CPU pprof file (default: auto-discover cpu.pb.gz in the run's artifacts dir)")
	candCPU := fs.String("candidate-cpu", "", "Candidate CPU pprof file (paired with -baseline-cpu)")
	cpuTop := fs.Int("cpu-top", 12, "Number of functions to show in the CPU-share diff")
	contTop := fs.Int("contention-top", 12, "Number of functions to show per mutex/block contention diff")
	all := fs.Bool("all", false, "Show all metrics, not just regressions")
	_ = fs.Parse(args)

	var base, cand []*report.RunSummary
	var baseDir, candDir string // where each side's profiles live

	switch {
	case *baselineRun != "" || *candidateRun != "":
		if *baselineRun == "" || *candidateRun == "" {
			fmt.Fprintln(os.Stderr, "-baseline-run and -candidate-run must be given together")
			os.Exit(2)
		}
		runs, err := report.LoadLedger(*ledger)
		if err != nil {
			fmt.Fprintln(os.Stderr, "ledger:", err)
			os.Exit(2)
		}
		if base, err = report.SelectByRun(runs, *baselineRun); err != nil {
			fmt.Fprintln(os.Stderr, "baseline:", err)
			os.Exit(2)
		}
		if cand, err = report.SelectByRun(runs, *candidateRun); err != nil {
			fmt.Fprintln(os.Stderr, "candidate:", err)
			os.Exit(2)
		}
		baseDir = report.ArtifactsDir(*ledger, base[0].RunID)
		candDir = report.ArtifactsDir(*ledger, cand[0].RunID)
	case *baseline != "" && *candidate != "":
		var err error
		if base, err = loadSummaries(*baseline); err != nil {
			fmt.Fprintln(os.Stderr, "baseline:", err)
			os.Exit(2)
		}
		if cand, err = loadSummaries(*candidate); err != nil {
			fmt.Fprintln(os.Stderr, "candidate:", err)
			os.Exit(2)
		}
		baseDir = firstDir(*baseline)
		candDir = firstDir(*candidate)
	default:
		fmt.Fprintln(os.Stderr, "usage: argus compare -baseline-run <sel> -candidate-run <sel>   (or -baseline a.json[,b] -candidate c.json[,d])")
		fmt.Fprintln(os.Stderr, "       [-latency 0.1] [-throughput 0.1] [-resource 0.15] [-noise-margin 1.5] [-all]")
		os.Exit(2)
	}

	rep := report.Compare(base, cand, report.Thresholds{
		Latency: *latency, Throughput: *throughput, Resource: *resource, NoiseMargin: *noiseMargin,
		LoadFactor: *loadFactor, MinLatencyDeltaMs: *minLatencyMs, MinSamples: *minSamples,
	})
	addHeapDiffs(rep, baseDir, candDir, *baseHeap, *candHeap, *resource, *heapTop)
	addCPUDiff(rep, baseDir, candDir, *baseCPU, *candCPU, *cpuTop)
	addContentionDiffs(rep, baseDir, candDir, *contTop)
	fmt.Print(rep.Render(!*all))

	if rep.Regressed {
		os.Exit(1)
	}
}

// addHeapDiffs attaches heap comparisons. If both -baseline-heap and
// -candidate-heap are given, it diffs exactly those; otherwise it looks for
// heap-{startup,end}.pb.gz in each side's artifacts dir and diffs every phase
// present on both. Missing profiles are skipped; a parse error is warned about
// but never aborts the comparison.
func addHeapDiffs(rep *report.Report, baseDir, candDir, baseHeap, candHeap string, resourceTh float64, topN int) {
	if baseHeap != "" && candHeap != "" {
		if err := rep.AddHeap(phaseFromName(candHeap), baseHeap, candHeap, resourceTh, topN); err != nil {
			fmt.Fprintln(os.Stderr, "heap:", err)
		}
		return
	}
	if baseHeap != "" || candHeap != "" {
		fmt.Fprintln(os.Stderr, "heap: -baseline-heap and -candidate-heap must be given together; ignoring")
	}
	for _, phase := range []string{"startup", "end"} {
		bp := profileInDir(baseDir, "heap-"+phase+".pb.gz")
		cp := profileInDir(candDir, "heap-"+phase+".pb.gz")
		if bp == "" || cp == "" {
			continue
		}
		if err := rep.AddHeap(phase, bp, cp, resourceTh, topN); err != nil {
			fmt.Fprintln(os.Stderr, "heap:", err)
		}
	}
}

// addCPUDiff attaches a CPU-share comparison. Explicit paths override discovery;
// otherwise it looks for cpu.pb.gz in each side's artifacts dir. Diagnostic only.
func addCPUDiff(rep *report.Report, baseDir, candDir, baseCPU, candCPU string, topN int) {
	if baseCPU == "" && candCPU == "" {
		baseCPU = profileInDir(baseDir, "cpu.pb.gz")
		candCPU = profileInDir(candDir, "cpu.pb.gz")
	} else if baseCPU == "" || candCPU == "" {
		fmt.Fprintln(os.Stderr, "cpu: -baseline-cpu and -candidate-cpu must be given together; ignoring")
		return
	}
	if baseCPU == "" || candCPU == "" {
		return
	}
	if err := rep.AddCPU(baseCPU, candCPU, topN); err != nil {
		fmt.Fprintln(os.Stderr, "cpu:", err)
	}
}

// addContentionDiffs attaches mutex and block contention comparisons discovered
// as {mutex,block}.pb.gz in each side's artifacts dir. Diagnostic only.
func addContentionDiffs(rep *report.Report, baseDir, candDir string, topN int) {
	for _, kind := range []string{"mutex", "block"} {
		bp := profileInDir(baseDir, kind+".pb.gz")
		cp := profileInDir(candDir, kind+".pb.gz")
		if bp == "" || cp == "" {
			continue
		}
		if err := rep.AddContention(kind, bp, cp, topN); err != nil {
			fmt.Fprintln(os.Stderr, "contention:", err)
		}
	}
}

// profileInDir returns dir/name if it exists, else "".
func profileInDir(dir, name string) string {
	if dir == "" {
		return ""
	}
	p := filepath.Join(dir, name)
	if _, err := os.Stat(p); err != nil {
		return ""
	}
	return p
}

// firstDir is the directory of the first path in a comma-separated file list.
func firstDir(csv string) string {
	first := strings.TrimSpace(strings.Split(csv, ",")[0])
	if first == "" {
		return ""
	}
	return filepath.Dir(first)
}

// phaseFromName derives a phase label from a heap filename
// ("heap-startup.pb.gz" -> "startup"), falling back to "custom".
func phaseFromName(path string) string {
	b := filepath.Base(path)
	b = strings.TrimSuffix(b, ".gz")
	b = strings.TrimSuffix(b, ".pb")
	b = strings.TrimPrefix(b, "heap-")
	if b == "" {
		return "custom"
	}
	return b
}

func loadSummaries(csv string) ([]*report.RunSummary, error) {
	var out []*report.RunSummary
	for _, p := range strings.Split(csv, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		s, err := report.Load(p)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no summary files given")
	}
	return out, nil
}
