package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/dgraph-io/argus/pkg/envinfo"
	"github.com/dgraph-io/argus/pkg/report"
)

// defaultLedger is the consolidated results store (one JSONL line per repeat).
const defaultLedger = "results/runs.jsonl"

// runGroup is a run's repeat lines collapsed to one catalog row.
type runGroup struct {
	runID   string
	started string
	first   *report.RunSummary // env/params are identical across repeats
	reps    int
	measS   float64 // means across repeats
	qps     float64
	rssMB   float64
	jemMB   float64
	cpuPct  float64
}

// groupByRun collapses ledger lines into per-run catalog rows, preserving first
// -appearance order and averaging the per-repeat numeric fields.
func groupByRun(runs []*report.RunSummary) []*runGroup {
	byID := map[string]*runGroup{}
	var order []string
	for _, s := range runs {
		g := byID[s.RunID]
		if g == nil {
			g = &runGroup{runID: s.RunID, started: s.StartedAt, first: s}
			byID[s.RunID] = g
			order = append(order, s.RunID)
		}
		g.reps++
		g.measS += s.MeasuredSeconds
		g.qps += s.Cluster.QueriesPerSec
		g.rssMB += s.Cluster.RSSMBTotal
		g.jemMB += s.Cluster.JemallocMBTotal
		g.cpuPct += s.Cluster.CPUPercentTotal
		if s.StartedAt < g.started {
			g.started = s.StartedAt
		}
	}
	out := make([]*runGroup, 0, len(order))
	for _, id := range order {
		g := byID[id]
		n := float64(g.reps)
		g.measS, g.qps, g.rssMB, g.jemMB, g.cpuPct = g.measS/n, g.qps/n, g.rssMB/n, g.jemMB/n, g.cpuPct/n
		out = append(out, g)
	}
	return out
}

// runLs implements `argus ls [filter]`: list the runs in the ledger as a table
// (default) or CSV catalog. filter is an optional run_id or k=v[,k=v] selector.
func runLs(args []string) {
	fs := flag.NewFlagSet("ls", flag.ExitOnError)
	ledger := fs.String("ledger", defaultLedger, "Results ledger (JSONL) to list")
	format := fs.String("format", "table", "Output format: table | csv")
	_ = fs.Parse(args)
	sel := strings.Join(fs.Args(), ",")

	runs, err := report.LoadLedger(*ledger)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ledger:", err)
		os.Exit(2)
	}
	matched, err := report.MatchRuns(runs, sel)
	if err != nil {
		fmt.Fprintln(os.Stderr, "filter:", err)
		os.Exit(2)
	}
	groups := groupByRun(matched)

	if *format == "csv" {
		lsCSV(groups)
		return
	}
	lsTable(groups)
}

func lsTable(groups []*runGroup) {
	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "RUN_ID\tVERSION\tBRANCH\tCOMMIT\tCPU_CLASS\tCORES\tRAM\tDISK\tDEPLOY\tTOPO\tSCALE\tWC\tREPS\tMEAS_S\tQPS\tRSS_MB\tJEM_MB")
	for _, g := range groups {
		e := g.first.Env
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\t%.0f\t%s\t%s\t%s\t%s\t%d\t%d\t%.0f\t%.0f\t%.0f\t%.0f\n",
			g.runID, e.DgraphVersion, dash(e.DgraphBranch), dash(e.DgraphCommit), dash(e.CPUClass), e.NumCPU, e.TotalRAMGB, dash(e.DiskClass),
			dash(e.Deploy), topoCell(e), fmtScale(g.first.Scale), g.first.WriteConcurrency,
			g.reps, g.measS, g.qps, g.rssMB, g.jemMB)
	}
	w.Flush()
	if len(groups) == 0 {
		fmt.Fprintln(os.Stderr, "(no runs)")
	}
}

func lsCSV(groups []*runGroup) {
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()
	w.Write([]string{
		"run_id", "started", "version", "branch", "commit", "cpu_class", "cores", "ram_gb", "disk_class",
		"deploy", "topology", "groups", "replicas", "learners", "scale", "write_concurrency", "read_concurrency",
		"repeats", "measured_s", "qps", "rss_mb", "jemalloc_mb", "cpu_pct",
	})
	for _, g := range groups {
		e := g.first.Env
		w.Write([]string{
			g.runID, g.started, e.DgraphVersion, e.DgraphBranch, e.DgraphCommit, e.CPUClass, itoa(e.NumCPU),
			ftoa(e.TotalRAMGB, 1), e.DiskClass, e.Deploy, e.Topology(), itoa(e.Groups), itoa(e.Replicas), itoa(e.NumLearners),
			fmtScale(g.first.Scale), itoa(g.first.WriteConcurrency), itoa(g.first.ReadConcurrency),
			itoa(g.reps), ftoa(g.measS, 1), ftoa(g.qps, 1), ftoa(g.rssMB, 0), ftoa(g.jemMB, 0), ftoa(g.cpuPct, 1),
		})
	}
}

// runImport backfills existing committed summary-*.json files into the ledger:
// all summaries in one directory become one run (they are its repeats), the
// run_id is derived from that directory, and env.Labels is parsed best-effort for
// deploy/scale/cpu_class when those structured fields are empty.
func runImport(args []string) {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	ledger := fs.String("ledger", defaultLedger, "Results ledger (JSONL) to append to")
	dry := fs.Bool("dry-run", false, "Print what would be imported without writing")
	_ = fs.Parse(args)
	if fs.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: argus import [-ledger runs.jsonl] <summary-glob>...")
		os.Exit(2)
	}

	// Collect files, grouped by directory (a directory == one run).
	byDir := map[string][]string{}
	var dirOrder []string
	for _, pat := range fs.Args() {
		matches, err := filepath.Glob(pat)
		if err != nil {
			fmt.Fprintln(os.Stderr, "glob:", err)
			os.Exit(2)
		}
		for _, m := range matches {
			d := filepath.Dir(m)
			if _, seen := byDir[d]; !seen {
				dirOrder = append(dirOrder, d)
			}
			byDir[d] = append(byDir[d], m)
		}
	}

	total := 0
	for _, dir := range dirOrder {
		files := byDir[dir]
		sort.Strings(files)
		runID := runIDFromDir(dir)
		for _, f := range files {
			s, err := report.Load(f)
			if err != nil {
				fmt.Fprintln(os.Stderr, "load", f+":", err)
				os.Exit(1)
			}
			if s.RunID == "" {
				s.RunID = runID
			}
			backfillFromLabels(s)
			if *dry {
				fmt.Printf("would import %s -> run_id=%s\n", f, s.RunID)
				continue
			}
			if err := report.AppendRun(*ledger, s); err != nil {
				fmt.Fprintln(os.Stderr, "append:", err)
				os.Exit(1)
			}
			total++
		}
	}
	if *dry {
		fmt.Printf("(dry run) %d dirs\n", len(dirOrder))
	} else {
		fmt.Printf("imported %d summaries from %d runs into %s\n", total, len(dirOrder), *ledger)
	}
}

// runIDFromDir turns a results directory into a stable, readable run id, e.g.
// "results/v25.3.7/M4-64GB-12core-1z-1a-docker" -> "v25.3.7-M4-64GB-12core-1z-1a-docker".
func runIDFromDir(dir string) string {
	d := filepath.ToSlash(filepath.Clean(dir))
	d = strings.TrimPrefix(d, "results/")
	return sanitizeToken(strings.ReplaceAll(d, "/", "-"))
}

// backfillFromLabels fills empty structured fields from the legacy k=v -label
// convention (machine=,deploy=,scale=,...) so old runs carry usable columns.
func backfillFromLabels(s *report.RunSummary) {
	for _, part := range strings.Split(s.Env.Labels, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch k {
		case "deploy":
			if s.Env.Deploy == "" {
				s.Env.Deploy = v
			}
		case "machine", "cpu":
			if s.Env.CPUClass == "" {
				s.Env.CPUClass = v
			}
		case "disk":
			if s.Env.DiskClass == "" {
				s.Env.DiskClass = v
			}
		case "scale":
			if s.Scale == 0 {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					s.Scale = f
				}
			}
		}
	}
}

// topoCell is the table's TOPO value: the shard/HA class, with a compact learner
// suffix (e.g. "sharded+HA+2L") when read-replicas are present.
func topoCell(e envinfo.Info) string {
	c := e.TopoClass()
	if e.NumLearners > 0 {
		c += fmt.Sprintf("+%dL", e.NumLearners)
	}
	return c
}

func fmtScale(f float64) string {
	if f == 0 {
		return "-"
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func itoa(i int) string { return strconv.Itoa(i) }

func ftoa(f float64, prec int) string { return strconv.FormatFloat(f, 'f', prec, 64) }
