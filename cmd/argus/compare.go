package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/dgraph-io/argus/pkg/report"
)

// runCompare implements `argus compare`: diff candidate run summaries against
// baseline summaries and exit non-zero if a regression is detected.
func runCompare(args []string) {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	baseline := fs.String("baseline", "", "Comma-separated baseline summary JSON files (released version)")
	candidate := fs.String("candidate", "", "Comma-separated candidate summary JSON files (version under test)")
	latency := fs.Float64("latency", report.DefaultThresholds.Latency, "Latency regression threshold (fraction, e.g. 0.10)")
	throughput := fs.Float64("throughput", report.DefaultThresholds.Throughput, "Throughput regression threshold (fraction)")
	resource := fs.Float64("resource", report.DefaultThresholds.Resource, "Cluster resource (RAM/CPU) regression threshold (fraction)")
	all := fs.Bool("all", false, "Show all metrics, not just regressions")
	_ = fs.Parse(args)

	if *baseline == "" || *candidate == "" {
		fmt.Fprintln(os.Stderr, "usage: argus compare -baseline a.json[,b.json] -candidate c.json[,d.json] [-latency 0.1] [-throughput 0.1] [-resource 0.15] [-all]")
		os.Exit(2)
	}

	base, err := loadSummaries(*baseline)
	if err != nil {
		fmt.Fprintln(os.Stderr, "baseline:", err)
		os.Exit(2)
	}
	cand, err := loadSummaries(*candidate)
	if err != nil {
		fmt.Fprintln(os.Stderr, "candidate:", err)
		os.Exit(2)
	}

	rep := report.Compare(base, cand, report.Thresholds{
		Latency: *latency, Throughput: *throughput, Resource: *resource,
	})
	fmt.Print(rep.Render(!*all))

	if rep.Regressed {
		os.Exit(1)
	}
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
