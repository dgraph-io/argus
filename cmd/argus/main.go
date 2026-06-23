package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/dgraph-io/argus/pkg/cluster"
	"github.com/dgraph-io/argus/pkg/clusterstats"
	"github.com/dgraph-io/argus/pkg/envinfo"
	"github.com/dgraph-io/argus/pkg/graphql"
	"github.com/dgraph-io/argus/pkg/harness"
	"github.com/dgraph-io/argus/pkg/output"
	"github.com/dgraph-io/argus/pkg/report"
	"github.com/dgraph-io/argus/pkg/testdata"
)

func main() {
	// Subcommand: argus compare ...
	if len(os.Args) > 1 && os.Args[1] == "compare" {
		runCompare(os.Args[2:])
		return
	}

	grpcAddr := flag.String("cluster", "localhost:9080", "Dgraph cluster gRPC address (DQL); seed for discovery")
	httpAddr := flag.String("graphql", "http://localhost:8080", "Dgraph alpha HTTP base URL (GraphQL); seed for discovery")
	alphas := flag.String("alphas", "", "Comma-separated alpha HTTP base URLs to load-balance over and scrape (default: discover from /state)")
	opsCSV := flag.String("ops-output", "argus-ops.csv", "CSV path for per-op throughput/latency snapshots")
	rtCSV := flag.String("runtime-output", "argus-runtime.csv", "CSV path for load-generator Go runtime/GC snapshots")
	clusterCSV := flag.String("cluster-output", "argus-cluster.csv", "CSV path for per-Dgraph-node resource/throughput snapshots")
	summaryOut := flag.String("summary-output", "argus-summary.json", "JSON path for the per-run summary used by `argus compare`")
	passes := flag.Int("passes", 10, "Fixed-work mode: run exactly N write passes (primary; set 0 to use -duration instead)")
	duration := flag.Duration("duration", 0, "Duration mode: run for this long (used only when -passes=0)")
	repeats := flag.Int("repeats", 1, "Repeat the whole measurement N times, emitting one summary per repeat (for a noise floor)")
	flushInterval := flag.Duration("flush-interval", 5*time.Second, "Snapshot flush cadence")
	writeConc := flag.Int("write-concurrency", 8, "Concurrent write workers")
	readConc := flag.Int("read-concurrency", 8, "Concurrent read workers")
	seed := flag.Int64("seed", 1, "Dataset seed (reserved; dataset is index-deterministic)")
	small := flag.Bool("small", false, "Use the small dataset for smoke tests")
	strict := flag.Bool("strict", false, "Fail fast on the first error (default: log and continue)")
	vectorStrict := flag.Bool("vector-strict", false, "Assert exact similar_to nearest-neighbor recall (regression gate; expected to fail on builds with the HNSW upsert issue)")
	dgraphBranch := flag.String("dgraph-branch", "", "Dgraph build branch under test, recorded in the report (from `dgraph version`; the cluster API exposes only the commit hash)")
	label := flag.String("label", "", "Free-form run annotation recorded in the report (e.g. machine=e2-standard-4,nodes=3,region=us-central1,config=sharded)")
	verbose := flag.Bool("v", false, "Verbose logging")
	flag.Parse()

	logLevel := slog.LevelInfo
	if *verbose {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		slog.Warn("shutdown signal received, stopping")
		cancel()
	}()

	cfg := runConfig{
		grpcAddr:      *grpcAddr,
		httpAddr:      *httpAddr,
		alphas:        *alphas,
		opsCSV:        *opsCSV,
		rtCSV:         *rtCSV,
		clusterCSV:    *clusterCSV,
		summaryOut:    *summaryOut,
		passes:        *passes,
		duration:      *duration,
		repeats:       *repeats,
		flushInterval: *flushInterval,
		writeConc:     *writeConc,
		readConc:      *readConc,
		seed:          *seed,
		small:         *small,
		strict:        *strict,
		vectorStrict:  *vectorStrict,
		dgraphBranch:  *dgraphBranch,
		label:         *label,
	}

	if err := run(ctx, cfg); err != nil {
		slog.Error("run failed", "err", err)
		os.Exit(1)
	}
}

type runConfig struct {
	grpcAddr      string
	httpAddr      string
	alphas        string
	opsCSV        string
	rtCSV         string
	clusterCSV    string
	summaryOut    string
	passes        int
	duration      time.Duration
	repeats       int
	flushInterval time.Duration
	writeConc     int
	readConc      int
	seed          int64
	small         bool
	strict        bool
	vectorStrict  bool
	dgraphBranch  string
	label         string
}

func run(ctx context.Context, cfg runConfig) error {
	// Resolve the set of alpha endpoints to load-balance over and scrape.
	endpoints := envinfo.ResolveAlphas(ctx, cfg.httpAddr, cfg.grpcAddr, cfg.alphas)
	grpcAddrs := make([]string, len(endpoints))
	httpBases := make([]string, len(endpoints))
	for i, e := range endpoints {
		grpcAddrs[i] = e.GRPC
		httpBases[i] = e.HTTP
	}
	slog.Info("connecting to cluster", "alphas", len(endpoints), "grpc", grpcAddrs, "http", httpBases)

	dgraphClient, err := cluster.NewDgraphClient(ctx, grpcAddrs)
	if err != nil {
		return fmt.Errorf("connect DQL: %w", err)
	}
	defer dgraphClient.Close()

	gqlClient := graphql.NewClient(httpBases...)
	if err := gqlClient.Health(ctx); err != nil {
		return fmt.Errorf("GraphQL health check: %w", err)
	}

	version, err := gqlClient.Version(ctx)
	if err != nil {
		slog.Warn("could not determine cluster version", "err", err)
		version = "unknown"
	}

	env := envinfo.Collect(ctx, cfg.httpAddr, cfg.grpcAddr, version, cfg.dgraphBranch)
	env.Labels = cfg.label
	slog.Info("environment",
		"dgraph_version", env.DgraphVersion, "branch", env.DgraphBranch,
		"topology", env.Topology, "host", env.Hostname,
		"cpu", env.NumCPU, "ram_gb", fmt.Sprintf("%.1f", env.TotalRAMGB), "os", env.OS,
		"labels", env.Labels,
	)

	scraper := clusterstats.New(endpoints)
	output.SetInterval(cfg.flushInterval)

	dataset := testdata.DefaultDataset()
	if cfg.small {
		dataset = testdata.SmallDataset()
	}
	// -passes is primary; fall back to -duration, else a default duration.
	passes, duration := cfg.passes, cfg.duration
	if passes <= 0 && duration <= 0 {
		duration = 30 * time.Minute
	}
	if cfg.repeats < 1 {
		cfg.repeats = 1
	}

	var failed bool
	for rep := 1; rep <= cfg.repeats; rep++ {
		suffix := ""
		if cfg.repeats > 1 {
			suffix = fmt.Sprintf("-%d", rep)
		}
		reporter, err := output.NewReporter(
			withSuffix(cfg.opsCSV, suffix), withSuffix(cfg.rtCSV, suffix), withSuffix(cfg.clusterCSV, suffix),
			output.RunMeta{Env: env, Seed: cfg.seed, StartedAt: time.Now()})
		if err != nil {
			return fmt.Errorf("open output: %w", err)
		}

		orch := harness.New(dgraphClient, gqlClient, reporter, scraper, harness.Options{
			Duration:         duration,
			FlushInterval:    cfg.flushInterval,
			WriteConcurrency: cfg.writeConc,
			ReadConcurrency:  cfg.readConc,
			Strict:           cfg.strict,
			VectorStrict:     cfg.vectorStrict,
			Passes:           passes,
			Seed:             cfg.seed,
			Env:              env,
			Dataset:          dataset,
		})

		if cfg.repeats > 1 {
			slog.Info("repeat starting", "repeat", rep, "of", cfg.repeats)
		}
		result, summary, runErr := orch.Run(ctx)
		reporter.Close()
		if summary != nil {
			summary.Repeat = rep
			sp := withSuffix(cfg.summaryOut, suffix)
			if err := report.WriteJSON(sp, summary); err != nil {
				slog.Warn("write summary failed", "err", err)
			} else {
				slog.Info("summary written", "path", sp)
			}
		}
		if runErr != nil {
			return runErr
		}

		passed := 0
		for _, c := range result.Checks {
			if c.Passed {
				passed++
			}
		}
		if !result.AllPassed {
			failed = true
		}
		slog.Info("run complete", "repeat", rep, "validation_passed", result.AllPassed,
			"checks", fmt.Sprintf("%d/%d", passed, len(result.Checks)))
	}

	if failed {
		return fmt.Errorf("validation failed")
	}
	return nil
}

// withSuffix inserts suffix before the file extension, e.g.
// withSuffix("argus-ops.csv", "-2") => "argus-ops-2.csv".
func withSuffix(path, suffix string) string {
	if suffix == "" {
		return path
	}
	ext := filepath.Ext(path)
	return path[:len(path)-len(ext)] + suffix + ext
}
