package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dgraph-io/argus/pkg/cluster"
	"github.com/dgraph-io/argus/pkg/clusterstats"
	"github.com/dgraph-io/argus/pkg/envinfo"
	"github.com/dgraph-io/argus/pkg/graphql"
	"github.com/dgraph-io/argus/pkg/harness"
	"github.com/dgraph-io/argus/pkg/output"
	"github.com/dgraph-io/argus/pkg/testdata"
)

func main() {
	grpcAddr := flag.String("cluster", "localhost:9080", "Dgraph cluster gRPC address (DQL); seed for discovery")
	httpAddr := flag.String("graphql", "http://localhost:8080", "Dgraph alpha HTTP base URL (GraphQL); seed for discovery")
	alphas := flag.String("alphas", "", "Comma-separated alpha HTTP base URLs to load-balance over and scrape (default: discover from /state)")
	opsCSV := flag.String("ops-output", "argus-ops.csv", "CSV path for per-op throughput/latency snapshots")
	rtCSV := flag.String("runtime-output", "argus-runtime.csv", "CSV path for load-generator Go runtime/GC snapshots")
	clusterCSV := flag.String("cluster-output", "argus-cluster.csv", "CSV path for per-Dgraph-node resource/throughput snapshots")
	duration := flag.Duration("duration", 30*time.Minute, "Target run duration (a write pass always completes)")
	flushInterval := flag.Duration("flush-interval", 5*time.Second, "Snapshot flush cadence")
	writeConc := flag.Int("write-concurrency", 8, "Concurrent write workers")
	readConc := flag.Int("read-concurrency", 8, "Concurrent read workers")
	seed := flag.Int64("seed", 1, "Dataset seed (reserved; dataset is index-deterministic)")
	small := flag.Bool("small", false, "Use the small dataset for smoke tests")
	strict := flag.Bool("strict", false, "Fail fast on the first error (default: log and continue)")
	vectorStrict := flag.Bool("vector-strict", false, "Assert exact similar_to nearest-neighbor recall (regression gate; expected to fail on builds with the HNSW upsert issue)")
	dgraphBranch := flag.String("dgraph-branch", "", "Dgraph build branch under test, recorded in the report (from `dgraph version`; the cluster API exposes only the commit hash)")
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
		duration:      *duration,
		flushInterval: *flushInterval,
		writeConc:     *writeConc,
		readConc:      *readConc,
		seed:          *seed,
		small:         *small,
		strict:        *strict,
		vectorStrict:  *vectorStrict,
		dgraphBranch:  *dgraphBranch,
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
	duration      time.Duration
	flushInterval time.Duration
	writeConc     int
	readConc      int
	seed          int64
	small         bool
	strict        bool
	vectorStrict  bool
	dgraphBranch  string
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
	slog.Info("environment",
		"dgraph_version", env.DgraphVersion, "branch", env.DgraphBranch,
		"topology", env.Topology, "host", env.Hostname,
		"cpu", env.NumCPU, "ram_gb", fmt.Sprintf("%.1f", env.TotalRAMGB), "os", env.OS,
	)

	scraper := clusterstats.New(endpoints)

	output.SetInterval(cfg.flushInterval)
	reporter, err := output.NewReporter(cfg.opsCSV, cfg.rtCSV, cfg.clusterCSV, output.RunMeta{
		Env:       env,
		Seed:      cfg.seed,
		StartedAt: time.Now(),
	})
	if err != nil {
		return fmt.Errorf("open output: %w", err)
	}
	defer reporter.Close()

	dataset := testdata.DefaultDataset()
	if cfg.small {
		dataset = testdata.SmallDataset()
	}
	orch := harness.New(dgraphClient, gqlClient, reporter, scraper, harness.Options{
		Duration:         cfg.duration,
		FlushInterval:    cfg.flushInterval,
		WriteConcurrency: cfg.writeConc,
		ReadConcurrency:  cfg.readConc,
		Strict:           cfg.strict,
		VectorStrict:     cfg.vectorStrict,
		Dataset:          dataset,
	})

	result, err := orch.Run(ctx)
	if err != nil {
		return err
	}

	passed := 0
	for _, c := range result.Checks {
		if c.Passed {
			passed++
		}
	}
	slog.Info("run complete",
		"ops_csv", cfg.opsCSV,
		"runtime_csv", cfg.rtCSV,
		"validation_passed", result.AllPassed,
		"checks", fmt.Sprintf("%d/%d", passed, len(result.Checks)),
	)
	if !result.AllPassed {
		return fmt.Errorf("validation failed: %d/%d checks passed", passed, len(result.Checks))
	}
	return nil
}
