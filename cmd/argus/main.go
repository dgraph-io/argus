package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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
	// Subcommands that read/write the results ledger rather than running a benchmark.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "compare":
			runCompare(os.Args[2:])
			return
		case "ls":
			runLs(os.Args[2:])
			return
		case "import":
			runImport(os.Args[2:])
			return
		}
	}

	grpcAddr := flag.String("cluster", "localhost:9080", "Dgraph cluster gRPC address (DQL); seed for discovery")
	httpAddr := flag.String("graphql", "http://localhost:8080", "Dgraph alpha HTTP base URL (GraphQL); seed for discovery")
	alphas := flag.String("alphas", "", "Comma-separated alpha HTTP base URLs to load-balance over and scrape (default: discover from /state)")
	opsCSV := flag.String("ops-output", "argus-ops.csv", "CSV path for per-op throughput/latency snapshots")
	rtCSV := flag.String("runtime-output", "argus-runtime.csv", "CSV path for load-generator Go runtime/GC snapshots")
	clusterCSV := flag.String("cluster-output", "argus-cluster.csv", "CSV path for per-Dgraph-node resource/throughput snapshots")
	summaryOut := flag.String("summary-output", "argus-summary.json", "JSON path for the per-run summary (empty to skip; prefer -ledger)")
	ledger := flag.String("ledger", "", "Append each repeat's summary as one JSONL line to this ledger file (the consolidated results store read by `argus compare`/`argus ls`)")
	runID := flag.String("run-id", "", "Stable id shared across this run's repeats (default: generated); keys the ledger rows and the artifacts dir")
	passes := flag.Int("passes", 10, "Fixed-work mode: run exactly N write passes (primary; set 0 to use -duration instead)")
	duration := flag.Duration("duration", 0, "Duration mode: run for this long (used only when -passes=0)")
	repeats := flag.Int("repeats", 1, "Repeat the whole measurement N times, emitting one summary per repeat (for a noise floor)")
	flushInterval := flag.Duration("flush-interval", 5*time.Second, "Snapshot flush cadence")
	writeConc := flag.Int("write-concurrency", 8, "Concurrent write workers")
	readConc := flag.Int("read-concurrency", 8, "Concurrent read workers")
	seed := flag.Int64("seed", 1, "Dataset seed (reserved; dataset is index-deterministic)")
	small := flag.Bool("small", false, "Use the small dataset for smoke tests")
	scale := flag.Float64("scale", 0, "Scale the default dataset by this factor (e.g. 0.5); 0 = default size, overrides -small")
	noDrop := flag.Bool("no-drop", false, "Skip the clean-slate drop-all at the start of each run (default: drop so the graph is exactly the dataset)")
	bulkLoad := flag.Bool("bulk-load", true, "Run the blind bulk-load phase (live-loader-shaped large insert batches) before the timed workload")
	bulkBatch := flag.Int("bulk-batch", 0, "Records per bulk-load mutation (0 = default ~1000 nquads/batch); larger batches give parallel mutations more to fan out")
	bulkRecords := flag.Int("bulk-records", 0, "Override the bulk-load record count (0 = derive from dataset/-scale)")
	bulkEdges := flag.Int("bulk-edges", -1, "brhub fan-in edges per bulk record (-1 = dataset default; 0 = scalar-only, no reverse-index apply work)")
	shardBalance := flag.Bool("shard-balance", false, "On sharded clusters, spread predicates across groups after schema (before load) so writes distribute; needs -zero. No-op for <2 groups")
	zeroAddr := flag.String("zero", "", "Zero admin HTTP base URL (e.g. http://dgraph-dgraph-zero:6080) for -shard-balance moveTablet calls")
	strict := flag.Bool("strict", false, "Fail fast on the first error (default: log and continue)")
	vectorStrict := flag.Bool("vector-strict", false, "Assert exact similar_to nearest-neighbor recall (regression gate; expected to fail on builds with the HNSW upsert issue)")
	dgraphBranch := flag.String("dgraph-branch", "", "Dgraph build branch recorded in the report (from `dgraph version`; empty for released tag builds, which report \"HEAD\")")
	dgraphCommit := flag.String("dgraph-commit", "", "Dgraph commit SHA-1 recorded in the report (from `dgraph version`); uniquely pins the build")
	cpuClass := flag.String("cpu-class", "", "CPU hardware class recorded in the report (e.g. \"Apple M4\", \"e2-standard-4\")")
	diskClass := flag.String("disk-class", "", "Disk/storage class recorded in the report (e.g. \"NVMe\", \"pd-ssd\", \"local-ssd\")")
	deploy := flag.String("deploy", "", "Deployment mechanism recorded in the report (e.g. \"docker-compose\", \"gke\")")
	label := flag.String("label", "", "Free-form run annotation for anything not covered by -cpu-class/-disk-class/-deploy")
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
		ledger:        *ledger,
		runID:         *runID,
		passes:        *passes,
		duration:      *duration,
		repeats:       *repeats,
		flushInterval: *flushInterval,
		writeConc:     *writeConc,
		readConc:      *readConc,
		seed:          *seed,
		small:         *small,
		scale:         *scale,
		noDrop:        *noDrop,
		bulkLoad:      *bulkLoad,
		bulkBatch:     *bulkBatch,
		bulkRecords:   *bulkRecords,
		bulkEdges:     *bulkEdges,
		shardBalance:  *shardBalance,
		zeroURL:       *zeroAddr,
		strict:        *strict,
		vectorStrict:  *vectorStrict,
		dgraphBranch:  *dgraphBranch,
		dgraphCommit:  *dgraphCommit,
		cpuClass:      *cpuClass,
		diskClass:     *diskClass,
		deploy:        *deploy,
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
	ledger        string
	runID         string
	passes        int
	duration      time.Duration
	repeats       int
	flushInterval time.Duration
	writeConc     int
	readConc      int
	seed          int64
	small         bool
	scale         float64
	noDrop        bool
	bulkLoad      bool
	bulkBatch     int
	bulkRecords   int
	bulkEdges     int
	shardBalance  bool
	zeroURL       string
	strict        bool
	vectorStrict  bool
	dgraphBranch  string
	dgraphCommit  string
	cpuClass      string
	diskClass     string
	deploy        string
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
	env.CPUClass = cfg.cpuClass
	env.DiskClass = cfg.diskClass
	env.Deploy = cfg.deploy
	env.DgraphCommit = cfg.dgraphCommit

	runID := cfg.runID
	if runID == "" {
		token := cfg.dgraphBranch
		if token == "" {
			token = version
		}
		runID = genRunID(token)
	}
	slog.Info("environment",
		"dgraph_version", env.DgraphVersion, "branch", env.DgraphBranch,
		"topology", env.Topology(), "host", env.Hostname,
		"cpu", env.NumCPU, "ram_gb", fmt.Sprintf("%.1f", env.TotalRAMGB), "os", env.OS,
		"labels", env.Labels,
	)

	scraper := clusterstats.New(endpoints)
	output.SetInterval(cfg.flushInterval)

	dataset := testdata.DefaultDataset()
	switch {
	case cfg.scale > 0:
		dataset = testdata.ScaledDataset(cfg.scale)
	case cfg.small:
		dataset = testdata.SmallDataset()
	}
	if cfg.bulkRecords > 0 {
		dataset.NumBulkRecords = cfg.bulkRecords
		dataset.BulkHubs = testdata.BulkHubCount(cfg.bulkRecords)
	}
	if cfg.bulkEdges >= 0 {
		dataset.BulkEdges = cfg.bulkEdges
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
			NoDrop:           cfg.noDrop,
			Passes:           passes,
			Seed:             cfg.seed,
			Env:              env,
			Dataset:          dataset,
			BulkLoad:         cfg.bulkLoad,
			BulkGroupSize:    cfg.bulkBatch,
			ShardBalance:     cfg.shardBalance,
			ZeroURL:          cfg.zeroURL,
		})

		if cfg.repeats > 1 {
			slog.Info("repeat starting", "repeat", rep, "of", cfg.repeats)
		}
		result, summary, runErr := orch.Run(ctx)
		reporter.Close()
		if summary != nil {
			summary.RunID = runID
			summary.Repeat = rep
			summary.Scale = cfg.scale
			if cfg.summaryOut != "" {
				sp := withSuffix(cfg.summaryOut, suffix)
				if err := report.WriteJSON(sp, summary); err != nil {
					slog.Warn("write summary failed", "err", err)
				} else {
					slog.Info("summary written", "path", sp)
				}
			}
			if cfg.ledger != "" {
				if err := report.AppendRun(cfg.ledger, summary); err != nil {
					slog.Warn("append to ledger failed", "err", err)
				} else {
					slog.Info("appended to ledger", "path", cfg.ledger, "run_id", runID, "repeat", rep)
				}
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

// genRunID builds a stable, filename-safe run id: the start timestamp, a token
// (branch or version), and 4 random bytes to avoid collisions between runs
// started in the same second.
func genRunID(token string) string {
	ts := time.Now().Format("20060102-150405")
	t := sanitizeToken(token)
	var b [4]byte
	if _, err := crand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s-%s", ts, t)
	}
	return fmt.Sprintf("%s-%s-%s", ts, t, hex.EncodeToString(b[:]))
}

// sanitizeToken keeps [A-Za-z0-9._-] and collapses anything else to '-'.
func sanitizeToken(s string) string {
	if s == "" {
		return "run"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
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
