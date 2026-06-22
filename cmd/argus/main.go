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
	"github.com/dgraph-io/argus/pkg/harness"
	"github.com/dgraph-io/argus/pkg/output"
)

func main() {
	clusterAddr := flag.String("cluster", "localhost:9080", "Dgraph cluster gRPC address")
	csvPath := flag.String("output", "argus-metrics.csv", "Path to CSV output file")
	duration := flag.Duration("duration", 30*time.Minute, "Target run duration")
	strict := flag.Bool("strict", false, "Fail fast on errors (default: log and continue)")
	verbose := flag.Bool("v", false, "Verbose logging")
	flag.Parse()

	logLevel := slog.LevelInfo
	if *verbose {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown on interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		slog.Info("shutdown signal received")
		cancel()
	}()

	if err := run(ctx, *clusterAddr, *csvPath, *duration, *strict); err != nil {
		slog.Error("harness failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, clusterAddr, csvPath string, duration time.Duration, strict bool) error {
	// Initialize cluster client
	slog.Info("connecting to cluster", "addr", clusterAddr)
	dgraphClient, err := cluster.NewDgraphClient(ctx, clusterAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to Dgraph cluster: %w", err)
	}
	defer dgraphClient.Close()

	version, err := dgraphClient.Version(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster version: %w", err)
	}
	slog.Info("cluster version", "version", version)

	// Initialize CSV writer
	csvWriter, err := output.NewCSVWriter(csvPath)
	if err != nil {
		return fmt.Errorf("failed to open CSV output: %w", err)
	}
	defer csvWriter.Close()

	// Run harness
	h := harness.New(dgraphClient, csvWriter, version, strict)
	if err := h.Run(ctx, duration); err != nil {
		return fmt.Errorf("harness execution failed: %w", err)
	}

	slog.Info("harness completed successfully", "output", csvPath)
	return nil
}
