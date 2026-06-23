// Package output writes periodic benchmark snapshots to CSV. Two files are
// produced per run: an operations file (per-op-type throughput and latency
// percentiles over each interval) and a runtime file (Go heap/GC/goroutine
// samples). Both carry the run environment (Dgraph version/branch, cluster
// topology, and host CPU/RAM) in a comment header so a CSV is self-describing
// and results are comparable across hardware and cluster configurations.
package output

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/argus/pkg/clusterstats"
	"github.com/dgraph-io/argus/pkg/envinfo"
	"github.com/dgraph-io/argus/pkg/metrics"
)

// Reporter owns the operations, runtime, and cluster CSV files for a run.
type Reporter struct {
	mu     sync.Mutex
	opsF   *os.File
	opsW   *csv.Writer
	rtF    *os.File
	rtW    *csv.Writer
	clF    *os.File
	clW    *csv.Writer
	closed bool
}

// RunMeta is descriptive metadata recorded at the top of each CSV.
type RunMeta struct {
	Env       envinfo.Info
	Seed      int64
	StartedAt time.Time
}

// NewReporter creates the three CSV files and writes their headers. clusterPath
// may be empty to skip the per-node cluster file.
func NewReporter(opsPath, runtimePath, clusterPath string, meta RunMeta) (*Reporter, error) {
	opsF, err := os.Create(opsPath)
	if err != nil {
		return nil, fmt.Errorf("create ops csv: %w", err)
	}
	rtF, err := os.Create(runtimePath)
	if err != nil {
		opsF.Close()
		return nil, fmt.Errorf("create runtime csv: %w", err)
	}

	r := &Reporter{
		opsF: opsF, opsW: csv.NewWriter(opsF),
		rtF: rtF, rtW: csv.NewWriter(rtF),
	}

	writeMeta(opsF, meta, "client operation throughput & latency (per interval)")
	writeMeta(rtF, meta, "load-generator (argus) Go runtime — NOT the Dgraph cluster")

	r.opsW.Write([]string{
		"timestamp", "elapsed_s", "op", "count", "errors",
		"ops_per_s", "avg_ms", "min_ms", "max_ms", "p50_ms", "p99_ms",
		"avg_server_ms", "touched_total",
	})
	r.rtW.Write([]string{
		"timestamp", "elapsed_s", "goroutines", "alloc_mb", "heap_alloc_mb",
		"heap_sys_mb", "heap_objects", "num_gc", "gc_pause_ms", "gc_cpu_pct", "next_gc_mb",
	})
	r.opsW.Flush()
	r.rtW.Flush()

	if clusterPath != "" {
		clF, err := os.Create(clusterPath)
		if err != nil {
			opsF.Close()
			rtF.Close()
			return nil, fmt.Errorf("create cluster csv: %w", err)
		}
		r.clF, r.clW = clF, csv.NewWriter(clF)
		writeMeta(clF, meta, "per-Dgraph-node resource & throughput; node=CLUSTER is the aggregate")
		r.clW.Write([]string{
			"timestamp", "elapsed_s", "node", "leader", "rss_mb", "cpu_pct", "goroutines",
			"heap_inuse_mb", "queries_per_s", "commits_per_s", "aborts_per_s",
			"pending_queries", "active_mutations", "server_avg_ms",
		})
		r.clW.Flush()
	}
	return r, nil
}

// WriteCluster appends one row per node plus the cluster aggregate row.
func (r *Reporter) WriteCluster(ts time.Time, elapsed time.Duration, samples []clusterstats.NodeSample) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.clW == nil {
		return nil
	}
	rows := append([]clusterstats.NodeSample{}, samples...)
	rows = append(rows, clusterstats.Aggregate(samples))
	for _, s := range rows {
		r.clW.Write([]string{
			ts.Format("2006-01-02T15:04:05.000"),
			f2(elapsed.Seconds()),
			s.Node,
			strconv.FormatBool(s.Leader),
			f2(s.RSSMB), f2(s.CPUPercent), f2(s.Goroutines), f2(s.HeapInuseMB),
			f2(s.QueriesPerSec), f2(s.CommitsPerSec), f2(s.AbortsPerSec),
			f2(s.PendingQueries), f2(s.ActiveMutations), f3(s.ServerAvgMs),
		})
	}
	r.clW.Flush()
	return r.clW.Error()
}

func writeMeta(f *os.File, m RunMeta, role string) {
	e := m.Env
	branch := e.DgraphBranch
	if branch == "" {
		branch = "unknown"
	}
	fmt.Fprintf(f, "# argus run | started=%s | seed=%d | metrics=%s\n", m.StartedAt.Format(time.RFC3339), m.Seed, role)
	fmt.Fprintf(f, "# dgraph | version=%s | branch=%s | cluster=%s | http=%s | cid=%s\n",
		e.DgraphVersion, branch, e.ClusterGRPC, e.ClusterHTTP, e.ClusterID)
	fmt.Fprintf(f, "# topology | %s | groups=%d | replicas=%d | alphas=%d | zeros=%d\n",
		e.Topology, e.Groups, e.Replicas, e.NumAlphas, e.NumZeros)
	fmt.Fprintf(f, "# host | name=%s | os=%s | cpu=%d | gomaxprocs=%d | ram_gb=%.2f | go=%s\n",
		e.Hostname, e.OS, e.NumCPU, e.GOMAXPROCS, e.TotalRAMGB, e.GoVersion)
}

// WriteOps appends one interval's per-op snapshots.
func (r *Reporter) WriteOps(ts time.Time, elapsed time.Duration, snaps []metrics.OpSnapshot) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("reporter closed")
	}
	for _, s := range snaps {
		r.opsW.Write([]string{
			ts.Format("2006-01-02T15:04:05.000"),
			f2(elapsed.Seconds()),
			s.Key,
			strconv.Itoa(s.Count),
			strconv.Itoa(s.Errors),
			f2(throughput(s.Count)),
			f3(s.AvgMs), f3(s.MinMs), f3(s.MaxMs), f3(s.P50Ms), f3(s.P99Ms),
			f3(s.AvgServerMs),
			strconv.FormatUint(s.TouchedTotal, 10),
		})
	}
	r.opsW.Flush()
	return r.opsW.Error()
}

// snapshotInterval is the cadence between snapshots, used to convert per-window
// counts into ops_per_s. Set via SetInterval before the run.
var snapshotInterval = 5 * time.Second

// SetInterval informs the reporter of the snapshot cadence for ops_per_s.
func SetInterval(d time.Duration) {
	if d > 0 {
		snapshotInterval = d
	}
}

func throughput(count int) float64 {
	s := snapshotInterval.Seconds()
	if s <= 0 {
		return 0
	}
	return float64(count) / s
}

// WriteRuntime appends one runtime sample.
func (r *Reporter) WriteRuntime(ts time.Time, elapsed time.Duration, s metrics.RuntimeSample) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("reporter closed")
	}
	r.rtW.Write([]string{
		ts.Format("2006-01-02T15:04:05.000"),
		f2(elapsed.Seconds()),
		strconv.Itoa(s.Goroutines),
		f2(s.AllocMB), f2(s.HeapAllocMB), f2(s.HeapSysMB),
		strconv.FormatUint(s.HeapObjects, 10),
		strconv.FormatUint(uint64(s.NumGC), 10),
		f3(s.GCPauseMs), f2(s.GCCPUPercent), f2(s.NextGCMB),
	})
	r.rtW.Flush()
	return r.rtW.Error()
}

// Close flushes and closes all files.
func (r *Reporter) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.opsW.Flush()
	r.rtW.Flush()
	err1 := r.opsF.Close()
	err2 := r.rtF.Close()
	if r.clW != nil {
		r.clW.Flush()
		if e := r.clF.Close(); e != nil && err2 == nil {
			err2 = e
		}
	}
	if err1 != nil {
		return err1
	}
	return err2
}

func f2(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }
func f3(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
