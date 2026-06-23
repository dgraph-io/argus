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

	"github.com/dgraph-io/argus/pkg/envinfo"
	"github.com/dgraph-io/argus/pkg/metrics"
)

// Reporter owns the operations and runtime CSV files for a run.
type Reporter struct {
	mu     sync.Mutex
	opsF   *os.File
	opsW   *csv.Writer
	rtF    *os.File
	rtW    *csv.Writer
	closed bool
}

// RunMeta is descriptive metadata recorded at the top of each CSV.
type RunMeta struct {
	Env       envinfo.Info
	Seed      int64
	StartedAt time.Time
}

// NewReporter creates the two CSV files and writes their headers.
func NewReporter(opsPath, runtimePath string, meta RunMeta) (*Reporter, error) {
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

	writeMeta(opsF, meta)
	writeMeta(rtF, meta)

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
	return r, nil
}

func writeMeta(f *os.File, m RunMeta) {
	e := m.Env
	branch := e.DgraphBranch
	if branch == "" {
		branch = "unknown"
	}
	fmt.Fprintf(f, "# argus run | started=%s | seed=%d\n", m.StartedAt.Format(time.RFC3339), m.Seed)
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

// Close flushes and closes both files.
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
	if err1 != nil {
		return err1
	}
	return err2
}

func f2(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }
func f3(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
