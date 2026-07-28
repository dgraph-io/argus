// Package metrics aggregates per-operation timing into bounded windows and
// samples Go runtime statistics. It deliberately does not retain a record per
// operation: a 30-minute concurrent run executes millions of ops, so the
// collector keeps windowed aggregates (reset on each snapshot) and the runtime
// sampler reads MemStats periodically rather than per-op.
package metrics

import (
	"runtime"
	"sort"
	"sync"
	"time"
)

// Op classifies an operation for aggregation. Key() combines type and label.
const (
	OpDQLWrite    = "dql-write"
	OpDQLDelete   = "dql-delete"
	OpDQLQuery    = "dql-query"
	OpGQLWrite    = "gql-write"
	OpGQLDelete   = "gql-delete"
	OpGQLQuery    = "gql-query"
	OpDQLLedger   = "dql-ledger"
	OpGQLLedger   = "gql-ledger"
	OpGQLSub      = "gql-sub"
	OpDQLBulkLoad = "dql-bulkload"
)

type window struct {
	count    int
	errors   int
	durs     []time.Duration
	serverNs uint64
	touched  uint64
}

// cumulative holds whole-run (never reset) stats per key, used for the final
// summary's true percentiles.
type cumulative struct {
	count    int
	errors   int
	hist     Histogram
	serverNs uint64
	touched  uint64
}

// Collector accumulates operation timings into per-key windows (for the
// time-series CSV) and per-key cumulative histograms (for the final summary).
type Collector struct {
	mu      sync.Mutex
	windows map[string]*window
	cum     map[string]*cumulative
}

// NewCollector creates an empty collector.
func NewCollector() *Collector {
	return &Collector{windows: make(map[string]*window), cum: make(map[string]*cumulative)}
}

// Observe records one operation outcome under key (e.g. "dql-query:filter-age").
// serverNs is server-reported total latency; touched is server-reported
// uids/nodes touched. Both may be zero when unavailable.
func (c *Collector) Observe(key string, d time.Duration, serverNs, touched uint64, ok bool) {
	ms := float64(d.Microseconds()) / 1000
	c.mu.Lock()
	defer c.mu.Unlock()

	w := c.windows[key]
	if w == nil {
		w = &window{}
		c.windows[key] = w
	}
	w.count++
	if !ok {
		w.errors++
	}
	w.durs = append(w.durs, d)
	w.serverNs += serverNs
	w.touched += touched

	cu := c.cum[key]
	if cu == nil {
		cu = &cumulative{}
		c.cum[key] = cu
	}
	cu.count++
	if !ok {
		cu.errors++
	}
	cu.hist.Observe(ms)
	cu.serverNs += serverNs
	cu.touched += touched
}

// OpSnapshot is the aggregate for one key over a window.
type OpSnapshot struct {
	Key          string
	Count        int
	Errors       int
	AvgMs        float64
	MinMs        float64
	MaxMs        float64
	P50Ms        float64
	P99Ms        float64
	AvgServerMs  float64
	TouchedTotal uint64
}

// Snapshot computes aggregates for every key and resets the windows, so each
// snapshot reflects throughput/latency over the interval since the last call.
func (c *Collector) Snapshot() []OpSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]OpSnapshot, 0, len(c.windows))
	for key, w := range c.windows {
		if w.count == 0 {
			continue
		}
		out = append(out, summarize(key, w))
	}
	// Reset windows in place.
	c.windows = make(map[string]*window, len(c.windows))

	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// OpSummary is the whole-run aggregate for one key, with true percentiles from
// the cumulative histogram. ThroughputPerSec is filled in by the caller (which
// knows the measured wall-clock).
type OpSummary struct {
	Key              string  `json:"key"`
	Count            int     `json:"count"`
	Errors           int     `json:"errors"`
	ThroughputPerSec float64 `json:"throughput_per_s"`
	AvgMs            float64 `json:"avg_ms"`
	P50Ms            float64 `json:"p50_ms"`
	P90Ms            float64 `json:"p90_ms"`
	P99Ms            float64 `json:"p99_ms"`
	P999Ms           float64 `json:"p999_ms"`
	MaxMs            float64 `json:"max_ms"`
	AvgServerMs      float64 `json:"avg_server_ms"`
	TouchedTotal     uint64  `json:"touched_total"`
}

// FinalSummary returns whole-run per-key aggregates (not reset). Percentiles
// come from the cumulative histogram; ThroughputPerSec is left zero for the
// caller to populate from the measured duration.
func (c *Collector) FinalSummary() []OpSummary {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]OpSummary, 0, len(c.cum))
	for key, cu := range c.cum {
		if cu.count == 0 {
			continue
		}
		s := OpSummary{
			Key:          key,
			Count:        cu.count,
			Errors:       cu.errors,
			AvgMs:        cu.hist.Mean(),
			P50Ms:        cu.hist.Percentile(50),
			P90Ms:        cu.hist.Percentile(90),
			P99Ms:        cu.hist.Percentile(99),
			P999Ms:       cu.hist.Percentile(99.9),
			MaxMs:        cu.hist.Max(),
			TouchedTotal: cu.touched,
		}
		if cu.count > 0 {
			s.AvgServerMs = float64(cu.serverNs) / float64(cu.count) / 1e6
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func summarize(key string, w *window) OpSnapshot {
	sort.Slice(w.durs, func(i, j int) bool { return w.durs[i] < w.durs[j] })
	var sum time.Duration
	for _, d := range w.durs {
		sum += d
	}
	n := len(w.durs)
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	s := OpSnapshot{
		Key:          key,
		Count:        w.count,
		Errors:       w.errors,
		MinMs:        ms(w.durs[0]),
		MaxMs:        ms(w.durs[n-1]),
		P50Ms:        ms(w.durs[pct(n, 50)]),
		P99Ms:        ms(w.durs[pct(n, 99)]),
		AvgMs:        ms(sum) / float64(n),
		TouchedTotal: w.touched,
	}
	if w.count > 0 {
		s.AvgServerMs = float64(w.serverNs) / float64(w.count) / 1e6
	}
	return s
}

func pct(n, p int) int {
	if n == 0 {
		return 0
	}
	idx := (p * n) / 100
	if idx >= n {
		idx = n - 1
	}
	return idx
}

// RuntimeSample is a point-in-time snapshot of Go runtime statistics.
type RuntimeSample struct {
	Goroutines   int
	AllocMB      float64
	HeapAllocMB  float64
	HeapSysMB    float64
	HeapObjects  uint64
	NumGC        uint32
	GCPauseMs    float64 // most recent GC pause
	GCCPUPercent float64
	NextGCMB     float64
}

// SampleRuntime reads current Go runtime/GC statistics.
func SampleRuntime() RuntimeSample {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	mb := func(b uint64) float64 { return float64(b) / 1024 / 1024 }
	return RuntimeSample{
		Goroutines:   runtime.NumGoroutine(),
		AllocMB:      mb(m.Alloc),
		HeapAllocMB:  mb(m.HeapAlloc),
		HeapSysMB:    mb(m.HeapSys),
		HeapObjects:  m.HeapObjects,
		NumGC:        m.NumGC,
		GCPauseMs:    float64(m.PauseNs[(m.NumGC+255)%256]) / 1e6,
		GCCPUPercent: m.GCCPUFraction * 100,
		NextGCMB:     mb(m.NextGC),
	}
}
