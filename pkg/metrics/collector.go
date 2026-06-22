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
	OpDQLWrite  = "dql-write"
	OpDQLDelete = "dql-delete"
	OpDQLQuery  = "dql-query"
	OpGQLWrite  = "gql-write"
	OpGQLDelete = "gql-delete"
	OpGQLQuery  = "gql-query"
)

type window struct {
	count    int
	errors   int
	durs     []time.Duration
	serverNs uint64
	touched  uint64
}

// Collector accumulates operation timings into per-key windows.
type Collector struct {
	mu      sync.Mutex
	windows map[string]*window
}

// NewCollector creates an empty collector.
func NewCollector() *Collector {
	return &Collector{windows: make(map[string]*window)}
}

// Observe records one operation outcome under key (e.g. "dql-query:filter-age").
// serverNs is server-reported total latency; touched is server-reported
// uids/nodes touched. Both may be zero when unavailable.
func (c *Collector) Observe(key string, d time.Duration, serverNs, touched uint64, ok bool) {
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
