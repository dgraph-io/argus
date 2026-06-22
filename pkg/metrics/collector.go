package metrics

import (
	"runtime"
	"sync"
	"time"
)

// Record represents a single performance measurement.
type Record struct {
	Timestamp       time.Time
	OperationType   string        // "write", "query-dql", "query-graphql", "schema", etc.
	Duration        time.Duration // How long the operation took
	Success         bool
	Error           string
	ItemCount       int           // For batch operations
	MemoryAlloc     uint64        // bytes allocated
	MemoryHeapAlloc uint64        // bytes in heap
	GCPauses        uint64        // GC pause time in microseconds
	GCCount         uint32        // Number of GC runs
}

// Collector gathers performance metrics and runtime statistics.
type Collector struct {
	mu       sync.Mutex
	records  []Record
	startMem runtime.MemStats
	lastGC   struct {
		count   uint32
		pauseNs uint64
	}
}

// NewCollector creates a new metrics collector.
func NewCollector() *Collector {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return &Collector{
		records: make([]Record, 0, 10000),
		startMem: m,
		lastGC: struct {
			count   uint32
			pauseNs uint64
		}{
			count:   m.NumGC,
			pauseNs: m.PauseNs[(m.NumGC+255)%256],
		},
	}
}

// Record adds a performance record.
func (c *Collector) Record(rec Record) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now()
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	rec.MemoryAlloc = m.Alloc
	rec.MemoryHeapAlloc = m.HeapAlloc
	rec.GCCount = m.NumGC

	if m.NumGC > c.lastGC.count {
		// Calculate cumulative pause time since last check
		rec.GCPauses = m.PauseNs[m.NumGC%256]
		c.lastGC.count = m.NumGC
	}

	c.records = append(c.records, rec)
}

// Records returns a copy of all collected records.
func (c *Collector) Records() []Record {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]Record, len(c.records))
	copy(out, c.records)
	return out
}

// Summary returns aggregate statistics.
func (c *Collector) Summary() map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	var totalSuccess, totalFail int
	opTypeCounts := make(map[string]int)
	var totalDuration time.Duration

	for _, r := range c.records {
		if r.Success {
			totalSuccess++
		} else {
			totalFail++
		}
		opTypeCounts[r.OperationType]++
		totalDuration += r.Duration
	}

	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)

	return map[string]interface{}{
		"total_operations": len(c.records),
		"successful":       totalSuccess,
		"failed":           totalFail,
		"by_type":          opTypeCounts,
		"total_duration":   totalDuration.String(),
		"avg_duration":     time.Duration(int64(totalDuration) / int64(len(c.records)+1)).String(),
		"memory_alloc_mb":  float64(endMem.Alloc) / 1024 / 1024,
		"heap_alloc_mb":    float64(endMem.HeapAlloc) / 1024 / 1024,
		"gc_runs":          endMem.NumGC,
	}
}

// Reset clears all records.
func (c *Collector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = c.records[:0]
}
