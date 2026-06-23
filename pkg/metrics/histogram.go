package metrics

import "math"

// Bounded exponential-bucket latency histogram. Buckets span ~10µs to ~30min
// with ~10% relative width, so percentiles over a whole run are accurate enough
// for regression comparison at fixed memory (no per-op retention of every
// sample). Both compared runs use identical bucket edges, so deltas are sound.
const (
	histBase    = 0.01 // ms (lower edge of bucket 0)
	histFactor  = 1.1
	histBuckets = 200
)

var histLogFactor = math.Log(histFactor)

// Histogram accumulates latency samples in milliseconds.
type Histogram struct {
	buckets [histBuckets]uint64
	count   uint64
	sumMs   float64
	maxMs   float64
}

// Observe records a latency sample in milliseconds.
func (h *Histogram) Observe(ms float64) {
	h.count++
	h.sumMs += ms
	if ms > h.maxMs {
		h.maxMs = ms
	}
	idx := 0
	if ms > histBase {
		idx = int(math.Log(ms/histBase) / histLogFactor)
		if idx >= histBuckets {
			idx = histBuckets - 1
		}
	}
	h.buckets[idx]++
}

// Percentile returns the p-th percentile (0..100) in milliseconds, using the
// geometric midpoint of the containing bucket.
func (h *Histogram) Percentile(p float64) float64 {
	if h.count == 0 {
		return 0
	}
	target := uint64(p / 100 * float64(h.count))
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i := 0; i < histBuckets; i++ {
		cum += h.buckets[i]
		if cum >= target {
			lo := histBase * math.Pow(histFactor, float64(i))
			return lo * math.Sqrt(histFactor) // geometric midpoint of [lo, lo*factor)
		}
	}
	return h.maxMs
}

// Mean returns the average sample in milliseconds.
func (h *Histogram) Mean() float64 {
	if h.count == 0 {
		return 0
	}
	return h.sumMs / float64(h.count)
}

// Max returns the largest observed sample in milliseconds.
func (h *Histogram) Max() float64 { return h.maxMs }
