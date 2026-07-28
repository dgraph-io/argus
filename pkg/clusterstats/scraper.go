// Package clusterstats scrapes each Dgraph alpha's Prometheus metrics endpoint
// and produces per-node samples plus a cluster aggregate. This is the
// server-side counterpart to the load generator's own runtime stats: it
// measures the system under test (each alpha's RAM, CPU, GC, query/txn
// throughput) so performance is comparable across hardware and cluster shapes.
//
// Aggregation respects metric semantics: gauges (RAM, goroutines) are kept
// per-node and summed for a cluster total; counters (queries, commits, CPU
// seconds) are turned into per-interval rates via deltas, then summed; the
// server latency average merges the histogram sum/count across nodes.
package clusterstats

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/argus/pkg/envinfo"
)

// metric names scraped from /debug/prometheus_metrics.
const (
	mRSS        = "process_resident_memory_bytes"
	mCPU        = "process_cpu_seconds_total"
	mGoroutines = "go_goroutines"
	mHeapInuse  = "go_memstats_heap_inuse_bytes"
	mJemalloc   = "dgraph_memory_alloc_bytes" // off-heap bytes allocated via jemalloc (z.Calloc)
	mQueries    = "dgraph_num_queries_total"
	mCommits    = "dgraph_txn_commits_total"
	mAborts     = "dgraph_txn_aborts_total"
	mPending    = "dgraph_pending_queries_total"
	mActiveMut  = "dgraph_active_mutations_total"
	mLeader     = "dgraph_raft_is_leader"
	mLatSum     = "dgraph_latency_sum"
	mLatCount   = "dgraph_latency_count"
)

var wantMetrics = map[string]bool{
	mRSS: true, mCPU: true, mGoroutines: true, mHeapInuse: true, mJemalloc: true,
	mQueries: true, mCommits: true, mAborts: true, mPending: true,
	mActiveMut: true, mLeader: true, mLatSum: true, mLatCount: true,
}

// NodeSample is one alpha's metrics over the interval since the last scrape.
type NodeSample struct {
	Node            string
	Leader          bool
	RSSMB           float64
	HeapInuseMB     float64
	JemallocMB      float64 // off-heap (jemalloc); RSS ≈ HeapInuse + Jemalloc + other
	Goroutines      float64
	CPUPercent      float64 // cores-worth: 100 == one core fully busy
	QueriesPerSec   float64
	CommitsPerSec   float64
	AbortsPerSec    float64
	PendingQueries  float64
	ActiveMutations float64
	ServerAvgMs     float64

	latSumDelta   float64 // for aggregate merge
	latCountDelta float64
}

type prevState struct {
	t        time.Time
	cpu      float64
	queries  float64
	commits  float64
	aborts   float64
	latSum   float64
	latCount float64
}

// Scraper polls a fixed set of alpha endpoints, remembering each node's prior
// counter values to compute rates.
type Scraper struct {
	endpoints  []envinfo.Endpoint
	httpClient *http.Client
	mu         sync.Mutex
	prev       map[string]prevState
}

// New creates a scraper for the given alpha endpoints.
func New(endpoints []envinfo.Endpoint) *Scraper {
	return &Scraper{
		endpoints:  endpoints,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		prev:       make(map[string]prevState),
	}
}

// Nodes reports how many endpoints are scraped.
func (s *Scraper) Nodes() int { return len(s.endpoints) }

// Sample scrapes every endpoint once and returns a per-node sample. Nodes that
// fail to scrape are omitted.
func (s *Scraper) Sample(ctx context.Context) []NodeSample {
	out := make([]NodeSample, 0, len(s.endpoints))
	for _, ep := range s.endpoints {
		if sample, ok := s.scrapeOne(ctx, ep); ok {
			out = append(out, sample)
		}
	}
	return out
}

func (s *Scraper) scrapeOne(ctx context.Context, ep envinfo.Endpoint) (NodeSample, bool) {
	node := nodeLabel(ep)
	cur, err := s.fetch(ctx, ep.HTTP)
	if err != nil {
		return NodeSample{}, false
	}
	now := time.Now()

	const mb = 1024 * 1024
	sample := NodeSample{
		Node:            node,
		Leader:          cur[mLeader] > 0,
		RSSMB:           cur[mRSS] / mb,
		HeapInuseMB:     cur[mHeapInuse] / mb,
		JemallocMB:      cur[mJemalloc] / mb,
		Goroutines:      cur[mGoroutines],
		PendingQueries:  cur[mPending],
		ActiveMutations: cur[mActiveMut],
	}

	s.mu.Lock()
	prev, hadPrev := s.prev[node]
	s.prev[node] = prevState{
		t: now, cpu: cur[mCPU], queries: cur[mQueries], commits: cur[mCommits],
		aborts: cur[mAborts], latSum: cur[mLatSum], latCount: cur[mLatCount],
	}
	s.mu.Unlock()

	if hadPrev {
		dt := now.Sub(prev.t).Seconds()
		if dt > 0 {
			sample.CPUPercent = (cur[mCPU] - prev.cpu) / dt * 100
			sample.QueriesPerSec = rate(cur[mQueries], prev.queries, dt)
			sample.CommitsPerSec = rate(cur[mCommits], prev.commits, dt)
			sample.AbortsPerSec = rate(cur[mAborts], prev.aborts, dt)
		}
		sample.latSumDelta = cur[mLatSum] - prev.latSum
		sample.latCountDelta = cur[mLatCount] - prev.latCount
		if sample.latCountDelta > 0 {
			sample.ServerAvgMs = sample.latSumDelta / sample.latCountDelta
		}
	}
	return sample, true
}

// Aggregate merges per-node samples into a single cluster sample (Node="CLUSTER").
// Gauges and rates are summed; the server latency average is recomputed from the
// merged histogram sum/count so it is not a mean-of-means.
func Aggregate(samples []NodeSample) NodeSample {
	agg := NodeSample{Node: "CLUSTER"}
	for _, s := range samples {
		agg.RSSMB += s.RSSMB
		agg.HeapInuseMB += s.HeapInuseMB
		agg.JemallocMB += s.JemallocMB
		agg.Goroutines += s.Goroutines
		agg.CPUPercent += s.CPUPercent
		agg.QueriesPerSec += s.QueriesPerSec
		agg.CommitsPerSec += s.CommitsPerSec
		agg.AbortsPerSec += s.AbortsPerSec
		agg.PendingQueries += s.PendingQueries
		agg.ActiveMutations += s.ActiveMutations
		agg.latSumDelta += s.latSumDelta
		agg.latCountDelta += s.latCountDelta
		if s.Leader {
			agg.Leader = true
		}
	}
	if agg.latCountDelta > 0 {
		agg.ServerAvgMs = agg.latSumDelta / agg.latCountDelta
	}
	return agg
}

func (s *Scraper) fetch(ctx context.Context, httpBase string) (map[string]float64, error) {
	url := strings.TrimRight(httpBase, "/") + "/debug/prometheus_metrics"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return parseMetrics(body), nil
}

// parseMetrics sums every series of each wanted metric name (ignoring labels).
func parseMetrics(body []byte) map[string]float64 {
	out := make(map[string]float64, len(wantMetrics))
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name := line
		if i := strings.IndexAny(line, "{ "); i >= 0 {
			name = line[:i]
		}
		if !wantMetrics[name] {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if v, err := strconv.ParseFloat(fields[len(fields)-1], 64); err == nil {
			out[name] += v
		}
	}
	return out
}

func rate(cur, prev, dt float64) float64 {
	if cur < prev || dt <= 0 { // counter reset or no interval
		return 0
	}
	return (cur - prev) / dt
}

func nodeLabel(ep envinfo.Endpoint) string {
	if ep.Addr != "" {
		return ep.Addr
	}
	return strings.TrimPrefix(strings.TrimPrefix(ep.HTTP, "http://"), "https://")
}
