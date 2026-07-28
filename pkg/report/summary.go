// Package report defines the machine-readable per-run summary that is the unit
// of regression comparison, plus the comparison itself. A run emits a summary
// JSON; `argus compare` diffs baseline summaries against candidate summaries and
// renders a verdict.
package report

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/dgraph-io/argus/pkg/envinfo"
	"github.com/dgraph-io/argus/pkg/metrics"
)

// RunSummary is the whole-run result for one measurement.
type RunSummary struct {
	RunID            string              `json:"run_id"` // shared across a run's repeats; keys the artifacts dir
	Env              envinfo.Info        `json:"env"`
	Mode             string              `json:"mode"` // "passes" or "duration"
	Passes           int                 `json:"passes"`
	Repeat           int                 `json:"repeat"`
	Scale            float64             `json:"scale"` // dataset scale factor (0 = default/unset)
	Seed             int64               `json:"seed"`
	WriteConcurrency int                 `json:"write_concurrency"`
	ReadConcurrency  int                 `json:"read_concurrency"`
	Dataset          DatasetInfo         `json:"dataset"`
	StartedAt        string              `json:"started_at"`
	MeasuredSeconds  float64             `json:"measured_seconds"`
	Ops              []metrics.OpSummary `json:"ops"`
	Cluster          ClusterSummary      `json:"cluster"`
	BulkLoad         *BulkLoadSummary    `json:"bulk_load,omitempty"`
}

// BulkLoadSummary captures the blind bulk-load phase: a live-loader-shaped run
// of large, conflict-free insert batches. NQuadsPerSec is the headline number
// (triples/s), the dimension where server-side parallel mutations should move.
type BulkLoadSummary struct {
	Records        int     `json:"records"`
	NQuads         int     `json:"nquads"`
	BatchRecords   int     `json:"batch_records"`
	Hubs           int     `json:"hubs"` // shared @reverse target nodes (0 = scalar-only)
	EdgesPerRecord int     `json:"edges_per_record"`
	Seconds        float64 `json:"seconds"`
	NQuadsPerSec   float64 `json:"nquads_per_s"`
	RecordsPerSec  float64 `json:"records_per_s"`
	BatchP50Ms     float64 `json:"batch_p50_ms"`
	BatchP99Ms     float64 `json:"batch_p99_ms"`
	Retries        int     `json:"retries"` // conflict retries during the fan-in load
	Errors         int     `json:"errors"`
}

// DatasetInfo records the corpus sizes, so a comparison can refuse runs whose
// workloads differ.
type DatasetInfo struct {
	Persons    int `json:"persons"`
	Posts      int `json:"posts"`
	Products   int `json:"products"`
	Reviews    int `json:"reviews"`
	Companies  int `json:"companies"`
	Categories int `json:"categories"`
	Dogs       int `json:"dogs"`
	Cats       int `json:"cats"`
}

// ClusterSummary is the steady-state server-side aggregate across all alphas
// (median of the CLUSTER rows over the run).
type ClusterSummary struct {
	Nodes           int     `json:"nodes"`
	RSSMBTotal      float64 `json:"rss_mb_total"`
	JemallocMBTotal float64 `json:"jemalloc_mb_total"` // off-heap (jemalloc); part of RSS the Go heap profile can't see
	CPUPercentTotal float64 `json:"cpu_pct_total"`
	GoroutinesTotal float64 `json:"goroutines_total"`
	QueriesPerSec   float64 `json:"queries_per_s"`
	CommitsPerSec   float64 `json:"commits_per_s"`
	AbortsPerSec    float64 `json:"aborts_per_s"`
	ServerAvgMs     float64 `json:"server_avg_ms"`
	// WriteBalanceCV is the coefficient of variation (stddev/mean) of per-alpha
	// mean active-mutations at steady state: ~0 = writes evenly spread across
	// alphas, high = concentrated on few. Only meaningful for sharded configs
	// (for HA, writes go to the group leader, so a high CV is expected, not a
	// problem). 0 when fewer than 2 alphas.
	WriteBalanceCV float64 `json:"write_balance_cv"`
}

// WriteJSON writes a summary to path.
func WriteJSON(path string, s *RunSummary) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// Load reads a summary from path.
func Load(path string) (*RunSummary, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s RunSummary
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &s, nil
}

// op returns the OpSummary for key, or nil.
func (s *RunSummary) op(key string) *metrics.OpSummary {
	for i := range s.Ops {
		if s.Ops[i].Key == key {
			return &s.Ops[i]
		}
	}
	return nil
}
