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
	Env             envinfo.Info        `json:"env"`
	Mode            string              `json:"mode"` // "passes" or "duration"
	Passes          int                 `json:"passes"`
	Repeat          int                 `json:"repeat"`
	Seed            int64               `json:"seed"`
	StartedAt       string              `json:"started_at"`
	MeasuredSeconds float64             `json:"measured_seconds"`
	Ops             []metrics.OpSummary `json:"ops"`
	Cluster         ClusterSummary      `json:"cluster"`
}

// ClusterSummary is the steady-state server-side aggregate across all alphas
// (median of the CLUSTER rows over the run).
type ClusterSummary struct {
	Nodes           int     `json:"nodes"`
	RSSMBTotal      float64 `json:"rss_mb_total"`
	CPUPercentTotal float64 `json:"cpu_pct_total"`
	GoroutinesTotal float64 `json:"goroutines_total"`
	QueriesPerSec   float64 `json:"queries_per_s"`
	CommitsPerSec   float64 `json:"commits_per_s"`
	AbortsPerSec    float64 `json:"aborts_per_s"`
	ServerAvgMs     float64 `json:"server_avg_ms"`
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
