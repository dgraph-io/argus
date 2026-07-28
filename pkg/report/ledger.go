package report

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// The ledger is a single append-only JSONL file: one line per repeat, each line
// a compact RunSummary. It replaces the per-run summary-N.json files so the run
// matrix (version × hardware × topology × tuning) lives in one committed file
// instead of a folder per scenario. `argus compare` and `argus ls` read it.

// AppendRun appends one RunSummary as a single JSONL line to path (created if
// absent). One Write of the full line keeps concurrent appends from interleaving
// for the line sizes we produce.
func AppendRun(path string, s *RunSummary) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return nil
}

// LoadLedger reads every non-blank line of a JSONL ledger into RunSummaries.
func LoadLedger(path string) ([]*RunSummary, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []*RunSummary
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 256*1024), 8*1024*1024) // ops arrays make lines large
	line := 0
	for sc.Scan() {
		line++
		b := strings.TrimSpace(sc.Text())
		if b == "" {
			continue
		}
		var s RunSummary
		if err := json.Unmarshal([]byte(b), &s); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, line, err)
		}
		out = append(out, &s)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// MatchRuns returns every ledger line matching sel — a run_id (exact, else
// prefix) or a "k=v[,k=v]" filter over the fields in fieldOf. An empty sel
// matches everything (used by `argus ls`). It does not enforce uniqueness.
func MatchRuns(runs []*RunSummary, sel string) ([]*RunSummary, error) {
	sel = strings.TrimSpace(sel)
	if sel == "" {
		return runs, nil
	}

	var match func(*RunSummary) bool
	if strings.Contains(sel, "=") {
		filters, err := parseFilter(sel)
		if err != nil {
			return nil, err
		}
		match = func(s *RunSummary) bool { return matchesFilter(s, filters) }
	} else {
		// run_id: exact wins; otherwise prefix.
		match = func(s *RunSummary) bool { return s.RunID == sel }
		if countRuns(runs, match) == 0 {
			match = func(s *RunSummary) bool { return strings.HasPrefix(s.RunID, sel) }
		}
	}

	var picked []*RunSummary
	for _, s := range runs {
		if match(s) {
			picked = append(picked, s)
		}
	}
	return picked, nil
}

// SelectByRun resolves sel to exactly one run's repeat lines, erroring — and
// listing the candidates — if the selection is empty or spans more than one
// distinct run_id, so a comparison never silently mixes two runs as repeats.
func SelectByRun(runs []*RunSummary, sel string) ([]*RunSummary, error) {
	if strings.TrimSpace(sel) == "" {
		return nil, fmt.Errorf("empty run selector")
	}
	picked, err := MatchRuns(runs, sel)
	if err != nil {
		return nil, err
	}
	ids := map[string]bool{}
	for _, s := range picked {
		ids[s.RunID] = true
	}
	switch len(ids) {
	case 1:
		return picked, nil
	case 0:
		return nil, fmt.Errorf("no run matches %q", sel)
	default:
		return nil, fmt.Errorf("selector %q is ambiguous — matches %d runs: %s", sel, len(ids), strings.Join(sortedKeys(ids), ", "))
	}
}

// ArtifactsDir is where a run's ephemeral profiles + interval CSVs live, keyed off
// the ledger location and the run id: <dir(ledger)>/artifacts/<run-id>.
func ArtifactsDir(ledgerPath, runID string) string {
	return filepath.Join(filepath.Dir(ledgerPath), "artifacts", runID)
}

type filter struct{ key, val string }

func parseFilter(sel string) ([]filter, error) {
	var out []filter
	for _, part := range strings.Split(sel, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("bad filter term %q (want key=value)", part)
		}
		out = append(out, filter{strings.TrimSpace(k), strings.TrimSpace(v)})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty filter")
	}
	return out, nil
}

// fieldOf exposes the run fields a filter can match, lowercased for comparison.
func fieldOf(s *RunSummary, key string) (string, bool) {
	switch key {
	case "run_id":
		return s.RunID, true
	case "version", "dgraph_version":
		return s.Env.DgraphVersion, true
	case "branch", "dgraph_branch":
		return s.Env.DgraphBranch, true
	case "commit", "dgraph_commit":
		return s.Env.DgraphCommit, true
	case "cpu_class", "cpu":
		return s.Env.CPUClass, true
	case "disk_class", "disk":
		return s.Env.DiskClass, true
	case "deploy":
		return s.Env.Deploy, true
	case "topology":
		return s.Env.Topology(), true
	case "learners", "num_learners":
		return fmt.Sprintf("%d", s.Env.NumLearners), true
	case "scale":
		return strconvFloat(s.Scale), true
	case "wc", "write_concurrency":
		return fmt.Sprintf("%d", s.WriteConcurrency), true
	case "label", "labels":
		return s.Env.Labels, true
	}
	return "", false
}

func matchesFilter(s *RunSummary, filters []filter) bool {
	for _, f := range filters {
		got, ok := fieldOf(s, f.key)
		if !ok {
			return false
		}
		// Substring, case-insensitive: lets `version=v25.3.8` match the
		// commit-suffixed `v25.3.8-preview1-...` and `cpu=M4` match "Apple M4".
		if !strings.Contains(strings.ToLower(got), strings.ToLower(f.val)) {
			return false
		}
	}
	return true
}

func countRuns(runs []*RunSummary, match func(*RunSummary) bool) int {
	ids := map[string]bool{}
	for _, s := range runs {
		if match(s) {
			ids[s.RunID] = true
		}
	}
	return len(ids)
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func strconvFloat(f float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.3f", f), "0"), ".")
}
