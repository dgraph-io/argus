package harness

import (
	"math"
	"testing"
)

func TestWriteBalanceCV(t *testing.T) {
	cases := []struct {
		name    string
		perNode map[string][]float64
		want    float64 // -1 means "just assert > threshold"
		thresh  float64
	}{
		{"single node -> 0", map[string][]float64{"a": {10, 10, 10}}, 0, 0},
		{"perfectly balanced -> 0", map[string][]float64{
			"a": {10, 10, 10}, "b": {10, 10, 10}, "c": {10, 10, 10}}, 0, 0},
		{"all-on-one -> high", map[string][]float64{
			"a": {30, 30, 30}, "b": {0, 0, 0}, "c": {0, 0, 0}}, math.Sqrt(200) / 10, 0},
		{"no samples -> 0", map[string][]float64{}, 0, 0},
	}
	for _, c := range cases {
		o := &Orchestrator{perNodeMut: c.perNode}
		got := o.writeBalanceCV()
		if math.Abs(got-c.want) > 1e-9 {
			t.Errorf("%s: writeBalanceCV = %.6f, want %.6f", c.name, got, c.want)
		}
	}
	// Sanity: concentrated must be much larger than a mild imbalance.
	mild := (&Orchestrator{perNodeMut: map[string][]float64{"a": {12, 12}, "b": {10, 10}, "c": {8, 8}}}).writeBalanceCV()
	conc := (&Orchestrator{perNodeMut: map[string][]float64{"a": {30, 30}, "b": {0, 0}, "c": {0, 0}}}).writeBalanceCV()
	if !(conc > mild*3) {
		t.Errorf("expected concentrated CV (%.3f) >> mild CV (%.3f)", conc, mild)
	}
}
