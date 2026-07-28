package envinfo

import "testing"

func TestTopoClass(t *testing.T) {
	cases := []struct {
		groups, replicas int
		want             string
	}{
		{0, 0, "unknown"},
		{1, 1, "single"},
		{1, 3, "HA"},
		{3, 1, "sharded"},
		{3, 3, "sharded+HA"},
	}
	for _, c := range cases {
		got := Info{Groups: c.groups, Replicas: c.replicas}.TopoClass()
		if got != c.want {
			t.Errorf("TopoClass(groups=%d,replicas=%d) = %q, want %q", c.groups, c.replicas, got, c.want)
		}
	}
}

// Learners are read-replicas: they show up in Topology()/NumLearners but must not
// change the HA classification (a 3-voter group with 2 learners is still "HA").
func TestTopologyLearnersDoNotChangeClass(t *testing.T) {
	i := Info{Groups: 1, Replicas: 3, NumAlphas: 5, NumZeros: 3, NumLearners: 2}
	if got := i.TopoClass(); got != "HA" {
		t.Errorf("TopoClass with learners = %q, want HA", got)
	}
	want := "HA (1 group(s) x 3 replica(s); 5 alphas, 3 zeros, 2 learners)"
	if got := i.Topology(); got != want {
		t.Errorf("Topology() = %q, want %q", got, want)
	}
	// No learners → no learner clause.
	if got := (Info{Groups: 1, Replicas: 1, NumAlphas: 1, NumZeros: 1}).Topology(); got != "single (1 group(s) x 1 replica(s); 1 alphas, 1 zeros)" {
		t.Errorf("Topology() without learners = %q", got)
	}
}
