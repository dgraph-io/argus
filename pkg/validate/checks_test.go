package validate

import (
	"encoding/json"
	"testing"

	"github.com/dgraph-io/argus/pkg/testdata"
)

// nodeFromPerson builds the personNode that a faithful round-trip would yield.
func nodeFromPerson(p testdata.Person) personNode {
	n := personNode{
		Xid: p.Xid, Name: p.Name, Email: p.Email, Age: p.Age, Score: p.Score,
		Active: p.Active, Bio: p.Bio, Tags: p.Tags, CreatedAt: p.CreatedAt,
	}
	if p.CompanyXid != "" {
		n.Company = &xidRef{Xid: p.CompanyXid}
	}
	for _, f := range p.FriendXids {
		n.Friends = append(n.Friends, xidRef{Xid: f})
	}
	if p.Location != "" {
		var gp geoPoint
		_ = json.Unmarshal([]byte(p.Location), &gp)
		n.Location = &gp
	}
	return n
}

func TestPersonDiffMatches(t *testing.T) {
	d := testdata.SmallDataset()
	for _, i := range []int{0, 1, 7, 42, 150} {
		p := d.Person(i)
		if diff := personDiff(p, nodeFromPerson(p)); diff != "" {
			t.Errorf("Person(%d) should match a faithful round-trip, got diff %q", i, diff)
		}
	}
}

func TestPersonDiffDetectsCorruption(t *testing.T) {
	d := testdata.SmallDataset()
	base := d.Person(0)

	mutate := []struct {
		name string
		f    func(*personNode)
	}{
		{"name", func(n *personNode) { n.Name = "WRONG" }},
		{"age", func(n *personNode) { n.Age += 1 }},
		{"score", func(n *personNode) { n.Score += 0.01 }},
		{"active", func(n *personNode) { n.Active = !n.Active }},
		{"bio", func(n *personNode) { n.Bio = "WRONG" }},
		{"tags", func(n *personNode) { n.Tags = []string{"nope"} }},
		{"company", func(n *personNode) { n.Company = &xidRef{Xid: "company-999"} }},
		{"company-missing", func(n *personNode) { n.Company = nil }},
		{"friends", func(n *personNode) { n.Friends = []xidRef{{Xid: "person-999"}} }},
		{"location", func(n *personNode) { n.Location.Coordinates[0] += 1.0 }},
		{"created_at", func(n *personNode) { n.CreatedAt = "2021-01-01T00:00:00Z" }},
	}
	for _, mc := range mutate {
		n := nodeFromPerson(base)
		mc.f(&n)
		if diff := personDiff(base, n); diff == "" {
			t.Errorf("corruption of %s should have been detected", mc.name)
		}
	}
}

func TestComparisonHelpers(t *testing.T) {
	if !floatEq(18.47, 18.47) || floatEq(18.47, 18.48) {
		t.Error("floatEq")
	}
	if !setEqual([]string{"a", "b", "b"}, []string{"b", "a"}) {
		t.Error("setEqual should treat inputs as sets (dedup, order-insensitive)")
	}
	if setEqual([]string{"a"}, []string{"a", "b"}) {
		t.Error("setEqual should reject differing sets")
	}
	if !timeEqual("2020-01-01T00:00:00Z", "2020-01-01T00:00:00.000Z") {
		t.Error("timeEqual should compare instants, not strings")
	}
	if timeEqual("2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z") {
		t.Error("timeEqual should reject different instants")
	}
}
