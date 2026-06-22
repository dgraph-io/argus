package testdata

import (
	"strings"
	"testing"
)

func TestPersonDeterministic(t *testing.T) {
	d := SmallDataset()
	a := d.Person(3)
	b := d.Person(3)
	if a.Name != b.Name || a.Age != b.Age || a.Score != b.Score || a.Email != b.Email || a.CreatedAt != b.CreatedAt {
		t.Fatalf("Person(3) not deterministic: %+v vs %+v", a, b)
	}
	if a.Xid != "person-3" {
		t.Errorf("unexpected xid %q", a.Xid)
	}
	if len(a.FriendXids) != 2 {
		t.Errorf("expected 2 friends, got %d", len(a.FriendXids))
	}
}

func TestPersonUpsertRDF(t *testing.T) {
	d := SmallDataset()
	req := PersonUpsert(d.Person(0))
	if len(req.Mutations) != 1 {
		t.Fatalf("expected 1 mutation, got %d", len(req.Mutations))
	}
	nq := string(req.Mutations[0].SetNquads)

	// Regression: booleans must use xs:boolean, not xs:bool.
	if !strings.Contains(nq, "^^<xs:boolean>") {
		t.Errorf("active should serialize as xs:boolean; nquads:\n%s", nq)
	}
	if strings.Contains(nq, "^^<xs:bool>") {
		t.Errorf("found invalid xs:bool RDF type; nquads:\n%s", nq)
	}
	for _, want := range []string{`<xid> "person-0"`, `<dgraph.type> "Person"`, `<company> uid(`, `<friends> uid(`} {
		if !strings.Contains(nq, want) {
			t.Errorf("missing %q in nquads:\n%s", want, nq)
		}
	}
	if !req.CommitNow {
		t.Error("upsert request should set CommitNow")
	}
}

func TestBulkPersonUpserts(t *testing.T) {
	d := SmallDataset()
	persons := make([]Person, 0, d.NumPersons/2)
	for i := 1; i < d.NumPersons; i += 2 { // odd indices, as the scheduler routes them
		persons = append(persons, d.Person(i))
	}
	reqs := BulkPersonUpserts(persons, BulkEdgeTarget)
	if len(reqs) < 2 {
		t.Fatalf("expected multiple bulk requests, got %d", len(reqs))
	}
	// Each batch (except possibly the last) should carry roughly BulkEdgeTarget
	// edges: count nquad lines and confirm batching actually happened.
	first := strings.Count(string(reqs[0].Mutations[0].SetNquads), " .\n")
	if first < BulkEdgeTarget || first > BulkEdgeTarget+40 {
		t.Errorf("first bulk batch has %d edges, want ~%d", first, BulkEdgeTarget)
	}
	// A bulk batch packs many distinct persons: it must reference several xids.
	q := reqs[0].Query
	if strings.Count(q, "as var(func: eq(xid,") < 4 {
		t.Errorf("bulk query block should bind several xids:\n%s", q)
	}
}

func TestEphemeralIsolated(t *testing.T) {
	d := SmallDataset()
	e := d.EphPerson(0)
	if e.Xid != "person-eph-0" {
		t.Errorf("unexpected ephemeral xid %q", e.Xid)
	}
	if e.CompanyXid != "" || len(e.FriendXids) != 0 {
		t.Errorf("ephemeral person should be edge-isolated, got company=%q friends=%v", e.CompanyXid, e.FriendXids)
	}
}

func TestGraphQLUpsertShape(t *testing.T) {
	d := SmallDataset()
	m := AddProduct(d.Product(0))
	if !strings.Contains(m.Query, "upsert: true") {
		t.Errorf("addProduct must use upsert:true for idempotency: %s", m.Query)
	}
	input, ok := m.Vars["input"].([]map[string]interface{})
	if !ok || len(input) != 1 {
		t.Fatalf("unexpected input vars: %#v", m.Vars["input"])
	}
	if input[0]["sku"] != "SKU-0" {
		t.Errorf("unexpected sku %v", input[0]["sku"])
	}
}
