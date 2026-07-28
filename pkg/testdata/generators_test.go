package testdata

import (
	"strconv"
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

func TestBulkRecordDeterministic(t *testing.T) {
	d := SmallDataset()
	a, b := d.BulkRecord(7), d.BulkRecord(7)
	if a != b {
		t.Fatalf("BulkRecord(7) not deterministic: %+v vs %+v", a, b)
	}
	if a.Brid != "bulk-7" {
		t.Errorf("unexpected brid %q", a.Brid)
	}
}

func TestBulkRecordInsertsBlind(t *testing.T) {
	d := SmallDataset()
	recs := make([]BulkRecord, d.NumBulkRecords)
	for i := range recs {
		recs[i] = d.BulkRecord(i)
	}
	const group = 140
	// Scalar-only path (no hubs): blind, conflict-free.
	reqs := BulkRecordInserts(recs, nil, nil, group)

	wantBatches := (d.NumBulkRecords + group - 1) / group
	if len(reqs) != wantBatches {
		t.Fatalf("got %d batches, want %d", len(reqs), wantBatches)
	}

	totalNquads := 0
	for _, r := range reqs {
		// Blind insert: no upsert query block, exactly one set mutation, commit-now.
		if r.Query != "" {
			t.Errorf("bulk insert must be blind, got query block: %q", r.Query)
		}
		if !r.CommitNow || len(r.Mutations) != 1 {
			t.Fatalf("unexpected request shape: commitNow=%v mutations=%d", r.CommitNow, len(r.Mutations))
		}
		nq := string(r.Mutations[0].SetNquads)
		if strings.Contains(nq, "uid(") {
			t.Errorf("blind insert should use blank nodes, not uid() refs: %q", nq)
		}
		if !strings.Contains(nq, "_:b") {
			t.Errorf("expected blank-node subjects, got %q", nq)
		}
		totalNquads += strings.Count(nq, " .\n")
	}
	if want := d.NumBulkRecords * NQuadsPerBulkRecord; totalNquads != want {
		t.Errorf("emitted %d nquads, want %d", totalNquads, want)
	}
}

func TestBulkRecordInsertsEdges(t *testing.T) {
	d := SmallDataset() // 500 records, 125 hubs, 4 edges
	recs := make([]BulkRecord, d.NumBulkRecords)
	for i := range recs {
		recs[i] = d.BulkRecord(i)
	}
	hubUIDs := make([]string, d.BulkHubs)
	for h := range hubUIDs {
		hubUIDs[h] = "0x" + strconv.Itoa(h+1) // stand-in captured uids
	}
	reqs := BulkRecordInserts(recs, hubUIDs, d.BulkHubIndices, 140)

	edges := 0
	for _, r := range reqs {
		nq := string(r.Mutations[0].SetNquads)
		edges += strings.Count(nq, "<brhub>")
		// Edges reference real uids, not blank nodes or query vars.
		if strings.Contains(nq, "<brhub> _:") || strings.Contains(nq, "uid(") {
			t.Errorf("brhub edges must target captured uids, got %q", nq)
		}
	}
	if want := d.NumBulkRecords * d.BulkEdges; edges != want {
		t.Errorf("emitted %d brhub edges, want %d", edges, want)
	}
	// Every record's hub set is distinct and the right size.
	for i := 0; i < d.NumBulkRecords; i++ {
		if got := len(d.BulkHubIndices(i)); got != d.BulkEdges {
			t.Fatalf("record %d has %d hub indices, want %d", i, got, d.BulkEdges)
		}
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
