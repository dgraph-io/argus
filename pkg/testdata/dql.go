package testdata

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dgraph-io/dgo/v250/protos/api"
)

// BulkEdgeTarget is the approximate number of nquads (edges) packed into a
// single bulk upsert mutation. A bulk batch is flushed once it reaches this.
const BulkEdgeTarget = 120

// upsertBuilder accumulates one upsert transaction: a query block binding each
// referenced xid to a var, and a set of nquads. Multiple entities can be added
// to the same builder to form a bulk mutation; a shared xid resolves to one
// var, so cross-references within the batch stay consistent and idempotent.
type upsertBuilder struct {
	vars  map[string]string
	query strings.Builder
	set   strings.Builder
	n     int
	edges int
}

func newUpsertBuilder() *upsertBuilder {
	b := &upsertBuilder{vars: make(map[string]string)}
	b.query.WriteString("query {\n")
	return b
}

// varFor returns the query var bound to xid, registering it on first use.
func (b *upsertBuilder) varFor(xid string) string {
	if v, ok := b.vars[xid]; ok {
		return v
	}
	v := fmt.Sprintf("v%d", b.n)
	b.n++
	b.vars[xid] = v
	fmt.Fprintf(&b.query, "  %s as var(func: eq(xid, %q))\n", v, xid)
	return v
}

func (b *upsertBuilder) build() *api.Request {
	b.query.WriteString("}")
	return &api.Request{
		Query:     b.query.String(),
		Mutations: []*api.Mutation{{SetNquads: []byte(b.set.String())}},
		CommitNow: true,
	}
}

// --- entity nquad emitters ---

func (b *upsertBuilder) addPerson(p Person) {
	v := ref(b.varFor(p.Xid))
	b.str(v, "xid", p.Xid)
	b.raw(v, "dgraph.type", `"Person"`)
	b.str(v, "name", p.Name)
	b.str(v, "email", p.Email)
	b.intVal(v, "age", p.Age)
	b.floatVal(v, "score", p.Score)
	b.boolVal(v, "active", p.Active)
	b.str(v, "bio", p.Bio)
	for _, t := range p.Tags {
		b.str(v, "tags_csv", t)
	}
	if p.Location != "" {
		b.geo(v, "location", p.Location)
	}
	if len(p.Embedding) > 0 {
		b.vector(v, "embedding", p.Embedding)
	}
	b.dateTime(v, "created_at", p.CreatedAt)
	if p.CompanyXid != "" {
		c := ref(b.varFor(p.CompanyXid))
		b.raw(v, "company", c)
		b.str(c, "xid", p.CompanyXid)
		b.raw(c, "dgraph.type", `"Company"`)
	}
	for _, f := range p.FriendXids {
		fr := ref(b.varFor(f))
		b.raw(v, "friends", fr)
		b.str(fr, "xid", f)
		b.raw(fr, "dgraph.type", `"Person"`)
	}
}

func (b *upsertBuilder) addCompany(c Company) {
	v := ref(b.varFor(c.Xid))
	b.str(v, "xid", c.Xid)
	b.raw(v, "dgraph.type", `"Company"`)
	b.str(v, "name", c.Name)
	b.str(v, "domain", c.Domain)
	b.dateTime(v, "founded", c.Founded)
	if c.Territory != "" {
		b.geo(v, "territory", c.Territory)
	}
}

func (b *upsertBuilder) addPost(p Post) {
	v := ref(b.varFor(p.Xid))
	b.str(v, "xid", p.Xid)
	b.raw(v, "dgraph.type", `"Post"`)
	b.str(v, "title", p.Title)
	b.str(v, "content", p.Content)
	b.intVal(v, "likes", p.Likes)
	b.dateTime(v, "created_at", p.CreatedAt)
	a := ref(b.varFor(p.AuthorXid))
	b.raw(v, "author", a)
	b.str(a, "xid", p.AuthorXid)
	b.raw(a, "dgraph.type", `"Person"`)
}

// --- single-op upserts ---

// PersonUpsert builds an idempotent single-record upsert for one person.
func PersonUpsert(p Person) *api.Request {
	b := newUpsertBuilder()
	b.addPerson(p)
	return b.build()
}

// CompanyUpsert ensures a company exists with full attributes.
func CompanyUpsert(c Company) *api.Request {
	b := newUpsertBuilder()
	b.addCompany(c)
	return b.build()
}

// PostUpsert ensures a post exists and links its author by xid.
func PostUpsert(p Post) *api.Request {
	b := newUpsertBuilder()
	b.addPost(p)
	return b.build()
}

// --- bulk upserts ---

// BulkPersonUpserts packs persons into upsert requests of roughly edgeTarget
// nquads each, producing fewer, larger transactions than the single-op path.
func BulkPersonUpserts(persons []Person, edgeTarget int) []*api.Request {
	return bulk(len(persons), edgeTarget, func(b *upsertBuilder, i int) { b.addPerson(persons[i]) })
}

// BulkPostUpserts packs posts into upsert requests of roughly edgeTarget nquads.
func BulkPostUpserts(posts []Post, edgeTarget int) []*api.Request {
	return bulk(len(posts), edgeTarget, func(b *upsertBuilder, i int) { b.addPost(posts[i]) })
}

func bulk(count, edgeTarget int, add func(*upsertBuilder, int)) []*api.Request {
	if edgeTarget < 1 {
		edgeTarget = BulkEdgeTarget
	}
	var reqs []*api.Request
	b := newUpsertBuilder()
	for i := 0; i < count; i++ {
		add(b, i)
		if b.edges >= edgeTarget {
			reqs = append(reqs, b.build())
			b = newUpsertBuilder()
		}
	}
	if b.edges > 0 {
		reqs = append(reqs, b.build())
	}
	return reqs
}

// BulkGroupSize is the default number of records packed into one blind bulk-load
// mutation. At NQuadsPerBulkRecord nquads each, ~140 records ≈ ~980 nquads per
// mutation — close to the live loader's default batch of 1000, so server-side
// parallel mutations get a comparable amount of index-apply work to fan out.
const BulkGroupSize = 140

// NQuadsPerBulkRecord is the fixed scalar nquad count one BulkRecord emits
// (brid, dgraph.type, brname, brnum, brscore, brtext, brflag). brhub edges, when
// present, add BulkEdges nquads on top of this.
const NQuadsPerBulkRecord = 7

// BulkHubInserts builds blind insert mutations that create numHubs BulkHub
// nodes, chunked groupSize per mutation. Blank-node names are "h{global index}"
// so the caller can merge each response's uid map into one hub->uid table.
func BulkHubInserts(numHubs, groupSize int) []*api.Request {
	if groupSize < 1 {
		groupSize = BulkGroupSize
	}
	var reqs []*api.Request
	var set strings.Builder
	n := 0
	flush := func() {
		if n == 0 {
			return
		}
		reqs = append(reqs, &api.Request{
			Mutations: []*api.Mutation{{SetNquads: []byte(set.String())}},
			CommitNow: true,
		})
		set.Reset()
		n = 0
	}
	for h := 0; h < numHubs; h++ {
		fmt.Fprintf(&set, "_:h%d <bhid> %q .\n", h, BulkHubXid(h))
		fmt.Fprintf(&set, "_:h%d <dgraph.type> \"BulkHub\" .\n", h)
		n++
		if n >= groupSize {
			flush()
		}
	}
	flush()
	return reqs
}

// BulkRecordInserts packs records into blind insert mutations of groupSize
// records each — no upsert query block, fresh blank nodes, so records never
// read-modify-write. When hubUIDs and hubIndices are supplied, each record (by
// its slice index, which equals its global index) also gets brhub edges to the
// named hubs by their captured uid, exercising @reverse posting-list apply.
// Passing nil hubIndices yields the scalar-only (conflict-free) path.
func BulkRecordInserts(records []BulkRecord, hubUIDs []string, hubIndices func(i int) []int, groupSize int) []*api.Request {
	if groupSize < 1 {
		groupSize = BulkGroupSize
	}
	var reqs []*api.Request
	var set strings.Builder
	n := 0
	flush := func() {
		if n == 0 {
			return
		}
		reqs = append(reqs, &api.Request{
			Mutations: []*api.Mutation{{SetNquads: []byte(set.String())}},
			CommitNow: true,
		})
		set.Reset()
		n = 0
	}
	for i, rec := range records {
		s := fmt.Sprintf("_:b%d", n)
		fmt.Fprintf(&set, "%s <brid> %q .\n", s, rec.Brid)
		fmt.Fprintf(&set, "%s <dgraph.type> \"BulkRecord\" .\n", s)
		fmt.Fprintf(&set, "%s <brname> \"%s\" .\n", s, escapeRDF(rec.Name))
		fmt.Fprintf(&set, "%s <brnum> \"%d\"^^<xs:int> .\n", s, rec.Num)
		fmt.Fprintf(&set, "%s <brscore> \"%g\"^^<xs:float> .\n", s, rec.Score)
		fmt.Fprintf(&set, "%s <brtext> \"%s\" .\n", s, escapeRDF(rec.Text))
		fmt.Fprintf(&set, "%s <brflag> \"%t\"^^<xs:boolean> .\n", s, rec.Flag)
		if hubIndices != nil && len(hubUIDs) > 0 {
			for _, h := range hubIndices(i) {
				if h >= 0 && h < len(hubUIDs) {
					fmt.Fprintf(&set, "%s <brhub> <%s> .\n", s, hubUIDs[h])
				}
			}
		}
		n++
		if n >= groupSize {
			flush()
		}
	}
	flush()
	return reqs
}

// DeleteByXid deletes the node with the given xid and all its predicates.
// Idempotent: deleting a missing node is a no-op.
func DeleteByXid(xid string) *api.Request {
	q := fmt.Sprintf("query { v as var(func: eq(xid, %q)) }", xid)
	return &api.Request{
		Query:     q,
		Mutations: []*api.Mutation{{DelNquads: []byte("uid(v) * * .")}},
		CommitNow: true,
	}
}

// --- nquad writers (each increments the edge count) ---

func ref(v string) string { return "uid(" + v + ")" }

func (b *upsertBuilder) str(subj, pred, val string) {
	fmt.Fprintf(&b.set, "%s <%s> \"%s\" .\n", subj, pred, escapeRDF(val))
	b.edges++
}

func (b *upsertBuilder) raw(subj, pred, objOrRef string) {
	fmt.Fprintf(&b.set, "%s <%s> %s .\n", subj, pred, objOrRef)
	b.edges++
}

func (b *upsertBuilder) intVal(subj, pred string, v int) {
	fmt.Fprintf(&b.set, "%s <%s> \"%d\"^^<xs:int> .\n", subj, pred, v)
	b.edges++
}

func (b *upsertBuilder) floatVal(subj, pred string, v float64) {
	fmt.Fprintf(&b.set, "%s <%s> \"%g\"^^<xs:float> .\n", subj, pred, v)
	b.edges++
}

func (b *upsertBuilder) boolVal(subj, pred string, v bool) {
	fmt.Fprintf(&b.set, "%s <%s> \"%t\"^^<xs:boolean> .\n", subj, pred, v)
	b.edges++
}

func (b *upsertBuilder) dateTime(subj, pred, iso string) {
	fmt.Fprintf(&b.set, "%s <%s> \"%s\"^^<xs:dateTime> .\n", subj, pred, iso)
	b.edges++
}

func (b *upsertBuilder) geo(subj, pred, geojson string) {
	fmt.Fprintf(&b.set, "%s <%s> \"%s\"^^<geo:geojson> .\n", subj, pred, escapeRDF(geojson))
	b.edges++
}

func (b *upsertBuilder) vector(subj, pred string, v []float32) {
	fmt.Fprintf(&b.set, "%s <%s> %q .\n", subj, pred, FormatVector(v))
	b.edges++
}

// FormatVector renders a vector as Dgraph's "[v0, v1, ...]" literal (without
// surrounding quotes), the form accepted both in RDF values and as the
// similar_to query argument.
func FormatVector(v []float32) string {
	parts := make([]string, len(v))
	for i, f := range v {
		parts[i] = strconv.FormatFloat(float64(f), 'g', -1, 32)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func escapeRDF(s string) string {
	r := strings.NewReplacer(
		`\`, `\\`,
		`"`, `\"`,
		"\n", `\n`,
		"\r", `\r`,
		"\t", `\t`,
	)
	return r.Replace(s)
}
