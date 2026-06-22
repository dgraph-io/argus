package testdata

import (
	"fmt"
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
