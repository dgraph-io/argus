package testdata

import (
	"fmt"
	"strings"

	"github.com/dgraph-io/dgo/v250/protos/api"
)

// PersonUpsert builds an idempotent upsert that creates/updates a person and
// connects its company and friend edges by xid. Friend/company placeholders
// receive their xid and type so later passes resolve to the same nodes.
func PersonUpsert(p Person) *api.Request {
	var q strings.Builder
	q.WriteString("query {\n")
	fmt.Fprintf(&q, "  v as var(func: eq(xid, %q))\n", p.Xid)
	if p.CompanyXid != "" {
		fmt.Fprintf(&q, "  c as var(func: eq(xid, %q))\n", p.CompanyXid)
	}
	for i, f := range p.FriendXids {
		fmt.Fprintf(&q, "  f%d as var(func: eq(xid, %q))\n", i, f)
	}
	q.WriteString("}")

	var s strings.Builder
	writeStr(&s, "uid(v)", "xid", p.Xid)
	writeRaw(&s, "uid(v)", "dgraph.type", `"Person"`)
	writeStr(&s, "uid(v)", "name", p.Name)
	writeStr(&s, "uid(v)", "email", p.Email)
	writeInt(&s, "uid(v)", "age", p.Age)
	writeFloat(&s, "uid(v)", "score", p.Score)
	writeBool(&s, "uid(v)", "active", p.Active)
	writeStr(&s, "uid(v)", "bio", p.Bio)
	for _, t := range p.Tags {
		writeStr(&s, "uid(v)", "tags_csv", t)
	}
	writeDateTime(&s, "uid(v)", "created_at", p.CreatedAt)
	if p.CompanyXid != "" {
		writeRaw(&s, "uid(v)", "company", "uid(c)")
		writeStr(&s, "uid(c)", "xid", p.CompanyXid)
		writeRaw(&s, "uid(c)", "dgraph.type", `"Company"`)
	}
	for i, f := range p.FriendXids {
		ref := fmt.Sprintf("uid(f%d)", i)
		writeRaw(&s, "uid(v)", "friends", ref)
		writeStr(&s, ref, "xid", f)
		writeRaw(&s, ref, "dgraph.type", `"Person"`)
	}

	return upsert(q.String(), s.String())
}

// CompanyUpsert ensures a company exists with full attributes.
func CompanyUpsert(c Company) *api.Request {
	q := fmt.Sprintf("query { v as var(func: eq(xid, %q)) }", c.Xid)
	var s strings.Builder
	writeStr(&s, "uid(v)", "xid", c.Xid)
	writeRaw(&s, "uid(v)", "dgraph.type", `"Company"`)
	writeStr(&s, "uid(v)", "name", c.Name)
	writeStr(&s, "uid(v)", "domain", c.Domain)
	writeDateTime(&s, "uid(v)", "founded", c.Founded)
	return upsert(q, s.String())
}

// PostUpsert ensures a post exists and links its author by xid.
func PostUpsert(p Post) *api.Request {
	q := fmt.Sprintf("query {\n  v as var(func: eq(xid, %q))\n  a as var(func: eq(xid, %q))\n}", p.Xid, p.AuthorXid)
	var s strings.Builder
	writeStr(&s, "uid(v)", "xid", p.Xid)
	writeRaw(&s, "uid(v)", "dgraph.type", `"Post"`)
	writeStr(&s, "uid(v)", "title", p.Title)
	writeStr(&s, "uid(v)", "content", p.Content)
	writeInt(&s, "uid(v)", "likes", p.Likes)
	writeDateTime(&s, "uid(v)", "created_at", p.CreatedAt)
	writeRaw(&s, "uid(v)", "author", "uid(a)")
	writeStr(&s, "uid(a)", "xid", p.AuthorXid)
	writeRaw(&s, "uid(a)", "dgraph.type", `"Person"`)
	return upsert(q, s.String())
}

// DeleteByXid deletes the node with the given xid and all its predicates.
// It is idempotent: deleting a missing node is a no-op.
func DeleteByXid(xid string) *api.Request {
	q := fmt.Sprintf("query { v as var(func: eq(xid, %q)) }", xid)
	return &api.Request{
		Query:     q,
		Mutations: []*api.Mutation{{DelNquads: []byte("uid(v) * * .")}},
		CommitNow: true,
	}
}

func upsert(query, setNquads string) *api.Request {
	return &api.Request{
		Query:     query,
		Mutations: []*api.Mutation{{SetNquads: []byte(setNquads)}},
		CommitNow: true,
	}
}

// --- RDF nquad writers ---

func writeStr(b *strings.Builder, subj, pred, val string) {
	fmt.Fprintf(b, "%s <%s> \"%s\" .\n", subj, pred, escapeRDF(val))
}

func writeRaw(b *strings.Builder, subj, pred, objOrRef string) {
	fmt.Fprintf(b, "%s <%s> %s .\n", subj, pred, objOrRef)
}

func writeInt(b *strings.Builder, subj, pred string, v int) {
	fmt.Fprintf(b, "%s <%s> \"%d\"^^<xs:int> .\n", subj, pred, v)
}

func writeFloat(b *strings.Builder, subj, pred string, v float64) {
	fmt.Fprintf(b, "%s <%s> \"%g\"^^<xs:float> .\n", subj, pred, v)
}

func writeBool(b *strings.Builder, subj, pred string, v bool) {
	fmt.Fprintf(b, "%s <%s> \"%t\"^^<xs:boolean> .\n", subj, pred, v)
}

func writeDateTime(b *strings.Builder, subj, pred, iso string) {
	fmt.Fprintf(b, "%s <%s> \"%s\"^^<xs:dateTime> .\n", subj, pred, iso)
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
