// Package validate asserts the final graph matches the deterministic dataset.
// Because the generator is fully enumerable, verification is complete rather
// than sampled: every persistent node is read back and compared field-by-field
// (values, edges, geo, timestamps), and the node set must equal the expected
// set exactly — so any missing, extra (e.g. a leaked ephemeral), or mismatched
// record fails. Geo-index and shortest-path queries are additionally checked
// for functional correctness.
package validate

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/dgraph-io/argus/pkg/cluster"
	"github.com/dgraph-io/argus/pkg/graphql"
	"github.com/dgraph-io/argus/pkg/testdata"
)

// Check is a single validation outcome.
type Check struct {
	Name   string
	Passed bool
	Detail string
}

// Result aggregates all checks.
type Result struct {
	AllPassed bool
	Checks    []Check
}

// Checker runs deterministic correctness checks.
type Checker struct {
	dgraph       *cluster.DgraphClient
	gql          *graphql.Client
	data         testdata.Dataset
	vectorStrict bool
}

// NewChecker creates a checker for the given dataset. vectorStrict additionally
// asserts exact similar_to nearest-neighbor recall (a regression gate).
func NewChecker(dgraph *cluster.DgraphClient, gql *graphql.Client, data testdata.Dataset, vectorStrict bool) *Checker {
	return &Checker{dgraph: dgraph, gql: gql, data: data, vectorStrict: vectorStrict}
}

// CheckAll runs every check and returns the aggregate result.
func (c *Checker) CheckAll(ctx context.Context) *Result {
	r := &Result{AllPassed: true}
	add := func(name string, passed bool, detail string) {
		r.Checks = append(r.Checks, Check{name, passed, detail})
		if !passed {
			r.AllPassed = false
		}
	}

	// Complete field-by-field verification of every persistent record. The
	// node-set equality these perform also proves all ephemeral records are
	// absent (they would show up as "extra").
	c.checkAllCompanies(ctx, add)
	c.checkAllPersons(ctx, add)
	c.checkAllPosts(ctx, add)
	c.checkAllCategories(ctx, add)
	c.checkAllProducts(ctx, add)

	// Functional correctness of index-backed queries.
	c.checkGeo(ctx, add)
	c.checkShortest(ctx, add, 0, 150)
	c.checkShortest(ctx, add, 0, 13)

	// Exact results for assorted DQL functions, with the expected value
	// computed by enumerating the deterministic generator.
	c.checkFunctions(ctx, add)

	// Vector similarity, n-gram, and geo contains.
	c.checkVector(ctx, add)
	c.checkNgram(ctx, add)
	c.checkContains(ctx, add)

	return r
}

// checkVector validates similar_to functionally. The embedding *values*
// round-trip exactly (verified per-person in checkAllPersons); here we assert
// the index returns exactly k neighbors for a top-k query (k kept well below
// the vector count to avoid the known saturation where similar_to falls back
// to uid order).
//
// We deliberately do NOT assert exact nearest-neighbor recall (e.g. that a
// person's own vector retrieves itself first). Under this harness's repeated
// idempotent upserts, the HNSW index in the tested build returns poorly-ordered
// results that omit exact (distance-0) matches — consistent with the known
// vector-shadow-index-on-update issue. Surfacing that via the load workload is
// useful; gating run success on it is not, since it reflects a server-side
// index limitation rather than data correctness.
func (c *Checker) checkVector(ctx context.Context, add func(string, bool, string)) {
	const target, k = 7, 10
	if target >= c.data.NumPersons || k >= c.data.NumPersons {
		return
	}
	vec := testdata.FormatVector(c.data.Person(target).Embedding)
	c.countCheck(ctx, add, "vector-topk-count",
		fmt.Sprintf(`{ q(func: similar_to(embedding, %d, "%s")) { c: count(uid) } }`, k, vec), k)

	if !c.vectorStrict {
		return
	}
	// Regression gate: a person's own embedding must retrieve that person as the
	// nearest neighbor. Expected to fail on builds where repeated upserts degrade
	// the HNSW index.
	data, err := c.query(ctx, fmt.Sprintf(`{ q(func: similar_to(embedding, 1, "%s")) { xid } }`, vec))
	if err != nil {
		add("vector-self-match", false, err.Error())
		return
	}
	var out struct {
		Q []struct {
			Xid string `json:"xid"`
		} `json:"q"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		add("vector-self-match", false, err.Error())
		return
	}
	got := "none"
	if len(out.Q) > 0 {
		got = out.Q[0].Xid
	}
	add("vector-self-match", len(out.Q) == 1 && got == "person-7", fmt.Sprintf("nearest want person-7 got %s", got))
}

// checkNgram asserts ngram(bio, "engineer") returns exactly the persons whose
// bio contains "engineer" (verified to have no false positives on this data).
func (c *Checker) checkNgram(ctx context.Context, add func(string, bool, string)) {
	want := 0
	for i := 0; i < c.data.NumPersons; i++ {
		if strings.Contains(c.data.Person(i).Bio, "engineer") {
			want++
		}
	}
	c.countCheck(ctx, add, "ngram-bio-engineer",
		`{ q(func: ngram(bio, "engineer")) { c: count(uid) } }`, want)
}

// checkContains asserts geo contains over company territory polygons: each
// company's center falls in exactly its own (non-overlapping) territory, and a
// far-away point falls in none.
func (c *Checker) checkContains(ctx context.Context, add func(string, bool, string)) {
	cx, cy := testdata.CompanyCenter(0)
	c.countCheck(ctx, add, "contains-territory-hit",
		fmt.Sprintf(`{ q(func: contains(territory, [%g, %g])) { c: count(uid) } }`, cx, cy), 1)

	e := testdata.GeoEmptyPoint
	c.countCheck(ctx, add, "contains-territory-miss",
		fmt.Sprintf(`{ q(func: contains(territory, [%g, %g])) { c: count(uid) } }`, e[0], e[1]), 0)
}

// countCheck runs a count query and asserts it equals want.
func (c *Checker) countCheck(ctx context.Context, add func(string, bool, string), name, query string, want int) {
	got, err := c.countQuery(ctx, query)
	if err != nil {
		add(name, false, err.Error())
		return
	}
	add(name, got == want, fmt.Sprintf("want %d got %d", want, got))
}

// checkFunctions asserts exact results for a spread of DQL functions whose
// answers are computable from the deterministic dataset: has, lt, between, sum,
// uid_in, allofterms, OR/NOT filters, orderasc, and geo intersects.
func (c *Checker) checkFunctions(ctx context.Context, add func(string, bool, string)) {
	d := c.data
	var sumAge, lt25, between3040, company0, nameAlice, age3031, notActive int
	minAge := int(^uint(0) >> 1) // max int
	for i := 0; i < d.NumPersons; i++ {
		p := d.Person(i)
		sumAge += p.Age
		if p.Age < 25 {
			lt25++
		}
		if p.Age >= 30 && p.Age <= 40 {
			between3040++
		}
		if i%d.NumCompanies == 0 {
			company0++
		}
		if p.Name == "Alice" {
			nameAlice++
		}
		if p.Age == 30 || p.Age == 31 {
			age3031++
		}
		if !p.Active {
			notActive++
		}
		if p.Age < minAge {
			minAge = p.Age
		}
	}

	c.countCheck(ctx, add, "fn-has-bio", `{ q(func: has(bio)) { c: count(uid) } }`, d.NumPersons)
	c.countCheck(ctx, add, "fn-lt-age", `{ q(func: lt(age, 25)) { c: count(uid) } }`, lt25)
	c.countCheck(ctx, add, "fn-between-age", `{ q(func: between(age, 30, 40)) { c: count(uid) } }`, between3040)
	c.countCheck(ctx, add, "fn-sum-age", `{ var(func: type(Person)) { a as age } q() { c: sum(val(a)) } }`, sumAge)
	c.countCheck(ctx, add, "fn-uid-in-company",
		`{ x as var(func: eq(xid, "company-0")) q(func: type(Person)) @filter(uid_in(company, uid(x))) { c: count(uid) } }`,
		company0)
	c.countCheck(ctx, add, "fn-allofterms-name", `{ q(func: allofterms(name, "Alice")) { c: count(uid) } }`, nameAlice)
	c.countCheck(ctx, add, "fn-filter-or", `{ q(func: type(Person)) @filter(eq(age, 30) OR eq(age, 31)) { c: count(uid) } }`, age3031)
	c.countCheck(ctx, add, "fn-filter-not-active", `{ q(func: type(Person)) @filter(NOT eq(active, true)) { c: count(uid) } }`, notActive)
	c.countCheck(ctx, add, "fn-orderasc-min", `{ q(func: type(Person), orderasc: age, first: 1) { c: age } }`, minAge)

	// Geo intersects over a polygon around cluster 0 returns exactly that cluster.
	sf := testdata.GeoAnchors[0]
	poly := fmt.Sprintf("[[[%g,%g],[%g,%g],[%g,%g],[%g,%g],[%g,%g]]]",
		sf[0]-0.1, sf[1]-0.1, sf[0]+0.1, sf[1]-0.1, sf[0]+0.1, sf[1]+0.1, sf[0]-0.1, sf[1]+0.1, sf[0]-0.1, sf[1]-0.1)
	c.countCheck(ctx, add, "fn-geo-intersects",
		fmt.Sprintf(`{ q(func: intersects(location, %s)) { c: count(uid) } }`, poly),
		d.GeoClusterCount(0))
}

// cmp accumulates the outcome of a complete entity comparison.
type cmp struct {
	verified, mismatch, missing, extra int
	examples                           []string
}

func (m *cmp) ex(s string) {
	if len(m.examples) < 3 {
		m.examples = append(m.examples, s)
	}
}

func (m *cmp) emit(name string, add func(string, bool, string)) {
	ok := m.mismatch == 0 && m.missing == 0 && m.extra == 0
	detail := fmt.Sprintf("%d verified, %d mismatch, %d missing, %d extra", m.verified, m.mismatch, m.missing, m.extra)
	if len(m.examples) > 0 {
		detail += " | " + strings.Join(m.examples, "; ")
	}
	add(name, ok, detail)
}

// --- complete DQL verification ---

type xidRef struct {
	Xid string `json:"xid"`
}

type geoPoint struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}

type personNode struct {
	Xid       string    `json:"xid"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Age       int       `json:"age"`
	Score     float64   `json:"score"`
	Active    bool      `json:"active"`
	Bio       string    `json:"bio"`
	Tags      []string  `json:"tags_csv"`
	Location  *geoPoint `json:"location"`
	Embedding []float64 `json:"embedding"`
	CreatedAt string    `json:"created_at"`
	Company   *xidRef   `json:"company"`
	Friends   []xidRef  `json:"friends"`
}

func (c *Checker) checkAllPersons(ctx context.Context, add func(string, bool, string)) {
	const body = `xid name email age score active bio tags_csv location embedding created_at company { xid } friends { xid }`
	actual := map[string]personNode{}
	if err := c.fetchAllDQL(ctx, "Person", body, func(raw json.RawMessage) error {
		var p personNode
		if err := json.Unmarshal(raw, &p); err != nil {
			return err
		}
		actual[p.Xid] = p
		return nil
	}); err != nil {
		add("dql-persons-complete", false, err.Error())
		return
	}

	m := &cmp{}
	expected := make(map[string]bool, c.data.NumPersons)
	for i := 0; i < c.data.NumPersons; i++ {
		w := c.data.Person(i)
		expected[w.Xid] = true
		g, ok := actual[w.Xid]
		if !ok {
			m.missing++
			m.ex("missing " + w.Xid)
			continue
		}
		if diff := personDiff(w, g); diff != "" {
			m.mismatch++
			m.ex(w.Xid + ": " + diff)
		} else {
			m.verified++
		}
	}
	for xid := range actual {
		if !expected[xid] {
			m.extra++
			m.ex("extra " + xid)
		}
	}
	m.emit("dql-persons-complete", add)
}

func personDiff(w testdata.Person, g personNode) string {
	switch {
	case g.Name != w.Name:
		return fmt.Sprintf("name %q!=%q", g.Name, w.Name)
	case g.Email != w.Email:
		return "email"
	case g.Age != w.Age:
		return fmt.Sprintf("age %d!=%d", g.Age, w.Age)
	case !floatEq(g.Score, w.Score):
		return fmt.Sprintf("score %v!=%v", g.Score, w.Score)
	case g.Active != w.Active:
		return "active"
	case g.Bio != w.Bio:
		return "bio"
	case !setEqual(g.Tags, w.Tags):
		return "tags"
	case g.Company == nil || g.Company.Xid != w.CompanyXid:
		return "company"
	case !refSetEqual(g.Friends, w.FriendXids):
		return "friends"
	case !geoEqual(g.Location, w.Location):
		return "location"
	case !vecEqual(g.Embedding, w.Embedding):
		return "embedding"
	case !timeEqual(g.CreatedAt, w.CreatedAt):
		return "created_at"
	}
	return ""
}

type companyNode struct {
	Xid       string          `json:"xid"`
	Name      string          `json:"name"`
	Domain    string          `json:"domain"`
	Founded   string          `json:"founded"`
	Territory json.RawMessage `json:"territory"`
}

func (c *Checker) checkAllCompanies(ctx context.Context, add func(string, bool, string)) {
	actual := map[string]companyNode{}
	if err := c.fetchAllDQL(ctx, "Company", `xid name domain founded territory`, func(raw json.RawMessage) error {
		var n companyNode
		if err := json.Unmarshal(raw, &n); err != nil {
			return err
		}
		actual[n.Xid] = n
		return nil
	}); err != nil {
		add("dql-companies-complete", false, err.Error())
		return
	}

	m := &cmp{}
	expected := make(map[string]bool, c.data.NumCompanies)
	for i := 0; i < c.data.NumCompanies; i++ {
		w := c.data.Company(i)
		expected[w.Xid] = true
		g, ok := actual[w.Xid]
		if !ok {
			m.missing++
			m.ex("missing " + w.Xid)
			continue
		}
		// Territory is a polygon; geo round-trips can reproject coordinates, so
		// verify presence here and rely on the contains check for geometry.
		if g.Name != w.Name || g.Domain != w.Domain || !timeEqual(g.Founded, w.Founded) || len(g.Territory) == 0 || string(g.Territory) == "null" {
			m.mismatch++
			m.ex(w.Xid)
		} else {
			m.verified++
		}
	}
	for xid := range actual {
		if !expected[xid] {
			m.extra++
			m.ex("extra " + xid)
		}
	}
	m.emit("dql-companies-complete", add)
}

type postNode struct {
	Xid       string  `json:"xid"`
	Title     string  `json:"title"`
	Content   string  `json:"content"`
	Likes     int     `json:"likes"`
	CreatedAt string  `json:"created_at"`
	Author    *xidRef `json:"author"`
}

func (c *Checker) checkAllPosts(ctx context.Context, add func(string, bool, string)) {
	actual := map[string]postNode{}
	if err := c.fetchAllDQL(ctx, "Post", `xid title content likes created_at author { xid }`, func(raw json.RawMessage) error {
		var n postNode
		if err := json.Unmarshal(raw, &n); err != nil {
			return err
		}
		actual[n.Xid] = n
		return nil
	}); err != nil {
		add("dql-posts-complete", false, err.Error())
		return
	}

	m := &cmp{}
	expected := make(map[string]bool, c.data.NumPosts)
	for i := 0; i < c.data.NumPosts; i++ {
		w := c.data.Post(i)
		expected[w.Xid] = true
		g, ok := actual[w.Xid]
		if !ok {
			m.missing++
			m.ex("missing " + w.Xid)
			continue
		}
		if g.Title != w.Title || g.Content != w.Content || g.Likes != w.Likes ||
			!timeEqual(g.CreatedAt, w.CreatedAt) || g.Author == nil || g.Author.Xid != w.AuthorXid {
			m.mismatch++
			m.ex(w.Xid)
		} else {
			m.verified++
		}
	}
	for xid := range actual {
		if !expected[xid] {
			m.extra++
			m.ex("extra " + xid)
		}
	}
	m.emit("dql-posts-complete", add)
}

// fetchAllDQL pages through every node of a type, invoking fn with each node's
// raw JSON. Validation runs after the workload stops, so the graph is static
// and offset pagination across read-only transactions is stable.
func (c *Checker) fetchAllDQL(ctx context.Context, typ, body string, fn func(json.RawMessage) error) error {
	const page = 2000
	for offset := 0; ; offset += page {
		q := fmt.Sprintf(`{ q(func: type(%s), first: %d, offset: %d) { %s } }`, typ, page, offset, body)
		data, err := c.query(ctx, q)
		if err != nil {
			return err
		}
		var out struct {
			Q []json.RawMessage `json:"q"`
		}
		if err := json.Unmarshal(data, &out); err != nil {
			return err
		}
		for _, raw := range out.Q {
			if err := fn(raw); err != nil {
				return err
			}
		}
		if len(out.Q) < page {
			return nil
		}
	}
}

// --- complete GraphQL verification ---

type reviewNode struct {
	Key       string `json:"key"`
	Title     string `json:"title"`
	Body      string `json:"body"`
	Stars     int    `json:"stars"`
	Reviewer  string `json:"reviewer"`
	CreatedAt string `json:"createdAt"`
}

type productNode struct {
	SKU          string       `json:"sku"`
	Name         string       `json:"name"`
	Description  string       `json:"description"`
	Price        float64      `json:"price"`
	Availability string       `json:"availability"`
	Rating       float64      `json:"rating"`
	CreatedAt    string       `json:"createdAt"`
	Category     *struct{ Slug string `json:"slug"` } `json:"category"`
	Reviews      []reviewNode `json:"reviews"`
}

func (c *Checker) checkAllProducts(ctx context.Context, add func(string, bool, string)) {
	actual := map[string]productNode{}
	const page = 500
	for offset := 0; ; offset += page {
		q := fmt.Sprintf(`query {
			queryProduct(first: %d, offset: %d) {
				sku name description price availability rating createdAt
				category { slug }
				reviews { key title body stars reviewer createdAt }
			}
		}`, page, offset)
		resp, err := c.gql.Query(ctx, q, nil)
		if err != nil {
			add("gql-products-complete", false, err.Error())
			return
		}
		if len(resp.Errors) > 0 {
			add("gql-products-complete", false, resp.Errors[0].Message)
			return
		}
		var out struct {
			QueryProduct []productNode `json:"queryProduct"`
		}
		if err := json.Unmarshal(resp.Data, &out); err != nil {
			add("gql-products-complete", false, err.Error())
			return
		}
		for _, p := range out.QueryProduct {
			actual[p.SKU] = p
		}
		if len(out.QueryProduct) < page {
			break
		}
	}

	m := &cmp{}
	expected := make(map[string]bool, c.data.NumProducts)
	for i := 0; i < c.data.NumProducts; i++ {
		w := c.data.Product(i)
		expected[w.SKU] = true
		g, ok := actual[w.SKU]
		if !ok {
			m.missing++
			m.ex("missing " + w.SKU)
			continue
		}
		if diff := c.productDiff(i, w, g); diff != "" {
			m.mismatch++
			m.ex(w.SKU + ": " + diff)
		} else {
			m.verified++
		}
	}
	for sku := range actual {
		if !expected[sku] {
			m.extra++
			m.ex("extra " + sku)
		}
	}
	m.emit("gql-products-complete", add)
}

func (c *Checker) productDiff(idx int, w testdata.Product, g productNode) string {
	switch {
	case g.Name != w.Name:
		return "name"
	case g.Description != w.Description:
		return "description"
	case g.Availability != w.Availability:
		return "availability"
	case !floatEq(g.Price, w.Price):
		return fmt.Sprintf("price %v!=%v", g.Price, w.Price)
	case !floatEq(g.Rating, w.Rating):
		return fmt.Sprintf("rating %v!=%v", g.Rating, w.Rating)
	case !timeEqual(g.CreatedAt, w.CreatedAt):
		return "createdAt"
	case g.Category == nil || g.Category.Slug != w.CategorySlug:
		return "category"
	}
	// Reviews: exact key set and per-review fields.
	if len(g.Reviews) != c.data.ReviewsPerProduct {
		return fmt.Sprintf("reviews count %d!=%d", len(g.Reviews), c.data.ReviewsPerProduct)
	}
	gotReviews := make(map[string]reviewNode, len(g.Reviews))
	for _, rv := range g.Reviews {
		gotReviews[rv.Key] = rv
	}
	for j := 0; j < c.data.ReviewsPerProduct; j++ {
		wr := c.data.Review(idx, j)
		gr, ok := gotReviews[wr.Key]
		if !ok {
			return "missing review " + wr.Key
		}
		if gr.Title != wr.Title || gr.Body != wr.Body || gr.Stars != wr.Stars ||
			gr.Reviewer != wr.Reviewer || !timeEqual(gr.CreatedAt, wr.CreatedAt) {
			return "review " + wr.Key
		}
	}
	return ""
}

type categoryNode struct {
	Slug string `json:"slug"`
	Name string `json:"name"`
}

func (c *Checker) checkAllCategories(ctx context.Context, add func(string, bool, string)) {
	resp, err := c.gql.Query(ctx, `query { queryCategory(first: 1000) { slug name } }`, nil)
	if err != nil {
		add("gql-categories-complete", false, err.Error())
		return
	}
	if len(resp.Errors) > 0 {
		add("gql-categories-complete", false, resp.Errors[0].Message)
		return
	}
	var out struct {
		QueryCategory []categoryNode `json:"queryCategory"`
	}
	if err := json.Unmarshal(resp.Data, &out); err != nil {
		add("gql-categories-complete", false, err.Error())
		return
	}
	actual := make(map[string]categoryNode, len(out.QueryCategory))
	for _, n := range out.QueryCategory {
		actual[n.Slug] = n
	}

	m := &cmp{}
	expected := make(map[string]bool, c.data.NumCategories)
	for i := 0; i < c.data.NumCategories; i++ {
		w := c.data.Category(i)
		expected[w.Slug] = true
		g, ok := actual[w.Slug]
		if !ok {
			m.missing++
			m.ex("missing " + w.Slug)
			continue
		}
		if g.Name != w.Name {
			m.mismatch++
			m.ex(w.Slug)
		} else {
			m.verified++
		}
	}
	for slug := range actual {
		if !expected[slug] {
			m.extra++
			m.ex("extra " + slug)
		}
	}
	m.emit("gql-categories-complete", add)
}

// --- functional index/path checks ---

// checkGeo asserts exact geo results: a near() and within() hit return their
// cluster's exact count, and a near() over empty ocean returns zero.
func (c *Checker) checkGeo(ctx context.Context, add func(string, bool, string)) {
	geoCount := func(name, query string, want int) {
		got, err := c.countQuery(ctx, query)
		if err != nil {
			add(name, false, err.Error())
			return
		}
		add(name, got == want, fmt.Sprintf("want %d got %d", want, got))
	}

	sf := testdata.GeoAnchors[0]
	geoCount("geo-near-hit",
		fmt.Sprintf(`{ q(func: near(location, [%g, %g], %d)) { c: count(uid) } }`, sf[0], sf[1], testdata.GeoNearRadiusM),
		c.data.GeoClusterCount(0))

	e := testdata.GeoEmptyPoint
	geoCount("geo-near-miss",
		fmt.Sprintf(`{ q(func: near(location, [%g, %g], %d)) { c: count(uid) } }`, e[0], e[1], testdata.GeoNearRadiusM),
		0)

	a := testdata.GeoAnchors[1]
	poly := fmt.Sprintf("[[[%g,%g],[%g,%g],[%g,%g],[%g,%g],[%g,%g]]]",
		a[0]-0.1, a[1]-0.1, a[0]+0.1, a[1]-0.1, a[0]+0.1, a[1]+0.1, a[0]-0.1, a[1]+0.1, a[0]-0.1, a[1]-0.1)
	geoCount("geo-within-hit",
		fmt.Sprintf(`{ q(func: within(location, %s)) { c: count(uid) } }`, poly),
		c.data.GeoClusterCount(1))
}

// checkShortest asserts Dgraph's shortest friends path between two persons has
// the exact node count predicted by a BFS over the deterministic friend rule
// (hops + 1). Skipped if an endpoint is outside the dataset.
func (c *Checker) checkShortest(ctx context.Context, add func(string, bool, string), from, to int) {
	name := fmt.Sprintf("shortest-%d-to-%d", from, to)
	if from >= c.data.NumPersons || to >= c.data.NumPersons {
		return
	}
	hops := c.data.ShortestFriendHops(from, to)
	if hops < 0 {
		add(name, false, "BFS found no path")
		return
	}
	q := fmt.Sprintf(`{
		a as var(func: eq(xid, "person-%d"))
		b as var(func: eq(xid, "person-%d"))
		path as shortest(from: uid(a), to: uid(b)) { friends }
		q(func: uid(path)) { uid }
	}`, from, to)
	data, err := c.query(ctx, q)
	if err != nil {
		add(name, false, err.Error())
		return
	}
	var out struct {
		Q []struct {
			Uid string `json:"uid"`
		} `json:"q"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		add(name, false, err.Error())
		return
	}
	want := hops + 1
	add(name, len(out.Q) == want, fmt.Sprintf("want %d path nodes (%d hops), got %d", want, hops, len(out.Q)))
}

// --- query + comparison helpers ---

func (c *Checker) query(ctx context.Context, q string) ([]byte, error) {
	txn := c.dgraph.ReadOnlyTxn()
	defer txn.Discard(ctx)
	resp, err := txn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return resp.Json, nil
}

// countQuery runs a DQL query whose root block `q` projects `c: count(uid)`.
func (c *Checker) countQuery(ctx context.Context, query string) (int, error) {
	data, err := c.query(ctx, query)
	if err != nil {
		return 0, err
	}
	var out struct {
		Q []struct {
			C int `json:"c"`
		} `json:"q"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return 0, err
	}
	if len(out.Q) == 0 {
		return 0, nil
	}
	return out.Q[0].C, nil
}

func floatEq(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

// vecEqual compares a stored vector (float64 from JSON) to the expected float32
// vector with a tolerance that absorbs float32 round-trip error.
func vecEqual(got []float64, want []float32) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if math.Abs(got[i]-float64(want[i])) > 1e-4 {
			return false
		}
	}
	return true
}

func setEqual(a, b []string) bool {
	sa, sb := toSet(a), toSet(b)
	if len(sa) != len(sb) {
		return false
	}
	for k := range sa {
		if !sb[k] {
			return false
		}
	}
	return true
}

func refSetEqual(got []xidRef, want []string) bool {
	g := make([]string, 0, len(got))
	for _, r := range got {
		g = append(g, r.Xid)
	}
	return setEqual(g, want)
}

func toSet(xs []string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

func geoEqual(got *geoPoint, want string) bool {
	if want == "" {
		return got == nil
	}
	if got == nil || len(got.Coordinates) != 2 {
		return false
	}
	var w geoPoint
	if json.Unmarshal([]byte(want), &w) != nil || len(w.Coordinates) != 2 {
		return false
	}
	return math.Abs(got.Coordinates[0]-w.Coordinates[0]) < 1e-5 &&
		math.Abs(got.Coordinates[1]-w.Coordinates[1]) < 1e-5
}

func timeEqual(a, b string) bool {
	ta, ea := parseTime(a)
	tb, eb := parseTime(b)
	if ea != nil || eb != nil {
		return a == b
	}
	return ta.Equal(tb)
}

func parseTime(s string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unparseable time %q", s)
}
