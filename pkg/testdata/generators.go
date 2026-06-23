// Package testdata defines a fixed, enumerable dataset and builds the
// idempotent upsert/delete operations that materialize it.
//
// Determinism: every record is derived purely from its index (no wall-clock,
// no global RNG), keyed by a stable external id (DQL: xid; GraphQL: @id slug/
// sku/key). Writes are idempotent upserts, so any order or repetition under
// concurrent load converges to the same logical graph. A deterministic ~5%
// "ephemeral" subset is created-and-deleted to exercise the delete path; the
// finalize pass deletes them, pinning the end state.
package testdata

import (
	"fmt"
	"math/rand"
	"time"
)

// baseTime anchors all generated timestamps so values never depend on the wall clock.
var baseTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

var (
	firstNames = []string{"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack", "Kate", "Leo", "Mona", "Noah", "Olivia", "Peter"}
	bios       = []string{"software engineer", "data scientist", "product manager", "interface designer", "devops engineer", "full-stack developer", "security researcher", "machine learning engineer"}
	companyNm  = []string{"Acme", "Globex", "Initech", "Umbrella", "Hooli", "Stark", "Wayne", "Wonka", "Cyberdyne", "Soylent"}
	tagPool    = []string{"go", "graph", "db", "perf", "test", "dgraph", "query", "index"}
)

// det returns a deterministic RNG seeded purely from i (Knuth multiplicative hash).
func det(i int) *rand.Rand {
	return rand.New(rand.NewSource(int64(uint64(i)*2654435761 + 1)))
}

// Geo layout: persistent persons are placed in NumGeoClusters well-separated
// clusters (by index % NumGeoClusters) with sub-kilometer jitter. A near/within
// query tight around one anchor returns exactly that cluster (a hit); a query
// over GeoEmptyPoint returns zero (an expected miss). Coordinates are [lon,lat].
const NumGeoClusters = 4

// GeoAnchors are the cluster centers, thousands of km apart so a tight radius
// never catches a neighboring cluster.
var GeoAnchors = [NumGeoClusters][2]float64{
	{-122.40, 37.77}, // San Francisco
	{-74.00, 40.71},  // New York
	{2.35, 48.85},    // Paris
	{139.69, 35.68},  // Tokyo
}

// GeoEmptyPoint sits in the open Atlantic, far from every anchor.
var GeoEmptyPoint = [2]float64{-30.0, 0.0}

// GeoNearRadiusM is a radius that covers a cluster's jitter (~1.5 km) yet
// excludes all other clusters (>5000 km away).
const GeoNearRadiusM = 5000

// GeoClusterCount returns the exact number of persistent persons in cluster c.
func (d Dataset) GeoClusterCount(c int) int {
	n := 0
	for i := c; i < d.NumPersons; i += NumGeoClusters {
		n++
	}
	return n
}

// geoPoint formats a GeoJSON Point for the given anchor with deterministic jitter.
func geoPoint(idx int, r *rand.Rand) string {
	a := GeoAnchors[idx%NumGeoClusters]
	jitter := func() float64 { return (r.Float64()*2 - 1) * 0.01 } // ±~1 km
	return fmt.Sprintf(`{"type":"Point","coordinates":[%g,%g]}`, a[0]+jitter(), a[1]+jitter())
}

// EmbedDim is the dimensionality of person embedding vectors.
const EmbedDim = 8

// embedding builds a deterministic, distinct vector for a person. Distinctness
// makes the nearest neighbor to a person's own vector unambiguously itself,
// which is the basis of the similar_to self-match check.
func embedding(r *rand.Rand) []float32 {
	v := make([]float32, EmbedDim)
	for j := range v {
		v[j] = float32(r.Float64())
	}
	return v
}

// CompanyCenter returns the deterministic center [lon,lat] of company i's
// territory. Centers are spread far apart so territories never overlap.
func CompanyCenter(i int) (float64, float64) {
	return -100.0 + float64(i)*2.0, 10.0 + float64(i)*1.0
}

// companyTerritory returns a GeoJSON Polygon (a ±0.05° box) around company i's
// center. Non-overlapping, so contains(territory, center_i) yields exactly i.
func companyTerritory(i int) string {
	cx, cy := CompanyCenter(i)
	const h = 0.05
	return fmt.Sprintf(`{"type":"Polygon","coordinates":[[[%g,%g],[%g,%g],[%g,%g],[%g,%g],[%g,%g]]]}`,
		cx-h, cy-h, cx+h, cy-h, cx+h, cy+h, cx-h, cy+h, cx-h, cy-h)
}

// Dataset describes the fixed sizes of the test corpus. Same Dataset => same
// logical graph.
type Dataset struct {
	NumCompanies      int
	NumPersons        int // persistent persons
	NumEphPersons     int // ephemeral persons (created then deleted)
	NumPosts          int
	NumCategories     int
	NumProducts       int // persistent products
	NumEphProducts    int // ephemeral products (created then deleted)
	ReviewsPerProduct int
	NumDogs           int // Animal-interface members
	NumCats           int
}

// DefaultDataset returns a corpus sized so a full write pass completes well
// within a 30-minute budget, with ~5% ephemeral churn in each domain.
func DefaultDataset() Dataset {
	const persons, products = 5000, 3000
	return Dataset{
		NumCompanies:      10,
		NumPersons:        persons,
		NumEphPersons:     persons / 20, // 5%
		NumPosts:          2000,
		NumCategories:     12,
		NumProducts:       products,
		NumEphProducts:    products / 20, // 5%
		ReviewsPerProduct: 3,
		NumDogs:           60,
		NumCats:           40,
	}
}

// SmallDataset returns a tiny corpus for smoke tests and quick determinism
// checks. Same ~5% ephemeral ratio and edge structure as DefaultDataset.
func SmallDataset() Dataset {
	const persons, products = 200, 120
	return Dataset{
		NumCompanies:      5,
		NumPersons:        persons,
		NumEphPersons:     persons / 20, // 5%
		NumPosts:          80,
		NumCategories:     6,
		NumProducts:       products,
		NumEphProducts:    products / 20, // 5%
		ReviewsPerProduct: 2,
		NumDogs:           12,
		NumCats:           8,
	}
}

// --- DQL social-domain records ---

// Person is a deterministic person record.
type Person struct {
	Xid        string
	Name       string
	Email      string
	Age        int
	Score      float64
	Active     bool
	Bio        string
	Tags       []string
	Location   string    // GeoJSON Point; empty for ephemeral
	Embedding  []float32 // HNSW vector; nil for ephemeral
	CreatedAt  string
	CompanyXid string   // empty for ephemeral (edge-isolated)
	FriendXids []string // empty for ephemeral
}

// Person returns the persistent person at index i.
func (d Dataset) Person(i int) Person {
	r := det(i)
	p := Person{
		Xid:        fmt.Sprintf("person-%d", i),
		Name:       firstNames[r.Intn(len(firstNames))],
		Email:      fmt.Sprintf("user%d@example.com", i),
		Age:        20 + r.Intn(60),
		Score:      float64(r.Intn(10000)) / 100.0,
		Active:     r.Intn(2) == 0,
		Bio:        bios[r.Intn(len(bios))],
		Tags:       pickTags(r),
		Location:   geoPoint(i, r),
		Embedding:  embedding(r),
		CreatedAt:  baseTime.Add(time.Duration(i) * time.Hour).Format(time.RFC3339),
		CompanyXid: fmt.Sprintf("company-%d", i%d.NumCompanies),
	}
	for _, fi := range d.friendIndices(i) {
		p.FriendXids = append(p.FriendXids, fmt.Sprintf("person-%d", fi))
	}
	return p
}

// friendOffsets define the deterministic out-edges of the friends graph: a +1
// ring connects everyone, and a +7 shortcut keeps shortest paths non-trivial.
// BFS over these offsets predicts Dgraph's shortest-path length exactly.
var friendOffsets = []int{1, 7}

// friendIndices returns person i's out-edge target indices (empty for tiny
// datasets where a deep subgraph is not meaningful).
func (d Dataset) friendIndices(i int) []int {
	if d.NumPersons <= 8 {
		return nil
	}
	out := make([]int, 0, len(friendOffsets))
	for _, o := range friendOffsets {
		out = append(out, (i+o)%d.NumPersons)
	}
	return out
}

// ShortestFriendHops returns the minimum number of directed friends edges from
// person `from` to person `to`, or -1 if unreachable. It is a plain BFS over
// the deterministic friend rule, used as the oracle for shortest-path checks.
func (d Dataset) ShortestFriendHops(from, to int) int {
	if from == to {
		return 0
	}
	dist := map[int]int{from: 0}
	queue := []int{from}
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		for _, v := range d.friendIndices(u) {
			if _, seen := dist[v]; seen {
				continue
			}
			dist[v] = dist[u] + 1
			if v == to {
				return dist[v]
			}
			queue = append(queue, v)
		}
	}
	return -1
}

// EphPerson returns the ephemeral (delete-target) person at index i. It is
// edge-isolated so deleting it never perturbs persistent-node edge state.
func (d Dataset) EphPerson(i int) Person {
	r := det(1_000_000 + i)
	return Person{
		Xid:       fmt.Sprintf("person-eph-%d", i),
		Name:      firstNames[r.Intn(len(firstNames))],
		Email:     fmt.Sprintf("eph%d@example.com", i),
		Age:       20 + r.Intn(60),
		Score:     float64(r.Intn(10000)) / 100.0,
		Active:    r.Intn(2) == 0,
		Bio:       bios[r.Intn(len(bios))],
		Tags:      pickTags(r),
		CreatedAt: baseTime.Add(time.Duration(i) * time.Hour).Format(time.RFC3339),
	}
}

// Company is a deterministic company record.
type Company struct {
	Xid       string
	Name      string
	Domain    string
	Founded   string
	Territory string // GeoJSON Polygon
}

// Company returns the company at index i.
func (d Dataset) Company(i int) Company {
	return Company{
		Xid:       fmt.Sprintf("company-%d", i),
		Name:      companyNm[i%len(companyNm)],
		Domain:    fmt.Sprintf("co-%d.example.com", i),
		Founded:   baseTime.Add(time.Duration(i) * 24 * time.Hour).Format(time.RFC3339),
		Territory: companyTerritory(i),
	}
}

// Post is a deterministic post record.
type Post struct {
	Xid       string
	Title     string
	Content   string
	Likes     int
	CreatedAt string
	AuthorXid string
}

// Post returns the post at index i, authored by a persistent person.
func (d Dataset) Post(i int) Post {
	r := det(2_000_000 + i)
	return Post{
		Xid:       fmt.Sprintf("post-%d", i),
		Title:     fmt.Sprintf("Post %d on %s", i, bios[r.Intn(len(bios))]),
		Content:   "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
		Likes:     r.Intn(1000),
		CreatedAt: baseTime.Add(time.Duration(i) * time.Hour).Format(time.RFC3339),
		AuthorXid: fmt.Sprintf("person-%d", i%d.NumPersons),
	}
}

// --- GraphQL product-domain records ---

// Category is a deterministic category record.
type Category struct {
	Slug string
	Name string
}

// Category returns the category at index i.
func (d Dataset) Category(i int) Category {
	return Category{
		Slug: fmt.Sprintf("cat-%d", i),
		Name: fmt.Sprintf("Category %d", i),
	}
}

// Product is a deterministic product record.
type Product struct {
	SKU          string
	Name         string
	Description  string
	Price        float64
	Availability string
	Rating       float64
	CreatedAt    string
	CategorySlug string
}

var availabilities = []string{"IN_STOCK", "OUT_OF_STOCK", "DISCONTINUED"}

// Product returns the persistent product at index i.
func (d Dataset) Product(i int) Product {
	r := det(3_000_000 + i)
	return Product{
		SKU:          fmt.Sprintf("SKU-%d", i),
		Name:         fmt.Sprintf("Product %d", i),
		Description:  "durable lightweight " + bios[r.Intn(len(bios))] + " grade item",
		Price:        float64(100+r.Intn(50000)) / 100.0,
		Availability: availabilities[i%len(availabilities)],
		Rating:       float64(r.Intn(50)) / 10.0,
		CreatedAt:    baseTime.Add(time.Duration(i) * time.Hour).Format(time.RFC3339),
		CategorySlug: fmt.Sprintf("cat-%d", i%d.NumCategories),
	}
}

// EphProduct returns the ephemeral (delete-target) product at index i.
func (d Dataset) EphProduct(i int) Product {
	r := det(4_000_000 + i)
	return Product{
		SKU:          fmt.Sprintf("SKU-EPH-%d", i),
		Name:         fmt.Sprintf("Ephemeral Product %d", i),
		Description:  "durable lightweight ephemeral item",
		Price:        float64(100+r.Intn(50000)) / 100.0,
		Availability: availabilities[i%len(availabilities)],
		Rating:       float64(r.Intn(50)) / 10.0,
		CreatedAt:    baseTime.Add(time.Duration(i) * time.Hour).Format(time.RFC3339),
		CategorySlug: fmt.Sprintf("cat-%d", i%d.NumCategories),
	}
}

// Review is a deterministic review record.
type Review struct {
	Key        string
	Title      string
	Body       string
	Stars      int
	Reviewer   string
	CreatedAt  string
	ProductSKU string
}

// Review returns review j of product i.
func (d Dataset) Review(productIdx, j int) Review {
	r := det(5_000_000 + productIdx*16 + j)
	return Review{
		Key:        fmt.Sprintf("rev-%d-%d", productIdx, j),
		Title:      fmt.Sprintf("Review %d of product %d", j, productIdx),
		Body:       "durable lightweight and well made",
		Stars:      1 + r.Intn(5),
		Reviewer:   firstNames[r.Intn(len(firstNames))],
		CreatedAt:  baseTime.Add(time.Duration(productIdx) * time.Hour).Format(time.RFC3339),
		ProductSKU: fmt.Sprintf("SKU-%d", productIdx),
	}
}

// --- Animal interface records ---

// Dog is a deterministic Dog (Animal interface) record.
type Dog struct {
	Xid     string
	Name    string
	GoodBoy bool
}

// Dog returns the dog at index i.
func (d Dataset) Dog(i int) Dog {
	r := det(8_000_000 + i)
	return Dog{
		Xid:     fmt.Sprintf("dog-%d", i),
		Name:    firstNames[r.Intn(len(firstNames))],
		GoodBoy: r.Intn(2) == 0,
	}
}

// Cat is a deterministic Cat (Animal interface) record.
type Cat struct {
	Xid   string
	Name  string
	Lives int
}

// Cat returns the cat at index i.
func (d Dataset) Cat(i int) Cat {
	r := det(9_000_000 + i)
	return Cat{
		Xid:   fmt.Sprintf("cat-%d", i),
		Name:  firstNames[r.Intn(len(firstNames))],
		Lives: 1 + r.Intn(9),
	}
}

// PostLikeDelta is the deterministic amount the ledger workload adds and then
// subtracts from post i's likes during the run. The +d/-d pair nets to zero, so
// the canonical value Post(i).Likes is what the field settles to (and is pinned
// by the finalize upsert and verified).
func (d Dataset) PostLikeDelta(i int) int {
	return 1 + det(6_000_000+i).Intn(50)
}

// ProductPriceDelta is the deterministic amount the ledger workload adds and
// then subtracts from product i's price during the run.
func (d Dataset) ProductPriceDelta(i int) float64 {
	return float64(1+det(7_000_000+i).Intn(1000)) / 100.0
}

func pickTags(r *rand.Rand) []string {
	n := 1 + r.Intn(3)
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, tagPool[r.Intn(len(tagPool))])
	}
	return out
}
