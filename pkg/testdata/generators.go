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
		CreatedAt:  baseTime.Add(time.Duration(i) * time.Hour).Format(time.RFC3339),
		CompanyXid: fmt.Sprintf("company-%d", i%d.NumCompanies),
	}
	if d.NumPersons > 8 {
		p.FriendXids = []string{
			fmt.Sprintf("person-%d", (i+1)%d.NumPersons),
			fmt.Sprintf("person-%d", (i+7)%d.NumPersons),
		}
	}
	return p
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
	Xid     string
	Name    string
	Domain  string
	Founded string
}

// Company returns the company at index i.
func (d Dataset) Company(i int) Company {
	return Company{
		Xid:     fmt.Sprintf("company-%d", i),
		Name:    companyNm[i%len(companyNm)],
		Domain:  fmt.Sprintf("co-%d.example.com", i),
		Founded: baseTime.Add(time.Duration(i) * 24 * time.Hour).Format(time.RFC3339),
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

func pickTags(r *rand.Rand) []string {
	n := 1 + r.Intn(3)
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, tagPool[r.Intn(len(tagPool))])
	}
	return out
}
