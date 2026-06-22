// Package validate asserts the final graph matches the deterministic dataset:
// exact type counts, ephemeral records absent, and keyed spot-checks of values
// and edges across both the DQL and GraphQL domains.
package validate

import (
	"context"
	"encoding/json"
	"fmt"

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
	dgraph *cluster.DgraphClient
	gql    *graphql.Client
	data   testdata.Dataset
}

// NewChecker creates a checker for the given dataset.
func NewChecker(dgraph *cluster.DgraphClient, gql *graphql.Client, data testdata.Dataset) *Checker {
	return &Checker{dgraph: dgraph, gql: gql, data: data}
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

	// Exact type counts (ephemeral deleted, so only persistent remain).
	c.checkCount(ctx, add, "Person", c.data.NumPersons)
	c.checkCount(ctx, add, "Company", c.data.NumCompanies)
	c.checkCount(ctx, add, "Post", c.data.NumPosts)

	// Ephemeral DQL node absent.
	if exists, err := c.dqlExists(ctx, "person-eph-0"); err != nil {
		add("dql-ephemeral-absent", false, err.Error())
	} else {
		add("dql-ephemeral-absent", !exists, fmt.Sprintf("person-eph-0 exists=%v", exists))
	}

	// DQL spot-check: person-0 values and edges.
	c.checkPersonSpot(ctx, add)

	// GraphQL exact counts.
	c.checkGQLCount(ctx, add, "aggregateProduct", c.data.NumProducts)
	c.checkGQLCount(ctx, add, "aggregateCategory", c.data.NumCategories)

	// Ephemeral GraphQL node absent + product spot-check.
	c.checkProductSpot(ctx, add)

	return r
}

func (c *Checker) checkCount(ctx context.Context, add func(string, bool, string), typ string, want int) {
	got, err := c.dqlCount(ctx, typ)
	if err != nil {
		add("dql-count-"+typ, false, err.Error())
		return
	}
	add("dql-count-"+typ, got == want, fmt.Sprintf("want %d got %d", want, got))
}

func (c *Checker) checkPersonSpot(ctx context.Context, add func(string, bool, string)) {
	want := c.data.Person(0)
	q := `{ q(func: eq(xid, "person-0")) { name age company { xid } friends { xid } } }`
	resp, err := c.query(ctx, q)
	if err != nil {
		add("dql-person-spot", false, err.Error())
		return
	}
	var out struct {
		Q []struct {
			Name    string `json:"name"`
			Age     int    `json:"age"`
			Company *struct {
				Xid string `json:"xid"`
			} `json:"company"`
			Friends []struct {
				Xid string `json:"xid"`
			} `json:"friends"`
		} `json:"q"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		add("dql-person-spot", false, err.Error())
		return
	}
	if len(out.Q) != 1 {
		add("dql-person-spot", false, fmt.Sprintf("expected 1 node, got %d", len(out.Q)))
		return
	}
	got := out.Q[0]
	ok := got.Name == want.Name && got.Age == want.Age && got.Company != nil && len(got.Friends) == len(want.FriendXids)
	add("dql-person-spot", ok, fmt.Sprintf("name=%s/%s age=%d/%d company=%v friends=%d/%d",
		got.Name, want.Name, got.Age, want.Age, got.Company != nil, len(got.Friends), len(want.FriendXids)))
}

func (c *Checker) checkProductSpot(ctx context.Context, add func(string, bool, string)) {
	// Ephemeral absent.
	absent, err := c.gqlGetProduct(ctx, "SKU-EPH-0")
	if err != nil {
		add("gql-ephemeral-absent", false, err.Error())
	} else {
		add("gql-ephemeral-absent", absent == nil, "SKU-EPH-0 should be deleted")
	}

	// Persistent product spot-check.
	want := c.data.Product(0)
	got, err := c.gqlGetProduct(ctx, "SKU-0")
	if err != nil {
		add("gql-product-spot", false, err.Error())
		return
	}
	if got == nil {
		add("gql-product-spot", false, "SKU-0 missing")
		return
	}
	ok := got.Name == want.Name && got.Category != nil && got.Category.Slug == want.CategorySlug && len(got.Reviews) == c.data.ReviewsPerProduct
	add("gql-product-spot", ok, fmt.Sprintf("name=%s/%s category=%v reviews=%d/%d",
		got.Name, want.Name, got.Category, len(got.Reviews), c.data.ReviewsPerProduct))
}

// --- DQL helpers ---

func (c *Checker) query(ctx context.Context, q string) ([]byte, error) {
	txn := c.dgraph.ReadOnlyTxn()
	defer txn.Discard(ctx)
	resp, err := txn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return resp.Json, nil
}

func (c *Checker) dqlCount(ctx context.Context, typ string) (int, error) {
	q := fmt.Sprintf(`{ q(func: type(%s)) { c: count(uid) } }`, typ)
	data, err := c.query(ctx, q)
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

func (c *Checker) dqlExists(ctx context.Context, xid string) (bool, error) {
	q := fmt.Sprintf(`{ q(func: eq(xid, %q)) { uid } }`, xid)
	data, err := c.query(ctx, q)
	if err != nil {
		return false, err
	}
	var out struct {
		Q []struct {
			Uid string `json:"uid"`
		} `json:"q"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return false, err
	}
	return len(out.Q) > 0, nil
}

// --- GraphQL helpers ---

type gqlProduct struct {
	SKU      string `json:"sku"`
	Name     string `json:"name"`
	Category *struct {
		Slug string `json:"slug"`
	} `json:"category"`
	Reviews []struct {
		Key string `json:"key"`
	} `json:"reviews"`
}

func (c *Checker) gqlGetProduct(ctx context.Context, sku string) (*gqlProduct, error) {
	const q = `query($sku: String!) {
		getProduct(sku: $sku) { sku name category { slug } reviews { key } }
	}`
	resp, err := c.gql.Query(ctx, q, map[string]interface{}{"sku": sku})
	if err != nil {
		return nil, err
	}
	if len(resp.Errors) > 0 {
		return nil, fmt.Errorf("graphql: %s", resp.Errors[0].Message)
	}
	var out struct {
		GetProduct *gqlProduct `json:"getProduct"`
	}
	if err := json.Unmarshal(resp.Data, &out); err != nil {
		return nil, err
	}
	return out.GetProduct, nil
}

func (c *Checker) checkGQLCount(ctx context.Context, add func(string, bool, string), field string, want int) {
	q := fmt.Sprintf(`{ %s { count } }`, field)
	resp, err := c.gql.Query(ctx, q, nil)
	if err != nil {
		add("gql-"+field, false, err.Error())
		return
	}
	if len(resp.Errors) > 0 {
		add("gql-"+field, false, resp.Errors[0].Message)
		return
	}
	var out map[string]struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(resp.Data, &out); err != nil {
		add("gql-"+field, false, err.Error())
		return
	}
	got := out[field].Count
	add("gql-"+field, got == want, fmt.Sprintf("want %d got %d", want, got))
}
