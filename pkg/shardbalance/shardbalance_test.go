package shardbalance

import "testing"

// mkTablets builds a tablet map (current group unused by assign) from bare names.
func mkTablets(names ...string) map[string]tablet {
	m := map[string]tablet{}
	for _, n := range names {
		m[n] = tablet{bare: n, ns: "0", current: "1"}
	}
	return m
}

func TestAssign(t *testing.T) {
	names := []string{
		// bare DQL predicates (shared across entities) -> all one "_dql" bucket
		"name", "age", "bio", "friends", "email", "xid", "title", "content", "likes", "company", "author",
		// GraphQL type-scoped predicates -> per-type buckets
		"Product.name", "Product.price", "Product.sku", "Product.rating", "Product.reviews",
		"Product.category", "Product.description", "Product.note", "Product.sku2", "Product.availability",
		"Review.body", "Review.stars", "Review.title", "Review.product", "Review.reviewer", "Review.key", "Review.createdAt",
		"Category.name", "Category.slug", "Category.products",
		"Animal.name", "Animal.xid",
		// reserved — must be skipped
		"dgraph.type", "dgraph.graphql.schema",
	}
	groups := []string{"1", "2", "3"}
	got := assign(mkTablets(names...), groups)

	// dgraph.* are never assigned.
	for _, r := range []string{"dgraph.type", "dgraph.graphql.schema"} {
		if _, ok := got[r]; ok {
			t.Errorf("reserved predicate %q should not be assigned", r)
		}
	}

	// Every predicate of a type must land on the same group (single-group writes).
	sameGroup := func(preds ...string) string {
		g := got[preds[0]]
		for _, p := range preds[1:] {
			if got[p] != g {
				t.Errorf("type not co-located: %q on %s but %q on %s", preds[0], g, p, got[p])
			}
		}
		return g
	}
	dqlG := sameGroup("name", "age", "bio", "friends", "email", "xid", "title", "content", "likes", "company", "author")
	prodG := sameGroup("Product.name", "Product.price", "Product.sku", "Product.rating", "Product.reviews",
		"Product.category", "Product.description", "Product.note", "Product.sku2", "Product.availability")
	revG := sameGroup("Review.body", "Review.stars", "Review.title", "Review.product", "Review.reviewer", "Review.key", "Review.createdAt")

	// The three heaviest families (_dql=11, Product=10, Review=7) must be on
	// three DISTINCT groups (biggest-first round-robin over 3 groups).
	if dqlG == prodG || dqlG == revG || prodG == revG {
		t.Errorf("heaviest families not spread: _dql=%s Product=%s Review=%s", dqlG, prodG, revG)
	}
	// Biggest bucket goes to the first (numerically-lowest) group.
	if dqlG != "1" || prodG != "2" || revG != "3" {
		t.Errorf("biggest-first placement wrong: _dql=%s (want 1) Product=%s (want 2) Review=%s (want 3)", dqlG, prodG, revG)
	}
}

func TestAssignDeterministic(t *testing.T) {
	names := []string{"a", "b", "X.one", "X.two", "Y.one", "Z.one"}
	g := []string{"1", "2", "3"}
	first := assign(mkTablets(names...), g)
	for i := 0; i < 5; i++ {
		if next := assign(mkTablets(names...), g); !equalMap(first, next) {
			t.Fatalf("assign not deterministic across runs")
		}
	}
}

func equalMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
