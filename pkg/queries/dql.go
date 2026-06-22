// Package queries holds DQL and GraphQL query templates exercised by the harness.
package queries

// DQLQuery is a named DQL query template.
type DQLQuery struct {
	Label string            // short identifier recorded in metrics
	Query string            // DQL query text
	Vars  map[string]string // optional variables for QueryWithVars
}

// DQLQueries returns the catalog of DQL queries exercising filters,
// aggregation, ordering, pagination, recursion, reverse edges, full-text and
// regexp search, math, and variable blocks against the social-domain data.
func DQLQueries() []DQLQuery {
	return []DQLQuery{
		{
			Label: "type-scan-paginated",
			Query: `{ q(func: type(Person), first: 25) { uid name age } }`,
		},
		{
			Label: "eq-index-lookup",
			Query: `{ q(func: eq(active, true), first: 50) { uid name active } }`,
		},
		{
			Label: "int-range-filter",
			Query: `{ q(func: type(Person)) @filter(ge(age, 30) AND le(age, 50)) { uid name age } }`,
		},
		{
			Label: "fulltext-search",
			Query: `{ q(func: alloftext(bio, "engineer")) { uid name bio } }`,
		},
		{
			Label: "term-search-name",
			Query: `{ q(func: anyofterms(name, "Alice Bob"), first: 50) { uid name } }`,
		},
		{
			Label: "order-by-score-desc",
			Query: `{ q(func: type(Person), orderdesc: score, first: 20) { uid name score } }`,
		},
		{
			Label: "count-aggregate",
			Query: `{ q(func: type(Person)) { count(uid) } }`,
		},
		{
			Label: "value-aggregate-avg-max",
			Query: `{
				var(func: type(Person)) { a as age }
				q() { avg_age: avg(val(a)) max_age: max(val(a)) min_age: min(val(a)) }
			}`,
		},
		{
			Label: "edge-traversal-friends",
			Query: `{ q(func: type(Person), first: 25) { name friends { name age } } }`,
		},
		{
			Label: "reverse-edge-friend-of",
			Query: `{ q(func: type(Person), first: 25) { name ~friends { name } } }`,
		},
		{
			Label: "edge-count-filter",
			Query: `{ q(func: type(Person)) @filter(gt(count(friends), 2)) { name count(friends) } }`,
		},
		{
			Label: "nested-company-employees",
			Query: `{ q(func: type(Company), first: 10) { name employees { name age } } }`,
		},
		{
			Label: "recurse-friends-depth3",
			Query: `{
				q(func: type(Person), first: 5) @recurse(depth: 3, loop: false) {
					name
					friends
				}
			}`,
		},
		{
			Label: "cascade-has-company",
			Query: `{ q(func: type(Person)) @cascade { name company { name } } }`,
		},
		{
			Label: "regexp-email",
			Query: `{ q(func: type(Person)) @filter(regexp(email, /user[0-9]+@example.com/)) { name email } }`,
		},
		{
			Label: "math-on-val",
			Query: `{
				var(func: type(Person)) { s as score }
				q(func: uid(s), first: 20) { name score: val(s) bonus: math(s * 1.1) }
			}`,
		},
		{
			Label: "groupby-company",
			Query: `{ q(func: type(Person)) @groupby(company) { count(uid) } }`,
		},
		{
			// Geo hit: tight radius around the San Francisco cluster anchor.
			Label: "geo-near-hit",
			Query: `{ q(func: near(location, [-122.40, 37.77], 5000)) { uid name } }`,
		},
		{
			// Geo expected-miss: open Atlantic, far from every cluster.
			Label: "geo-near-miss",
			Query: `{ q(func: near(location, [-30.0, 0.0], 5000)) { uid name } }`,
		},
		{
			// Geo hit: bounding box around the New York cluster anchor.
			Label: "geo-within-hit",
			Query: `{ q(func: within(location, [[[-74.1, 40.61], [-73.9, 40.61], [-73.9, 40.81], [-74.1, 40.81], [-74.1, 40.61]]])) { uid name } }`,
		},
		{
			// Shortest path across a deep span of the friends subgraph (~24 hops).
			Label: "shortest-friends-deep",
			Query: `{
				a as var(func: eq(xid, "person-0"))
				b as var(func: eq(xid, "person-150"))
				path as shortest(from: uid(a), to: uid(b)) { friends }
				q(func: uid(path)) { uid xid }
			}`,
		},
		{
			// Shortest path across a shorter span (~7 hops).
			Label: "shortest-friends-short",
			Query: `{
				a as var(func: eq(xid, "person-0"))
				b as var(func: eq(xid, "person-13"))
				path as shortest(from: uid(a), to: uid(b)) { friends }
				q(func: uid(path)) { uid xid }
			}`,
		},
		{
			Label: "vars-eq-lookup",
			Query: `query q($a: int) { q(func: eq(age, $a), first: 25) { uid name age } }`,
			Vars:  map[string]string{"$a": "30"},
		},
		{
			Label: "allofterms-name",
			Query: `{ q(func: allofterms(name, "Alice"), first: 50) { uid name } }`,
		},
		{
			Label: "anyoftext-bio",
			Query: `{ q(func: anyoftext(bio, "engineer scientist designer")) { uid bio } }`,
		},
		{
			Label: "match-fuzzy-bio",
			Query: `{ q(func: match(bio, "enginer", 8), first: 50) { uid bio } }`,
		},
		{
			Label: "has-friends",
			Query: `{ q(func: has(friends), first: 50) { uid } }`,
		},
		{
			Label: "lt-age",
			Query: `{ q(func: lt(age, 25)) { uid age } }`,
		},
		{
			Label: "between-age",
			Query: `{ q(func: between(age, 30, 40)) { uid age } }`,
		},
		{
			Label: "sum-aggregate",
			Query: `{ var(func: type(Person)) { a as age } q() { total_age: sum(val(a)) } }`,
		},
		{
			Label: "uid-in-company",
			Query: `{
				c as var(func: eq(xid, "company-0"))
				q(func: type(Person)) @filter(uid_in(company, uid(c))) { uid name }
			}`,
		},
		{
			Label: "filter-or",
			Query: `{ q(func: type(Person)) @filter(eq(age, 30) OR eq(age, 31)) { uid age } }`,
		},
		{
			Label: "filter-not-active",
			Query: `{ q(func: type(Person)) @filter(NOT eq(active, true)) { uid active } }`,
		},
		{
			Label: "order-asc-age",
			Query: `{ q(func: type(Person), orderasc: age, first: 20) { uid age } }`,
		},
		{
			Label: "expand-all",
			Query: `{ q(func: eq(xid, "person-0")) { expand(_all_) } }`,
		},
		{
			Label: "normalize-projection",
			Query: `{ q(func: type(Person), first: 25) @normalize { person_name: name person_age: age } }`,
		},
		{
			Label: "math-rich",
			Query: `{
				var(func: type(Person)) { s as score }
				q(func: uid(s), first: 20) { score: val(s) derived: math(sqrt(s) + floor(s) + exp(0)) }
			}`,
		},
		{
			// Geo intersects: a polygon around the San Francisco cluster.
			Label: "geo-intersects-hit",
			Query: `{ q(func: intersects(location, [[[-122.5, 37.70], [-122.3, 37.70], [-122.3, 37.85], [-122.5, 37.85], [-122.5, 37.70]]])) { uid name } }`,
		},
		{
			// Vector similarity (HNSW): 10 nearest neighbors to a fixed query vector.
			Label: "similar-to-vector",
			Query: `{ q(func: similar_to(embedding, 10, "[0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]")) { uid } }`,
		},
		{
			// N-gram substring search over bio.
			Label: "ngram-bio",
			Query: `{ q(func: ngram(bio, "engineer"), first: 50) { uid bio } }`,
		},
		{
			// Geo contains: company-0's territory polygon contains its center point.
			Label: "geo-contains-territory",
			Query: `{ q(func: contains(territory, [-100.0, 10.0])) { uid name } }`,
		},
	}
}
