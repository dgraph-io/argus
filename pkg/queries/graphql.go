package queries

// GraphQLQuery is a named GraphQL query template.
type GraphQLQuery struct {
	Label string                 // short identifier recorded in metrics
	Query string                 // GraphQL query text
	Vars  map[string]interface{} // optional variables
}

// GraphQLQueries returns the catalog of GraphQL queries exercising filters,
// ordering, pagination, aggregation, nested traversal, and full-text search
// against the product-catalog domain.
func GraphQLQueries() []GraphQLQuery {
	return []GraphQLQuery{
		{
			Label: "list-products-paginated",
			Query: `query { queryProduct(first: 25) { sku name price availability } }`,
		},
		{
			Label: "filter-price-range",
			Query: `query($lo: Float!, $hi: Float!) {
				queryProduct(filter: { price: { between: { min: $lo, max: $hi } } }) {
					sku name price
				}
			}`,
			Vars: map[string]interface{}{"lo": 10.0, "hi": 100.0},
		},
		{
			Label: "filter-availability-enum",
			Query: `query { queryProduct(filter: { availability: { eq: IN_STOCK } }, first: 50) { sku name } }`,
		},
		{
			Label: "fulltext-description",
			Query: `query { queryProduct(filter: { description: { alloftext: "durable lightweight" } }) { sku name } }`,
		},
		{
			Label: "order-by-rating",
			Query: `query { queryProduct(order: { desc: rating }, first: 20) { sku name rating } }`,
		},
		{
			Label: "aggregate-products",
			Query: `query { aggregateProduct { count priceMin priceMax priceAvg } }`,
		},
		{
			Label: "nested-product-reviews",
			Query: `query { queryProduct(first: 15) { sku name reviews { stars reviewer title } } }`,
		},
		{
			Label: "nested-category-products",
			Query: `query { queryCategory(first: 10) { slug name products { sku name price } } }`,
		},
		{
			Label: "get-by-id-sku",
			Query: `query($sku: String!) { getProduct(sku: $sku) { sku name price category { name } } }`,
			Vars:  map[string]interface{}{"sku": "SKU-1"},
		},
		{
			Label: "reviews-by-stars",
			Query: `query { queryReview(filter: { stars: { ge: 4 } }, first: 50) { title stars reviewer } }`,
		},
		{
			Label: "deep-product-category-reviews",
			Query: `query {
				queryProduct(first: 10) {
					sku name
					category { name }
					reviews(order: { desc: stars }, first: 3) { stars title }
				}
			}`,
		},
		{
			Label: "filter-and",
			Query: `query { queryProduct(filter: { and: [{ price: { ge: 50 } }, { availability: { eq: IN_STOCK } }] }) { sku } }`,
		},
		{
			Label: "filter-or-not",
			Query: `query { queryProduct(filter: { or: [{ availability: { eq: DISCONTINUED } }, { not: { rating: { ge: 1 } } }] }) { sku } }`,
		},
		{
			Label: "filter-in",
			Query: `query { queryProduct(filter: { availability: { in: [IN_STOCK, DISCONTINUED] } }) { sku } }`,
		},
		{
			Label: "filter-has",
			Query: `query { queryProduct(filter: { has: rating }) { sku } }`,
		},
		{
			Label: "filter-regexp",
			Query: `query { queryProduct(filter: { name: { regexp: "/^Product 1.*/" } }) { sku name } }`,
		},
		{
			Label: "cascade",
			Query: `query { queryProduct(first: 50) @cascade { sku category { slug } } }`,
		},
		{
			Label: "nested-filter",
			Query: `query { queryProduct(first: 20) { sku reviews(filter: { stars: { ge: 4 } }) { stars } } }`,
		},
		{
			Label: "aggregate-sum",
			Query: `query { aggregateProduct { count priceSum priceMin priceMax } }`,
		},
		{
			Label: "introspection-typename",
			Query: `query { __typename queryProduct(first: 1) { __typename sku } }`,
		},
		{
			Label: "introspection-type",
			Query: `query { __type(name: "Product") { name kind fields { name } } }`,
		},
		{
			Label: "interface-query-fragments",
			Query: `query { queryAnimal { xid name __typename ... on Dog { goodBoy } ... on Cat { lives } } }`,
		},
		{
			Label: "interop-get-personview",
			Query: `query { getPersonView(pxid: "person-0") { pxid pname age bio } }`,
		},
		{
			Label: "interop-aggregate-personview",
			Query: `query { aggregatePersonView { count } }`,
		},
	}
}
