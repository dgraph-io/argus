package testdata

// This file builds GraphQL add/delete mutations. All add mutations use
// upsert:true keyed by @id fields (slug/sku/key), so repeated application is
// idempotent and converges to the same logical graph.

// GraphQLMutation is a GraphQL mutation with its variables.
type GraphQLMutation struct {
	Query string
	Vars  map[string]interface{}
}

// AddCategory upserts a category by slug.
func AddCategory(c Category) GraphQLMutation {
	return GraphQLMutation{
		Query: `mutation($input: [AddCategoryInput!]!) {
			addCategory(input: $input, upsert: true) { numUids }
		}`,
		Vars: map[string]interface{}{
			"input": []map[string]interface{}{
				{"slug": c.Slug, "name": c.Name},
			},
		},
	}
}

// AddProduct upserts a product by sku, linking its category by slug.
func AddProduct(p Product) GraphQLMutation {
	return GraphQLMutation{
		Query: `mutation($input: [AddProductInput!]!) {
			addProduct(input: $input, upsert: true) { numUids }
		}`,
		Vars: map[string]interface{}{
			"input": []map[string]interface{}{
				{
					"sku":          p.SKU,
					"name":         p.Name,
					"description":  p.Description,
					"price":        p.Price,
					"availability": p.Availability,
					"rating":       p.Rating,
					"createdAt":    p.CreatedAt,
					"category":     map[string]interface{}{"slug": p.CategorySlug},
				},
			},
		},
	}
}

// AddReview upserts a review by key, linking its product by sku.
func AddReview(r Review) GraphQLMutation {
	return GraphQLMutation{
		Query: `mutation($input: [AddReviewInput!]!) {
			addReview(input: $input, upsert: true) { numUids }
		}`,
		Vars: map[string]interface{}{
			"input": []map[string]interface{}{
				{
					"key":       r.Key,
					"title":     r.Title,
					"body":      r.Body,
					"stars":     r.Stars,
					"reviewer":  r.Reviewer,
					"createdAt": r.CreatedAt,
					"product":   map[string]interface{}{"sku": r.ProductSKU},
				},
			},
		},
	}
}

// DeleteProduct deletes the product with the given sku. Idempotent: deleting a
// missing product matches nothing and returns numUids 0.
func DeleteProduct(sku string) GraphQLMutation {
	return GraphQLMutation{
		Query: `mutation($f: ProductFilter!) {
			deleteProduct(filter: $f) { numUids }
		}`,
		Vars: map[string]interface{}{
			"f": map[string]interface{}{"sku": map[string]interface{}{"eq": sku}},
		},
	}
}
