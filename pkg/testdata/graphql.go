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
		Vars: map[string]interface{}{"input": []map[string]interface{}{productInput(p)}},
	}
}

func productInput(p Product) map[string]interface{} {
	return map[string]interface{}{
		"sku":          p.SKU,
		"name":         p.Name,
		"description":  p.Description,
		"price":        p.Price,
		"availability": p.Availability,
		"rating":       p.Rating,
		"createdAt":    p.CreatedAt,
		"category":     map[string]interface{}{"slug": p.CategorySlug},
	}
}

func reviewInput(r Review) map[string]interface{} {
	return map[string]interface{}{
		"key":       r.Key,
		"title":     r.Title,
		"body":      r.Body,
		"stars":     r.Stars,
		"reviewer":  r.Reviewer,
		"createdAt": r.CreatedAt,
	}
}

// AddProductDeep upserts a product together with its reviews in a single deep
// (nested) mutation, rather than linking pre-created reviews. The reviews are
// created inline and linked via @hasInverse; @id keys keep it idempotent.
func AddProductDeep(p Product, reviews []Review) GraphQLMutation {
	in := productInput(p)
	rs := make([]map[string]interface{}, len(reviews))
	for i, r := range reviews {
		rs[i] = reviewInput(r) // no product link needed; @hasInverse sets it
	}
	in["reviews"] = rs
	return GraphQLMutation{
		Query: `mutation($input: [AddProductInput!]!) {
			addProduct(input: $input, upsert: true) { numUids }
		}`,
		Vars: map[string]interface{}{"input": []map[string]interface{}{in}},
	}
}

// SetProductNote sets a transient note on a product (exercises updateX set on a
// previously-unset field).
func SetProductNote(sku, note string) GraphQLMutation {
	return GraphQLMutation{
		Query: `mutation($sku: String!, $note: String!) {
			updateProduct(input: { filter: { sku: { eq: $sku } }, set: { note: $note } }) { numUids }
		}`,
		Vars: map[string]interface{}{"sku": sku, "note": note},
	}
}

// RemoveProductNote removes the given note value from a product (exercises
// updateX remove). Removing by value clears it when it matches.
func RemoveProductNote(sku, note string) GraphQLMutation {
	return GraphQLMutation{
		Query: `mutation($sku: String!, $note: String!) {
			updateProduct(input: { filter: { sku: { eq: $sku } }, remove: { note: $note } }) { numUids }
		}`,
		Vars: map[string]interface{}{"sku": sku, "note": note},
	}
}

// AddDog upserts a Dog (Animal interface member) by xid.
func AddDog(d Dog) GraphQLMutation {
	return GraphQLMutation{
		Query: `mutation($input: [AddDogInput!]!) {
			addDog(input: $input, upsert: true) { numUids }
		}`,
		Vars: map[string]interface{}{"input": []map[string]interface{}{
			{"xid": d.Xid, "name": d.Name, "goodBoy": d.GoodBoy},
		}},
	}
}

// AddCat upserts a Cat (Animal interface member) by xid.
func AddCat(c Cat) GraphQLMutation {
	return GraphQLMutation{
		Query: `mutation($input: [AddCatInput!]!) {
			addCat(input: $input, upsert: true) { numUids }
		}`,
		Vars: map[string]interface{}{"input": []map[string]interface{}{
			{"xid": c.Xid, "name": c.Name, "lives": c.Lives},
		}},
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
