// Package schema holds the test schemas Argus applies to a cluster.
//
// To exercise GraphQL and DQL in a single graph without predicate collisions,
// the two schemas use disjoint type/predicate namespaces:
//
//   - DQL uses a social domain (Person, Company, Post, Tag, Comment) with bare
//     predicates (name, email, friends, ...).
//   - GraphQL uses a product-catalog domain (Product, Category, Review). Dgraph
//     auto-generates Type.field predicates (Product.name, ...) for these, so
//     they never clash with the DQL predicates above.
package schema

// DQLSchema returns the DQL schema covering diverse Dgraph features:
// scalar types, lists, uid edges, reverse edges, and a range of indexes
// (exact, hash, term, fulltext, trigram, int, datetime, geo).
func DQLSchema() string {
	return `
type Person {
  xid
  name
  email
  age
  score
  active
  bio
  tags_csv
  location
  embedding
  created_at
  friends
  company
}

type Company {
  xid
  name
  domain
  founded
  territory
  employees
}

type Post {
  xid
  title
  content
  likes
  created_at
  author
  topics
}

type Tag {
  tag_name
  posts
}

type Comment {
  text
  created_at
  comment_author
  comment_post
}

xid: string @index(exact) @upsert .
name: string @index(exact, term, fulltext) @lang .
email: string @index(hash, trigram) @upsert .
age: int @index(int) .
score: float @index(float) .
active: bool @index(bool) .
bio: string @index(fulltext, trigram, ngram) .
tags_csv: [string] @index(exact) .
location: geo @index(geo) .
embedding: float32vector @index(hnsw(metric: "euclidean")) .
created_at: datetime @index(hour) .
friends: [uid] @reverse @count .
company: uid @reverse .
domain: string @index(exact) .
founded: datetime .
territory: geo @index(geo) .
employees: [uid] @reverse .
title: string @index(fulltext, term) .
content: string @index(fulltext) .
likes: int @index(int) .
author: uid @reverse .
topics: [uid] @reverse .
tag_name: string @index(exact) @upsert .
posts: [uid] @reverse .
text: string @index(fulltext) .
comment_author: uid @reverse .
comment_post: uid @reverse .
`
}

// GraphQLSchema returns the GraphQL SDL covering @id, @search (multiple index
// kinds), @hasInverse bidirectional edges, enums, and scalar variety.
func GraphQLSchema() string {
	return `
enum Availability {
  IN_STOCK
  OUT_OF_STOCK
  DISCONTINUED
}

type Category {
  id: ID!
  slug: String! @id
  name: String! @search(by: [term])
  products: [Product] @hasInverse(field: category)
}

type Product {
  id: ID!
  sku: String! @id
  name: String! @search(by: [term, fulltext])
  description: String @search(by: [fulltext])
  price: Float! @search
  availability: Availability! @search
  rating: Float @search
  createdAt: DateTime! @search
  category: Category
  reviews: [Review] @hasInverse(field: product)
}

type Review {
  id: ID!
  key: String! @id
  title: String! @search(by: [fulltext])
  body: String @search(by: [fulltext])
  stars: Int! @search
  reviewer: String! @search(by: [hash])
  createdAt: DateTime! @search
  product: Product
}

# Counter drives the WebSocket subscription latency benchmark. @withSubscription
# enables getCounter/queryCounter subscriptions; the writer increments tick and
# a subscriber measures commit-to-push propagation latency.
type Counter @withSubscription {
  id: String! @id
  tick: Int! @search
}
`
}
