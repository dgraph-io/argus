package schema

// GetTestSchema returns the test schema covering diverse Dgraph features.
func GetTestSchema() string {
	return `
type Person {
  name: String
  email: String
  age: Int
  friends: [Person]
  company: Company
  bio: String @index(fulltext)
  created_at: DateTime
}

type Company {
  name: String
  domain: String
  employees: [Person]
  founded: DateTime
}

type Post {
  title: String @index(fulltext)
  content: String
  author: Person
  tags: [Tag]
  likes: Int
  created_at: DateTime
}

type Tag {
  name: String @index(exact)
  posts: [Post]
}

type Comment {
  text: String
  author: Person
  post: Post
  created_at: DateTime
}

# Predicates with various indexes and types
name: string @index(exact, fulltext) .
email: string @index(exact) .
age: int @index(int) .
friends: [uid] @reverse .
company: uid @reverse .
bio: string @index(fulltext) .
created_at: datetime @index(day) .
domain: string @index(exact) .
employees: [uid] @reverse .
founded: datetime .
title: string @index(fulltext) .
content: string .
author: uid @reverse .
tags: [uid] @reverse .
likes: int @index(int) .
text: string .
post: uid @reverse .
`
}
