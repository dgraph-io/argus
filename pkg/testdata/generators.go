package testdata

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/dgraph-io/dgo/v250/protos/api"
)

// Generator produces test data mutations.
type Generator struct {
	rng   *rand.Rand
	count int
}

// NewGenerator creates a new test data generator.
func NewGenerator() *Generator {
	return &Generator{
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// GenerateMutation returns a JSON mutation for a single node.
func (g *Generator) GenerateMutation() *api.Mutation {
	g.count++

	node := map[string]interface{}{
		"dgraph.type": "Person",
		"name":        g.randomName(),
		"email":       fmt.Sprintf("user%d@example.com", g.count),
		"age":         int32(20 + g.rng.Intn(60)),
		"bio":         g.randomBio(),
		"created_at":  time.Now().UTC().Format(time.RFC3339),
	}

	data, _ := json.Marshal(node)

	return &api.Mutation{
		SetJson: data,
	}
}

func (g *Generator) randomName() string {
	names := []string{
		"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
		"Iris", "Jack", "Kate", "Leo", "Mona", "Noah", "Olivia", "Peter",
	}
	return names[g.rng.Intn(len(names))]
}

func (g *Generator) randomBio() string {
	bios := []string{
		"Software engineer",
		"Data scientist",
		"Product manager",
		"Designer",
		"DevOps engineer",
		"Full-stack developer",
		"Security researcher",
		"ML engineer",
	}
	return bios[g.rng.Intn(len(bios))]
}

// GeneratePerson creates a Person node with random data.
func GeneratePerson(id int) map[string]interface{} {
	rng := rand.New(rand.NewSource(int64(id)))
	return map[string]interface{}{
		"dgraph.type": "Person",
		"name":        fmt.Sprintf("Person_%d", id),
		"email":       fmt.Sprintf("person%d@example.com", id),
		"age":         20 + rng.Intn(60),
		"bio":         fmt.Sprintf("Bio for person %d", id),
		"created_at":  time.Now().UTC().Format(time.RFC3339),
	}
}

// GenerateCompany creates a Company node with random data.
func GenerateCompany(id int) map[string]interface{} {
	return map[string]interface{}{
		"dgraph.type": "Company",
		"name":        fmt.Sprintf("Company_%d", id),
		"domain":      fmt.Sprintf("company%d.example.com", id),
		"founded":     time.Now().UTC().Format(time.RFC3339),
	}
}

// GeneratePost creates a Post node with random data.
func GeneratePost(id int) map[string]interface{} {
	return map[string]interface{}{
		"dgraph.type": "Post",
		"title":       fmt.Sprintf("Post Title %d", id),
		"content":     fmt.Sprintf("This is the content for post %d", id),
		"likes":       0,
		"created_at":  time.Now().UTC().Format(time.RFC3339),
	}
}
