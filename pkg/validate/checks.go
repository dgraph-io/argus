package validate

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/argus/pkg/cluster"
)

// CheckResult holds the result of validation checks.
type CheckResult struct {
	AllPassed bool
	CheckCount int
	Errors    []string
}

// Checker performs correctness validation.
type Checker struct {
	dgraph *cluster.DgraphClient
}

// NewChecker creates a new correctness checker.
func NewChecker(dgraph *cluster.DgraphClient) *Checker {
	return &Checker{dgraph: dgraph}
}

// CheckAll runs all correctness checks.
func (c *Checker) CheckAll(ctx context.Context) (*CheckResult, error) {
	result := &CheckResult{
		AllPassed:  true,
		CheckCount: 0,
		Errors:     make([]string, 0),
	}

	checks := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"schema_applied", c.checkSchemaApplied},
		{"data_exists", c.checkDataExists},
		{"person_type_count", c.checkPersonTypeCount},
		{"index_functionality", c.checkIndexFunctionality},
	}

	for _, check := range checks {
		result.CheckCount++
		if err := check.fn(ctx); err != nil {
			result.AllPassed = false
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", check.name, err))
		}
	}

	return result, nil
}

func (c *Checker) checkSchemaApplied(ctx context.Context) error {
	// Query for a known type to verify schema was applied
	query := `{ q(func: type(Person)) { uid } }`
	txn := c.dgraph.ReadOnlyTxn()
	defer txn.Discard(ctx)

	_, err := txn.Query(ctx, query)
	return err
}

func (c *Checker) checkDataExists(ctx context.Context) error {
	query := `{ q(func: has(uid)) { uid } }`
	txn := c.dgraph.ReadOnlyTxn()
	defer txn.Discard(ctx)

	resp, err := txn.Query(ctx, query)
	if err != nil {
		return err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp, &result); err != nil {
		return err
	}

	// Check that we have some data
	q := result["q"]
	if q == nil {
		return fmt.Errorf("no data found in graph")
	}

	arr, ok := q.([]interface{})
	if !ok || len(arr) == 0 {
		return fmt.Errorf("expected non-empty result set")
	}

	return nil
}

func (c *Checker) checkPersonTypeCount(ctx context.Context) error {
	query := `{ q(func: type(Person)) { uid } }`
	txn := c.dgraph.ReadOnlyTxn()
	defer txn.Discard(ctx)

	resp, err := txn.Query(ctx, query)
	if err != nil {
		return err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp, &result); err != nil {
		return err
	}

	// At least some Person nodes should exist
	q := result["q"]
	if q == nil {
		return fmt.Errorf("no Person nodes found")
	}

	arr, ok := q.([]interface{})
	if !ok || len(arr) == 0 {
		return fmt.Errorf("expected at least one Person node")
	}

	return nil
}

func (c *Checker) checkIndexFunctionality(ctx context.Context) error {
	// Test that indexes are working by querying with an index
	query := `{ q(func: eq(email, "user1@example.com")) { uid email } }`
	txn := c.dgraph.ReadOnlyTxn()
	defer txn.Discard(ctx)

	_, err := txn.Query(ctx, query)
	// It's OK if no results are found; the point is the index query should work
	return err
}
