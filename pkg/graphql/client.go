// Package graphql provides a thin HTTP client for Dgraph's GraphQL endpoints.
//
// We deliberately use net/http rather than a community GraphQL client. The
// struct-based clients (shurcooL/graphql, hasura/go-graphql-client) require
// compile-time Go structs per query, which is a poor fit for a benchmark
// harness that fires many dynamic query strings. machinebox/graphql handles
// raw strings but is unmaintained. A thin native client gives precise timing
// control, zero dependency risk, and direct access to the touched_uids and
// tracing extensions Dgraph returns.
package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client is an HTTP client for Dgraph GraphQL and admin endpoints.
type Client struct {
	httpClient *http.Client
	graphqlURL string // e.g. http://localhost:8080/graphql
	adminURL   string // e.g. http://localhost:8080/admin
}

// Request is a GraphQL request body.
type Request struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables,omitempty"`
}

// Response is a GraphQL response body.
type Response struct {
	Data       json.RawMessage `json:"data"`
	Errors     []GqlError      `json:"errors,omitempty"`
	Extensions *Extensions     `json:"extensions,omitempty"`
}

// GqlError is a single GraphQL error.
type GqlError struct {
	Message string `json:"message"`
}

// Extensions holds Dgraph's GraphQL response extensions.
type Extensions struct {
	TouchedUids uint64 `json:"touched_uids"`
	Tracing     *struct {
		Duration int64 `json:"duration"` // nanoseconds
	} `json:"tracing,omitempty"`
}

// NewClient creates a GraphQL client. baseURL is the alpha HTTP address,
// e.g. "http://localhost:8080".
func NewClient(baseURL string) *Client {
	baseURL = strings.TrimRight(baseURL, "/")
	return &Client{
		httpClient: &http.Client{Timeout: 60 * time.Second},
		graphqlURL: baseURL + "/graphql",
		adminURL:   baseURL + "/admin",
	}
}

// Query executes a GraphQL query against /graphql.
func (c *Client) Query(ctx context.Context, query string, vars map[string]interface{}) (*Response, error) {
	return c.do(ctx, c.graphqlURL, query, vars)
}

// Admin executes a GraphQL query/mutation against /admin.
func (c *Client) Admin(ctx context.Context, query string, vars map[string]interface{}) (*Response, error) {
	return c.do(ctx, c.adminURL, query, vars)
}

// UpdateSchema sets the GraphQL schema via the admin updateGQLSchema mutation.
func (c *Client) UpdateSchema(ctx context.Context, schema string) error {
	const mutation = `mutation($sch: String!) {
		updateGQLSchema(input: { set: { schema: $sch } }) {
			gqlSchema { id }
		}
	}`
	resp, err := c.Admin(ctx, mutation, map[string]interface{}{"sch": schema})
	if err != nil {
		return fmt.Errorf("updateGQLSchema failed: %w", err)
	}
	if len(resp.Errors) > 0 {
		return fmt.Errorf("updateGQLSchema returned errors: %s", resp.Errors[0].Message)
	}
	return nil
}

// Health checks the GraphQL admin endpoint is responsive.
func (c *Client) Health(ctx context.Context) error {
	resp, err := c.Admin(ctx, `{ health { status } }`, nil)
	if err != nil {
		return err
	}
	if len(resp.Errors) > 0 {
		return fmt.Errorf("health check error: %s", resp.Errors[0].Message)
	}
	return nil
}

// Version returns the cluster's Dgraph version via the admin health query.
func (c *Client) Version(ctx context.Context) (string, error) {
	resp, err := c.Admin(ctx, `{ health { version } }`, nil)
	if err != nil {
		return "", err
	}
	if len(resp.Errors) > 0 {
		return "", fmt.Errorf("version query error: %s", resp.Errors[0].Message)
	}
	var out struct {
		Health []struct {
			Version string `json:"version"`
		} `json:"health"`
	}
	if err := json.Unmarshal(resp.Data, &out); err != nil {
		return "", fmt.Errorf("decode health: %w", err)
	}
	if len(out.Health) == 0 || out.Health[0].Version == "" {
		return "unknown", nil
	}
	return out.Health[0].Version, nil
}

func (c *Client) do(ctx context.Context, url, query string, vars map[string]interface{}) (*Response, error) {
	body, err := json.Marshal(Request{Query: query, Variables: vars})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer httpResp.Body.Close()

	data, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d: %s", httpResp.StatusCode, truncate(data, 256))
	}

	var resp Response
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	return &resp, nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
