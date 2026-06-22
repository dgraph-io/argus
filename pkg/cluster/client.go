package cluster

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DgraphClient wraps the dgo client with health checking and version detection.
type DgraphClient struct {
	Client *dgo.Dgraph
	conn   *grpc.ClientConn
}

// NewDgraphClient creates a new connection to a Dgraph cluster.
func NewDgraphClient(ctx context.Context, addr string) (*DgraphClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial cluster: %w", err)
	}

	dc := &DgraphClient{
		Client: dgo.NewDgraphClient(api.NewDgraphClient(conn)),
		conn:   conn,
	}

	// Test connection
	if err := dc.Health(ctx); err != nil {
		dc.Close()
		return nil, fmt.Errorf("cluster health check failed: %w", err)
	}

	return dc, nil
}

// Health performs a basic health check by executing a simple query.
func (c *DgraphClient) Health(ctx context.Context) error {
	query := `{ q(func: has(uid)) { uid } }`
	_, err := c.Client.NewReadOnlyTxn().Query(ctx, query)
	if err != nil {
		return fmt.Errorf("health check query failed: %w", err)
	}
	return nil
}

// Version retrieves the Dgraph cluster version.
func (c *DgraphClient) Version(ctx context.Context) (string, error) {
	state, err := c.Client.NewReadOnlyTxn().QueryWithRwSet(ctx, "{ version(func: type(dgraph.type)) { uid } }")
	if err != nil {
		return "", fmt.Errorf("failed to query cluster state: %w", err)
	}

	// The version info is in the gRPC response headers, which dgo doesn't expose easily.
	// For now, return a placeholder; in practice we'd need to parse this from cluster headers.
	_ = state
	slog.Debug("version query executed", "txn_start_ts", state.TxnStartTs)
	return "unknown", nil
}

// ApplySchema applies a schema to the cluster.
func (c *DgraphClient) ApplySchema(ctx context.Context, schema string) error {
	op := &api.Operation{Schema: schema}
	return c.Client.Alter(ctx, op)
}

// Txn returns a transaction for read-write operations.
func (c *DgraphClient) Txn() *dgo.Txn {
	return c.Client.NewTxn()
}

// ReadOnlyTxn returns a read-only transaction.
func (c *DgraphClient) ReadOnlyTxn() *dgo.Txn {
	return c.Client.NewReadOnlyTxn()
}

// Close closes the cluster connection.
func (c *DgraphClient) Close() error {
	return c.conn.Close()
}
