package cluster

import (
	"context"
	"fmt"

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

// Health performs a basic health check by executing a trivial valid query.
// Querying a (possibly nonexistent) type returns an empty result rather than
// an error, which is all we need to confirm the gRPC endpoint is responsive.
func (c *DgraphClient) Health(ctx context.Context) error {
	query := `{ q(func: type(ArgusHealthProbe), first: 1) { uid } }`
	_, err := c.Client.NewReadOnlyTxn().Query(ctx, query)
	if err != nil {
		return fmt.Errorf("health check query failed: %w", err)
	}
	return nil
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
