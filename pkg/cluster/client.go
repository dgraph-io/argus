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
	conns  []*grpc.ClientConn
}

// NewDgraphClient connects to one or more alpha gRPC addresses. When several are
// given, dgo round-robins requests across them, spreading load over the cluster
// rather than funnelling it through a single alpha.
func NewDgraphClient(ctx context.Context, addrs []string) (*DgraphClient, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no cluster addresses provided")
	}
	dc := &DgraphClient{}
	apiClients := make([]api.DgraphClient, 0, len(addrs))
	for _, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			dc.Close()
			return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
		}
		dc.conns = append(dc.conns, conn)
		apiClients = append(apiClients, api.NewDgraphClient(conn))
	}
	dc.Client = dgo.NewDgraphClient(apiClients...)

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

// DropAll removes all data and schema, so a run starts from a clean slate and
// the deterministic dataset is exactly what the graph contains.
func (c *DgraphClient) DropAll(ctx context.Context) error {
	return c.Client.Alter(ctx, &api.Operation{DropOp: api.Operation_ALL})
}

// Txn returns a transaction for read-write operations.
func (c *DgraphClient) Txn() *dgo.Txn {
	return c.Client.NewTxn()
}

// ReadOnlyTxn returns a read-only transaction.
func (c *DgraphClient) ReadOnlyTxn() *dgo.Txn {
	return c.Client.NewReadOnlyTxn()
}

// Close closes all cluster connections.
func (c *DgraphClient) Close() error {
	var err error
	for _, conn := range c.conns {
		if e := conn.Close(); e != nil {
			err = e
		}
	}
	return err
}
