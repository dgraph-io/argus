// Package workload runs the concurrent read/write benchmark. Writers and
// readers execute simultaneously over the time budget. Writes are idempotent
// upserts partitioned by stable key, so the graph converges deterministically;
// a finalize pass pins the end state (persistent upserted, ephemeral deleted).
package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/argus/pkg/cluster"
	"github.com/dgraph-io/argus/pkg/graphql"
	"github.com/dgraph-io/argus/pkg/metrics"
	"github.com/dgraph-io/argus/pkg/queries"
	"github.com/dgraph-io/argus/pkg/testdata"
	"github.com/dgraph-io/dgo/v250/protos/api"
)

// Retry budgets for write upserts. Concurrent cross-referencing upserts (a
// person writes its friends' keys) conflict and abort; dgo does not auto-retry.
// The timed phase retries a few times (best-effort load); the finalize pass
// runs serially with a generous budget so the deterministic end state is
// guaranteed to commit.
const (
	timedWriteAttempts    = 4
	finalizeWriteAttempts = 50
)

// Config tunes concurrency and the dataset.
type Config struct {
	WriteConcurrency int
	ReadConcurrency  int
	Dataset          testdata.Dataset
	Strict           bool
}

// Scheduler executes the mixed workload against the cluster.
type Scheduler struct {
	dgraph    *cluster.DgraphClient
	gql       *graphql.Client
	collector *metrics.Collector
	cfg       Config

	cancel   context.CancelFunc
	errMu    sync.Mutex
	firstErr error
}

// New creates a workload scheduler.
func New(dgraph *cluster.DgraphClient, gql *graphql.Client, collector *metrics.Collector, cfg Config) *Scheduler {
	if cfg.WriteConcurrency < 1 {
		cfg.WriteConcurrency = 8
	}
	if cfg.ReadConcurrency < 1 {
		cfg.ReadConcurrency = 8
	}
	return &Scheduler{dgraph: dgraph, gql: gql, collector: collector, cfg: cfg}
}

// Run executes write passes and read load concurrently until the deadline,
// always completing any in-flight write pass (completeness wins), then runs a
// finalize pass to pin the deterministic end state. Returns the first error
// seen in strict mode.
func (s *Scheduler) Run(ctx context.Context, deadline time.Time) error {
	ctx, s.cancel = context.WithCancel(ctx)
	defer s.cancel()

	// Readers run for the whole duration of the write loop.
	readCtx, cancelReaders := context.WithCancel(ctx)
	var readers sync.WaitGroup
	for i := 0; i < s.cfg.ReadConcurrency; i++ {
		readers.Add(1)
		go func(id int) {
			defer readers.Done()
			s.readLoop(readCtx, id)
		}(i)
	}

	passes := 0
	for passes == 0 || time.Now().Before(deadline) {
		if ctx.Err() != nil {
			break
		}
		slog.Info("write pass starting", "pass", passes+1)
		s.WritePass(ctx, s.cfg.WriteConcurrency, timedWriteAttempts, true)
		passes++
		slog.Info("write pass complete", "pass", passes)
	}

	cancelReaders()
	readers.Wait()

	slog.Info("running finalize pass (serial, guarantees deterministic end state)")
	s.Finalize(ctx)

	// In non-strict mode, transient conflicts are retried and the serial
	// finalize pass guarantees correctness, so validation (not a logged op
	// error) decides success. Strict mode surfaces the first error.
	if s.cfg.Strict {
		return s.Err()
	}
	return nil
}

// WritePass applies one full deterministic write pass at the given concurrency,
// retrying each write up to `attempts` times on transaction conflict. Stages run
// sequentially (create-before-link ordering); work within a stage is fanned out
// across `conc` workers.
func (s *Scheduler) WritePass(ctx context.Context, conc, attempts int, ledger bool) {
	d := s.cfg.Dataset

	// Stage 1: categories and companies (referenced by later stages).
	stage1 := make([]task, 0, d.NumCategories+d.NumCompanies)
	for i := 0; i < d.NumCategories; i++ {
		c := d.Category(i)
		stage1 = append(stage1, func(ctx context.Context) { s.gqlWrite(ctx, "category", testdata.AddCategory(c), attempts) })
	}
	for i := 0; i < d.NumCompanies; i++ {
		c := d.Company(i)
		stage1 = append(stage1, func(ctx context.Context) { s.dqlWrite(ctx, metrics.OpDQLWrite, "company", testdata.CompanyUpsert(c), attempts) })
	}
	runPool(ctx, conc, stage1)

	// Stage 2: persons, products, posts, reviews — mixed DQL+GraphQL load.
	// DQL writes are split 50/50 by index: even indices go through single-record
	// upserts, odd indices are packed into bulk upserts (~BulkEdgeTarget edges
	// each). Both styles carry distinct labels so the CSV can compare them.
	stage2 := make([]task, 0, d.NumPersons+d.NumProducts+d.NumPosts+d.NumProducts*d.ReviewsPerProduct)

	var bulkPersons []testdata.Person
	for i := 0; i < d.NumPersons; i++ {
		p := d.Person(i)
		if i%2 == 0 {
			stage2 = append(stage2, func(ctx context.Context) { s.dqlWrite(ctx, metrics.OpDQLWrite, "person", testdata.PersonUpsert(p), attempts) })
		} else {
			bulkPersons = append(bulkPersons, p)
		}
	}
	for _, req := range testdata.BulkPersonUpserts(bulkPersons, testdata.BulkEdgeTarget) {
		r := req
		stage2 = append(stage2, func(ctx context.Context) { s.dqlWrite(ctx, metrics.OpDQLWrite, "person-bulk", r, attempts) })
	}

	var bulkPosts []testdata.Post
	for i := 0; i < d.NumPosts; i++ {
		p := d.Post(i)
		if i%2 == 0 {
			stage2 = append(stage2, func(ctx context.Context) { s.dqlWrite(ctx, metrics.OpDQLWrite, "post", testdata.PostUpsert(p), attempts) })
		} else {
			bulkPosts = append(bulkPosts, p)
		}
	}
	for _, req := range testdata.BulkPostUpserts(bulkPosts, testdata.BulkEdgeTarget) {
		r := req
		stage2 = append(stage2, func(ctx context.Context) { s.dqlWrite(ctx, metrics.OpDQLWrite, "post-bulk", r, attempts) })
	}

	for i := 0; i < d.NumProducts; i++ {
		p := d.Product(i)
		stage2 = append(stage2, func(ctx context.Context) { s.gqlWrite(ctx, "product", testdata.AddProduct(p), attempts) })
	}
	for i := 0; i < d.NumProducts; i++ {
		for j := 0; j < d.ReviewsPerProduct; j++ {
			r := d.Review(i, j)
			stage2 = append(stage2, func(ctx context.Context) { s.gqlWrite(ctx, "review", testdata.AddReview(r), attempts) })
		}
	}
	runPool(ctx, conc, stage2)

	// Stage 3: ephemeral churn — create then delete, exercising the delete path.
	stage3 := make([]task, 0, d.NumEphPersons+d.NumEphProducts)
	for i := 0; i < d.NumEphPersons; i++ {
		p := d.EphPerson(i)
		stage3 = append(stage3, func(ctx context.Context) {
			s.dqlWrite(ctx, metrics.OpDQLWrite, "eph-person", testdata.PersonUpsert(p), attempts)
			s.dqlWrite(ctx, metrics.OpDQLDelete, "eph-person", testdata.DeleteByXid(p.Xid), attempts)
		})
	}
	for i := 0; i < d.NumEphProducts; i++ {
		p := d.EphProduct(i)
		stage3 = append(stage3, func(ctx context.Context) {
			s.gqlWrite(ctx, "eph-product", testdata.AddProduct(p), attempts)
			s.gqlDelete(ctx, "eph-product", testdata.DeleteProduct(p.SKU), attempts)
		})
	}
	runPool(ctx, conc, stage3)

	if !ledger {
		return
	}

	// Stage 4 (timed phase only): ledger churn. Each post's likes and each
	// product's price is incremented then decremented by a deterministic delta
	// via read-modify-write, exercising in-place mutation under concurrent load.
	// The +d/-d pair nets to zero; the canonical value is pinned by the normal
	// (ledger-free) finalize pass and verified by the complete checks.
	ledgerTasks := make([]task, 0, d.NumPosts+d.NumProducts)
	for i := 0; i < d.NumPosts; i++ {
		xid := d.Post(i).Xid
		delta := d.PostLikeDelta(i)
		ledgerTasks = append(ledgerTasks, func(ctx context.Context) {
			s.rmwLikes(ctx, xid, delta, attempts)
			s.rmwLikes(ctx, xid, -delta, attempts)
		})
	}
	for i := 0; i < d.NumProducts; i++ {
		sku := d.Product(i).SKU
		delta := d.ProductPriceDelta(i)
		ledgerTasks = append(ledgerTasks, func(ctx context.Context) {
			s.rmwPrice(ctx, sku, delta)
			s.rmwPrice(ctx, sku, -delta)
		})
	}
	runPool(ctx, conc, ledgerTasks)
}

// Finalize guarantees the deterministic end state. It runs serially (no
// concurrent writers => no transaction conflicts) with a generous retry budget,
// re-applying every persistent record and deleting every ephemeral record. All
// operations are idempotent, so this converges regardless of mid-run churn.
func (s *Scheduler) Finalize(ctx context.Context) {
	d := s.cfg.Dataset

	// Serial full pass pins every persistent record's values and edges. Ledger
	// churn is disabled here so likes/price settle to their canonical values.
	s.WritePass(ctx, 1, finalizeWriteAttempts, false)

	// Explicitly delete every ephemeral key in case the churn above was
	// interrupted between create and delete.
	dels := make([]task, 0, d.NumEphPersons+d.NumEphProducts)
	for i := 0; i < d.NumEphPersons; i++ {
		xid := d.EphPerson(i).Xid
		dels = append(dels, func(ctx context.Context) { s.dqlWrite(ctx, metrics.OpDQLDelete, "eph-person", testdata.DeleteByXid(xid), finalizeWriteAttempts) })
	}
	for i := 0; i < d.NumEphProducts; i++ {
		sku := d.EphProduct(i).SKU
		dels = append(dels, func(ctx context.Context) { s.gqlDelete(ctx, "eph-product", testdata.DeleteProduct(sku), finalizeWriteAttempts) })
	}
	runPool(ctx, 1, dels)
}

func (s *Scheduler) readLoop(ctx context.Context, id int) {
	dqlQs := queries.DQLQueries()
	gqlQs := queries.GraphQLQueries()
	for i := id; ctx.Err() == nil; i++ {
		if i%2 == 0 {
			q := dqlQs[(i/2)%len(dqlQs)]
			s.dqlQuery(ctx, q)
		} else {
			q := gqlQs[(i/2)%len(gqlQs)]
			s.gqlQuery(ctx, q)
		}
	}
}

// --- execution + metrics ---

func (s *Scheduler) dqlWrite(ctx context.Context, opType, label string, req *api.Request, attempts int) {
	start := time.Now()
	var resp *api.Response
	var err error
	for a := 0; a < attempts; a++ {
		txn := s.dgraph.Txn()
		resp, err = txn.Do(ctx, req)
		txn.Discard(ctx)
		if err == nil || ctx.Err() != nil || !isConflict(err) {
			break
		}
		backoff(a)
	}
	s.observe(opType+":"+label, time.Since(start), dqlServerNs(resp), dqlTouched(resp), err)
	s.onErr(opType+":"+label, err)
}

func (s *Scheduler) dqlQuery(ctx context.Context, q queries.DQLQuery) {
	start := time.Now()
	txn := s.dgraph.ReadOnlyTxn()
	defer txn.Discard(ctx)
	var resp *api.Response
	var err error
	if len(q.Vars) > 0 {
		resp, err = txn.QueryWithVars(ctx, q.Query, q.Vars)
	} else {
		resp, err = txn.Query(ctx, q.Query)
	}
	s.observe(metrics.OpDQLQuery+":"+q.Label, time.Since(start), dqlServerNs(resp), dqlTouched(resp), err)
	s.onErr("dql-query:"+q.Label, err)
}

func (s *Scheduler) gqlWrite(ctx context.Context, label string, m testdata.GraphQLMutation, attempts int) {
	s.gqlExec(ctx, metrics.OpGQLWrite+":"+label, m.Query, m.Vars, attempts)
}

func (s *Scheduler) gqlDelete(ctx context.Context, label string, m testdata.GraphQLMutation, attempts int) {
	s.gqlExec(ctx, metrics.OpGQLDelete+":"+label, m.Query, m.Vars, attempts)
}

func (s *Scheduler) gqlQuery(ctx context.Context, q queries.GraphQLQuery) {
	s.gqlExec(ctx, metrics.OpGQLQuery+":"+q.Label, q.Query, q.Vars, 1)
}

func (s *Scheduler) gqlExec(ctx context.Context, key, query string, vars map[string]interface{}, attempts int) {
	start := time.Now()
	var resp *graphql.Response
	var err error
	for a := 0; a < attempts; a++ {
		resp, err = s.gql.Query(ctx, query, vars)
		if err == nil && resp != nil && len(resp.Errors) > 0 {
			err = fmt.Errorf("graphql: %s", resp.Errors[0].Message)
		}
		if err == nil || ctx.Err() != nil || !isConflict(err) {
			break
		}
		backoff(a)
	}
	d := time.Since(start)
	var serverNs, touched uint64
	if resp != nil && resp.Extensions != nil {
		touched = resp.Extensions.TouchedUids
		if resp.Extensions.Tracing != nil {
			serverNs = uint64(resp.Extensions.Tracing.Duration)
		}
	}
	s.observe(key, d, serverNs, touched, err)
	s.onErr(key, err)
}

// rmwLikes performs an optimistic-concurrency read-modify-write on a post's
// likes within one transaction: it reads the current value, writes value+delta,
// and commits — which aborts (and retries) if another writer touched likes
// meanwhile. This genuinely exercises Dgraph's transaction conflict handling.
func (s *Scheduler) rmwLikes(ctx context.Context, xid string, delta, attempts int) {
	start := time.Now()
	label := "likes-incr"
	if delta < 0 {
		label = "likes-decr"
	}
	var err error
	for a := 0; a < attempts; a++ {
		txn := s.dgraph.Txn()
		var resp *api.Response
		resp, err = txn.Query(ctx, fmt.Sprintf(`{ q(func: eq(xid, %q)) { uid likes } }`, xid))
		if err != nil {
			txn.Discard(ctx)
			if ctx.Err() != nil {
				break
			}
			backoff(a)
			continue
		}
		uid, cur, found := parseUIDLikes(resp.Json)
		if !found {
			txn.Discard(ctx)
			return // post not yet created this instant; skip silently
		}
		mu := &api.Mutation{SetNquads: []byte(fmt.Sprintf("<%s> <likes> \"%d\"^^<xs:int> .", uid, cur+delta))}
		if _, err = txn.Mutate(ctx, mu); err == nil {
			err = txn.Commit(ctx)
		}
		txn.Discard(ctx)
		if err == nil || ctx.Err() != nil || !isConflict(err) {
			break
		}
		backoff(a)
	}
	s.observe(metrics.OpDQLLedger+":"+label, time.Since(start), 0, 0, err)
	s.onErr("dql-ledger:"+label, err)
}

// rmwPrice performs a read-modify-write on a product's price over GraphQL
// (get then updateProduct). GraphQL has no client transaction, so this is
// last-write-wins rather than conflict-detected.
func (s *Scheduler) rmwPrice(ctx context.Context, sku string, delta float64) {
	start := time.Now()
	label := "price-incr"
	if delta < 0 {
		label = "price-decr"
	}
	cur, found, err := s.getPrice(ctx, sku)
	if err == nil && !found {
		return // product not yet created this instant; skip silently
	}
	if err == nil {
		const mut = `mutation($sku: String!, $p: Float!) {
			updateProduct(input: { filter: { sku: { eq: $sku } }, set: { price: $p } }) { numUids }
		}`
		var resp *graphql.Response
		resp, err = s.gql.Query(ctx, mut, map[string]interface{}{"sku": sku, "p": cur + delta})
		if err == nil && resp != nil && len(resp.Errors) > 0 {
			err = fmt.Errorf("graphql: %s", resp.Errors[0].Message)
		}
	}
	s.observe(metrics.OpGQLLedger+":"+label, time.Since(start), 0, 0, err)
	s.onErr("gql-ledger:"+label, err)
}

func (s *Scheduler) getPrice(ctx context.Context, sku string) (float64, bool, error) {
	resp, err := s.gql.Query(ctx, `query($sku: String!) { getProduct(sku: $sku) { price } }`,
		map[string]interface{}{"sku": sku})
	if err != nil {
		return 0, false, err
	}
	if len(resp.Errors) > 0 {
		return 0, false, fmt.Errorf("graphql: %s", resp.Errors[0].Message)
	}
	var out struct {
		GetProduct *struct {
			Price float64 `json:"price"`
		} `json:"getProduct"`
	}
	if err := json.Unmarshal(resp.Data, &out); err != nil {
		return 0, false, err
	}
	if out.GetProduct == nil {
		return 0, false, nil
	}
	return out.GetProduct.Price, true, nil
}

func parseUIDLikes(data []byte) (uid string, likes int, found bool) {
	var out struct {
		Q []struct {
			UID   string `json:"uid"`
			Likes int    `json:"likes"`
		} `json:"q"`
	}
	if json.Unmarshal(data, &out) != nil || len(out.Q) == 0 {
		return "", 0, false
	}
	return out.Q[0].UID, out.Q[0].Likes, true
}

func (s *Scheduler) observe(key string, d time.Duration, serverNs, touched uint64, err error) {
	s.collector.Observe(key, d, serverNs, touched, err == nil)
}

func (s *Scheduler) onErr(op string, err error) {
	if err == nil {
		return
	}
	slog.Debug("operation error", "op", op, "err", err)
	s.errMu.Lock()
	if s.firstErr == nil {
		s.firstErr = fmt.Errorf("%s: %w", op, err)
	}
	s.errMu.Unlock()
	if s.cfg.Strict {
		s.cancel()
	}
}

// Err returns the first error observed (nil if none).
func (s *Scheduler) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.firstErr
}

// isConflict reports whether err is a retryable transaction conflict/abort.
func isConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "aborted") ||
		strings.Contains(msg, "conflict") ||
		strings.Contains(msg, "please retry")
}

// backoff sleeps a short, attempt-scaled interval between retries.
func backoff(attempt int) {
	d := time.Duration(attempt+1) * 3 * time.Millisecond
	if d > 60*time.Millisecond {
		d = 60 * time.Millisecond
	}
	time.Sleep(d)
}

func dqlServerNs(resp *api.Response) uint64 {
	if resp != nil && resp.Latency != nil {
		return resp.Latency.TotalNs
	}
	return 0
}

func dqlTouched(resp *api.Response) uint64 {
	if resp == nil || resp.Metrics == nil {
		return 0
	}
	var sum uint64
	for _, v := range resp.Metrics.NumUids {
		sum += v
	}
	return sum
}

// --- worker pool ---

type task func(context.Context)

// runPool runs tasks across conc workers. It does not abort on the time budget
// (callers govern whether to start a pass); it stops dispatching only on hard
// context cancellation (e.g. SIGINT or strict-mode failure).
func runPool(ctx context.Context, conc int, tasks []task) {
	if conc < 1 {
		conc = 1
	}
	ch := make(chan task)
	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range ch {
				if ctx.Err() != nil {
					continue // drain without executing once cancelled
				}
				t(ctx)
			}
		}()
	}
	for _, t := range tasks {
		if ctx.Err() != nil {
			break
		}
		ch <- t
	}
	close(ch)
	wg.Wait()
}
