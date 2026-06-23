// Package subscription measures GraphQL subscription propagation latency: the
// time from committing a write to a WebSocket subscriber receiving an update
// reflecting it.
//
// Dgraph subscriptions are polling-based: the server re-runs the subscription
// query every poll-interval (default 1s), hashes the result, and pushes only on
// change. Multiple writes within an interval coalesce into one snapshot. To get
// a latency sample per write despite coalescing, a writer increments a Counter's
// tick at a steady cadence and records each commit time; the subscriber, on each
// pushed snapshot, attributes its arrival time to every not-yet-seen tick up to
// the snapshot's value. So a write's latency is the gap from its commit to the
// first push that reflects it.
package subscription

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/argus/pkg/graphql"
	"github.com/dgraph-io/argus/pkg/metrics"
	"github.com/gorilla/websocket"
)

const (
	counterID  = "latency"
	metricKey  = metrics.OpGQLSub + ":counter-latency"
	wsProtocol = "graphql-ws" // Dgraph uses the legacy subscriptions-transport-ws protocol
)

// Benchmark drives the subscription latency measurement.
type Benchmark struct {
	gql      *graphql.Client
	wsURL    string
	collector *metrics.Collector
	cadence  time.Duration

	mu     sync.Mutex
	commit map[int]time.Time // tick -> commit time
}

// New creates a subscription benchmark. cadence is how often the counter is
// incremented (a value near the server poll-interval yields the cleanest
// latency distribution).
func New(gql *graphql.Client, collector *metrics.Collector, cadence time.Duration) *Benchmark {
	if cadence <= 0 {
		cadence = time.Second
	}
	ws := strings.Replace(strings.Replace(gql.Endpoint(), "https://", "wss://", 1), "http://", "ws://", 1)
	return &Benchmark{
		gql:       gql,
		wsURL:     ws,
		collector: collector,
		cadence:   cadence,
		commit:    make(map[int]time.Time),
	}
}

// Run starts the writer and subscriber concurrently and blocks until ctx is
// cancelled. Errors are logged, not fatal: the subscription benchmark is an
// add-on to the main workload.
func (b *Benchmark) Run(ctx context.Context) {
	if err := b.ensureCounter(ctx); err != nil {
		slog.Warn("subscription: could not create counter; skipping", "err", err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); b.writeLoop(ctx) }()
	go func() { defer wg.Done(); b.subscribeLoop(ctx) }()
	wg.Wait()
}

func (b *Benchmark) ensureCounter(ctx context.Context) error {
	const m = `mutation($input: [AddCounterInput!]!) {
		addCounter(input: $input, upsert: true) { numUids }
	}`
	resp, err := b.gql.Query(ctx, m, map[string]interface{}{
		"input": []map[string]interface{}{{"id": counterID, "tick": 0}},
	})
	if err != nil {
		return err
	}
	if len(resp.Errors) > 0 {
		return fmt.Errorf("addCounter: %s", resp.Errors[0].Message)
	}
	return nil
}

// writeLoop increments the counter at the configured cadence, recording each
// commit time for the subscriber to match against.
func (b *Benchmark) writeLoop(ctx context.Context) {
	const m = `mutation($id: String!, $tick: Int!) {
		updateCounter(input: { filter: { id: { eq: $id } }, set: { tick: $tick } }) { numUids }
	}`
	ticker := time.NewTicker(b.cadence)
	defer ticker.Stop()
	tick := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick++
			resp, err := b.gql.Query(ctx, m, map[string]interface{}{"id": counterID, "tick": tick})
			if err != nil || (resp != nil && len(resp.Errors) > 0) {
				continue // transient; the next tick will advance the value
			}
			b.mu.Lock()
			b.commit[tick] = time.Now()
			b.mu.Unlock()
		}
	}
}

// subscribeLoop maintains a subscription, reconnecting on error until ctx ends.
func (b *Benchmark) subscribeLoop(ctx context.Context) {
	for ctx.Err() == nil {
		if err := b.subscribeOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Debug("subscription dropped; reconnecting", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
}

type opMessage struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func (b *Benchmark) subscribeOnce(ctx context.Context) error {
	dialer := websocket.Dialer{Subprotocols: []string{wsProtocol}, HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, b.wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// Close the connection when ctx ends so the blocking ReadJSON unblocks.
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	if err := conn.WriteJSON(opMessage{Type: "connection_init", Payload: json.RawMessage("{}")}); err != nil {
		return err
	}
	const sub = `subscription { getCounter(id: "` + counterID + `") { tick } }`
	startPayload, _ := json.Marshal(map[string]interface{}{"query": sub})
	started := false

	next := 1 // first tick we still need a latency sample for
	for {
		var m opMessage
		if err := conn.ReadJSON(&m); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		switch m.Type {
		case "connection_ack":
			if err := conn.WriteJSON(opMessage{Type: "start", ID: "1", Payload: startPayload}); err != nil {
				return err
			}
			started = true
		case "data":
			if !started {
				continue
			}
			tick, ok := parseTick(m.Payload)
			if !ok {
				continue
			}
			b.recordLatencies(tick, time.Now(), &next)
		case "error":
			return fmt.Errorf("subscription error: %s", string(m.Payload))
		case "complete":
			return nil
		}
	}
}

// recordLatencies attributes the arrival time of a snapshot with value `tick`
// to every unseen write up to tick, emitting one latency sample each.
func (b *Benchmark) recordLatencies(tick int, now time.Time, next *int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := *next; i <= tick; i++ {
		if c, ok := b.commit[i]; ok {
			b.collector.Observe(metricKey, now.Sub(c), 0, 0, true)
		}
	}
	if tick >= *next {
		*next = tick + 1
	}
}

func parseTick(payload json.RawMessage) (int, bool) {
	var p struct {
		Data struct {
			GetCounter *struct {
				Tick int `json:"tick"`
			} `json:"getCounter"`
		} `json:"data"`
	}
	if json.Unmarshal(payload, &p) != nil || p.Data.GetCounter == nil {
		return 0, false
	}
	return p.Data.GetCounter.Tick, true
}
