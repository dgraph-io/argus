// Package shardbalance deterministically places predicate tablets across a
// sharded Dgraph cluster's groups so that writes distribute instead of piling
// onto whichever group Zero happened to concentrate them on.
//
// Dgraph shards by predicate (a predicate is one tablet on one group), so write
// load spreads only when writes hit distinct predicates that live on distinct
// groups, and each write transaction stays within one group. This package
// therefore co-locates each entity type's predicates (GraphQL "Type.field" ->
// bucket "Type") and spreads the largest families across groups. Bare DQL
// predicates are SHARED across entities (e.g. xid/name), so they can't be split
// by entity — they're all placed on one group and their writes stay group-bound
// (an inherent limit; see the shard3 findings). GraphQL type-scoped predicates
// distribute cleanly.
//
// Run it after schema application and BEFORE any data load, when tablets are
// empty so moves are instant. It reads /state and issues Zero /moveTablet calls,
// which must hit the Zero leader (fine with a single zero, the default here). It
// is a no-op for clusters with fewer than 2 groups.
package shardbalance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Result summarizes a rebalance pass.
type Result struct {
	Groups  int
	Moved   int
	Skipped int // predicate already on its target group
	Failed  int
}

type tablet struct {
	bare    string // predicate without the "<ns>-" prefix, e.g. "Person.name"
	ns      string // namespace id ("0" for the root namespace)
	current string // current group id
}

// /state keys are namespace-prefixed, e.g. "0-Person.name"; moveTablet wants the
// bare predicate ("Person.name") plus a separate namespace param.
var nsPrefix = regexp.MustCompile(`^(\d+)-(.*)$`)

// assign computes the target group for each bare predicate. It buckets by entity
// type (GraphQL "Type.field" -> "Type"; every bare/dotless DQL predicate -> a
// single shared "_dql" bucket), then assigns the biggest buckets first, round
// robin across the (sorted) groups, so the heaviest write families land on
// distinct groups. Pure and deterministic — unit-tested without a cluster.
func assign(tablets map[string]tablet, groups []string) map[string]string {
	buckets := map[string][]string{}
	for bare := range tablets {
		if strings.HasPrefix(bare, "dgraph.") {
			continue // reserved/internal predicates — leave them where they are
		}
		key := "_dql"
		if i := strings.IndexByte(bare, '.'); i >= 0 {
			key = bare[:i]
		}
		buckets[key] = append(buckets[key], bare)
	}
	keys := make([]string, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	// Biggest bucket first; ties broken by name so the mapping is deterministic.
	sort.Slice(keys, func(a, b int) bool {
		if la, lb := len(buckets[keys[a]]), len(buckets[keys[b]]); la != lb {
			return la > lb
		}
		return keys[a] < keys[b]
	})
	out := map[string]string{}
	for i, k := range keys {
		g := groups[i%len(groups)]
		for _, bare := range buckets[k] {
			out[bare] = g
		}
	}
	return out
}

// Rebalance reads /state from zeroURL, computes the placement, and issues the
// moveTablet calls needed to reach it. No-op (Groups<2) for non-sharded clusters.
func Rebalance(ctx context.Context, zeroURL string) (Result, error) {
	st, err := fetchState(ctx, zeroURL)
	if err != nil {
		return Result{}, fmt.Errorf("fetch /state: %w", err)
	}
	groups := make([]string, 0, len(st.Groups))
	for g := range st.Groups {
		groups = append(groups, g)
	}
	sort.Slice(groups, func(i, j int) bool { return groupNum(groups[i]) < groupNum(groups[j]) })

	res := Result{Groups: len(groups)}
	if len(groups) < 2 {
		return res, nil // single group: nothing to distribute
	}

	tablets := map[string]tablet{}
	for g, gd := range st.Groups {
		for full := range gd.Tablets {
			bare, ns := full, "0"
			if m := nsPrefix.FindStringSubmatch(full); m != nil {
				ns, bare = m[1], m[2]
			}
			tablets[bare] = tablet{bare: bare, ns: ns, current: g}
		}
	}

	targets := assign(tablets, groups)
	// Sort predicates for stable, readable move ordering/logs.
	preds := make([]string, 0, len(targets))
	for p := range targets {
		preds = append(preds, p)
	}
	sort.Strings(preds)
	for _, bare := range preds {
		t, g := tablets[bare], targets[bare]
		if t.current == g {
			res.Skipped++
			continue
		}
		if err := moveTablet(ctx, zeroURL, t.bare, t.ns, g); err != nil {
			res.Failed++
			slog.Warn("shard-balance: moveTablet failed", "predicate", t.bare, "ns", t.ns, "group", g, "err", err)
			continue
		}
		res.Moved++
	}
	return res, nil
}

type stateResp struct {
	Groups map[string]struct {
		Tablets map[string]json.RawMessage `json:"tablets"`
	} `json:"groups"`
}

func fetchState(ctx context.Context, zeroURL string) (stateResp, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(zeroURL, "/")+"/state", nil)
	if err != nil {
		return stateResp{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return stateResp{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return stateResp{}, err
	}
	if resp.StatusCode/100 != 2 {
		return stateResp{}, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var st stateResp
	if err := json.Unmarshal(body, &st); err != nil {
		return stateResp{}, err
	}
	return st, nil
}

// moveTablet asks Zero to move a (bare) predicate to a group. Empty-tablet moves
// return quickly; the timeout leaves slack in case a tablet has some data.
func moveTablet(ctx context.Context, zeroURL, pred, ns, group string) error {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	q := url.Values{}
	q.Set("tablet", pred)
	q.Set("group", group)
	q.Set("namespace", ns)
	u := strings.TrimRight(zeroURL, "/") + "/moveTablet?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func groupNum(g string) int {
	n, err := strconv.Atoi(g)
	if err != nil {
		return 1 << 30 // non-numeric group ids sort last, deterministically
	}
	return n
}
