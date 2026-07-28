package report

import (
	"fmt"
	"strings"

	"github.com/google/pprof/profile"
)

// ContentionDiff compares two mutex- or block-contention profiles by share of
// blocked time (the "delay" sample, nanoseconds). It localizes an off-CPU
// regression — latency lost to lock contention or blocking on channels, I/O, and
// syscalls — the way CPUDiff localizes on-CPU cost. Diagnostic only; never gates.
type ContentionDiff struct {
	Kind          string // "mutex" or "block"
	BaselinePath  string
	CandidatePath string
	BaseTotal     int64 // total delay nanoseconds
	CandTotal     int64
	Sites         []shareSite // biggest |DeltaPP| first
}

// contentionSkip are frame prefixes to walk past when attributing contention.
// Go's mutex/block profiler blames the synchronization primitive itself
// (sync.(*Mutex).Unlock, runtime.selectgo, ...), which is the same for every
// lock; the actionable owner is the nearest caller outside these packages.
var contentionSkip = []string{"sync.", "sync/atomic.", "runtime.", "internal/runtime/"}

// attributedFunc returns the nearest stack frame (leaf-to-root) that isn't a
// synchronization/runtime primitive — i.e. the code that held the lock or did the
// blocking call. Falls back to the leaf if the whole stack is runtime/sync.
func attributedFunc(s *profile.Sample) string {
	leaf := ""
	for _, loc := range s.Location { // Location[0] = leaf, then outward
		for _, ln := range loc.Line { // Line[0] = innermost inlined frame
			if ln.Function == nil {
				continue
			}
			name := ln.Function.Name
			if leaf == "" {
				leaf = name
			}
			if !hasAnyPrefix(name, contentionSkip) {
				return name
			}
		}
	}
	if leaf != "" {
		return leaf
	}
	return "(unknown)"
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

// DiffContention diffs two contention profiles' "delay" samples by share,
// attributing each sample to the caller that held the lock (not the sync
// primitive). kind is "mutex" or "block" and only labels the output.
func DiffContention(kind, basePath, candPath string, topN int) (*ContentionDiff, error) {
	sites, bTot, cTot, err := diffShare("delay", basePath, candPath, topN, attributedFunc)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", kind, err)
	}
	return &ContentionDiff{
		Kind: kind, BaselinePath: basePath, CandidatePath: candPath,
		BaseTotal: bTot, CandTotal: cTot, Sites: sites,
	}, nil
}

// Render draws the contention section with total blocked time for context.
func (d *ContentionDiff) Render() string {
	return renderShare(fmt.Sprintf("%s contention (share of blocked time, by lock holder; %.2fs baseline / %.2fs candidate blocked)",
		d.Kind, float64(d.BaseTotal)/1e9, float64(d.CandTotal)/1e9), d.Sites)
}
