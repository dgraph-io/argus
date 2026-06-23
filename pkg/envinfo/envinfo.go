// Package envinfo captures the run environment so performance results are
// comparable across hardware and cluster configurations: the load-generator
// host (CPU/RAM/OS), and the Dgraph cluster's version, build branch, and
// topology (group/shard and replica/HA layout) read from the alpha /state.
package envinfo

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"
)

// Info is the captured environment for a run.
type Info struct {
	// Load-generator host (the machine running argus).
	Hostname   string
	OS         string // GOOS/GOARCH
	NumCPU     int    // logical CPUs visible to the process
	GOMAXPROCS int
	TotalRAMGB float64
	GoVersion  string

	// Dgraph cluster (the system under test).
	DgraphVersion string // includes the commit hash, e.g. v25.3.4-7-g1577f25c1
	DgraphBranch  string // operator-supplied; not exposed by the cluster API
	ClusterGRPC   string
	ClusterHTTP   string
	ClusterID     string
	Groups        int // alpha groups == shards
	Replicas      int // members per group == replication / HA factor
	NumAlphas     int
	NumZeros      int
	Topology      string // human summary, e.g. "sharded+HA (3 groups x 3 replicas)"
}

// Collect gathers host facts and queries the cluster topology from httpBase
// (the alpha HTTP base URL). version comes from the cluster's health query;
// branch is operator-supplied (empty if unknown).
func Collect(ctx context.Context, httpBase, grpcAddr, version, branch string) Info {
	host, _ := os.Hostname()
	info := Info{
		Hostname:      host,
		OS:            runtime.GOOS + "/" + runtime.GOARCH,
		NumCPU:        runtime.NumCPU(),
		GOMAXPROCS:    runtime.GOMAXPROCS(0),
		TotalRAMGB:    float64(totalRAMBytes()) / (1 << 30),
		GoVersion:     runtime.Version(),
		DgraphVersion: version,
		DgraphBranch:  branch,
		ClusterGRPC:   grpcAddr,
		ClusterHTTP:   httpBase,
	}
	info.fetchTopology(ctx, httpBase)
	info.Topology = info.topologySummary()
	return info
}

type stateResp struct {
	Groups map[string]struct {
		Members map[string]struct {
			Addr   string `json:"addr"`
			AmDead bool   `json:"amDead"`
		} `json:"members"`
	} `json:"groups"`
	Zeros map[string]json.RawMessage `json:"zeros"`
	CID   string                     `json:"cid"`
}

func (i *Info) fetchTopology(ctx context.Context, httpBase string) {
	url := strings.TrimRight(httpBase, "/") + "/state"
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	var st stateResp
	if json.Unmarshal(body, &st) != nil {
		return
	}
	i.ClusterID = st.CID
	i.Groups = len(st.Groups)
	i.NumZeros = len(st.Zeros)
	for _, g := range st.Groups {
		i.NumAlphas += len(g.Members)
		if len(g.Members) > i.Replicas {
			i.Replicas = len(g.Members) // max members in any group == replication factor
		}
	}
}

func (i *Info) topologySummary() string {
	if i.Groups == 0 {
		return "unknown"
	}
	sharded := i.Groups > 1
	ha := i.Replicas > 1
	var kind string
	switch {
	case sharded && ha:
		kind = "sharded+HA"
	case sharded:
		kind = "sharded"
	case ha:
		kind = "HA"
	default:
		kind = "single"
	}
	return fmt.Sprintf("%s (%d group(s) x %d replica(s); %d alphas, %d zeros)",
		kind, i.Groups, i.Replicas, i.NumAlphas, i.NumZeros)
}
