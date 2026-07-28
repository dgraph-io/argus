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
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
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

	// Operator-supplied config dimensions that can't be auto-detected. CPUClass
	// and DiskClass are hardware classes (e.g. "Apple M4", "e2-standard-4";
	// "NVMe", "pd-ssd"); Deploy is the deployment mechanism (e.g. "docker-compose",
	// "gke"). These become first-class ledger columns for sorting the run matrix.
	CPUClass  string
	DiskClass string
	Deploy    string

	// Operator-supplied free-form annotation for anything not covered above.
	Labels string

	// Dgraph cluster (the system under test).
	DgraphVersion string // version tag from the cluster (health), e.g. v25.3.6
	DgraphBranch  string // human branch name; operator-supplied (not on the cluster API; empty for released tag builds, which report "HEAD")
	DgraphCommit  string // commit SHA-1; operator-supplied from `dgraph version` — the one field that uniquely pins any build, released or feature
	ClusterGRPC   string
	ClusterHTTP   string
	ClusterID     string
	Groups        int // alpha groups == shards
	Replicas      int // voting members per group == replication / HA factor (excludes learners)
	NumAlphas     int // total alphas across groups (voting + learners)
	NumLearners   int // non-voting read-replica alphas (Raft learners)
	NumZeros      int
	// Topology is not stored — it's derived from the counts above via the
	// Topology() / TopoClass() methods, so it can't drift from its source.
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
	return info
}

type stateResp struct {
	Groups map[string]struct {
		Members map[string]struct {
			Addr    string `json:"addr"`
			AmDead  bool   `json:"amDead"`
			Learner bool   `json:"learner"` // non-voting read replica
		} `json:"members"`
	} `json:"groups"`
	Zeros map[string]json.RawMessage `json:"zeros"`
	CID   string                     `json:"cid"`
}

func (i *Info) fetchTopology(ctx context.Context, httpBase string) {
	st, err := fetchState(ctx, httpBase)
	if err != nil {
		return
	}
	i.ClusterID = st.CID
	i.Groups = len(st.Groups)
	i.NumZeros = len(st.Zeros)
	for _, g := range st.Groups {
		voting := 0
		for _, m := range g.Members {
			i.NumAlphas++
			if m.Learner {
				i.NumLearners++
			} else {
				voting++
			}
		}
		if voting > i.Replicas {
			i.Replicas = voting // HA/replication factor = max VOTING members per group
		}
	}
}

func fetchState(ctx context.Context, httpBase string) (stateResp, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(httpBase, "/")+"/state", nil)
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
	var st stateResp
	if err := json.Unmarshal(body, &st); err != nil {
		return stateResp{}, err
	}
	return st, nil
}

// Endpoint pairs an alpha's HTTP base URL (for GraphQL + metrics scraping) with
// its gRPC address (for dgo).
type Endpoint struct {
	HTTP string
	GRPC string
	Addr string // the /state member address (group/leader context), if discovered
}

// ResolveAlphas determines the alpha endpoints to load-balance across and
// scrape. If alphasCSV is non-empty it is the authoritative list of HTTP base
// URLs (gRPC derived as HTTP port + 1000). Otherwise endpoints are discovered
// from the seed's /state, mapping each member's internal address (e.g. :7080)
// to HTTP (:8080) and gRPC (:9080) via the standard port offsets. Falls back to
// the seed endpoints if discovery yields nothing.
func ResolveAlphas(ctx context.Context, seedHTTP, seedGRPC, alphasCSV string) []Endpoint {
	if alphasCSV != "" {
		var eps []Endpoint
		for _, h := range strings.Split(alphasCSV, ",") {
			h = strings.TrimSpace(h)
			if h == "" {
				continue
			}
			eps = append(eps, Endpoint{HTTP: h, GRPC: shiftPort(httpHostPort(h), 1000)})
		}
		if len(eps) > 0 {
			return eps
		}
	}

	st, err := fetchState(ctx, seedHTTP)
	if err == nil {
		var eps []Endpoint
		seen := map[string]bool{}
		for _, g := range st.Groups {
			for _, m := range g.Members {
				if m.AmDead || seen[m.Addr] {
					continue
				}
				seen[m.Addr] = true
				eps = append(eps, Endpoint{
					HTTP: "http://" + shiftPort(m.Addr, 1000),
					GRPC: shiftPort(m.Addr, 2000),
					Addr: m.Addr,
				})
			}
		}
		if len(eps) > 0 {
			return eps
		}
	}
	return []Endpoint{{HTTP: seedHTTP, GRPC: seedGRPC}}
}

// httpHostPort extracts host:port from an http(s) URL.
func httpHostPort(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}
	return u.Host
}

// shiftPort returns host:(port+delta) for a host:port string; on parse failure
// it returns the input unchanged.
func shiftPort(hostPort string, delta int) string {
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		return hostPort
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return hostPort
	}
	return net.JoinHostPort(host, strconv.Itoa(p+delta))
}

// TopoClass classifies the cluster shape from the shard count (Groups) and the
// HA factor (Replicas, voting members per group): single / HA / sharded /
// sharded+HA. Learners are read-replicas, orthogonal to this classification, so
// they don't change it — surface them via NumLearners / Topology().
func (i Info) TopoClass() string {
	if i.Groups == 0 {
		return "unknown"
	}
	sharded := i.Groups > 1
	ha := i.Replicas > 1
	switch {
	case sharded && ha:
		return "sharded+HA"
	case sharded:
		return "sharded"
	case ha:
		return "HA"
	default:
		return "single"
	}
}

// Topology is the human summary derived from the counts, e.g.
// "sharded+HA (3 group(s) x 3 replica(s); 9 alphas, 3 zeros, 2 learners)".
func (i Info) Topology() string {
	if i.Groups == 0 {
		return "unknown"
	}
	s := fmt.Sprintf("%s (%d group(s) x %d replica(s); %d alphas, %d zeros",
		i.TopoClass(), i.Groups, i.Replicas, i.NumAlphas, i.NumZeros)
	if i.NumLearners > 0 {
		s += fmt.Sprintf(", %d learners", i.NumLearners)
	}
	return s + ")"
}
