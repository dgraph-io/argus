# Results

Every benchmark run is recorded as rows in a single append-only **ledger**,
[`runs.jsonl`](runs.jsonl) — one JSONL line per repeat, each line a full
`RunSummary` (env + config + per-op latency/throughput + cluster + bulk-load).
One committed file holds the whole matrix (Dgraph version × CPU class × cores ×
RAM × topology × disk class × deployment × tuning), so a new scenario adds *lines*
instead of another folder.

```
results/
├── runs.jsonl                          # THE results store (append-only JSONL, committed)
├── run-common.sh                       # shared run driver (sourced by every run.sh)
├── artifacts/<run-id>/                 # gitignored: pprof profiles + per-interval CSVs
└── v25.3.7/                            # per-config reproducibility inputs only:
    └── M4-64GB-12core-1z-1a-docker/
        ├── docker-compose.yml          #   the cluster definition
        └── run.sh                      #   thin wrapper: pins knobs, sources run-common.sh
```

The config dimensions are first-class columns on each row: auto-detected
`cores`/`ram_gb`/`groups`/`replicas`/`learners` (topology is derived from
`groups`+`replicas`), plus operator-supplied `cpu_class`, `disk_class`, `deploy`,
and `scale`/`write_concurrency`. `argus compare` warns if two runs differ in
anything but the Dgraph version.

## Browsing the ledger — `argus ls`

```bash
argus ls                                   # table of every run
argus ls version=v25.3.8-preview1          # filter (substring, case-insensitive)
argus ls deploy=gke,cpu_class=e2 -format csv   # flat CSV catalog to stdout
```

`ls` collapses a run's repeats into one row and shows the headline means
(measured_s, qps, rss_mb, jemalloc_mb). `-format csv` emits the full flat catalog
(the spreadsheet-scannable view) derived from the JSONL on demand.

## Visual two-run diff — `compare.html`

[`compare.html`](compare.html) is a dependency-free page that loads `runs.jsonl`,
lets you pick any two `run_id`s, and shows the gain/loss on **every recorded
metric** (per-op throughput/latency, cluster resource, batch-insert) as a
color-coded Δ% heatmap — green = better, red = worse, per each metric's direction
(latency/RSS lower-is-better, throughput higher-is-better). By default it is
**noise-aware**: compared values are the **median across a run's repeats** (so a
single outlier repeat — a lone multi-second GC/compaction stall — can't dominate
the tail metrics like `p999`/`max`), and it only colors a delta that clears
`noise × 1.5` (rel-stddev across repeats), dimming within-noise deltas to neutral
— the same idea as `argus compare`'s gate, so drift (e.g. a uniformly longer run)
doesn't paint a wall of red. A checkbox toggles the raw view. It also flags the
same mismatches `argus compare` warns about (same build, differing topology/host/
workload, too few repeats). Open it by serving the folder:

```bash
cd results && python3 -m http.server   # then open http://localhost:8000/compare.html
```

The page auto-loads a sibling `runs.jsonl` whenever the browser allows it: always
when served over http, and also on a plain `file://` double-click in Firefox
(which permits same-directory reads). Chrome and Safari sandbox `file://` reads,
so there use the file picker on the page or serve the folder as above. It's a
quick eyeball companion to `argus compare`; the CLI stays the source of truth for
the gated verdict.

## Running — config folder + shared driver

Each `results/<version>/<config>/` folder holds just the **reproducibility
inputs**:

- **`docker-compose.yml`** — the cluster definition (or, for GKE, a pointer to the
  deploy that produced it).
- **`run.sh`** — a thin wrapper that pins the per-run knobs and `source`s
  [`run-common.sh`](run-common.sh). Knobs: `SCALE`, `WRITECONC`, `REPEATS`,
  `DESC`, and the config-class columns `CPU_CLASS`, `DISK_CLASS`, `DEPLOY`,
  `LABEL`. The Dgraph **branch and version are not hand-typed** — the driver
  reads them from the running image via `dgraph version` after the cluster is up
  (the cluster API exposes only the commit hash, not the human branch name), so
  the recorded identity is authoritative; `BRANCH` remains only as an optional
  fallback if that probe fails. The driver builds argus, brings the cluster up,
  captures profiles around the workload, runs argus, and **appends each repeat to
  `results/runs.jsonl`** with a generated `run-id`.

The driver writes the heavy, ephemeral artifacts — pprof profiles
(`heap-startup`, `heap-end`, `cpu`, and opt-in `mutex`/`block`) and the
per-interval CSVs (`ops`/`runtime`/`cluster`) — to **`results/artifacts/<run-id>/`**,
which is **gitignored**. The ledger is the durable record; commit an individual
run's artifacts explicitly (`git add -f`) only when a specific investigation is
worth keeping. Opt into contention profiles with `CONTENTION=1` plus
`--profile_mode=mutex` (or `=block`) on the alpha in the compose file (one mode
per run).

## Comparing versions

Select the two runs from the ledger by run-id (exact or prefix) or a `k=v` filter;
each selector must resolve to exactly one run (else `compare` errors and lists the
candidates):

```bash
argus compare \
  -baseline-run  v25.3.7-M4-64GB-12core-1z-1a-docker \
  -candidate-run v25.3.8-preview1-M4-64GB-12core-1z-1a-docker
```

`compare` exits non-zero on a regression and warns if the two sides differ in
anything but the Dgraph version. It auto-discovers the run's heap/CPU (and, if
present, mutex/block) profiles under `artifacts/<run-id>/` and diffs them, so a
flagged regression comes with its allocation site / hot function / contended lock.
(Explicit summary file lists still work via `-baseline`/`-candidate` for ad-hoc
comparisons outside the ledger.)

### Preferred: a fresh GKE deployment per side

For any comparison you intend to *trust*, run each version on its own
**freshly-provisioned GKE cluster** (identical machine type, node count, and
region on both sides — see [deploy/](../deploy/)), then tear it down. This is the
preferred environment, for reasons this project learned the hard way:

- **Real cores and SSD.** The local docker-compose tier is single-alpha and
  Docker-on-Mac **I/O bound** — it commits only a few txns/s, so it masks anything
  CPU- or parallelism-related and reports latency that's mostly disk-wait.
- **Isolation from drift.** A reused local box has per-run latency that drifts
  ±30–60% with background load and thermals; that drift, not the code, dominated
  our v25.3.7↔v25.3.8-preview1 latency deltas. Dedicated GKE nodes with nothing
  else scheduled are far more stationary.
- **A clean cluster isolates the version.** Fresh state per side removes carryover
  (cache warmth, compaction backlog, leftover data) that a long-lived cluster
  accumulates, so the only difference between the two runs is the Dgraph build.

Provision both sides the *same* way (same manifest, same node pool) so the compare
warning about mismatched host/topology stays quiet. **The local docker-compose
tier is for iteration and smoke tests** — deltas, not shippable verdicts.

### On repeat count

Even on GKE, use **≥5 repeats** (`REPEATS=5`); on the local box it's mandatory.
Two repeats can't estimate the true run-to-run spread — adjacent repeats often
agree with each other while missing the variance, so the measured `noise%`
*understates* it and a 2-repeat compare can manufacture (or hide) a broad
"regression" from drift alone. When in doubt, trust the `avg` deltas over `p99`
(`p99` is a single bucketed tail sample per repeat) and confirm any finding
reproduces across runs.

## Migrating old per-folder summaries

Legacy runs stored as `summary-N.json` files fold into the ledger with
`argus import` (all summaries in one directory become one run; `run_id` is derived
from the directory; `deploy`/`scale`/`cpu_class` are backfilled best-effort from
the old `-label`):

```bash
argus import 'results/*/*/summary*.json'   # append to results/runs.jsonl
```
