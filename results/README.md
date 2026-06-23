# Results

Benchmark runs are recorded in tiered folders:

```
results/<dgraph-version>/<hardware + cluster config>/
```

e.g. `results/v25.3.6/M4 - 64GB - 12core - 1 Zero 1 Alpha - Docker-compose/`.
The folder name captures everything that isn't the Dgraph version itself
(machine, RAM, cores, topology, deployment) so two runs are only comparable when
their folders match except for the version tier.

## Convention: each config folder is self-contained

Every config folder checks in both the environment and the exact run:

- **`docker-compose.yml`** — defines the cluster (or a pointer to how it was
  deployed, for GKE etc.).
- **`run.sh`** — the exact, reproducible argus command: builds argus, brings the
  cluster up, waits for health, runs with the right flags (incl. `-label`
  matching the folder), and writes results into the same folder.
- **`summary.json`** (and `summary-N.json` for repeats) — the machine-readable
  result, used by `argus compare`. **This is the artifact to commit.**

So `git`-tracking a folder preserves how the cluster was built, how argus was
run, and the summarized result.

Note: `*.csv` is git-ignored, so the bulky per-interval `ops.csv` /
`runtime.csv` / `cluster.csv` are kept locally but not committed; the
`summary.json` (which carries the full-run percentiles + cluster steady-state)
is what gets versioned. Add a `!results/**/*.csv` exception to `.gitignore` if
you want the time-series committed too.

## Comparing versions

Run the same config on the released and candidate versions (same hardware), then:

```bash
argus compare \
  -baseline  results/v25.3.6/<config>/summary-*.json \
  -candidate results/v25.4.0/<config>/summary-*.json
```

`compare` exits non-zero on a regression and warns if the two sides differ in
anything but the Dgraph version.
