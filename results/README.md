# Results

Benchmark runs are recorded in tiered folders — **Dgraph version**, then
**hardware + cluster config**:

```
results/
└── v25.3.6/                                          # tier 1: Dgraph version
    └── M4 - 64GB - 12core - 1 Zero 1 Alpha - Docker-compose/   # tier 2: hw + config
        ├── README.md          # what this config is and how to reproduce it
        ├── docker-compose.yml # the cluster definition
        ├── run.sh             # the exact argus command (checked in)
        ├── summary-1.json     # committed result (one per repeat)
        └── summary-2.json
```

The config-folder name encodes everything that isn't the version — machine, RAM,
cores, topology (`1 Zero 1 Alpha`, `1 Zero 3 HA`, `3 group sharded`, …), and
deployment (`Docker-compose`, `GKE`, …). Two runs are comparable only when their
folders match in every tier **except** the version. A run also stamps these into
the summary (`env`, `dataset`, `label`), and `argus compare` warns if they differ.

## Convention: each config folder is self-contained

Every config folder checks in the environment, the exact run, and the result:

- **`README.md`** — one paragraph on what this config is, the run parameters, and
  any caveats (e.g. "single alpha isolates the version; numbers are Docker-on-Mac
  I/O bound, so use deltas not magnitudes").
- **`docker-compose.yml`** — defines the cluster (or, for GKE, a pointer to the
  deploy that produced it).
- **`run.sh`** — the exact, reproducible argus command: builds argus, brings the
  cluster up, waits for health, runs with the right flags (incl. `-label`
  matching the folder), writes results into the same folder. **Always record the
  command here** so a run is reproducible from the checked-in repo alone.
- **`summary-N.json`** — the machine-readable per-run result (one per repeat),
  used by `argus compare`.
- **`ops-N.csv` / `runtime-N.csv` / `cluster-N.csv`** — the per-interval
  time-series (client throughput/latency, load-generator runtime, per-node
  cluster metrics). Useful for plotting how a run behaved over time.

**All of these are committed** — `summary-*.json` is the comparison unit, and
the CSVs preserve the full time-series so a recorded run can be re-examined
later without re-running it. `git`-tracking a config folder thus captures how
the cluster was built (`docker-compose.yml`), how argus was run (`run.sh`), and
the complete result (summaries + CSVs).

## Comparing versions

Run the same config on the released and candidate versions (same hardware), then:

```bash
argus compare \
  -baseline  results/v25.3.6/<config>/summary-*.json \
  -candidate results/v25.4.0/<config>/summary-*.json
```

`compare` exits non-zero on a regression and warns if the two sides differ in
anything but the Dgraph version.
