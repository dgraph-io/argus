# v25.3.6 — M4 / 64GB / 12-core — 1 Zero + 1 Alpha (Docker-compose)

Baseline benchmark for **Dgraph v25.3.6** on an Apple M4 (64 GB, 12 core), a
single zero + single alpha brought up via `docker-compose` on this machine.

## Purpose

This is a **release baseline** for local regression smoke tests: run a candidate
build through the same config and `argus compare` against the `summary-*.json`
here. A **single alpha** is deliberate — on one machine, HA replication just
triples CPU/disk work with no extra hardware, so a single alpha isolates the
Dgraph version's own behavior. For real HA/sharded numbers, use a GKE config
where replicas land on separate nodes.

## Reproduce

```bash
./run.sh            # drops, builds the corpus, runs, writes summary-{1,2}.json
./run.sh --down     # also stop the cluster afterwards
```

`run.sh` runs `-scale 0.5 -passes 3 -repeats 2` (drop-all clean slate each
repeat). Tune `SCALE` in `run.sh` to hit a target wall-clock — here ~9 min/repeat
(~19 min total). Dataset at scale 0.5: 2500 persons, 1000 posts, 1500 products
(+3 reviews each), 5 companies, 6 categories, 30 dogs, 20 cats.

## Result (captured 2026-06-23)

Both repeats: **34/34 validation checks pass**. Headline figures:

| metric | repeat 1 | repeat 2 |
| --- | --- | --- |
| `dql-write:person` throughput | ~9/s | ~9/s |
| `dql-write:person` p50 / p99 | 310 ms / 1566 ms | 341 ms / 1424 ms |
| `gql-write:product` p99 | 31.5 ms | 23.6 ms |
| cluster RSS | 4157 MB | 3690 MB |
| cluster CPU | 183% | 185% |
| cluster queries/s | 67 | 73 |

## Caveats

- **Docker-on-Mac I/O bound.** Write commits go through Docker Desktop's
  virtualized filesystem, so absolute write latency/throughput is far worse than
  bare metal or GKE SSDs. Treat these as **relative version-vs-version deltas**,
  not absolute magnitudes.
- **Compare with repeats per side.** A single summary per side has no run-to-run
  variance, so the noise gate is 0 and normal jitter (e.g. `set-note` p99 swings
  ~20% here) reads as a false regression. Always pass both repeats:

  ```bash
  argus compare \
    -baseline  ./summary-1.json,./summary-2.json \
    -candidate ../../<next-version>/<same-config>/summary-1.json,...,summary-2.json
  ```
