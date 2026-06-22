Argus is a Dgraph testing and benchmarking tool designed to:

1. Exercise nearly every feature of Dgraph for correctness
2. Benchmark write and query performance, including go statistics (gc performance, etc) and system performance (heap usage, etc)

I want the harness to take around 30 minutes to run. The final state of the graph should be checked for correctness.

Let's write collected statistics to a CSV file for each run, include the version (and git branch `dgraph version`) of Dgraph and stats, etc.

It's designed to be pointed at any Dgraph cluster, that cluster could be local or in a k8s environment.

Test both GraphQL and DQL in a single graph.

It's OK to just use dgo for the client and any go GraphQL client available.

Repos for reference:

- Dgraph: /Users/matthew/code/dgraph
- dgo: /Users/matthew/code/dgo
- Docs: https://docs.dgraph.io

