# GraphShard Lab Design

## Purpose

GraphShard Lab is an in-memory Rust prototype for studying how graph data placement affects:

- shard balance;
- cross-shard traversal;
- query locality;
- correctness.

It is not a production distributed database. Its purpose is to isolate and compare placement strategies under controlled workloads.

## Graph model

Users are graph nodes.

A directed follow relationship is an edge:

```text
User 1 → User 2

Outgoing edges are stored with their source user.

If User 1 belongs to Shard 2, all outgoing relationships from User 1 are stored on Shard 2, even when their targets belong to other shards.

Reference graph

Each experiment creates a normal, non-sharded graph.

This graph provides the expected answer for every two-hop query.

The same users and edges are then loaded into the sharded graphs. Their query results must match the reference graph before any benchmark metrics are accepted.

Logical shards

A ShardedGraph contains multiple in-memory Graph instances:

ShardedGraph
├── Shard 0
├── Shard 1
├── Shard 2
└── Shard 3

All shards exist inside one process.

Crossing between them is counted logically. No actual network request occurs.

Placement strategies
Hash placement
shard = user_id % shard_count

Hash placement distributes sequential user IDs evenly.

It does not consider graph relationships, so connected users may be scattered across shards.

Naive community placement

Users belonging to the same community are kept together.

Communities are assigned in repeating shard order:

community 0 → shard 0
community 1 → shard 1
community 2 → shard 2
community 3 → shard 3
community 4 → shard 0

This improves graph locality but may create severe imbalance when communities have different sizes.

Balanced community placement

Balanced placement processes communities from largest to smallest.

For each community:

find the shard currently storing the fewest users;
assign the entire community to that shard;
update the shard load;
repeat.

This is a greedy largest-first placement algorithm.

Communities remain intact and are never split.

Query execution

A two-hop query follows paths shaped like:

A → B → C

The query:

locates the shard containing A;
reads A's outgoing edges;
locates every first-hop node B;
reads each B node's outgoing edges;
collects unique second-hop nodes C;
counts cross-shard edges;
counts unique shards touched.

The source node is excluded from its own result.

Duplicate second-hop results are removed.

Workload generation

The project generates synthetic community graphs.

Parameters include:

total users;
number or sizes of communities;
outgoing edges per user;
local edges per user;
external edges per user;
random seed.

The generator ensures every user receives the requested number of unique targets.

Determinism

Workloads use a seeded random-number generator.

Using the same parameters and seed produces the same graph.

Every placement strategy in an experiment receives the exact same edge list, preventing unfair comparisons between different workloads.

Metrics
Cross-shard hops

A hop is counted whenever a traversed edge connects users stored on different shards.

Shards touched

The number of unique shards involved in one query.

Maximum imbalance
(maximum shard load - average shard load)
------------------------------------------ × 100
             average shard load

This is calculated separately for users and stored edges.

Correctness validation

For each source user:

run the two-hop query on the reference graph;
run it on the sharded graph;
sort both result sets;
compare them;
fail immediately if they differ.

Placement metrics are only reported after correctness has been verified.

Experiments
Locality sweep

The locality sweep keeps the graph size constant while changing how many edges remain inside each user's community.

It compares hash placement with community placement.

Uneven communities

The uneven-community experiment uses communities of different sizes and compares:

hash placement;
naive community placement;
balanced community placement.

This demonstrates the trade-off between shard balance and graph locality.

Limitations
Shards are not separate machines.
Data is stored only in memory.
Cross-shard hops are logical, not network requests.
Real latency and throughput are not measured.
Community membership is supplied in advance.
Oversized communities are not split.
Nodes are not replicated.
Shards do not migrate dynamically.
Workloads are synthetic.
Future work

Possible extensions include:

independent Tokio tasks for shards;
message channels between shards;
configurable simulated network delay;
concurrent query execution;
p50, p95, and p99 latency measurements;
hotspot workloads;
popular-node replication;
dynamic migration and rebalancing;
persistent shard storage;
automatic community detection.
