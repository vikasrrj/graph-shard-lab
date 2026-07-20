# GraphShard Lab

A Rust research prototype for comparing graph-sharding strategies and measuring the trade-off between shard balance and cross-shard traversal.

This project grew out of experimenting with graph databases and asking a simple question:

> How should connected graph data be placed across shards?

## Results at a glance

The project compares three strategies:

- **Hash placement** spreads users evenly but ignores graph relationships.
- **Naive community placement** keeps connected communities together but can overload shards.
- **Balanced community placement** keeps communities together while placing them on the least-loaded shards.

### Strong-community workload

With 10,000 users, 8 edges per user, and 7 of those 8 edges staying inside the user’s community:

| Strategy | Average cross-shard hops |
|---|---:|
| Hash | 54.02 |
| Community | 7.46 |

Community placement produced **86.19% fewer logical cross-shard hops** in this synthetic workload.

![Community strength versus cross-shard traversal](docs/images/locality_sweep.svg)

### Uneven-community workload

Community sizes:

```text
[4000, 2500, 1500, 1000, 1000]

| Strategy           | Users per shard            | Maximum imbalance | Average hops |
| ------------------ | -------------------------- | ----------------: | -----------: |
| Hash               | `[2500, 2500, 2500, 2500]` |                0% |        53.97 |
| Naive community    | `[5000, 2500, 1500, 1000]` |              100% |         8.02 |
| Balanced community | `[4000, 2500, 1500, 2000]` |               60% |         8.79 |

Balanced community placement reduced the naive strategy’s maximum imbalance from 100% to 60%, while retaining most of its locality advantage.

The remaining imbalance cannot be removed without splitting the largest 4,000-user community.

What it measures

For each two-hop query, GraphShard Lab records:

logical cross-shard hops;
unique shards touched;
users stored per shard;
edges stored per shard;
maximum user and edge imbalance.

Every sharded query is checked against a normal non-sharded reference graph. The benchmark fails if a placement strategy returns an incorrect result.

Placement strategies
Hash placement
shard = user_id % shard_count

Simple and well balanced, but connected users may be scattered across shards.

Naive community placement

Users in the same community stay together. Communities are assigned to shards in repeating order.

This improves locality, but uneven communities may overload one shard.

Balanced community placement

Communities are processed from largest to smallest. Each one is assigned to the currently least-loaded shard.

This is a greedy placement strategy that improves balance without splitting communities.

Run it

Run the tests:

cargo test

Run the optimized benchmarks:

cargo run --release

Generated result files:

results/locality_sweep.csv
results/uneven_communities.csv

Regenerate the charts:

python scripts/generate_charts.py
Project structure
src/
├── balanced.rs
├── lib.rs
├── main.rs
├── sharded.rs
├── uneven.rs
└── workload.rs

tests/
└── tiny_graph.rs

results/
├── locality_sweep.csv
└── uneven_communities.csv
Limitations

This is a research prototype, not a production distributed database.

All shards run inside one Rust process.
Data is stored in memory.
Cross-shard hops are logical measurements, not real network requests.
The project does not measure real latency or throughput.
Community membership is provided in advance.
Oversized communities are not split.
Replication and dynamic shard migration are not implemented.

Therefore, “86% fewer logical cross-shard hops” does not mean “86% faster.”

Future work
represent shards as independent Tokio tasks;
simulate network delay;
measure p50, p95, and p99 query latency;
add hotspot workloads;
replicate frequently accessed nodes;
support dynamic rebalancing;
split oversized communities;
persist shard data to disk.
Conclusion

Hash placement gives excellent balance but ignores graph structure.

Community placement can greatly reduce cross-shard traversal when communities are strong, but uneven communities may overload shards.

Balanced community placement provides a middle ground: it preserves most of the locality benefit while improving shard balance without splitting communities.
