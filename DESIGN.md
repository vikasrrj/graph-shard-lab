# GraphShard Lab Design

## Scope

GraphShard Lab is an in-memory Rust research prototype.

It models graph data distributed across logical shards and compares data-placement strategies using synthetic workloads.

The project focuses on placement behavior, query correctness, locality, and shard balance. It does not attempt to implement storage durability, networking, replication, consensus, or production query execution.

## Data model

The graph contains users and directed follow relationships.

```text
User 1 → User 2

