# GraphShard Lab

GraphShard Lab is a Rust research prototype for comparing graph data-placement strategies and measuring the trade-off between shard balance and cross-shard traversal.

It answers one question:

> How should a graph database distribute connected users across shards?

The project compares three strategies:

- **Hash placement** — spreads users evenly but ignores relationships.
- **Naive community placement** — keeps connected communities together but may overload shards.
- **Balanced community placement** — keeps communities together while assigning them to the least-loaded shards.

## Why this matters

Graph queries frequently follow relationships between nodes.

For example:

```text
Alice → Bob → Charlie
