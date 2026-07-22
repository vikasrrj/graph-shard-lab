# GraphShard Lab

GraphShard Lab is a Rust research prototype for studying how graph data placement and query execution affect shard balance and cross-shard work.

It focuses on one main question:

> How should connected graph data be placed and queried across shards?

The project compares:

- hash-based placement;
- community-based placement;
- balanced community placement;
- direct two-hop query execution;
- batched two-hop query execution.
- hub-heavy workload generation and hotspot analysis;
- bounded LRU cache simulation;
- degree-based cache warming experiments.

All shards are logical, in-memory shards running inside one Rust process.

## Key findings

### Community-aware placement improves locality

In a synthetic workload with:

- 10,000 users;
- 8 outgoing edges per user;
- 7 of those 8 edges staying inside the user’s community;
- 4 logical shards;

the results were:

| Placement | Average cross-shard hops |
|---|---:|
| Hash | 54.02 |
| Community | 7.46 |

Community placement produced **86.19% fewer logical cross-shard hops** than hash placement in this workload.

![Community strength versus cross-shard traversal](docs/images/locality_sweep.svg)

### Batching reduces logical shard requests

The project compares two ways of executing the same two-hop query.

**Direct execution**

Each first-hop user is read separately.

```text
Read user A
Read user B
Read user C
```

**Batched execution**

First-hop users are grouped by shard.

```text
Shard 1: read users A and B together
Shard 2: read user C
```

Direct execution used `9.00` logical shard requests per query.

In the original locality sweep, batched execution used between `2.00` and `4.52` requests, reducing logical shard requests by **49.74% to 77.78%**.

Both execution methods returned exactly the same query results.

![Direct versus batched shard requests](docs/images/batching_requests.svg)

### Batching across multiple seeds and shard counts

To make the batching result more trustworthy, GraphShard Lab also runs a second sweep across:

- **5 random seeds**: `42, 43, 44, 45, 46`
- **4 shard counts**: `2, 4, 8, 16`
- **2 locality levels**:
  - `4` local edges per user
  - `7` local edges per user

This produces `5 × 4 × 2 = 40` benchmark settings.

Key result:

- Under **moderate locality** (`4` local edges), batching reduced logical shard requests by **37.61% to 67.19%**.
- Under **strong locality** (`7` local edges), batching reduced logical shard requests by **66.67% to 71.87%**.

This shows:

- batching stays effective across different generated graphs;
- batching helps less as shard count increases when connected users are more spread out;
- batching remains highly effective when community locality is strong.

![Batching benefit versus shard count](docs/images/batching_by_shards.svg)

These are logical request counts inside an in-process prototype, not real network requests or measured latency.

## Uneven communities create a balance trade-off

The uneven-community workload uses these community sizes:

```text
[4000, 2500, 1500, 1000, 1000]
```

| Placement           | Users per shard              | Max imbalance | Avg hops | Batched request reduction |
|----------------------|-------------------------------|---------------:|----------:|---------------------------:|
| Hash                 | `[2500, 2500, 2500, 2500]`    | 0%             | 53.97     | 48.84%                     |
| Naive community      | `[5000, 2500, 1500, 1000]`    | 100%           | 8.02      | 67.91%                     |
| Balanced community   | `[4000, 2500, 1500, 2000]`    | 60%            | 8.79      | 66.93%                     |

Hash placement gives perfect balance but poor graph locality.

Naive community placement gives strong locality but may overload a shard.

Balanced community placement keeps most of the locality benefit while improving the distribution.

The remaining 60% imbalance cannot be removed without splitting the largest 4,000-user community.

![Shard balance versus graph locality](docs/images/uneven_tradeoff.svg)

## Hub-heavy hotspots and caching

The hub-heavy workload contains:

- 10,000 users;
- 100 hub users;
- 8 outgoing edges per user;
- 2 edges per user targeting hubs.

The hubs represent only **1% of all users**, but receive **25% of all logical adjacency reads**.

| User type | Average adjacency reads |
|---|---:|
| Hub | 200.00 |
| Normal user | 6.06 |

An average hub adjacency list is therefore read **33× more often** than an average normal-user adjacency list.

This creates a concentrated hot set that can benefit from caching.

### Cold bounded LRU cache

The cache starts empty and stores user IDs representing cached adjacency lists.

A cache hit means that a repeated logical adjacency lookup could be served from the cache rather than the main graph structure.

| Capacity | Overall hit rate | Hub hit rate | Normal-user hit rate |
|---:|---:|---:|---:|
| 25 | 1.60% | 5.90% | 0.17% |
| 50 | 3.14% | 11.43% | 0.38% |
| 100 | 6.12% | 22.12% | 0.78% |
| 250 | 13.95% | 49.56% | 2.08% |
| 500 | 22.22% | 76.05% | 4.28% |
| 1,000 | 30.67% | 95.47% | 9.07% |

At capacity `1,000`, the cache served **95.47% of repeated hub accesses**, compared with only **9.07% of normal-user accesses**.

This shows that concentrated graph hotspots are much more cacheable than a large, weakly reused normal-user set.

![Cold LRU cache hit rates](docs/images/cache_baseline.svg)


## Distributed shard simulation

The project includes an asynchronous distributed-shard simulation built with Tokio.

Each logical shard runs in its own Tokio task and owns a separate local `Graph`. A coordinator routes users and queries to shard workers through bounded `mpsc` channels. Each request uses a `oneshot` channel for its response.

```text
Coordinator
    |
    | bounded command messages
    v
+----------+  +----------+  +----------+  +----------+
| Shard 0  |  | Shard 1  |  | Shard 2  |  | Shard 3  |
| worker   |  | worker   |  | worker   |  | worker   |
+----------+  +----------+  +----------+  +----------+
```

## Distributed query execution

Two implementations of the two-hop query are compared:

Direct: sends one shard request for every first-hop user.
Batched: groups first-hop users by owning shard and sends one batch message per shard.
Batch requests to different shards are dispatched concurrently.

For a query whose first-hop users are distributed across several shards:

Direct:
source read
→ first-hop read
→ first-hop read
→ first-hop read
→ ...

Batched:
source read
→ concurrent batch request to each required shard

Simulated network delay

Shard read messages can be configured with a simulated delay. An individual read pays one delay, while a batch containing multiple adjacency-list reads also pays one delay.

This models the latency advantage of reducing message round trips. It is a single-process simulation and does not represent real network transport, serialization, node failure, or separate machines.

Latency benchmark

Configuration:

4 shard workers
100 query sources
3 repetitions per source
300 samples per strategy
2 ms simulated delay per shard read message
Strategy	p50	p95	p99
Direct	17,175 µs	30,037 µs	31,215 µs
Batched	6,884 µs	7,494 µs	8,075 µs
Reduction	59.92%	75.05%	74.13%

The batched implementation reduced median latency by 59.92% and p99 latency by 74.13% in this simulated workload.




### Degree-based cache warming

The warming experiment preloads the most-followed hubs before measured traffic begins.

Warming had little effect across the complete 80,000-access run because a cold LRU cache gradually learned the popular users itself.

However, warming improved startup behavior during the first 1,000 accesses:

| Capacity | Cold startup hit rate | Warmed startup hit rate | Improvement |
|---:|---:|---:|---:|
| 250 | 12.60% | 17.10% | +4.50 points |
| 500 | 17.30% | 24.50% | +7.20 points |
| 1,000 | 18.80% | 28.00% | +9.20 points |

Across the complete workload, warming improved the total hit rate by at most `0.13` percentage points.

Therefore, the main benefit of warming in this experiment is avoiding cold-start misses rather than improving steady-state behavior.

![Cold versus degree-warmed startup cache](docs/images/cache_warming.svg)

These are simulated logical cache hits. The cache currently stores user IDs rather than actual adjacency-list data, and the experiment does not measure real latency.

### Real shard-local adjacency caches

The cache was then integrated into actual sharded two-hop query execution.

Each of the four logical shards owns an independent LRU cache containing real adjacency lists:


user ID → IDs of users followed

On a cache miss, the query reads the adjacency list from the owning shard’s graph and inserts it into that shard’s cache. On a hit, the cached adjacency list is used directly.

Every cached query result was checked against the uncached reference graph.



| Capacity per shard | Total capacity | Cache hits | Cache misses | Hit rate |
| -----------------: | -------------: | ---------: | -----------: | -------: |
|                 25 |            100 |      4,935 |       75,065 |    6.17% |
|                 50 |            200 |      9,300 |       70,700 |   11.62% |
|                100 |            400 |     15,561 |       64,439 |   19.45% |
|                250 |          1,000 |     24,560 |       55,440 |   30.70% |

With 1,000 total cached adjacency lists, 30.70% of accesses were served from actual shard-local caches while returning identical query results.

Real cache warming produced the following startup result:

| Capacity per shard | Cold first 1,000 | Warm first 1,000 |  Improvement |
| -----------------: | ---------------: | ---------------: | -----------: |
|                 25 |            5.70% |            6.90% | +1.20 points |
|                 50 |           10.90% |           14.40% | +3.50 points |
|                100 |           16.30% |           22.50% | +6.20 points |
|                250 |           18.80% |           28.00% | +9.20 points |


Across the complete workload, warming improved the hit rate by only 0.12 percentage points at the largest capacity because the cold LRU cache learned the hot set during traffic.

These measurements show cache reuse, not query-speed improvement. The shards and caches still run inside one process, and real latency is not measured.



## Graph model

Users are graph nodes.

A directed `FOLLOWS` relationship is an edge:
   
```text
Alice → Bob
Bob → Charlie
```

A two-hop query starting from Alice follows:

```text
Alice → Bob → Charlie
```

The result is Charlie.

The project removes duplicate results and excludes the source user from its own result.

## Logical shards

A shard is a container holding part of the graph.

```text
ShardedGraph
├── Shard 0
├── Shard 1
├── Shard 2
└── Shard 3
```

Users are assigned to shards according to a placement strategy.

Outgoing edges are stored with their source user.

For example:

```text
Alice is stored on Shard 0
Bob is stored on Shard 2

Alice → Bob is stored with Alice on Shard 0
```

All shards exist inside one Rust process. No real network communication occurs.

## Placement strategies

### Hash placement

```text
shard = user_id % shard_count
```

Hash placement spreads sequential user IDs evenly across shards.

It provides good balance but ignores graph relationships, so connected users may be placed far apart.

### Naive community placement

Users belonging to the same community are kept together.

Communities are assigned to shards in repeating order:

```text
Community 0 → Shard 0
Community 1 → Shard 1
Community 2 → Shard 2
Community 3 → Shard 3
Community 4 → Shard 0
```

This improves locality but may create severe imbalance when community sizes differ.

### Balanced community placement

Communities are processed from largest to smallest.

Each community is assigned to the currently least-loaded shard.

```text
1. Sort communities by size
2. Find the least-loaded shard
3. Place the next community there
4. Repeat
```

Communities remain intact and are not split.

## Query execution strategies

### Direct execution

The direct method reads the outgoing edges of each first-hop user separately.

If a source user follows eight users, the query performs:

```text
1 source read
8 first-hop reads
9 logical shard requests
```

### Batched execution

The batched method groups first-hop users by their shard.

Instead of reading three users from the same shard separately:

```text
Read A from Shard 1
Read B from Shard 1
Read C from Shard 1
```

it treats them as one logical request:

```text
Read [A, B, C] from Shard 1
```

This changes how the work is organized but does not change the query result.

## Correctness

Every sharded query is checked against a normal, non-sharded reference graph.

For each source user:

1. run the query on the reference graph;
2. run direct execution on the sharded graph;
3. run batched execution on the sharded graph;
4. sort the result sets;
5. confirm that all results match.

The benchmark stops if any strategy returns an incorrect answer.

The current test suite contains 28 passing tests.

## Metrics

GraphShard Lab records:

- logical cross-shard hops;
- unique shards touched;
- direct logical shard requests;
- batched logical shard requests;
- request reduction percentage;
- users per shard;
- edges per shard;
- maximum user imbalance;
- maximum edge imbalance.

### Cross-shard hop

A cross-shard hop is counted when a traversed edge connects users stored on different shards.

```text
Alice on Shard 0
Bob on Shard 2

Alice → Bob = one cross-shard hop
```

### Shard imbalance

Maximum imbalance is calculated relative to the average shard load:

```text
(maximum shard load - average shard load)
------------------------------------------ × 100
             average shard load
```

## Workloads

The project generates deterministic synthetic graph workloads.

Parameters include:

- total users;
- number of communities;
- community sizes;
- edges per user;
- local edges per user;
- hub count;
- hub-targeting edges per user;
- random seed;
- shard count.

Using the same seed and parameters produces the same graph.

Current benchmark families:

1. **Locality sweep**  
   Equal-sized communities with `0, 2, 4, 6, 7, or 8` local edges per user.

2. **Uneven-community workload**  
   Community sizes:

   ```text
   [4000, 2500, 1500, 1000, 1000]

3. **Multi-seed, multi-shard batching sweep**
   Tested across:
   - seeds `42, 43, 44, 45, 46`
   - shard counts `2, 4, 8, 16`
   - locality levels `4` and `7`

4. **Hub-heavy workload**  
   A small set of hub users receives a large share of incoming edges and repeated adjacency reads.

5. **Cold-cache sweep**  
   The hub-heavy access stream is replayed through bounded LRU caches with capacities from 25 to 1,000.

6. **Cache-warming sweep**  
   The same access stream is tested with caches preloaded using the most-followed hubs.

## Run the project

### Requirements

- Rust and Cargo
- Python 3
- Matplotlib

On Arch-based systems:

```bash
sudo pacman -S python-matplotlib
```

### Run tests

```bash
cargo test
```

### Run benchmarks

```bash
cargo run --release
```

### Generate charts

```bash
python scripts/generate_charts.py
```

## Generated results

Benchmark CSV files:

```text
results/locality_sweep.csv
results/uneven_communities.csv
results/batching_sweep.csv
results/hub_hotspot.csv
results/cache_baseline.csv
results/cache_warming.csv
results/real_sharded_cache.csv
results/real_sharded_cache_warming.csv
```

Generated charts:

```text
docs/images/locality_sweep.svg
docs/images/batching_requests.svg
docs/images/batching_by_shards.svg
docs/images/uneven_tradeoff.svg
docs/images/cache_baseline.svg
docs/images/cache_warming.svg
docs/images/real_sharded_cache_warming.svg
```

## Project structure

```text
graph-shard-lab/
├── src/
│   ├── balanced.rs
│   ├── cache.rs
│   ├── lib.rs
│   ├── main.rs
│   ├── sharded.rs
│   ├── uneven.rs
│   └── workload.rs
├── tests/
│   └── tiny_graph.rs
├── results/
│   ├── locality_sweep.csv
│   ├── uneven_communities.csv
│   ├── batching_sweep.csv
│   ├── hub_hotspot.csv
│   ├── cache_baseline.csv
│   └── cache_warming.csv
├── scripts/
│   └── generate_charts.py
├── docs/
│   └── images/
│       ├── locality_sweep.svg
│       ├── batching_requests.svg
│       ├── batching_by_shards.svg
│       ├── uneven_tradeoff.svg
│       ├── cache_baseline.svg
│       └── cache_warming.svg
├── DESIGN.md
├── Cargo.toml
└── README.md
```

## Limitations

GraphShard Lab is a research prototype, not a production distributed database.

- All shards and shard-local caches run inside one process.
- Data is stored only in memory.
- Cross-shard hops and shard requests are logical measurements.
- No real network communication occurs.
- Real latency and throughput are not measured.
- Community membership is supplied in advance.
- Oversized communities are not split.
- Nodes are not replicated.
- Data is not persisted to disk.
- There is no failover or replication protocol.
- Workloads are synthetic.
- Cache capacity counts adjacency-list entries rather than memory bytes.
- Cached adjacency lists are cloned when returned.
- Cache warming uses complete workload degree information, which is an idealized assumption.
- Cache-hit improvements are not equivalent to measured latency improvements.

The project retains an earlier ID-only cache simulator for comparison. Actual cached sharded queries use independent shard-local caches containing complete adjacency-list data.

## Future work

Possible extensions include:


- comparing LRU with LFU and other eviction policies;
- warming caches using observed traffic rather than complete workload knowledge;
- shard workers implemented as Tokio tasks;
- message channels between shards;
- configurable simulated network delay;
- p50, p95, and p99 simulated latency;
- hot-node replication;
- dynamic shard rebalancing;
- oversized-community splitting;
- persistent storage.
- - cache invalidation when edges are added or removed;
- byte-bounded caches instead of entry-count limits;
- a more efficient constant-time LRU implementation;


## Conclusion

Hash placement provides strong shard balance but ignores graph structure.

Community placement can greatly reduce cross-shard traversal when communities are strong, but uneven communities can overload individual shards.

Balanced community placement provides a middle ground by preserving most of the locality benefit while improving shard distribution.

Batched query execution adds another improvement: users located on the same shard can be fetched together, reducing logical shard requests without changing query correctness.
