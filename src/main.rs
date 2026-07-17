use graph_shard_lab::{
    Graph,
    sharded::{Placement, QueryResult, ShardedGraph},
};

const USER_COUNT: u64 = 10_000;
const COMMUNITY_COUNT: u64 = 10;
const LOCAL_EDGES_PER_USER: u64 = 7;
const SHARD_COUNT: usize = 4;
const QUERY_COUNT: u64 = 10_000;

fn main() -> Result<(), String> {
    let community_size = USER_COUNT / COMMUNITY_COUNT;

    println!("Building the reference graph...");
    let reference = build_reference_graph()?;

    println!("Building hash-placed graph...");
    let hash_graph = build_sharded_graph(Placement::Hash)?;

    println!("Building community-placed graph...\n");
    let community_graph = build_sharded_graph(Placement::Community { community_size })?;

    print_distribution("Hash", &hash_graph);
    print_distribution("Community", &community_graph);

    let hash_stats = validate_and_measure("Hash", &reference, &hash_graph)?;

    let community_stats = validate_and_measure("Community", &reference, &community_graph)?;

    println!("\nComparison");
    println!("----------");

    print_stats("Hash", &hash_stats);
    print_stats("Community", &community_stats);

    let hop_reduction = percentage_reduction(
        hash_stats.average_cross_shard_hops,
        community_stats.average_cross_shard_hops,
    );

    println!(
        "\nCommunity placement reduced cross-shard hops by {:.2}%",
        hop_reduction
    );

    Ok(())
}

#[derive(Debug)]
struct AggregateStats {
    average_shards_touched: f64,
    average_cross_shard_hops: f64,
    minimum_cross_shard_hops: usize,
    maximum_cross_shard_hops: usize,
}

fn validate_and_measure(
    label: &str,
    reference: &Graph,
    sharded: &ShardedGraph,
) -> Result<AggregateStats, String> {
    let mut total_shards_touched = 0_usize;
    let mut total_cross_shard_hops = 0_usize;
    let mut minimum_cross_shard_hops = usize::MAX;
    let mut maximum_cross_shard_hops = 0_usize;

    for source in 1..=QUERY_COUNT {
        let mut expected = reference.get_two_hop_ids(source);
        let result = sharded.get_two_hop_with_stats(source);

        validate_result(source, &mut expected, &result)?;

        total_shards_touched += result.shards_touched;
        total_cross_shard_hops += result.cross_shard_hops;

        minimum_cross_shard_hops = minimum_cross_shard_hops.min(result.cross_shard_hops);

        maximum_cross_shard_hops = maximum_cross_shard_hops.max(result.cross_shard_hops);
    }

    println!("Validated {QUERY_COUNT} queries for {label} placement.");

    Ok(AggregateStats {
        average_shards_touched: total_shards_touched as f64 / QUERY_COUNT as f64,

        average_cross_shard_hops: total_cross_shard_hops as f64 / QUERY_COUNT as f64,

        minimum_cross_shard_hops,
        maximum_cross_shard_hops,
    })
}

fn validate_result(
    source: u64,
    expected: &mut Vec<u64>,
    actual: &QueryResult,
) -> Result<(), String> {
    let mut actual_ids = actual.user_ids.clone();

    expected.sort_unstable();
    actual_ids.sort_unstable();

    if *expected != actual_ids {
        return Err(format!("Correctness mismatch for source user {source}"));
    }

    Ok(())
}

fn print_stats(label: &str, stats: &AggregateStats) {
    println!("\n{label} placement:");

    println!(
        "  Average shards touched: {:.2}",
        stats.average_shards_touched
    );

    println!(
        "  Average cross-shard hops: {:.2}",
        stats.average_cross_shard_hops
    );

    println!(
        "  Cross-shard hop range: {} to {}",
        stats.minimum_cross_shard_hops, stats.maximum_cross_shard_hops
    );
}

fn percentage_reduction(before: f64, after: f64) -> f64 {
    if before == 0.0 {
        return 0.0;
    }

    ((before - after) / before) * 100.0
}

fn imbalance_percentage(values: &[usize]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let total: usize = values.iter().sum();
    let average = total as f64 / values.len() as f64;
    let maximum = *values.iter().max().unwrap() as f64;

    if average == 0.0 {
        return 0.0;
    }

    ((maximum - average) / average) * 100.0
}

fn print_distribution(label: &str, graph: &ShardedGraph) {
    let users = graph.users_per_shard();
    let edges = graph.edges_per_shard();

    println!("{label} users per shard: {users:?}");
    println!("{label} edges per shard: {edges:?}");

    println!(
        "{label} user imbalance: {:.2}%",
        imbalance_percentage(&users)
    );

    println!(
        "{label} edge imbalance: {:.2}%\n",
        imbalance_percentage(&edges)
    );
}

fn build_reference_graph() -> Result<Graph, String> {
    let mut graph = Graph::new();

    add_users_to_graph(&mut graph)?;
    add_edges_to_graph(&mut graph)?;

    Ok(graph)
}

fn build_sharded_graph(placement: Placement) -> Result<ShardedGraph, String> {
    let mut graph = ShardedGraph::with_placement(SHARD_COUNT, placement)?;

    for id in 1..=USER_COUNT {
        graph.add_user(id, &format!("user-{id}"))?;
    }

    let community_size = USER_COUNT / COMMUNITY_COUNT;

    for source in 1..=USER_COUNT {
        for offset in 1..=LOCAL_EDGES_PER_USER {
            let target = local_target(source, offset, community_size);

            graph.add_follow(source, target)?;
        }

        graph.add_follow(
            source,
            next_community_target(source, USER_COUNT, community_size),
        )?;
    }

    Ok(graph)
}

fn add_users_to_graph(graph: &mut Graph) -> Result<(), String> {
    for id in 1..=USER_COUNT {
        graph.add_user(id, &format!("user-{id}"))?;
    }

    Ok(())
}

fn add_edges_to_graph(graph: &mut Graph) -> Result<(), String> {
    let community_size = USER_COUNT / COMMUNITY_COUNT;

    for source in 1..=USER_COUNT {
        for offset in 1..=LOCAL_EDGES_PER_USER {
            graph.add_follow(source, local_target(source, offset, community_size))?;
        }

        graph.add_follow(
            source,
            next_community_target(source, USER_COUNT, community_size),
        )?;
    }

    Ok(())
}

fn local_target(source: u64, offset: u64, community_size: u64) -> u64 {
    let zero_based_source = source - 1;

    let community_start = (zero_based_source / community_size) * community_size;

    let position = zero_based_source % community_size;

    community_start + ((position + offset) % community_size) + 1
}

fn next_community_target(source: u64, user_count: u64, community_size: u64) -> u64 {
    ((source - 1 + community_size) % user_count) + 1
}
