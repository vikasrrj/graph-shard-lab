use std::{
    fs::{File, create_dir_all},
    io::{BufWriter, Write},
};

use graph_shard_lab::{
    Graph,
    sharded::{Placement, QueryResult, ShardedGraph},
    workload::{CommunityWorkload, generate_community_workload},
};

const USER_COUNT: u64 = 10_000;
const COMMUNITY_COUNT: u64 = 10;
const EDGES_PER_USER: u64 = 8;
const SHARD_COUNT: usize = 4;
const QUERY_COUNT: u64 = 10_000;
const SEED: u64 = 42;

const LOCAL_EDGE_COUNTS: [u64; 6] = [0, 2, 4, 6, 7, 8];

fn main() -> Result<(), String> {
    let community_size = USER_COUNT / COMMUNITY_COUNT;

    println!(
        "Corrected locality sweep\n\
         Users: {USER_COUNT}\n\
         Communities: {COMMUNITY_COUNT}\n\
         Edges per user: {EDGES_PER_USER}\n\
         Shards: {SHARD_COUNT}\n\
         Seed: {SEED}\n"
    );

    println!(
        "{:<12} {:<12} {:<16} {:<12} {:<16}",
        "Local edges", "Hash hops", "Community hops", "Reduction", "Community shards"
    );

    println!("{}", "-".repeat(72));

    let mut csv_rows = Vec::new();

    csv_rows.push(
        "local_edges,hash_hops,community_hops,reduction_percent,community_shards".to_string(),
    );

    for local_edges_per_user in LOCAL_EDGE_COUNTS {
        /*
        Generate one edge list.

        The normal graph, hash graph, and community graph all receive
        this exact same list, so the comparison is fair.
        */
        let workload = generate_community_workload(
            USER_COUNT,
            COMMUNITY_COUNT,
            EDGES_PER_USER,
            local_edges_per_user,
            SEED,
        )?;

        let reference = build_reference_graph(&workload)?;

        let hash_graph = build_sharded_graph(&workload, Placement::Hash)?;

        let community_graph =
            build_sharded_graph(&workload, Placement::Community { community_size })?;

        let hash_stats = validate_and_measure(&reference, &hash_graph)?;

        let community_stats = validate_and_measure(&reference, &community_graph)?;

        let reduction = percentage_reduction(
            hash_stats.average_cross_shard_hops,
            community_stats.average_cross_shard_hops,
        );

        println!(
            "{:<12} {:<12.2} {:<16.2} {:<11.2}% {:<16.2}",
            local_edges_per_user,
            hash_stats.average_cross_shard_hops,
            community_stats.average_cross_shard_hops,
            reduction,
            community_stats.average_shards_touched
        );

        csv_rows.push(format!(
            "{},{:.2},{:.2},{:.2},{:.2}",
            local_edges_per_user,
            hash_stats.average_cross_shard_hops,
            community_stats.average_cross_shard_hops,
            reduction,
            community_stats.average_shards_touched
        ));
    }

    write_csv("results/locality_sweep.csv", &csv_rows)?;

    println!("\nSaved results to results/locality_sweep.csv");

    /*
    Build one representative community-placed graph so we can inspect
    how unevenly users and edges are distributed across shards.
    */
    let example_workload =
        generate_community_workload(USER_COUNT, COMMUNITY_COUNT, EDGES_PER_USER, 7, SEED)?;

    let community_graph =
        build_sharded_graph(&example_workload, Placement::Community { community_size })?;

    let users = community_graph.users_per_shard();
    let edges = community_graph.edges_per_shard();

    println!("\nCommunity placement distribution:");
    println!("Users per shard: {users:?}");
    println!("Edges per shard: {edges:?}");

    println!(
        "Maximum user imbalance: {:.2}%",
        imbalance_percentage(&users)
    );

    println!(
        "Maximum edge imbalance: {:.2}%",
        imbalance_percentage(&edges)
    );

    Ok(())
}

struct AggregateStats {
    average_shards_touched: f64,
    average_cross_shard_hops: f64,
}

fn build_reference_graph(workload: &CommunityWorkload) -> Result<Graph, String> {
    let mut graph = Graph::new();

    for id in 1..=workload.user_count {
        graph.add_user(id, &format!("user-{id}"))?;
    }

    for &(source, target) in &workload.edges {
        graph.add_follow(source, target)?;
    }

    Ok(graph)
}

fn build_sharded_graph(
    workload: &CommunityWorkload,
    placement: Placement,
) -> Result<ShardedGraph, String> {
    let mut graph = ShardedGraph::with_placement(SHARD_COUNT, placement)?;

    for id in 1..=workload.user_count {
        graph.add_user(id, &format!("user-{id}"))?;
    }

    for &(source, target) in &workload.edges {
        graph.add_follow(source, target)?;
    }

    Ok(graph)
}

fn validate_and_measure(
    reference: &Graph,
    sharded: &ShardedGraph,
) -> Result<AggregateStats, String> {
    let mut total_shards_touched = 0_usize;
    let mut total_cross_shard_hops = 0_usize;

    for source in 1..=QUERY_COUNT {
        let mut expected = reference.get_two_hop_ids(source);
        let actual = sharded.get_two_hop_with_stats(source);

        validate_result(source, &mut expected, &actual)?;

        total_shards_touched += actual.shards_touched;
        total_cross_shard_hops += actual.cross_shard_hops;
    }

    Ok(AggregateStats {
        average_shards_touched: total_shards_touched as f64 / QUERY_COUNT as f64,

        average_cross_shard_hops: total_cross_shard_hops as f64 / QUERY_COUNT as f64,
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

fn write_csv(path: &str, rows: &[String]) -> Result<(), String> {
    create_dir_all("results")
        .map_err(|error| format!("Failed to create results directory: {error}"))?;

    let file = File::create(path).map_err(|error| format!("Failed to create CSV file: {error}"))?;

    let mut writer = BufWriter::new(file);

    for row in rows {
        writeln!(writer, "{row}").map_err(|error| format!("Failed to write CSV row: {error}"))?;
    }

    writer
        .flush()
        .map_err(|error| format!("Failed to finish CSV file: {error}"))?;

    Ok(())
}
