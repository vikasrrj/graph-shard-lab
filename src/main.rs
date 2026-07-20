use std::{
    fs::{File, create_dir_all},
    io::{BufWriter, Write},
};

use graph_shard_lab::{
    Graph,
    sharded::{Placement, QueryResult, ShardedGraph},
    uneven::generate_uneven_community_workload,
    workload::{CommunityWorkload, generate_community_workload},
};

const USER_COUNT: u64 = 10_000;
const COMMUNITY_COUNT: u64 = 10;
const EDGES_PER_USER: u64 = 8;
const SHARD_COUNT: usize = 4;
const SEED: u64 = 42;

const LOCAL_EDGE_COUNTS: [u64; 6] = [0, 2, 4, 6, 7, 8];

const UNEVEN_COMMUNITY_SIZES: [u64; 5] = [4_000, 2_500, 1_500, 1_000, 1_000];

const UNEVEN_LOCAL_EDGES: u64 = 7;

fn main() -> Result<(), String> {
    run_locality_sweep()?;
    run_uneven_community_benchmark()?;

    Ok(())
}

fn run_locality_sweep() -> Result<(), String> {
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
        "{:<12} {:<12} {:<16} {:<12} {:<16} {:<12} {:<13} {:>13}",
        "Local edges",
        "Hash hops",
        "Community hops",
        "Reduction",
        "Community shards",
        "Direct reqs",
        "Batched reqs",
        "Req reduction",
    );

    println!("{}", "-".repeat(124));

    let mut csv_rows = Vec::new();

    csv_rows.push(
        "local_edges,hash_hops,community_hops,\
         reduction_percent,community_shards"
            .replace(' ', ""),
    );

    for local_edges_per_user in LOCAL_EDGE_COUNTS {
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

        let hash_stats = validate_and_measure(&reference, &hash_graph, workload.user_count)?;

        let community_stats =
            validate_and_measure(&reference, &community_graph, workload.user_count)?;

        let reduction = percentage_reduction(
            hash_stats.average_cross_shard_hops,
            community_stats.average_cross_shard_hops,
        );

        let reduction_text = format!("{reduction:.2}%");

        println!(
            "{:<12} {:<12.2} {:<16.2} {:<12} {:<16.2} {:<12.2} {:<13.2} {:>12.2}%",
            local_edges_per_user,
            hash_stats.average_cross_shard_hops,
            community_stats.average_cross_shard_hops,
            reduction_text,
            community_stats.average_shards_touched,
            community_stats.average_direct_shard_requests,
            community_stats.average_batched_shard_requests,
            community_stats.request_reduction_percent,
        );

        csv_rows.push(format!(
            "{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
            local_edges_per_user,
            hash_stats.average_cross_shard_hops,
            community_stats.average_cross_shard_hops,
            reduction,
            community_stats.average_shards_touched,
            community_stats.average_direct_shard_requests,
            community_stats.average_batched_shard_requests,
            community_stats.request_reduction_percent,
        ));
    }

    write_csv("results/locality_sweep.csv", &csv_rows)?;

    println!("\nSaved results to results/locality_sweep.csv");

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

fn run_uneven_community_benchmark() -> Result<(), String> {
    println!("\n");
    println!("Uneven community benchmark");
    println!("Community sizes: {:?}", UNEVEN_COMMUNITY_SIZES);
    println!("Edges per user: {EDGES_PER_USER}");
    println!("Local edges per user: {UNEVEN_LOCAL_EDGES}");
    println!("Shards: {SHARD_COUNT}");
    println!("Seed: {SEED}\n");

    let workload = generate_uneven_community_workload(
        &UNEVEN_COMMUNITY_SIZES,
        EDGES_PER_USER,
        UNEVEN_LOCAL_EDGES,
        SEED,
    )?;

    /*
    The reference graph is not sharded.

    It tells us the correct query answers.
    */
    let reference = build_reference_graph(&workload)?;

    /*
    Strategy 1: Hash placement.

    Users are spread according to their IDs.
    */
    let hash_graph = build_sharded_graph(&workload, Placement::Hash)?;

    /*
    Strategy 2: Naive community placement.

    Communities are assigned in repeating order:

    community 0 -> shard 0
    community 1 -> shard 1
    community 2 -> shard 2
    community 3 -> shard 3
    community 4 -> shard 0
    */
    let naive_assignment: Vec<usize> = (0..UNEVEN_COMMUNITY_SIZES.len())
        .map(|community_id| community_id % SHARD_COUNT)
        .collect();

    let naive_graph = build_sharded_graph(
        &workload,
        Placement::BalancedCommunity {
            community_sizes: UNEVEN_COMMUNITY_SIZES.to_vec(),

            community_to_shard: naive_assignment,
        },
    )?;

    /*
    Strategy 3: Balanced community placement.

    The largest communities are placed first.
    Every next community goes to the shard with
    the fewest users.
    */
    let balanced_graph = build_balanced_graph(&workload, UNEVEN_COMMUNITY_SIZES.to_vec())?;

    let hash_stats = validate_and_measure(&reference, &hash_graph, workload.user_count)?;

    let naive_stats = validate_and_measure(&reference, &naive_graph, workload.user_count)?;

    let balanced_stats = validate_and_measure(&reference, &balanced_graph, workload.user_count)?;
    println!(
        "{:<22} {:<28} {:>14} {:>14} {:>12} {:>12} {:>13}",
        "Strategy",
        "Users per shard",
        "User imbalance",
        "Average hops",
        "Direct reqs",
        "Batched reqs",
        "Req reduction",
    );

    println!("{}", "-".repeat(126));

    print_strategy_result("Hash", &hash_graph, &hash_stats);

    print_strategy_result("Naive community", &naive_graph, &naive_stats);

    print_strategy_result("Balanced community", &balanced_graph, &balanced_stats);

    println!("\nEdge distribution:");

    println!("Hash:               {:?}", hash_graph.edges_per_shard());

    println!("Naive community:    {:?}", naive_graph.edges_per_shard());

    println!("Balanced community: {:?}", balanced_graph.edges_per_shard());

    let csv_rows = vec![
        "strategy,average_cross_shard_hops,\
         average_shards_touched,user_imbalance_percent,\
         edge_imbalance_percent"
            .replace(' ', ""),
        strategy_csv_row("hash", &hash_graph, &hash_stats),
        strategy_csv_row("naive_community", &naive_graph, &naive_stats),
        strategy_csv_row("balanced_community", &balanced_graph, &balanced_stats),
    ];

    write_csv("results/uneven_communities.csv", &csv_rows)?;

    println!(
        "\nSaved results to \
         results/uneven_communities.csv"
    );

    Ok(())
}

struct AggregateStats {
    average_shards_touched: f64,
    average_cross_shard_hops: f64,
    average_direct_shard_requests: f64,
    average_batched_shard_requests: f64,
    request_reduction_percent: f64,
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

    populate_sharded_graph(&mut graph, workload)?;

    Ok(graph)
}

fn build_balanced_graph(
    workload: &CommunityWorkload,
    community_sizes: Vec<u64>,
) -> Result<ShardedGraph, String> {
    let mut graph = ShardedGraph::with_balanced_communities(SHARD_COUNT, community_sizes)?;

    populate_sharded_graph(&mut graph, workload)?;

    Ok(graph)
}

fn populate_sharded_graph(
    graph: &mut ShardedGraph,
    workload: &CommunityWorkload,
) -> Result<(), String> {
    for id in 1..=workload.user_count {
        graph.add_user(id, &format!("user-{id}"))?;
    }

    for &(source, target) in &workload.edges {
        graph.add_follow(source, target)?;
    }

    Ok(())
}

fn validate_and_measure(
    reference: &Graph,
    sharded: &ShardedGraph,
    query_count: u64,
) -> Result<AggregateStats, String> {
    let mut total_shards_touched = 0_usize;
    let mut total_cross_shard_hops = 0_usize;
    let mut total_direct_shard_requests = 0_usize;
    let mut total_batched_shard_requests = 0_usize;

    for source in 1..=query_count {
        let mut expected = reference.get_two_hop_ids(source);

        let direct = sharded.get_two_hop_with_stats(source);
        validate_result(source, &mut expected, &direct)?;

        let batched = sharded.get_two_hop_batched_with_stats(source);
        validate_result(source, &mut expected, &batched)?;

        total_shards_touched += direct.shards_touched;
        total_cross_shard_hops += direct.cross_shard_hops;
        total_direct_shard_requests += direct.shard_requests;
        total_batched_shard_requests += batched.shard_requests;
    }

    let average_direct_shard_requests = total_direct_shard_requests as f64 / query_count as f64;

    let average_batched_shard_requests = total_batched_shard_requests as f64 / query_count as f64;

    let request_reduction_percent = if average_direct_shard_requests == 0.0 {
        0.0
    } else {
        (average_direct_shard_requests - average_batched_shard_requests)
            / average_direct_shard_requests
            * 100.0
    };

    Ok(AggregateStats {
        average_shards_touched: total_shards_touched as f64 / query_count as f64,
        average_cross_shard_hops: total_cross_shard_hops as f64 / query_count as f64,
        average_direct_shard_requests,
        average_batched_shard_requests,
        request_reduction_percent,
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
fn print_strategy_result(name: &str, graph: &ShardedGraph, stats: &AggregateStats) {
    let users = graph.users_per_shard();
    let users_text = format!("{users:?}");

    println!(
        "{:<22} {:<28} {:>13.2}% {:>14.2} {:>12.2} {:>12.2} {:>12.2}%",
        name,
        users_text,
        imbalance_percentage(&users),
        stats.average_cross_shard_hops,
        stats.average_direct_shard_requests,
        stats.average_batched_shard_requests,
        stats.request_reduction_percent,
    );
}

fn strategy_csv_row(name: &str, graph: &ShardedGraph, stats: &AggregateStats) -> String {
    let users = graph.users_per_shard();
    let edges = graph.edges_per_shard();

    format!(
        "{},{:.2},{:.2},{:.2},{:.2}",
        name,
        stats.average_cross_shard_hops,
        stats.average_shards_touched,
        imbalance_percentage(&users),
        imbalance_percentage(&edges)
    )
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
