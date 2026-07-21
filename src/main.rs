use std::{
    fs::{File, create_dir_all},
    io::{BufWriter, Write},
};

use graph_shard_lab::{
    Graph,
    cache::LruCache,
    sharded::{Placement, QueryResult, ShardedGraph},
    uneven::generate_uneven_community_workload,
    workload::{
        CommunityWorkload, HubWorkload, generate_community_workload, generate_hub_workload,
    },
};

const USER_COUNT: u64 = 10_000;
const COMMUNITY_COUNT: u64 = 10;
const EDGES_PER_USER: u64 = 8;
const SHARD_COUNT: usize = 4;
const SEED: u64 = 42;

const SWEEP_SEEDS: [u64; 5] = [42, 43, 44, 45, 46];
const SWEEP_SHARD_COUNTS: [usize; 4] = [2, 4, 8, 16];
const SWEEP_COMMUNITY_COUNT: u64 = 16;
const SWEEP_LOCAL_EDGE_COUNTS: [u64; 2] = [4, 7];

const LOCAL_EDGE_COUNTS: [u64; 6] = [0, 2, 4, 6, 7, 8];

const UNEVEN_COMMUNITY_SIZES: [u64; 5] = [4_000, 2_500, 1_500, 1_000, 1_000];

const UNEVEN_LOCAL_EDGES: u64 = 7;
const HUB_COUNT: u64 = 100;
const HUB_EDGES_PER_USER: u64 = 2;
const CACHE_CAPACITIES: [usize; 6] = [25, 50, 100, 250, 500, 1_000];
const CACHE_STARTUP_WINDOW: usize = 1000;
const REAL_CACHE_CAPACITIES_PER_SHARD: [usize; 4] = [25, 50, 100, 250];

fn main() -> Result<(), String> {
    run_locality_sweep()?;
    run_uneven_community_benchmark()?;
    run_multi_seed_shard_sweep()?;
    run_hub_hotspot_baseline()?;
    run_hotspot_cache_baseline()?;
    run_hotspot_cache_warming_benchmark()?;
    run_real_sharded_cache_benchmark()?;
    run_real_sharded_cache_warming_benchmark()?;

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
         reduction_percent,community_shards,\
         direct_shard_requests,batched_shard_requests,\
         request_reduction_percent"
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
         edge_imbalance_percent,direct_shard_requests,\
         batched_shard_requests,request_reduction_percent"
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
#[derive(Debug)]
struct CacheRunStats {
    hits: u64,
    misses: u64,
    startup_hits: u64,
    startup_accesses: u64,
}

#[derive(Debug)]
struct RealCacheRunStats {
    hits: usize,
    misses: usize,
    startup_hits: usize,
    startup_misses: usize,
}

struct AggregateStats {
    average_shards_touched: f64,
    average_cross_shard_hops: f64,
    average_direct_shard_requests: f64,
    average_batched_shard_requests: f64,
    request_reduction_percent: f64,
}
fn run_multi_seed_shard_sweep() -> Result<(), String> {
    let community_size = USER_COUNT / SWEEP_COMMUNITY_COUNT;

    println!(
        "\nMulti-seed and multi-shard batching sweep\n\
         Users: {USER_COUNT}\n\
         Communities: {SWEEP_COMMUNITY_COUNT}\n\
         Edges per user: {EDGES_PER_USER}\n\
         Seeds: {SWEEP_SEEDS:?}\n"
    );

    println!(
        "{:<12} {:<10} {:<10} {:<16} {:<17} {:<15}",
        "Local edges", "Shards", "Seeds", "Direct requests", "Batched requests", "Reduction",
    );

    println!("{}", "-".repeat(86));

    let mut csv_rows = vec![
        "local_edges_per_user,shard_count,seed_count,\
         average_direct_shard_requests,average_batched_shard_requests,\
         request_reduction_percent"
            .replace(' ', ""),
    ];

    for local_edges_per_user in SWEEP_LOCAL_EDGE_COUNTS {
        for shard_count in SWEEP_SHARD_COUNTS {
            let mut total_direct_requests = 0.0;
            let mut total_batched_requests = 0.0;

            for seed in SWEEP_SEEDS {
                let workload = generate_community_workload(
                    USER_COUNT,
                    SWEEP_COMMUNITY_COUNT,
                    EDGES_PER_USER,
                    local_edges_per_user,
                    seed,
                )?;

                let reference = build_reference_graph(&workload)?;

                let sharded = build_sharded_graph_with_shard_count(
                    &workload,
                    Placement::Community { community_size },
                    shard_count,
                )?;

                let stats = validate_and_measure(&reference, &sharded, workload.user_count)?;

                total_direct_requests += stats.average_direct_shard_requests;
                total_batched_requests += stats.average_batched_shard_requests;
            }

            let seed_count = SWEEP_SEEDS.len() as f64;

            let average_direct_requests = total_direct_requests / seed_count;

            let average_batched_requests = total_batched_requests / seed_count;

            let reduction = percentage_reduction(average_direct_requests, average_batched_requests);

            println!(
                "{:<12} {:<10} {:<10} {:<16.2} {:<17.2} {:>13.2}%",
                local_edges_per_user,
                shard_count,
                SWEEP_SEEDS.len(),
                average_direct_requests,
                average_batched_requests,
                reduction,
            );

            csv_rows.push(format!(
                "{},{},{},{:.2},{:.2},{:.2}",
                local_edges_per_user,
                shard_count,
                SWEEP_SEEDS.len(),
                average_direct_requests,
                average_batched_requests,
                reduction,
            ));
        }
    }

    write_csv("results/batching_sweep.csv", &csv_rows)?;

    println!("\nSaved results to results/batching_sweep.csv");

    Ok(())
}

fn run_hub_hotspot_baseline() -> Result<(), String> {
    let workload = generate_hub_workload(
        USER_COUNT,
        HUB_COUNT,
        EDGES_PER_USER,
        HUB_EDGES_PER_USER,
        SEED,
    )?;

    // Index 0 is unused because user IDs begin at 1.
    let mut adjacency_reads = vec![0_u64; (workload.user_count + 1) as usize];

    /*
    Every edge source -> target means that target appears as a
    first-hop user when querying source.

    The two-hop query must then read target's adjacency list.
    */
    for &(_, target) in &workload.edges {
        adjacency_reads[target as usize] += 1;
    }

    let total_reads = workload.edges.len() as u64;

    let hub_reads: u64 = workload
        .hub_ids
        .iter()
        .map(|&hub_id| adjacency_reads[hub_id as usize])
        .sum();

    let normal_reads = total_reads - hub_reads;

    let hub_user_count = workload.hub_ids.len() as u64;
    let normal_user_count = workload.user_count - hub_user_count;

    let hub_read_share = hub_reads as f64 / total_reads as f64 * 100.0;

    let average_hub_reads = hub_reads as f64 / hub_user_count as f64;

    let average_normal_reads = normal_reads as f64 / normal_user_count as f64;

    let hotspot_multiplier = if average_normal_reads == 0.0 {
        0.0
    } else {
        average_hub_reads / average_normal_reads
    };

    let mut ranked_users: Vec<(u64, u64)> = (1..=workload.user_count)
        .map(|user_id| (user_id, adjacency_reads[user_id as usize]))
        .collect();

    ranked_users.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

    println!(
        "\nHub-heavy hotspot baseline\n\
         Users: {USER_COUNT}\n\
         Hubs: {HUB_COUNT}\n\
         Edges per user: {EDGES_PER_USER}\n\
         Hub edges per user: {HUB_EDGES_PER_USER}\n\
         Seed: {SEED}\n"
    );

    println!("Total adjacency reads: {total_reads}");
    println!("Reads targeting hubs: {hub_reads}");
    println!("Hub share of reads: {hub_read_share:.2}%");
    println!("Average reads per hub: {average_hub_reads:.2}");
    println!(
        "Average reads per normal user: \
         {average_normal_reads:.2}"
    );
    println!(
        "Average hub-to-normal read multiplier: \
         {hotspot_multiplier:.2}x"
    );

    println!("\nTop 10 most-read adjacency lists:");

    println!(
        "{:<6} {:<10} {:<10} {:<10}",
        "Rank", "User", "Type", "Reads",
    );

    println!("{}", "-".repeat(40));

    for (index, &(user_id, reads)) in ranked_users.iter().take(10).enumerate() {
        let user_type = if user_id <= HUB_COUNT {
            "hub"
        } else {
            "normal"
        };

        println!(
            "{:<6} {:<10} {:<10} {:<10}",
            index + 1,
            user_id,
            user_type,
            reads,
        );
    }

    let mut csv_rows = vec!["user_id,is_hub,adjacency_reads".to_string()];

    for (user_id, reads) in ranked_users {
        let is_hub = user_id <= HUB_COUNT;

        csv_rows.push(format!("{user_id},{is_hub},{reads}"));
    }

    write_csv("results/hub_hotspot.csv", &csv_rows)?;

    println!("\nSaved results to results/hub_hotspot.csv");

    Ok(())
}
fn run_hotspot_cache_baseline() -> Result<(), String> {
    let workload = generate_hub_workload(
        USER_COUNT,
        HUB_COUNT,
        EDGES_PER_USER,
        HUB_EDGES_PER_USER,
        SEED,
    )?;

    println!(
        "\nCold LRU cache on hub-heavy workload\n\
         Total logical adjacency reads: {}\n\
         Cache starts empty\n",
        workload.edges.len(),
    );

    println!(
        "{:<10} {:<10} {:<10} {:<12} {:<12} {:<12}",
        "Capacity", "Hits", "Misses", "Hit rate", "Hub rate", "Normal rate",
    );

    println!("{}", "-".repeat(72));

    let mut csv_rows = vec![
        "capacity,total_accesses,hits,misses,hit_rate_percent,\
         hub_hit_rate_percent,normal_hit_rate_percent,\
         main_graph_reads_avoided"
            .replace(' ', ""),
    ];

    for capacity in CACHE_CAPACITIES {
        let mut cache = LruCache::new(capacity)?;

        let mut hits = 0_u64;
        let mut misses = 0_u64;

        let mut hub_hits = 0_u64;
        let mut hub_misses = 0_u64;

        let mut normal_hits = 0_u64;
        let mut normal_misses = 0_u64;

        /*
        The edge order represents running one query from each source.

        For every source -> target edge, the two-hop query reads
        target's adjacency list.
        */
        for &(_, target) in &workload.edges {
            let is_hub = target <= HUB_COUNT;
            let cache_hit = cache.access(target);

            match (cache_hit, is_hub) {
                (true, true) => {
                    hits += 1;
                    hub_hits += 1;
                }
                (true, false) => {
                    hits += 1;
                    normal_hits += 1;
                }
                (false, true) => {
                    misses += 1;
                    hub_misses += 1;
                }
                (false, false) => {
                    misses += 1;
                    normal_misses += 1;
                }
            }
        }

        let total_accesses = hits + misses;

        if total_accesses != workload.edges.len() as u64 {
            return Err("Cache accounting does not match workload".to_string());
        }

        let hit_rate = percentage(hits, total_accesses);

        let hub_hit_rate = percentage(hub_hits, hub_hits + hub_misses);

        let normal_hit_rate = percentage(normal_hits, normal_hits + normal_misses);

        println!(
            "{:<10} {:<10} {:<10} {:>10.2}% {:>10.2}% {:>10.2}%",
            capacity, hits, misses, hit_rate, hub_hit_rate, normal_hit_rate,
        );

        csv_rows.push(format!(
            "{capacity},{total_accesses},{hits},{misses},\
             {hit_rate:.2},{hub_hit_rate:.2},\
             {normal_hit_rate:.2},{hits}"
        ));
    }

    write_csv("results/cache_baseline.csv", &csv_rows)?;

    println!("\nSaved results to results/cache_baseline.csv");

    Ok(())
}

fn percentage(part: u64, total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }

    part as f64 / total as f64 * 100.0
}

fn simulate_cache_run(
    edges: &[(u64, u64)],
    capacity: usize,
    preloaded_user_ids: &[u64],
) -> Result<CacheRunStats, String> {
    let mut cache = LruCache::new(capacity)?;

    /*
    Preloading happens before measured traffic begins.

    Calling access() inserts the user ID into the cache.
    We deliberately do not count these preload operations
    as cache hits or misses.
    */
    for &user_id in preloaded_user_ids.iter().take(capacity) {
        cache.access(user_id);
    }

    let mut hits = 0_u64;
    let mut misses = 0_u64;

    let mut startup_hits = 0_u64;
    let mut startup_accesses = 0_u64;

    for (index, &(_, target)) in edges.iter().enumerate() {
        let cache_hit = cache.access(target);

        if cache_hit {
            hits += 1;
        } else {
            misses += 1;
        }

        if index < CACHE_STARTUP_WINDOW {
            startup_accesses += 1;

            if cache_hit {
                startup_hits += 1;
            }
        }
    }

    Ok(CacheRunStats {
        hits,
        misses,
        startup_hits,
        startup_accesses,
    })
}

fn run_hotspot_cache_warming_benchmark() -> Result<(), String> {
    let workload = generate_hub_workload(
        USER_COUNT,
        HUB_COUNT,
        EDGES_PER_USER,
        HUB_EDGES_PER_USER,
        SEED,
    )?;

    /*
    Count how many incoming edges each hub receives.

    A hub with more incoming edges will be read more often
    during this two-hop workload.
    */
    let mut hub_read_counts = vec![0_u64; (HUB_COUNT + 1) as usize];

    for &(_, target) in &workload.edges {
        if target <= HUB_COUNT {
            hub_read_counts[target as usize] += 1;
        }
    }

    /*
    Sort hubs from most popular to least popular.

    Example:
    [59, 25, 53, ...]
    */
    let mut ranked_hubs: Vec<u64> = (1..=HUB_COUNT).collect();

    ranked_hubs.sort_by(|left, right| {
        hub_read_counts[*right as usize]
            .cmp(&hub_read_counts[*left as usize])
            .then_with(|| left.cmp(right))
    });

    println!(
        "\nDegree-warmed LRU cache on hub-heavy workload\n\
         Cache warming: preload the most-followed hubs\n\
         Startup window: first {CACHE_STARTUP_WINDOW} reads\n"
    );

    println!(
        "{:<10} {:<11} {:<13} {:<13} {:<15} {:<15}",
        "Capacity", "Preloaded", "Cold total", "Warm total", "Cold first 1k", "Warm first 1k",
    );

    println!("{}", "-".repeat(86));

    let mut csv_rows = vec![
        "capacity,preloaded_hubs,cold_hits,cold_misses,\
         warmed_hits,warmed_misses,cold_hit_rate_percent,\
         warmed_hit_rate_percent,cold_startup_hit_rate_percent,\
         warmed_startup_hit_rate_percent"
            .replace(' ', ""),
    ];

    for capacity in CACHE_CAPACITIES {
        let preload_count = capacity.min(ranked_hubs.len());

        let preloaded_hubs = &ranked_hubs[..preload_count];

        let cold = simulate_cache_run(&workload.edges, capacity, &[])?;

        let warmed = simulate_cache_run(&workload.edges, capacity, preloaded_hubs)?;

        let cold_total_accesses = cold.hits + cold.misses;

        let warmed_total_accesses = warmed.hits + warmed.misses;

        let cold_hit_rate = percentage(cold.hits, cold_total_accesses);

        let warmed_hit_rate = percentage(warmed.hits, warmed_total_accesses);

        let cold_startup_hit_rate = percentage(cold.startup_hits, cold.startup_accesses);

        let warmed_startup_hit_rate = percentage(warmed.startup_hits, warmed.startup_accesses);

        println!(
            "{:<10} {:<11} {:>11.2}% {:>11.2}% {:>13.2}% {:>13.2}%",
            capacity,
            preload_count,
            cold_hit_rate,
            warmed_hit_rate,
            cold_startup_hit_rate,
            warmed_startup_hit_rate,
        );

        csv_rows.push(format!(
            "{capacity},{preload_count},{},{},{},{},\
             {:.2},{:.2},{:.2},{:.2}",
            cold.hits,
            cold.misses,
            warmed.hits,
            warmed.misses,
            cold_hit_rate,
            warmed_hit_rate,
            cold_startup_hit_rate,
            warmed_startup_hit_rate,
        ));
    }

    write_csv("results/cache_warming.csv", &csv_rows)?;

    println!("\nSaved results to results/cache_warming.csv");

    Ok(())
}

fn build_hub_reference_graph(workload: &HubWorkload) -> Result<Graph, String> {
    let mut graph = Graph::new();

    for id in 1..=workload.user_count {
        graph.add_user(id, &format!("user-{id}"))?;
    }

    for &(source, target) in &workload.edges {
        graph.add_follow(source, target)?;
    }

    Ok(graph)
}

fn build_cached_hub_sharded_graph(
    workload: &HubWorkload,
    cache_capacity_per_shard: usize,
) -> Result<ShardedGraph, String> {
    let mut graph = ShardedGraph::with_placement_and_cache(
        SHARD_COUNT,
        Placement::Hash,
        cache_capacity_per_shard,
    )?;

    for id in 1..=workload.user_count {
        graph.add_user(id, &format!("user-{id}"))?;
    }

    for &(source, target) in &workload.edges {
        graph.add_follow(source, target)?;
    }

    Ok(graph)
}

fn run_real_sharded_cache_benchmark() -> Result<(), String> {
    let workload = generate_hub_workload(
        USER_COUNT,
        HUB_COUNT,
        EDGES_PER_USER,
        HUB_EDGES_PER_USER,
        SEED,
    )?;

    let reference = build_hub_reference_graph(&workload)?;

    println!(
        "\nReal per-shard adjacency cache benchmark\n\
         Shards: {SHARD_COUNT}\n\
         Queries: {}\n\
         Each shard owns an independent cache\n",
        workload.user_count,
    );

    println!(
        "{:<14} {:<14} {:<12} {:<12} {:<12}",
        "Per shard", "Total capacity", "Hits", "Misses", "Hit rate",
    );

    println!("{}", "-".repeat(70));

    let mut csv_rows = vec![
        "cache_capacity_per_shard,total_cache_capacity,\
         total_queries,total_accesses,cache_hits,cache_misses,\
         hit_rate_percent"
            .replace(' ', ""),
    ];

    for capacity_per_shard in REAL_CACHE_CAPACITIES_PER_SHARD {
        let mut sharded = build_cached_hub_sharded_graph(&workload, capacity_per_shard)?;

        let mut total_hits = 0_usize;
        let mut total_misses = 0_usize;

        for source in 1..=workload.user_count {
            let mut expected = reference.get_two_hop_ids(source);

            let cached = sharded.get_two_hop_with_cache_stats(source)?;

            let mut actual = cached.user_ids;

            expected.sort_unstable();
            actual.sort_unstable();

            if actual != expected {
                return Err(format!(
                    "Cached query returned incorrect users for source {source}"
                ));
            }

            total_hits += cached.cache_hits;
            total_misses += cached.cache_misses;
        }

        let total_accesses = total_hits + total_misses;

        let expected_accesses = workload.edges.len();

        if total_accesses != expected_accesses {
            return Err(format!(
                "Expected {expected_accesses} cache accesses, \
                 but recorded {total_accesses}"
            ));
        }

        let hit_rate = percentage(total_hits as u64, total_accesses as u64);

        let total_capacity = capacity_per_shard * SHARD_COUNT;

        println!(
            "{:<14} {:<14} {:<12} {:<12} {:>10.2}%",
            capacity_per_shard, total_capacity, total_hits, total_misses, hit_rate,
        );

        csv_rows.push(format!(
            "{},{},{},{},{},{},{:.2}",
            capacity_per_shard,
            total_capacity,
            workload.user_count,
            total_accesses,
            total_hits,
            total_misses,
            hit_rate,
        ));
    }

    write_csv("results/real_sharded_cache.csv", &csv_rows)?;

    println!(
        "\nSaved results to \
         results/real_sharded_cache.csv"
    );

    Ok(())
}

fn run_real_cached_queries(
    reference: &Graph,
    sharded: &mut ShardedGraph,
    query_count: u64,
) -> Result<RealCacheRunStats, String> {
    if CACHE_STARTUP_WINDOW as u64 % EDGES_PER_USER != 0 {
        return Err("Startup window must divide evenly by edges per user".to_string());
    }

    let startup_query_count = CACHE_STARTUP_WINDOW as u64 / EDGES_PER_USER;

    let mut hits = 0_usize;
    let mut misses = 0_usize;

    let mut startup_hits = 0_usize;
    let mut startup_misses = 0_usize;

    for source in 1..=query_count {
        let mut expected = reference.get_two_hop_ids(source);

        let cached = sharded.get_two_hop_with_cache_stats(source)?;

        let mut actual = cached.user_ids;

        expected.sort_unstable();
        actual.sort_unstable();

        if actual != expected {
            return Err(format!(
                "Cached query returned incorrect users for source {source}"
            ));
        }

        hits += cached.cache_hits;
        misses += cached.cache_misses;

        /*
        Every query has exactly eight first-hop adjacency accesses.

        1,000 accesses / 8 accesses per query
        = first 125 queries.
        */
        if source <= startup_query_count {
            startup_hits += cached.cache_hits;
            startup_misses += cached.cache_misses;
        }
    }

    Ok(RealCacheRunStats {
        hits,
        misses,
        startup_hits,
        startup_misses,
    })
}

fn run_real_sharded_cache_warming_benchmark() -> Result<(), String> {
    let workload = generate_hub_workload(
        USER_COUNT,
        HUB_COUNT,
        EDGES_PER_USER,
        HUB_EDGES_PER_USER,
        SEED,
    )?;

    let reference = build_hub_reference_graph(&workload)?;

    let mut hub_read_counts = vec![0_u64; (HUB_COUNT + 1) as usize];

    for &(_, target) in &workload.edges {
        if target <= HUB_COUNT {
            hub_read_counts[target as usize] += 1;
        }
    }

    let mut ranked_hubs: Vec<u64> = (1..=HUB_COUNT).collect();

    ranked_hubs.sort_by(|left, right| {
        hub_read_counts[*right as usize]
            .cmp(&hub_read_counts[*left as usize])
            .then_with(|| left.cmp(right))
    });

    println!(
        "\nReal per-shard cache warming benchmark\n\
         Shards: {SHARD_COUNT}\n\
         Hubs preloaded: {HUB_COUNT}\n\
         Startup window: first {CACHE_STARTUP_WINDOW} accesses\n"
    );

    println!(
        "{:<12} {:<13} {:<13} {:<16} {:<16}",
        "Per shard", "Cold total", "Warm total", "Cold first 1k", "Warm first 1k",
    );

    println!("{}", "-".repeat(78));

    let mut csv_rows = vec![
        "cache_capacity_per_shard,total_cache_capacity,\
         preloaded_hubs,cold_hits,cold_misses,warmed_hits,\
         warmed_misses,cold_hit_rate_percent,\
         warmed_hit_rate_percent,\
         cold_startup_hit_rate_percent,\
         warmed_startup_hit_rate_percent"
            .replace(' ', ""),
    ];

    for capacity_per_shard in REAL_CACHE_CAPACITIES_PER_SHARD {
        let mut cold_graph = build_cached_hub_sharded_graph(&workload, capacity_per_shard)?;

        let cold = run_real_cached_queries(&reference, &mut cold_graph, workload.user_count)?;

        let mut warmed_graph = build_cached_hub_sharded_graph(&workload, capacity_per_shard)?;

        /*
        Each hub is inserted into the cache belonging to
        the shard that owns that hub.
        */
        for &hub_id in &ranked_hubs {
            warmed_graph.warm_cache_for_user(hub_id)?;
        }

        let warmed = run_real_cached_queries(&reference, &mut warmed_graph, workload.user_count)?;

        let cold_total = cold.hits + cold.misses;

        let warmed_total = warmed.hits + warmed.misses;

        let cold_startup_total = cold.startup_hits + cold.startup_misses;

        let warmed_startup_total = warmed.startup_hits + warmed.startup_misses;

        let cold_hit_rate = percentage(cold.hits as u64, cold_total as u64);

        let warmed_hit_rate = percentage(warmed.hits as u64, warmed_total as u64);

        let cold_startup_rate = percentage(cold.startup_hits as u64, cold_startup_total as u64);

        let warmed_startup_rate =
            percentage(warmed.startup_hits as u64, warmed_startup_total as u64);

        println!(
            "{:<12} {:>11.2}% {:>11.2}% {:>14.2}% {:>14.2}%",
            capacity_per_shard,
            cold_hit_rate,
            warmed_hit_rate,
            cold_startup_rate,
            warmed_startup_rate,
        );

        csv_rows.push(format!(
            "{},{},{},{},{},{},{},{:.2},{:.2},{:.2},{:.2}",
            capacity_per_shard,
            capacity_per_shard * SHARD_COUNT,
            ranked_hubs.len(),
            cold.hits,
            cold.misses,
            warmed.hits,
            warmed.misses,
            cold_hit_rate,
            warmed_hit_rate,
            cold_startup_rate,
            warmed_startup_rate,
        ));
    }

    write_csv("results/real_sharded_cache_warming.csv", &csv_rows)?;

    println!(
        "\nSaved results to \
         results/real_sharded_cache_warming.csv"
    );

    Ok(())
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
    build_sharded_graph_with_shard_count(workload, placement, SHARD_COUNT)
}

fn build_sharded_graph_with_shard_count(
    workload: &CommunityWorkload,
    placement: Placement,
    shard_count: usize,
) -> Result<ShardedGraph, String> {
    let mut graph = ShardedGraph::with_placement(shard_count, placement)?;

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
        "{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
        name,
        stats.average_cross_shard_hops,
        stats.average_shards_touched,
        imbalance_percentage(&users),
        imbalance_percentage(&edges),
        stats.average_direct_shard_requests,
        stats.average_batched_shard_requests,
        stats.request_reduction_percent,
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
