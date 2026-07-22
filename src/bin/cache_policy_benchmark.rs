use graph_shard_lab::cache::{AdjacencyLruCache, EvictionPolicy};
use std::collections::BTreeSet;
use std::fs;

const SHARD_COUNT: usize = 4;
const USER_COUNT: u64 = 10_000;
const HUB_COUNT: u64 = 100;
const TOTAL_QUERIES: u64 = 10_000;
const FIRST_HOPS_PER_QUERY: usize = 8;
const SECOND_HOPS_PER_USER: u64 = 8;

const CAPACITIES_PER_SHARD: [usize; 4] = [25, 50, 100, 250];

#[derive(Debug)]
struct RunStats {
    queries: usize,
    hits: usize,
    misses: usize,
}

impl RunStats {
    fn total_accesses(&self) -> usize {
        self.hits + self.misses
    }

    fn hit_rate_percent(&self) -> f64 {
        let total = self.total_accesses();

        if total == 0 {
            0.0
        } else {
            self.hits as f64 * 100.0 / total as f64
        }
    }
}

fn policy_name(policy: EvictionPolicy) -> &'static str {
    match policy {
        EvictionPolicy::Lru => "lru",
        EvictionPolicy::Fifo => "fifo",
        EvictionPolicy::Lfu => "lfu",
    }
}

fn shard_for(user_id: u64) -> usize {
    user_id as usize % SHARD_COUNT
}

fn adjacency_for(user_id: u64) -> Vec<u64> {
    (1..=SECOND_HOPS_PER_USER)
        .map(|offset| {
            1 + ((user_id
                .wrapping_mul(31)
                .wrapping_add(offset.wrapping_mul(17)))
                % USER_COUNT)
        })
        .collect()
}

fn first_hops_for(source: u64) -> Vec<u64> {
    let normal_user_count = USER_COUNT - HUB_COUNT;

    let mut first_hops = Vec::with_capacity(FIRST_HOPS_PER_QUERY);

    // Two of eight accesses target the hot set: 25% hub traffic.
    first_hops.push(1 + ((source - 1) % HUB_COUNT));
    first_hops.push(1 + ((source + 36) % HUB_COUNT));

    // Six accesses target normal users.
    for offset in 0..6_u64 {
        let normal_user = HUB_COUNT + 1 + ((source * 97 + offset * 7_919) % normal_user_count);

        first_hops.push(normal_user);
    }

    first_hops
}

fn uncached_two_hop(source: u64) -> Vec<u64> {
    let mut result = BTreeSet::new();

    for first_hop in first_hops_for(source) {
        for second_hop in adjacency_for(first_hop) {
            result.insert(second_hop);
        }
    }

    result.into_iter().collect()
}

fn build_caches(
    capacity_per_shard: usize,
    policy: EvictionPolicy,
) -> Result<Vec<AdjacencyLruCache>, String> {
    (0..SHARD_COUNT)
        .map(|_| AdjacencyLruCache::new_with_policy(capacity_per_shard, policy))
        .collect()
}

fn warm_hubs(caches: &mut [AdjacencyLruCache]) {
    for user_id in 1..=HUB_COUNT {
        let shard_id = shard_for(user_id);
        let _ = caches[shard_id].insert(user_id, adjacency_for(user_id));
    }
}

fn cached_two_hop(source: u64, caches: &mut [AdjacencyLruCache]) -> (Vec<u64>, usize, usize) {
    let mut result = BTreeSet::new();
    let mut hits = 0;
    let mut misses = 0;

    for first_hop in first_hops_for(source) {
        let shard_id = shard_for(first_hop);

        let adjacency = match caches[shard_id].get_shared(first_hop) {
            Some(cached) => {
                hits += 1;
                cached
            }

            None => {
                misses += 1;

                let adjacency = adjacency_for(first_hop);
                caches[shard_id].insert_shared(first_hop, adjacency)
            }
        };

        result.extend(adjacency.iter().copied());
    }

    (result.into_iter().collect(), hits, misses)
}

fn run_queries(
    capacity_per_shard: usize,
    policy: EvictionPolicy,
    warmed: bool,
) -> Result<RunStats, String> {
    let mut caches = build_caches(capacity_per_shard, policy)?;

    if warmed {
        warm_hubs(&mut caches);
    }

    let mut total_hits = 0;
    let mut total_misses = 0;

    for source in 1..=TOTAL_QUERIES {
        let expected = uncached_two_hop(source);

        let (actual, hits, misses) = cached_two_hop(source, &mut caches);

        if actual != expected {
            return Err(format!(
                "{} returned incorrect two-hop results for source {source}",
                policy_name(policy)
            ));
        }

        total_hits += hits;
        total_misses += misses;
    }

    let expected_accesses = TOTAL_QUERIES as usize * FIRST_HOPS_PER_QUERY;

    if total_hits + total_misses != expected_accesses {
        return Err(format!(
            "Expected {expected_accesses} accesses, measured {}",
            total_hits + total_misses
        ));
    }

    Ok(RunStats {
        queries: TOTAL_QUERIES as usize,
        hits: total_hits,
        misses: total_misses,
    })
}

fn main() -> Result<(), String> {
    let policies = [
        EvictionPolicy::Lru,
        EvictionPolicy::Fifo,
        EvictionPolicy::Lfu,
    ];

    let mut csv_rows = vec![
        "policy,mode,capacity_per_shard,total_capacity,total_queries,\
         total_accesses,cache_hits,cache_misses,hit_rate_percent"
            .replace(' ', ""),
    ];

    println!(
        "{:<6} {:<7} {:>10} {:>10} {:>10} {:>10}",
        "Policy", "Mode", "Capacity", "Hits", "Misses", "Hit rate"
    );

    println!("{}", "-".repeat(64));

    for policy in policies {
        for capacity_per_shard in CAPACITIES_PER_SHARD {
            for warmed in [false, true] {
                let stats = run_queries(capacity_per_shard, policy, warmed)?;

                let mode = if warmed { "warmed" } else { "cold" };
                let total_capacity = capacity_per_shard * SHARD_COUNT;

                println!(
                    "{:<6} {:<7} {:>10} {:>10} {:>10} {:>9.2}%",
                    policy_name(policy),
                    mode,
                    total_capacity,
                    stats.hits,
                    stats.misses,
                    stats.hit_rate_percent(),
                );

                csv_rows.push(format!(
                    "{},{},{},{},{},{},{},{},{:.4}",
                    policy_name(policy),
                    mode,
                    capacity_per_shard,
                    total_capacity,
                    stats.queries,
                    stats.total_accesses(),
                    stats.hits,
                    stats.misses,
                    stats.hit_rate_percent(),
                ));
            }
        }
    }

    fs::create_dir_all("results")
        .map_err(|error| format!("Failed to create results directory: {error}"))?;

    fs::write(
        "results/cache_policy_benchmark.csv",
        csv_rows.join("\n") + "\n",
    )
    .map_err(|error| format!("Failed to write CSV: {error}"))?;

    println!();
    println!("All two-hop query results verified.");
    println!("Saved results/cache_policy_benchmark.csv");

    Ok(())
}
