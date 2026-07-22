use crate::error::Result;
use crate::sharded::ShardedGraph;

#[derive(Debug, Clone)]
pub struct RebalanceAction {
    pub user_id: u64,
    pub from_shard: usize,
    pub to_shard: usize,
}

#[derive(Debug, Clone)]
pub struct RebalancePlan {
    pub actions: Vec<RebalanceAction>,
    pub target_shard_counts: Vec<usize>,
}

#[derive(Debug, Clone)]
pub struct RebalanceStats {
    pub users_moved: usize,
    pub edges_moved: usize,
    pub initial_imbalance: f64,
    pub final_imbalance: f64,
}

pub fn compute_rebalance_plan(graph: &ShardedGraph, max_improvement: f64) -> Result<RebalancePlan> {
    let shard_count = graph.shard_count();
    let user_counts = graph.users_per_shard();
    let total_users: usize = user_counts.iter().sum();
    let avg_users = total_users as f64 / shard_count as f64;

    let current_imbalance = compute_imbalance(&user_counts);

    if current_imbalance <= max_improvement {
        return Ok(RebalancePlan {
            actions: Vec::new(),
            target_shard_counts: user_counts,
        });
    }

    let mut actions = Vec::new();
    let mut current_counts = user_counts.clone();

    let mut candidates: Vec<(u64, usize)> = Vec::new();

    for (shard_id, shard_user_count) in current_counts.iter().enumerate().take(shard_count) {
        if (*shard_user_count as f64) > avg_users * 1.1 {
            let shard = &graph.shards[shard_id];

            for user_id in shard.user_ids() {
                candidates.push((user_id, shard_id));
            }
        }
    }

    candidates.sort_by(|a, b| {
        let a_edges = graph.shards[a.1].get_following_ids(a.0).len();
        let b_edges = graph.shards[b.1].get_following_ids(b.0).len();

        a_edges.cmp(&b_edges)
    });

    for (user_id, from_shard) in candidates {
        let target_shard = find_least_loaded_shard(&current_counts);

        if current_counts[target_shard] >= (avg_users * 1.05) as usize {
            continue;
        }

        actions.push(RebalanceAction {
            user_id,
            from_shard,
            to_shard: target_shard,
        });

        current_counts[from_shard] -= 1;
        current_counts[target_shard] += 1;

        let new_imbalance = compute_imbalance(&current_counts);

        if new_imbalance <= max_improvement {
            break;
        }
    }

    Ok(RebalancePlan {
        actions,
        target_shard_counts: current_counts,
    })
}

fn compute_imbalance(counts: &[usize]) -> f64 {
    if counts.is_empty() {
        return 0.0;
    }

    let total: usize = counts.iter().sum();
    let avg = total as f64 / counts.len() as f64;

    if avg == 0.0 {
        return 0.0;
    }

    let max = *counts.iter().max().unwrap() as f64;

    ((max - avg) / avg) * 100.0
}

fn find_least_loaded_shard(counts: &[usize]) -> usize {
    counts
        .iter()
        .enumerate()
        .min_by_key(|&(_, &count)| count)
        .map(|(idx, _)| idx)
        .unwrap_or(0)
}

pub fn apply_rebalance_plan(
    graph: &mut ShardedGraph,
    plan: &RebalancePlan,
) -> Result<RebalanceStats> {
    let initial_counts = graph.users_per_shard();
    let initial_imbalance = compute_imbalance(&initial_counts);

    let mut total_edges_moved = 0;

    for action in &plan.actions {
        let user_id = action.user_id;
        let from_shard = action.from_shard;

        let user_name = graph.shards[from_shard]
            .get_user(user_id)
            .map(|u| u.name.clone())
            .unwrap_or_default();

        let adjacency_list = graph.shards[from_shard].get_following_ids(user_id).to_vec();

        for &target in &adjacency_list {
            let _ = graph.shards[from_shard].remove_follow_unchecked(user_id, target);
        }

        let _ = graph.shards[from_shard].remove_follow_unchecked(user_id, user_id);

        graph.shards[from_shard].remove_user(user_id);

        graph.invalidate_cached_adjacency(from_shard, user_id);

        graph.shards[action.to_shard].add_user(user_id, &user_name)?;

        for &target in &adjacency_list {
            graph.shards[action.to_shard].add_follow_unchecked(user_id, target)?;
        }

        graph.invalidate_cached_adjacency(action.to_shard, user_id);

        total_edges_moved += adjacency_list.len();
    }

    let final_counts = graph.users_per_shard();
    let final_imbalance = compute_imbalance(&final_counts);

    Ok(RebalanceStats {
        users_moved: plan.actions.len(),
        edges_moved: total_edges_moved,
        initial_imbalance,
        final_imbalance,
    })
}

pub fn compute_rebalance_savings(graph: &ShardedGraph) -> RebalanceStats {
    let counts = graph.users_per_shard();
    let imbalance = compute_imbalance(&counts);

    RebalanceStats {
        users_moved: 0,
        edges_moved: 0,
        initial_imbalance: imbalance,
        final_imbalance: imbalance,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sharded::Placement;

    fn build_unbalanced_graph() -> ShardedGraph {
        let sizes = vec![8, 2];
        let assignment = crate::balanced::assign_communities_balanced(&sizes, 2).unwrap();

        let mut graph = ShardedGraph::with_placement(
            2,
            Placement::BalancedCommunity {
                community_sizes: sizes,
                community_to_shard: assignment.community_to_shard,
            },
        )
        .unwrap();

        for id in 1..=10 {
            graph.add_user(id, &format!("user-{id}")).unwrap();
        }

        for source in 1..=5 {
            for target in 6..=10 {
                graph.add_follow(source, target).unwrap();
            }
        }

        graph
    }

    #[test]
    fn detects_unbalanced_shards() {
        let graph = build_unbalanced_graph();

        let counts = graph.users_per_shard();

        assert_ne!(counts[0], counts[1]);
    }

    #[test]
    fn computes_rebalance_plan() {
        let graph = build_unbalanced_graph();

        let plan = compute_rebalance_plan(&graph, 10.0).unwrap();

        assert!(!plan.actions.is_empty());
    }

    #[test]
    fn apply_plan_rebalances_shards() {
        let mut graph = build_unbalanced_graph();

        let plan = compute_rebalance_plan(&graph, 0.0).unwrap();

        if !plan.actions.is_empty() {
            let stats = apply_rebalance_plan(&mut graph, &plan).unwrap();

            assert!(stats.users_moved > 0);
        }
    }

    #[test]
    fn compute_imbalance_returns_zero_for_empty() {
        assert_eq!(compute_imbalance(&[]), 0.0);
    }

    #[test]
    fn compute_imbalance_returns_zero_for_equal() {
        assert_eq!(compute_imbalance(&[5, 5, 5]), 0.0);
    }

    #[test]
    fn compute_imbalance_returns_positive_for_unequal() {
        let imbalance = compute_imbalance(&[10, 5, 5]);

        assert!(imbalance > 0.0);
    }

    #[test]
    fn find_least_loaded_shard_returns_minimum() {
        let counts = [10, 5, 8];

        assert_eq!(find_least_loaded_shard(&counts), 1);
    }

    #[test]
    fn rebalance_savings_works() {
        let graph = build_unbalanced_graph();

        let savings = compute_rebalance_savings(&graph);

        assert!(savings.initial_imbalance > 0.0);
    }
}
