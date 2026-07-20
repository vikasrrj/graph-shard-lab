use std::collections::HashSet;

use rand::{Rng, SeedableRng, rngs::StdRng};

use crate::workload::CommunityWorkload;

pub fn generate_uneven_community_workload(
    community_sizes: &[u64],
    edges_per_user: u64,
    local_edges_per_user: u64,
    seed: u64,
) -> Result<CommunityWorkload, String> {
    if community_sizes.is_empty() {
        return Err("At least one community is required".to_string());
    }

    if community_sizes.contains(&0) {
        return Err("Community sizes must be greater than zero".to_string());
    }

    if local_edges_per_user > edges_per_user {
        return Err("Local edges cannot exceed total edges per user".to_string());
    }

    let user_count = community_sizes
        .iter()
        .try_fold(0_u64, |total, size| total.checked_add(*size))
        .ok_or_else(|| "Total user count is too large".to_string())?;

    let external_edges_per_user = edges_per_user - local_edges_per_user;

    for &community_size in community_sizes {
        let possible_local_targets = community_size.saturating_sub(1);

        let possible_external_targets = user_count - community_size;

        if local_edges_per_user > possible_local_targets {
            return Err(format!(
                "A community of size {community_size} cannot provide \
                 {local_edges_per_user} unique local targets per user"
            ));
        }

        if external_edges_per_user > possible_external_targets {
            return Err(format!(
                "A community of size {community_size} cannot provide \
                 {external_edges_per_user} unique external targets per user"
            ));
        }
    }

    let mut ranges = Vec::with_capacity(community_sizes.len());

    let mut next_start = 1_u64;

    for &community_size in community_sizes {
        let end = next_start + community_size - 1;

        ranges.push((next_start, end));

        next_start = end + 1;
    }

    let mut rng = StdRng::seed_from_u64(seed);

    let mut edges = Vec::new();

    for &(community_start, community_end) in &ranges {
        for source in community_start..=community_end {
            let mut selected_targets = HashSet::new();

            let mut local_edges_added = 0_u64;

            while local_edges_added < local_edges_per_user {
                let target = rng.gen_range(community_start..=community_end);

                if target != source && selected_targets.insert(target) {
                    edges.push((source, target));
                    local_edges_added += 1;
                }
            }

            let mut external_edges_added = 0_u64;

            while external_edges_added < external_edges_per_user {
                let target = rng.gen_range(1..=user_count);

                let target_is_external = target < community_start || target > community_end;

                if target_is_external && selected_targets.insert(target) {
                    edges.push((source, target));
                    external_edges_added += 1;
                }
            }
        }
    }

    Ok(CommunityWorkload {
        user_count,
        community_count: community_sizes.len() as u64,
        edges,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_expected_user_and_edge_counts() {
        let sizes = [4, 3, 2, 1];

        let workload = generate_uneven_community_workload(&sizes, 2, 0, 42).unwrap();

        assert_eq!(workload.user_count, 10);
        assert_eq!(workload.community_count, 4);
        assert_eq!(workload.edges.len(), 20);
    }

    #[test]
    fn generates_requested_local_edges() {
        let sizes = [4, 3, 3];

        let workload = generate_uneven_community_workload(&sizes, 2, 1, 42).unwrap();

        for source in 1..=workload.user_count {
            let source_community = community_for_user(source, &sizes);

            let source_edges: Vec<_> = workload
                .edges
                .iter()
                .filter(|(edge_source, _)| *edge_source == source)
                .collect();

            assert_eq!(source_edges.len(), 2);

            let local_count = source_edges
                .iter()
                .filter(|(_, target)| community_for_user(*target, &sizes) == source_community)
                .count();

            assert_eq!(local_count, 1);
        }
    }

    #[test]
    fn same_seed_produces_same_workload() {
        let sizes = [4, 3, 3];

        let first = generate_uneven_community_workload(&sizes, 2, 1, 42).unwrap();

        let second = generate_uneven_community_workload(&sizes, 2, 1, 42).unwrap();

        assert_eq!(first.edges, second.edges);
    }

    #[test]
    fn rejects_invalid_workloads() {
        assert!(generate_uneven_community_workload(&[], 2, 1, 42,).is_err());

        assert!(generate_uneven_community_workload(&[4, 0, 3], 2, 1, 42,).is_err());

        assert!(generate_uneven_community_workload(&[4, 3], 2, 3, 42,).is_err());

        assert!(generate_uneven_community_workload(&[2, 1], 3, 1, 42,).is_err());
    }

    fn community_for_user(user_id: u64, community_sizes: &[u64]) -> usize {
        let mut final_user_id = 0_u64;

        for (community_id, community_size) in community_sizes.iter().enumerate() {
            final_user_id += community_size;

            if user_id <= final_user_id {
                return community_id;
            }
        }

        panic!("User ID is outside community ranges");
    }
}
