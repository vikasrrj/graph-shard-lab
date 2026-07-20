#[derive(Debug, PartialEq, Eq)]
pub struct BalancedAssignment {
    // Index = community ID, value = assigned shard ID.
    pub community_to_shard: Vec<usize>,

    // Final number of users assigned to every shard.
    pub users_per_shard: Vec<u64>,
}

pub fn assign_communities_balanced(
    community_sizes: &[u64],
    shard_count: usize,
) -> Result<BalancedAssignment, String> {
    if shard_count == 0 {
        return Err("Shard count must be greater than zero".to_string());
    }

    if community_sizes.is_empty() {
        return Err("At least one community is required".to_string());
    }

    if community_sizes.contains(&0) {
        return Err("Community sizes must be greater than zero".to_string());
    }

    /*
    Store community IDs, then sort them from largest community
    to smallest community.
    */
    let mut community_ids: Vec<usize> = (0..community_sizes.len()).collect();

    community_ids.sort_by(|left, right| {
        community_sizes[*right]
            .cmp(&community_sizes[*left])
            .then_with(|| left.cmp(right))
    });

    let mut community_to_shard = vec![0; community_sizes.len()];

    let mut users_per_shard = vec![0_u64; shard_count];

    for community_id in community_ids {
        /*
        Find the shard currently holding the fewest users.

        If two shards have the same load, choose the lower shard ID
        so results stay deterministic.
        */
        let shard_id = users_per_shard
            .iter()
            .enumerate()
            .min_by_key(|(shard_id, load)| (**load, *shard_id))
            .map(|(shard_id, _)| shard_id)
            .expect("At least one shard exists");

        community_to_shard[community_id] = shard_id;

        users_per_shard[shard_id] += community_sizes[community_id];
    }

    Ok(BalancedAssignment {
        community_to_shard,
        users_per_shard,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balances_uneven_communities() {
        let community_sizes = [4_000, 2_500, 1_500, 1_000, 1_000];

        let assignment = assign_communities_balanced(&community_sizes, 4).unwrap();

        assert_eq!(assignment.community_to_shard, vec![0, 1, 2, 3, 3]);

        assert_eq!(assignment.users_per_shard, vec![4_000, 2_500, 1_500, 2_000]);
    }

    #[test]
    fn balances_equal_communities() {
        let community_sizes = [1_000, 1_000, 1_000, 1_000];

        let assignment = assign_communities_balanced(&community_sizes, 4).unwrap();

        assert_eq!(assignment.users_per_shard, vec![1_000, 1_000, 1_000, 1_000]);
    }

    #[test]
    fn rejects_invalid_input() {
        assert!(assign_communities_balanced(&[1_000], 0).is_err());

        assert!(assign_communities_balanced(&[], 4).is_err());

        assert!(assign_communities_balanced(&[1_000, 0], 4).is_err());
    }
}
