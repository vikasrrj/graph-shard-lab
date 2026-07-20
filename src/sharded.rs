use std::collections::HashSet;

use crate::{Graph, User, balanced::assign_communities_balanced};

#[derive(Debug, Clone)]
pub enum Placement {
    Hash,

    /*
    Equal-sized communities.

    Example with community_size = 4:

    Community 0: users 1-4
    Community 1: users 5-8
    Community 2: users 9-12
    */
    Community {
        community_size: u64,
    },

    /*
    Uneven communities.

    community_sizes describes how many users each community contains.

    community_to_shard tells us where every community is stored.
    */
    BalancedCommunity {
        community_sizes: Vec<u64>,
        community_to_shard: Vec<usize>,
    },
}

#[derive(Debug)]
pub struct QueryResult {
    pub user_ids: Vec<u64>,
    pub shards_touched: usize,
    pub cross_shard_hops: usize,
}

pub struct ShardedGraph {
    shards: Vec<Graph>,
    placement: Placement,
}

impl ShardedGraph {
    pub fn new(shard_count: usize) -> Result<Self, String> {
        Self::with_placement(shard_count, Placement::Hash)
    }

    /*
    This constructor calculates the balanced community assignment
    and then creates the sharded graph using that assignment.
    */
    pub fn with_balanced_communities(
        shard_count: usize,
        community_sizes: Vec<u64>,
    ) -> Result<Self, String> {
        let assignment = assign_communities_balanced(&community_sizes, shard_count)?;

        Self::with_placement(
            shard_count,
            Placement::BalancedCommunity {
                community_sizes,
                community_to_shard: assignment.community_to_shard,
            },
        )
    }

    pub fn with_placement(shard_count: usize, placement: Placement) -> Result<Self, String> {
        if shard_count == 0 {
            return Err("Shard count must be greater than zero".to_string());
        }

        match &placement {
            Placement::Hash => {}

            Placement::Community { community_size } => {
                if *community_size == 0 {
                    return Err("Community size must be greater than zero".to_string());
                }
            }

            Placement::BalancedCommunity {
                community_sizes,
                community_to_shard,
            } => {
                if community_sizes.is_empty() {
                    return Err("At least one community is required".to_string());
                }

                if community_sizes.contains(&0) {
                    return Err("Community sizes must be greater than zero".to_string());
                }

                if community_sizes.len() != community_to_shard.len() {
                    return Err("Every community must have a shard assignment".to_string());
                }

                if community_to_shard
                    .iter()
                    .any(|shard_id| *shard_id >= shard_count)
                {
                    return Err("Community assignment contains an invalid shard".to_string());
                }

                community_sizes
                    .iter()
                    .try_fold(0_u64, |total, size| total.checked_add(*size))
                    .ok_or_else(|| "Total community size is too large".to_string())?;
            }
        }

        let mut shards = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            shards.push(Graph::new());
        }

        Ok(Self { shards, placement })
    }

    /*
    Returns None when the user ID is invalid or falls outside the
    configured balanced-community ranges.
    */
    fn try_shard_for(&self, user_id: u64) -> Option<usize> {
        if user_id == 0 {
            return None;
        }

        match &self.placement {
            Placement::Hash => Some(user_id as usize % self.shards.len()),

            Placement::Community { community_size } => {
                let community_id = (user_id - 1) / community_size;

                Some(community_id as usize % self.shards.len())
            }

            Placement::BalancedCommunity {
                community_sizes,
                community_to_shard,
            } => {
                let mut final_user_id = 0_u64;

                for (community_id, community_size) in community_sizes.iter().enumerate() {
                    final_user_id = final_user_id.checked_add(*community_size)?;

                    if user_id <= final_user_id {
                        return community_to_shard.get(community_id).copied();
                    }
                }

                None
            }
        }
    }

    fn shard_for(&self, user_id: u64) -> usize {
        self.try_shard_for(user_id)
            .unwrap_or_else(|| panic!("User ID {user_id} is outside the configured placement"))
    }

    pub fn placement_for_user(&self, user_id: u64) -> usize {
        self.shard_for(user_id)
    }

    pub fn add_user(&mut self, id: u64, name: &str) -> Result<(), String> {
        if id == 0 {
            return Err("User ID must be greater than zero".to_string());
        }

        let shard_id = self
            .try_shard_for(id)
            .ok_or_else(|| format!("User {id} is outside the configured community ranges"))?;

        self.shards[shard_id].add_user(id, name)
    }

    pub fn add_follow(&mut self, source: u64, target: u64) -> Result<(), String> {
        if self.get_user(source).is_none() {
            return Err(format!("Source user {source} does not exist"));
        }

        if self.get_user(target).is_none() {
            return Err(format!("Target user {target} does not exist"));
        }

        let source_shard = self
            .try_shard_for(source)
            .ok_or_else(|| format!("Cannot find shard for user {source}"))?;

        self.shards[source_shard].add_follow_unchecked(source, target)
    }

    pub fn get_user(&self, id: u64) -> Option<&User> {
        let shard_id = self.try_shard_for(id)?;
        self.shards[shard_id].get_user(id)
    }

    pub fn get_following_ids(&self, source: u64) -> &[u64] {
        let Some(shard_id) = self.try_shard_for(source) else {
            return &[];
        };

        self.shards[shard_id].get_following_ids(source)
    }

    pub fn get_two_hop_with_stats(&self, source: u64) -> QueryResult {
        let Some(source_shard) = self.try_shard_for(source) else {
            return QueryResult {
                user_ids: Vec::new(),
                shards_touched: 0,
                cross_shard_hops: 0,
            };
        };

        let mut user_ids = Vec::new();
        let mut seen_users = HashSet::new();
        let mut touched_shards = HashSet::new();
        let mut cross_shard_hops = 0;

        touched_shards.insert(source_shard);

        for first_hop in self.get_following_ids(source) {
            let Some(first_hop_shard) = self.try_shard_for(*first_hop) else {
                continue;
            };

            touched_shards.insert(first_hop_shard);

            if first_hop_shard != source_shard {
                cross_shard_hops += 1;
            }

            for second_hop in self.get_following_ids(*first_hop) {
                let Some(second_hop_shard) = self.try_shard_for(*second_hop) else {
                    continue;
                };

                touched_shards.insert(second_hop_shard);

                if second_hop_shard != first_hop_shard {
                    cross_shard_hops += 1;
                }

                if *second_hop != source && seen_users.insert(*second_hop) {
                    user_ids.push(*second_hop);
                }
            }
        }

        QueryResult {
            user_ids,
            shards_touched: touched_shards.len(),
            cross_shard_hops,
        }
    }

    pub fn get_two_hop_ids(&self, source: u64) -> Vec<u64> {
        self.get_two_hop_with_stats(source).user_ids
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn user_count(&self) -> usize {
        self.shards.iter().map(Graph::user_count).sum()
    }

    pub fn edge_count(&self) -> usize {
        self.shards.iter().map(Graph::edge_count).sum()
    }

    pub fn users_per_shard(&self) -> Vec<usize> {
        self.shards.iter().map(Graph::user_count).collect()
    }

    pub fn edges_per_shard(&self) -> Vec<usize> {
        self.shards.iter().map(Graph::edge_count).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_placement_balances_users() {
        let mut graph = ShardedGraph::new(4).unwrap();

        for id in 1..=8 {
            graph.add_user(id, &format!("user-{id}")).unwrap();
        }

        assert_eq!(graph.users_per_shard(), vec![2, 2, 2, 2]);
    }

    #[test]
    fn community_placement_keeps_communities_together() {
        let mut graph =
            ShardedGraph::with_placement(2, Placement::Community { community_size: 4 }).unwrap();

        for id in 1..=8 {
            graph.add_user(id, &format!("user-{id}")).unwrap();
        }

        assert_eq!(graph.users_per_shard(), vec![4, 4]);

        assert_eq!(graph.shard_for(1), graph.shard_for(4));

        assert_eq!(graph.shard_for(5), graph.shard_for(8));

        assert_ne!(graph.shard_for(1), graph.shard_for(5));
    }

    #[test]
    fn balanced_placement_handles_uneven_communities() {
        /*
        Communities:

        users 1-4  = size 4
        users 5-7  = size 3
        users 8-9  = size 2
        user 10    = size 1

        Balanced assignment across three shards:

        shard 0 = 4 users
        shard 1 = 3 users
        shard 2 = 3 users
        */
        let mut graph = ShardedGraph::with_balanced_communities(3, vec![4, 3, 2, 1]).unwrap();

        for id in 1..=10 {
            graph.add_user(id, &format!("user-{id}")).unwrap();
        }

        assert_eq!(graph.users_per_shard(), vec![4, 3, 3]);

        assert_eq!(graph.shard_for(1), graph.shard_for(4));

        assert_eq!(graph.shard_for(5), graph.shard_for(7));

        assert_eq!(graph.shard_for(8), graph.shard_for(10));

        assert!(graph.add_user(11, "outside-range").is_err());
    }

    #[test]
    fn supports_cross_shard_edges() {
        let mut graph = ShardedGraph::new(4).unwrap();

        graph.add_user(1, "Alice").unwrap();
        graph.add_user(2, "Bob").unwrap();

        graph.add_follow(1, 2).unwrap();

        assert_eq!(graph.get_following_ids(1), &[2]);
    }

    #[test]
    fn supports_cross_shard_two_hop_queries() {
        let mut graph = ShardedGraph::new(4).unwrap();

        graph.add_user(1, "Alice").unwrap();
        graph.add_user(2, "Bob").unwrap();
        graph.add_user(3, "Charlie").unwrap();
        graph.add_user(4, "Diana").unwrap();

        graph.add_follow(1, 2).unwrap();
        graph.add_follow(2, 3).unwrap();
        graph.add_follow(2, 4).unwrap();

        assert_eq!(graph.get_two_hop_ids(1), vec![3, 4]);
    }

    #[test]
    fn records_query_statistics() {
        let mut graph = ShardedGraph::new(4).unwrap();

        graph.add_user(1, "Alice").unwrap();
        graph.add_user(2, "Bob").unwrap();
        graph.add_user(3, "Charlie").unwrap();

        graph.add_follow(1, 2).unwrap();
        graph.add_follow(2, 3).unwrap();

        let result = graph.get_two_hop_with_stats(1);

        assert_eq!(result.user_ids, vec![3]);
        assert_eq!(result.shards_touched, 3);
        assert_eq!(result.cross_shard_hops, 2);
    }
}
