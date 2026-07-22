use std::collections::{BTreeMap, HashSet};

use crate::{Graph, User, balanced::assign_communities_balanced, cache::AdjacencyLruCache};
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
    pub shard_requests: usize,
}

#[derive(Debug)]
pub struct CachedQueryResult {
    pub user_ids: Vec<u64>,
    pub shards_touched: usize,
    pub cross_shard_hops: usize,
    pub shard_requests: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
}

pub struct ShardedGraph {
    shards: Vec<Graph>,

    // None means caching is disabled.
    //
    // Some(caches) contains exactly one independent cache
    // for each logical shard.
    caches: Option<Vec<AdjacencyLruCache>>,

    placement: Placement,
}

impl ShardedGraph {
    pub fn new(shard_count: usize) -> Result<Self, String> {
        Self::with_placement(shard_count, Placement::Hash)
    }

    pub fn warm_cache_for_user(&mut self, user_id: u64) -> Result<(), String> {
        let shard_id = self
            .try_shard_for(user_id)
            .ok_or_else(|| format!("Cannot find shard for user {user_id}"))?;

        if self.shards[shard_id].get_user(user_id).is_none() {
            return Err(format!("User {user_id} does not exist"));
        }

        let adjacency_list = self.shards[shard_id].get_following_ids(user_id).to_vec();

        let caches = self
            .caches
            .as_mut()
            .ok_or_else(|| "Caching is disabled for this ShardedGraph".to_string())?;

        caches[shard_id].insert(user_id, adjacency_list);

        Ok(())
    }

    pub fn new_with_cache(
        shard_count: usize,
        cache_capacity_per_shard: usize,
    ) -> Result<Self, String> {
        Self::with_placement_and_cache(shard_count, Placement::Hash, cache_capacity_per_shard)
    }

    pub fn new_with_byte_bounded_cache(
        shard_count: usize,
        cache_capacity_per_shard: usize,
        cache_byte_capacity_per_shard: usize,
    ) -> Result<Self, String> {
        Self::with_placement_and_byte_bounded_cache(
            shard_count,
            Placement::Hash,
            cache_capacity_per_shard,
            cache_byte_capacity_per_shard,
        )
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

        Ok(Self {
            shards,
            caches: None,
            placement,
        })
    }
    pub fn with_placement_and_cache(
        shard_count: usize,
        placement: Placement,
        cache_capacity_per_shard: usize,
    ) -> Result<Self, String> {
        let mut graph = Self::with_placement(shard_count, placement)?;

        let mut caches = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            caches.push(AdjacencyLruCache::new(cache_capacity_per_shard)?);
        }

        graph.caches = Some(caches);

        Ok(graph)
    }

    pub fn with_placement_and_byte_bounded_cache(
        shard_count: usize,
        placement: Placement,
        cache_capacity_per_shard: usize,
        cache_byte_capacity_per_shard: usize,
    ) -> Result<Self, String> {
        let mut graph = Self::with_placement(shard_count, placement)?;

        let mut caches = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            caches.push(AdjacencyLruCache::new_with_byte_capacity(
                cache_capacity_per_shard,
                cache_byte_capacity_per_shard,
            )?);
        }

        graph.caches = Some(caches);

        Ok(graph)
    }
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

        let edge_already_exists = self.shards[source_shard]
            .get_following_ids(source)
            .contains(&target);

        self.shards[source_shard].add_follow_unchecked(source, target)?;

        if !edge_already_exists {
            self.invalidate_cached_adjacency(source_shard, source);
        }

        Ok(())
    }

    pub fn remove_follow(&mut self, source: u64, target: u64) -> Result<bool, String> {
        if self.get_user(source).is_none() {
            return Err(format!("Source user {source} does not exist"));
        }

        if self.get_user(target).is_none() {
            return Err(format!("Target user {target} does not exist"));
        }

        let source_shard = self
            .try_shard_for(source)
            .ok_or_else(|| format!("Cannot find shard for user {source}"))?;

        let removed = self.shards[source_shard].remove_follow_unchecked(source, target)?;

        if removed {
            self.invalidate_cached_adjacency(source_shard, source);
        }

        Ok(removed)
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
    fn invalidate_cached_adjacency(&mut self, shard_id: usize, user_id: u64) {
        if let Some(caches) = self.caches.as_mut() {
            caches[shard_id].invalidate(user_id);
        }
    }

    pub fn get_two_hop_with_stats(&self, source: u64) -> QueryResult {
        let Some(source_shard) = self.try_shard_for(source) else {
            return QueryResult {
                user_ids: Vec::new(),
                shards_touched: 0,
                cross_shard_hops: 0,
                shard_requests: 0,
            };
        };

        let mut user_ids = Vec::new();
        let mut seen_users = HashSet::new();
        let mut touched_shards = HashSet::new();
        let mut cross_shard_hops = 0;
        let mut shard_requests = 1;

        touched_shards.insert(source_shard);

        for first_hop in self.get_following_ids(source) {
            let Some(first_hop_shard) = self.try_shard_for(*first_hop) else {
                continue;
            };

            touched_shards.insert(first_hop_shard);

            if first_hop_shard != source_shard {
                cross_shard_hops += 1;
            }

            shard_requests += 1;

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
            shard_requests,
        }
    }

    pub fn get_two_hop_with_cache_stats(
        &mut self,
        source: u64,
    ) -> Result<CachedQueryResult, String> {
        if self.caches.is_none() {
            return Err("Caching is disabled for this ShardedGraph".to_string());
        }

        let Some(source_shard) = self.try_shard_for(source) else {
            return Ok(CachedQueryResult {
                user_ids: Vec::new(),
                shards_touched: 0,
                cross_shard_hops: 0,
                shard_requests: 0,
                cache_hits: 0,
                cache_misses: 0,
            });
        };

        /*
        Copy this list so we do not keep an immutable borrow
        of self while mutating the caches.
        */
        let first_hops = self.get_following_ids(source).to_vec();

        let mut user_ids = Vec::new();
        let mut seen_users = HashSet::new();
        let mut touched_shards = HashSet::new();

        let mut cross_shard_hops = 0;
        let mut shard_requests = 1;

        let mut cache_hits = 0;
        let mut cache_misses = 0;

        touched_shards.insert(source_shard);

        for first_hop in first_hops {
            let Some(first_hop_shard) = self.try_shard_for(first_hop) else {
                continue;
            };

            touched_shards.insert(first_hop_shard);

            if first_hop_shard != source_shard {
                cross_shard_hops += 1;
            }

            /*
            The query still contacts the owning shard.

            A cache hit avoids reading the shard's main Graph,
            but it does not remove the logical shard request.
            */
            shard_requests += 1;

            let cached_adjacency_list = {
                let caches = self.caches.as_mut().expect("Caching was checked above");

                caches[first_hop_shard].get(first_hop)
            };

            let second_hops = match cached_adjacency_list {
                Some(adjacency_list) => {
                    cache_hits += 1;
                    adjacency_list
                }

                None => {
                    cache_misses += 1;

                    let adjacency_list = self.shards[first_hop_shard]
                        .get_following_ids(first_hop)
                        .to_vec();

                    {
                        let caches = self.caches.as_mut().expect("Caching was checked above");

                        caches[first_hop_shard].insert(first_hop, adjacency_list.clone());
                    }

                    adjacency_list
                }
            };

            for second_hop in second_hops {
                let Some(second_hop_shard) = self.try_shard_for(second_hop) else {
                    continue;
                };

                touched_shards.insert(second_hop_shard);

                if second_hop_shard != first_hop_shard {
                    cross_shard_hops += 1;
                }

                if second_hop != source && seen_users.insert(second_hop) {
                    user_ids.push(second_hop);
                }
            }
        }

        Ok(CachedQueryResult {
            user_ids,
            shards_touched: touched_shards.len(),
            cross_shard_hops,
            shard_requests,
            cache_hits,
            cache_misses,
        })
    }

    pub fn get_two_hop_batched_with_stats(&self, source: u64) -> QueryResult {
        let Some(source_shard) = self.try_shard_for(source) else {
            return QueryResult {
                user_ids: Vec::new(),
                shards_touched: 0,
                cross_shard_hops: 0,
                shard_requests: 0,
            };
        };

        let mut user_ids = Vec::new();
        let mut seen_users = HashSet::new();
        let mut touched_shards = HashSet::new();
        let mut cross_shard_hops = 0;

        touched_shards.insert(source_shard);

        let mut first_hops_by_shard: BTreeMap<usize, Vec<u64>> = BTreeMap::new();

        for first_hop in self.get_following_ids(source) {
            let Some(first_hop_shard) = self.try_shard_for(*first_hop) else {
                continue;
            };

            touched_shards.insert(first_hop_shard);

            if first_hop_shard != source_shard {
                cross_shard_hops += 1;
            }

            first_hops_by_shard
                .entry(first_hop_shard)
                .or_default()
                .push(*first_hop);
        }

        let shard_requests = 1 + first_hops_by_shard.len();

        for (first_hop_shard, first_hops) in first_hops_by_shard {
            for first_hop in first_hops {
                for second_hop in self.get_following_ids(first_hop) {
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
        }

        QueryResult {
            user_ids,
            shards_touched: touched_shards.len(),
            cross_shard_hops,
            shard_requests,
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
    fn creates_byte_bounded_cache_for_every_shard() {
        let graph = ShardedGraph::new_with_byte_bounded_cache(4, 100, 4096).unwrap();

        let caches = graph.caches.as_ref().unwrap();

        assert_eq!(caches.len(), 4);

        for cache in caches {
            assert_eq!(cache.capacity(), 100);
            assert_eq!(cache.byte_capacity(), Some(4096));
            assert_eq!(cache.current_bytes(), 0);
        }
    }

    #[test]
    fn byte_bounded_cache_rejects_zero_byte_capacity() {
        assert!(ShardedGraph::new_with_byte_bounded_cache(4, 100, 0).is_err());
    }

    #[test]
    fn oversized_adjacency_is_not_cached_by_sharded_graph() {
        let mut graph = ShardedGraph::new_with_byte_bounded_cache(2, 10, 1).unwrap();

        graph.add_user(1, "Alice").unwrap();
        graph.add_user(2, "Bob").unwrap();
        graph.add_user(3, "Charlie").unwrap();

        graph.add_follow(1, 2).unwrap();
        graph.add_follow(2, 3).unwrap();

        let first = graph.get_two_hop_with_cache_stats(1).unwrap();

        assert_eq!(first.user_ids, vec![3]);
        assert_eq!(first.cache_hits, 0);
        assert_eq!(first.cache_misses, 1);

        // The adjacency list cannot fit into a one-byte cache,
        // so the second query must miss again.
        let second = graph.get_two_hop_with_cache_stats(1).unwrap();

        assert_eq!(second.user_ids, vec![3]);
        assert_eq!(second.cache_hits, 0);
        assert_eq!(second.cache_misses, 1);
    }

    #[test]
    fn warming_preloads_real_adjacency_data() {
        let mut graph = build_cached_sample_graph();

        graph.warm_cache_for_user(2).unwrap();

        let result = graph.get_two_hop_with_cache_stats(1).unwrap();

        // User 1 follows users 2 and 3.
        // User 2 was warmed, while user 3 was not.
        assert_eq!(result.cache_hits, 1);
        assert_eq!(result.cache_misses, 1);
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

    fn build_cached_sample_graph() -> ShardedGraph {
        let mut graph = ShardedGraph::new_with_cache(2, 10).unwrap();

        for id in 1..=4 {
            graph.add_user(id, &format!("user-{id}")).unwrap();
        }

        graph.add_follow(1, 2).unwrap();
        graph.add_follow(1, 3).unwrap();
        graph.add_follow(2, 3).unwrap();
        graph.add_follow(3, 4).unwrap();

        graph
    }

    #[test]
    fn cached_sharded_query_matches_uncached_query() {
        let mut graph = build_cached_sample_graph();

        let mut expected = graph.get_two_hop_with_stats(1).user_ids;

        let cached = graph.get_two_hop_with_cache_stats(1).unwrap();

        let mut actual = cached.user_ids;

        expected.sort_unstable();
        actual.sort_unstable();

        assert_eq!(actual, expected);
    }

    #[test]
    fn cached_sharded_query_misses_then_hits() {
        let mut graph = build_cached_sample_graph();

        let first = graph.get_two_hop_with_cache_stats(1).unwrap();

        assert_eq!(first.cache_hits, 0);
        assert_eq!(first.cache_misses, 2);

        let second = graph.get_two_hop_with_cache_stats(1).unwrap();

        assert_eq!(second.cache_hits, 2);
        assert_eq!(second.cache_misses, 0);
    }

    #[test]
    fn cached_query_rejects_graph_without_caches() {
        let mut graph = ShardedGraph::new(2).unwrap();

        assert!(graph.get_two_hop_with_cache_stats(1).is_err());
    }

    #[test]
    fn existing_constructor_keeps_caching_disabled() {
        let graph = ShardedGraph::new(4).unwrap();

        assert!(graph.caches.is_none());
    }

    #[test]
    fn cached_constructor_creates_one_cache_per_shard() {
        let graph = ShardedGraph::new_with_cache(4, 100).unwrap();

        let caches = graph.caches.as_ref().unwrap();

        assert_eq!(caches.len(), 4);

        for cache in caches {
            assert_eq!(cache.capacity(), 100);
            assert!(cache.is_empty());
        }
    }

    #[test]
    fn cached_constructor_rejects_zero_capacity() {
        assert!(ShardedGraph::new_with_cache(4, 0).is_err());
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
        assert_eq!(result.shard_requests, 2);
    }

    #[test]
    fn edge_mutations_invalidate_cached_adjacency_lists() {
        let mut graph = ShardedGraph::new_with_cache(2, 10).unwrap();

        graph.add_user(1, "Alice").unwrap();
        graph.add_user(2, "Bob").unwrap();
        graph.add_user(3, "Charlie").unwrap();
        graph.add_user(4, "Diana").unwrap();

        graph.add_follow(1, 2).unwrap();
        graph.add_follow(2, 3).unwrap();

        // First query loads User 2's adjacency list into its shard cache.
        let first = graph.get_two_hop_with_cache_stats(1).unwrap();

        assert_eq!(first.user_ids, vec![3]);
        assert_eq!(first.cache_hits, 0);
        assert_eq!(first.cache_misses, 1);

        // User 2's adjacency list changes from [3] to [3, 4].
        graph.add_follow(2, 4).unwrap();

        // The old cached [3] must have been invalidated.
        let after_add = graph.get_two_hop_with_cache_stats(1).unwrap();

        assert_eq!(after_add.user_ids, vec![3, 4]);
        assert_eq!(after_add.cache_hits, 0);
        assert_eq!(after_add.cache_misses, 1);

        // The fresh [3, 4] is now cached.
        let repeated = graph.get_two_hop_with_cache_stats(1).unwrap();

        assert_eq!(repeated.user_ids, vec![3, 4]);
        assert_eq!(repeated.cache_hits, 1);
        assert_eq!(repeated.cache_misses, 0);

        // Remove 2 → 3, changing User 2's adjacency list to [4].
        let removed = graph.remove_follow(2, 3).unwrap();

        assert!(removed);

        // Removal must invalidate the cached [3, 4].
        let after_remove = graph.get_two_hop_with_cache_stats(1).unwrap();

        assert_eq!(after_remove.user_ids, vec![4]);
        assert_eq!(after_remove.cache_hits, 0);
        assert_eq!(after_remove.cache_misses, 1);
    }

    #[test]
    fn batches_first_hop_reads_by_shard() {
        let mut graph = ShardedGraph::new(4).unwrap();

        for id in [1, 2, 3, 6, 7] {
            graph.add_user(id, &format!("user-{id}")).unwrap();
        }

        graph.add_follow(1, 2).unwrap();
        graph.add_follow(1, 6).unwrap();
        graph.add_follow(2, 3).unwrap();
        graph.add_follow(6, 7).unwrap();

        let direct = graph.get_two_hop_with_stats(1);
        let batched = graph.get_two_hop_batched_with_stats(1);

        let mut direct_ids = direct.user_ids;
        let mut batched_ids = batched.user_ids;

        direct_ids.sort_unstable();
        batched_ids.sort_unstable();

        assert_eq!(direct_ids, batched_ids);
        assert_eq!(direct_ids, vec![3, 7]);

        assert_eq!(direct.shard_requests, 3);
        assert_eq!(batched.shard_requests, 2);

        assert_eq!(direct.shards_touched, batched.shards_touched);
        assert_eq!(direct.cross_shard_hops, batched.cross_shard_hops);
    }
}
