use crate::cache::{AdjacencyLruCache, EvictionPolicy};
use crate::error::{GraphError, Result};
use std::collections::{BTreeMap, HashSet};

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
    pub(crate) shards: Vec<Graph>,

    pub(crate) caches: Option<Vec<AdjacencyLruCache>>,

    // One access-frequency map per logical shard.
    //
    // user ID -> number of observed adjacency reads
    observed_adjacency_accesses: Vec<std::collections::HashMap<u64, u64>>,

    placement: Placement,

    pub(crate) replicated_users: HashSet<u64>,
}

impl ShardedGraph {
    pub fn new(shard_count: usize) -> Result<Self> {
        Self::with_placement(shard_count, Placement::Hash)
    }

    pub fn warm_cache_for_user(&mut self, user_id: u64) -> Result<()> {
        let shard_id = self
            .try_shard_for(user_id)
            .ok_or(GraphError::ShardNotFound(user_id))?;

        if self.shards[shard_id].get_user(user_id).is_none() {
            return Err(GraphError::UserNotFound(user_id));
        }

        let adjacency_list = self.shards[shard_id].get_following_ids(user_id).to_vec();

        let caches = self.caches.as_mut().ok_or(GraphError::CachingDisabled)?;

        caches[shard_id].insert(user_id, adjacency_list);

        Ok(())
    }

    pub fn observed_adjacency_access_count(&self, user_id: u64) -> u64 {
        let Some(shard_id) = self.try_shard_for(user_id) else {
            return 0;
        };

        self.observed_adjacency_accesses[shard_id]
            .get(&user_id)
            .copied()
            .unwrap_or(0)
    }

    pub fn clear_observed_adjacency_accesses(&mut self) {
        for accesses in &mut self.observed_adjacency_accesses {
            accesses.clear();
        }
    }

    pub fn warm_cache_from_observed_traffic(
        &mut self,
        max_entries_per_shard: usize,
    ) -> Result<usize> {
        if max_entries_per_shard == 0 {
            return Err(GraphError::WarmEntryLimitZero);
        }

        let cache_limits: Vec<usize> = self
            .caches
            .as_ref()
            .ok_or(GraphError::CachingDisabled)?
            .iter()
            .map(|cache| cache.capacity().min(max_entries_per_shard))
            .collect();

        /*
        Build the plan before mutably borrowing the caches.

        Each shard ranks users by:
        1. highest observed access count
        2. lowest user ID for deterministic ties
        */
        let mut warm_plan = Vec::with_capacity(self.shards.len());

        for (shard_id, cache_limit) in cache_limits.iter().enumerate().take(self.shards.len()) {
            let mut ranked_users: Vec<(u64, u64)> = self.observed_adjacency_accesses[shard_id]
                .iter()
                .map(|(&user_id, &access_count)| (user_id, access_count))
                .collect();

            ranked_users.sort_by(|(left_user, left_count), (right_user, right_count)| {
                right_count
                    .cmp(left_count)
                    .then_with(|| left_user.cmp(right_user))
            });

            let mut selected_entries: Vec<(u64, Vec<u64>)> = ranked_users
                .into_iter()
                .take(*cache_limit)
                .map(|(user_id, _)| {
                    let adjacency_list = self.shards[shard_id].get_following_ids(user_id).to_vec();

                    (user_id, adjacency_list)
                })
                .collect();

            /*
            Insert colder selected entries first and hotter entries last.

            This makes the hottest entries the newest entries if a byte
            limit forces eviction during warming.
            */
            selected_entries.reverse();

            warm_plan.push(selected_entries);
        }

        let caches = self.caches.as_mut().expect("Caching was checked above");

        for cache in caches.iter_mut() {
            cache.clear();
        }

        for (shard_id, entries) in warm_plan.into_iter().enumerate() {
            for (user_id, adjacency_list) in entries {
                caches[shard_id].insert(user_id, adjacency_list);
            }
        }

        let warmed_entry_count = caches.iter().map(|cache| cache.len()).sum();

        Ok(warmed_entry_count)
    }

    pub fn new_with_cache(shard_count: usize, cache_capacity_per_shard: usize) -> Result<Self> {
        Self::with_placement_and_cache(shard_count, Placement::Hash, cache_capacity_per_shard)
    }

    pub fn new_with_byte_bounded_cache(
        shard_count: usize,
        cache_capacity_per_shard: usize,
        cache_byte_capacity_per_shard: usize,
    ) -> Result<Self> {
        Self::with_placement_and_byte_bounded_cache(
            shard_count,
            Placement::Hash,
            cache_capacity_per_shard,
            cache_byte_capacity_per_shard,
        )
    }

    pub fn new_with_cache_policy(
        shard_count: usize,
        cache_capacity_per_shard: usize,
        policy: EvictionPolicy,
    ) -> Result<Self> {
        Self::with_placement_and_cache_policy(
            shard_count,
            Placement::Hash,
            cache_capacity_per_shard,
            policy,
        )
    }

    pub fn new_with_policy_and_byte_bounded_cache(
        shard_count: usize,
        cache_capacity_per_shard: usize,
        cache_byte_capacity_per_shard: usize,
        policy: EvictionPolicy,
    ) -> Result<Self> {
        Self::with_placement_and_policy_and_byte_bounded_cache(
            shard_count,
            Placement::Hash,
            cache_capacity_per_shard,
            cache_byte_capacity_per_shard,
            policy,
        )
    }

    pub fn with_placement_and_cache_policy(
        shard_count: usize,
        placement: Placement,
        cache_capacity_per_shard: usize,
        policy: EvictionPolicy,
    ) -> Result<Self> {
        let mut graph = Self::with_placement(shard_count, placement)?;

        let mut caches = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            caches.push(AdjacencyLruCache::new_with_policy(
                cache_capacity_per_shard,
                policy,
            )?);
        }

        graph.caches = Some(caches);

        Ok(graph)
    }

    pub fn with_placement_and_policy_and_byte_bounded_cache(
        shard_count: usize,
        placement: Placement,
        cache_capacity_per_shard: usize,
        cache_byte_capacity_per_shard: usize,
        policy: EvictionPolicy,
    ) -> Result<Self> {
        let mut graph = Self::with_placement(shard_count, placement)?;

        let mut caches = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            caches.push(AdjacencyLruCache::new_with_policy_and_byte_capacity(
                cache_capacity_per_shard,
                cache_byte_capacity_per_shard,
                policy,
            )?);
        }

        graph.caches = Some(caches);

        Ok(graph)
    }

    /*
    This constructor calculates the balanced community assignment
    and then creates the sharded graph using that assignment.
    */
    pub fn with_balanced_communities(
        shard_count: usize,
        community_sizes: Vec<u64>,
    ) -> Result<Self> {
        let assignment = assign_communities_balanced(&community_sizes, shard_count)?;

        Self::with_placement(
            shard_count,
            Placement::BalancedCommunity {
                community_sizes,
                community_to_shard: assignment.community_to_shard,
            },
        )
    }

    pub fn with_placement(shard_count: usize, placement: Placement) -> Result<Self> {
        if shard_count == 0 {
            return Err(GraphError::ZeroShardCount);
        }

        match &placement {
            Placement::Hash => {}

            Placement::Community { community_size } => {
                if *community_size == 0 {
                    return Err(GraphError::ZeroCommunitySize);
                }
            }

            Placement::BalancedCommunity {
                community_sizes,
                community_to_shard,
            } => {
                if community_sizes.is_empty() {
                    return Err(GraphError::EmptyCommunities);
                }

                if community_sizes.contains(&0) {
                    return Err(GraphError::ZeroCommunitySizes);
                }

                if community_sizes.len() != community_to_shard.len() {
                    return Err(GraphError::CommunitySizeMismatch);
                }

                if community_to_shard
                    .iter()
                    .any(|shard_id| *shard_id >= shard_count)
                {
                    return Err(GraphError::InvalidShardInAssignment);
                }

                community_sizes
                    .iter()
                    .try_fold(0_u64, |total, size| total.checked_add(*size))
                    .ok_or(GraphError::CommunitySizeOverflow)?;
            }
        }

        let mut shards = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            shards.push(Graph::new());
        }

        Ok(Self {
            shards,
            caches: None,
            observed_adjacency_accesses: vec![std::collections::HashMap::new(); shard_count],
            placement,
            replicated_users: HashSet::new(),
        })
    }
    pub fn with_placement_and_cache(
        shard_count: usize,
        placement: Placement,
        cache_capacity_per_shard: usize,
    ) -> Result<Self> {
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
    ) -> Result<Self> {
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
    pub(crate) fn try_shard_for(&self, user_id: u64) -> Option<usize> {
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

    pub(crate) fn shard_for(&self, user_id: u64) -> usize {
        self.try_shard_for(user_id)
            .unwrap_or_else(|| panic!("User ID {user_id} is outside the configured placement"))
    }

    pub(crate) fn shard_for_user_local_first(
        &self,
        user_id: u64,
        requesting_shard: usize,
    ) -> Option<usize> {
        if self.replicated_users.contains(&user_id)
            && self.shards[requesting_shard].get_user(user_id).is_some()
        {
            return Some(requesting_shard);
        }
        self.try_shard_for(user_id)
    }

    pub fn placement_for_user(&self, user_id: u64) -> usize {
        self.shard_for(user_id)
    }

    pub fn add_user(&mut self, id: u64, name: &str) -> Result<()> {
        if id == 0 {
            return Err(GraphError::ZeroShardCount);
        }

        let shard_id = self
            .try_shard_for(id)
            .ok_or(GraphError::OutsideCommunityRange(id))?;

        self.shards[shard_id].add_user(id, name)
    }

    pub fn add_follow(&mut self, source: u64, target: u64) -> Result<()> {
        if self.get_user(source).is_none() {
            return Err(GraphError::SourceUserNotFound(source));
        }

        if self.get_user(target).is_none() {
            return Err(GraphError::TargetUserNotFound(target));
        }

        let source_shard = self
            .try_shard_for(source)
            .ok_or(GraphError::ShardNotFound(source))?;

        let edge_already_exists = self.shards[source_shard]
            .get_following_ids(source)
            .contains(&target);

        self.shards[source_shard].add_follow_unchecked(source, target)?;

        if !edge_already_exists {
            self.invalidate_cached_adjacency(source_shard, source);
        }

        Ok(())
    }

    pub fn remove_follow(&mut self, source: u64, target: u64) -> Result<bool> {
        if self.get_user(source).is_none() {
            return Err(GraphError::SourceUserNotFound(source));
        }

        if self.get_user(target).is_none() {
            return Err(GraphError::TargetUserNotFound(target));
        }

        let source_shard = self
            .try_shard_for(source)
            .ok_or(GraphError::ShardNotFound(source))?;

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
    pub(crate) fn invalidate_cached_adjacency(&mut self, shard_id: usize, user_id: u64) {
        if let Some(caches) = self.caches.as_mut() {
            caches[shard_id].invalidate(user_id);
        }
    }

    pub(crate) fn record_observed_adjacency_access(&mut self, shard_id: usize, user_id: u64) {
        let access_count = self.observed_adjacency_accesses[shard_id]
            .entry(user_id)
            .or_insert(0);

        *access_count = access_count.saturating_add(1);
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

    pub fn get_two_hop_with_cache_stats(&mut self, source: u64) -> Result<CachedQueryResult> {
        if self.caches.is_none() {
            return Err(GraphError::CachingDisabled);
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
            let Some(first_hop_shard) = self.shard_for_user_local_first(first_hop, source_shard)
            else {
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

            self.record_observed_adjacency_access(first_hop_shard, first_hop);

            let cached_adjacency_list = {
                let caches = self.caches.as_mut().expect("Caching was checked above");

                caches[first_hop_shard].get_shared(first_hop)
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

                        caches[first_hop_shard].insert_shared(first_hop, adjacency_list)
                    }
                }
            };

            for &second_hop in second_hops.iter() {
                let Some(second_hop_shard) =
                    self.shard_for_user_local_first(second_hop, first_hop_shard)
                else {
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

    pub fn user_ids_per_shard(&self) -> Vec<Vec<u64>> {
        self.shards.iter().map(Graph::user_ids).collect()
    }

    pub fn placement_info(&self) -> String {
        match &self.placement {
            Placement::Hash => "Hash".to_string(),
            Placement::Community { community_size } => {
                format!("Community:{community_size}")
            }
            Placement::BalancedCommunity {
                community_sizes,
                community_to_shard,
            } => {
                let sizes: Vec<String> = community_sizes.iter().map(|s| s.to_string()).collect();
                let assignments: Vec<String> =
                    community_to_shard.iter().map(|s| s.to_string()).collect();
                format!(
                    "BalancedCommunity:{}:{}",
                    sizes.join(","),
                    assignments.join(",")
                )
            }
        }
    }
}

pub fn parse_placement_info(info: &str) -> Result<Placement> {
    if info == "Hash" {
        return Ok(Placement::Hash);
    }

    if let Some(size_str) = info.strip_prefix("Community:") {
        let community_size: u64 = size_str
            .parse()
            .map_err(|e| GraphError::IoError(format!("Invalid community size: {e}")))?;
        return Ok(Placement::Community { community_size });
    }

    if let Some(rest) = info.strip_prefix("BalancedCommunity:") {
        let parts: Vec<&str> = rest.split(':').collect();
        if parts.len() != 2 {
            return Err(GraphError::IoError(
                "Invalid BalancedCommunity format".to_string(),
            ));
        }

        let community_sizes: Vec<u64> = parts[0]
            .split(',')
            .map(|s| {
                s.parse()
                    .map_err(|e| GraphError::IoError(format!("Invalid community size: {e}")))
            })
            .collect::<Result<Vec<u64>>>()?;

        let community_to_shard: Vec<usize> = parts[1]
            .split(',')
            .map(|s| {
                s.parse()
                    .map_err(|e| GraphError::IoError(format!("Invalid shard assignment: {e}")))
            })
            .collect::<Result<Vec<usize>>>()?;

        return Ok(Placement::BalancedCommunity {
            community_sizes,
            community_to_shard,
        });
    }

    Err(GraphError::IoError(format!(
        "Unknown placement type: {info}"
    )))
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
    fn creates_requested_cache_policy_for_every_shard() {
        let graph = ShardedGraph::new_with_cache_policy(4, 100, EvictionPolicy::Lfu).unwrap();

        let caches = graph.caches.as_ref().unwrap();

        assert_eq!(caches.len(), 4);

        for cache in caches {
            assert_eq!(cache.policy(), EvictionPolicy::Lfu);
            assert_eq!(cache.capacity(), 100);
            assert_eq!(cache.byte_capacity(), None);
        }
    }

    #[test]
    fn creates_policy_cache_with_byte_limit_for_every_shard() {
        let graph =
            ShardedGraph::new_with_policy_and_byte_bounded_cache(3, 50, 4096, EvictionPolicy::Fifo)
                .unwrap();

        let caches = graph.caches.as_ref().unwrap();

        assert_eq!(caches.len(), 3);

        for cache in caches {
            assert_eq!(cache.policy(), EvictionPolicy::Fifo);
            assert_eq!(cache.capacity(), 50);
            assert_eq!(cache.byte_capacity(), Some(4096));
            assert_eq!(cache.current_bytes(), 0);
        }
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
    fn observed_traffic_warming_preloads_hottest_adjacencies() {
        let mut graph = ShardedGraph::new_with_cache(1, 2).unwrap();

        for user_id in 1..=6 {
            graph.add_user(user_id, &format!("user-{user_id}")).unwrap();
        }

        // Querying User 1 reads adjacency lists for Users 2 and 3.
        graph.add_follow(1, 2).unwrap();
        graph.add_follow(1, 3).unwrap();

        // These queries create two additional accesses for User 2.
        graph.add_follow(4, 2).unwrap();
        graph.add_follow(5, 2).unwrap();

        graph.get_two_hop_with_cache_stats(1).unwrap();
        graph.get_two_hop_with_cache_stats(4).unwrap();
        graph.get_two_hop_with_cache_stats(5).unwrap();

        assert_eq!(graph.observed_adjacency_access_count(2), 3);

        assert_eq!(graph.observed_adjacency_access_count(3), 1);

        // Clear the existing cache and warm only one user per shard.
        let warmed = graph.warm_cache_from_observed_traffic(1).unwrap();

        assert_eq!(warmed, 1);

        /*
        User 2 was hottest and should be a hit.
        User 3 was not warmed and should be a miss.
        */
        let result = graph.get_two_hop_with_cache_stats(1).unwrap();

        assert_eq!(result.cache_hits, 1);
        assert_eq!(result.cache_misses, 1);
    }

    #[test]
    fn observed_access_counts_can_be_cleared() {
        let mut graph = ShardedGraph::new_with_cache(1, 10).unwrap();

        graph.add_user(1, "Alice").unwrap();
        graph.add_user(2, "Bob").unwrap();

        graph.add_follow(1, 2).unwrap();

        graph.get_two_hop_with_cache_stats(1).unwrap();

        assert_eq!(graph.observed_adjacency_access_count(2), 1);

        graph.clear_observed_adjacency_accesses();

        assert_eq!(graph.observed_adjacency_access_count(2), 0);
    }

    #[test]
    fn observed_traffic_warming_requires_enabled_cache() {
        let mut graph = ShardedGraph::new(2).unwrap();

        assert!(graph.warm_cache_from_observed_traffic(10).is_err());
    }

    #[test]
    fn observed_traffic_warming_rejects_zero_limit() {
        let mut graph = ShardedGraph::new_with_cache(2, 10).unwrap();

        assert!(graph.warm_cache_from_observed_traffic(0).is_err());
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
