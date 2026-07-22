use crate::error::{GraphError, Result};
use crate::sharded::ShardedGraph;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct ReplicationStats {
    pub replicated_users: usize,
    pub total_replicated_edges: usize,
}

impl ShardedGraph {
    pub fn replicated_users(&self) -> &HashSet<u64> {
        &self.replicated_users
    }

    pub fn replicate_user(&mut self, user_id: u64) -> Result<ReplicationStats> {
        let home_shard = self
            .try_shard_for(user_id)
            .ok_or(GraphError::ShardNotFound(user_id))?;

        if self.shards[home_shard].get_user(user_id).is_none() {
            return Err(GraphError::UserNotFound(user_id));
        }

        if self.replicated_users.contains(&user_id) {
            return Ok(self.replication_stats());
        }

        let adjacency_list = self.shards[home_shard].get_following_ids(user_id).to_vec();

        let user_name = self.shards[home_shard]
            .get_user(user_id)
            .map(|u| u.name.clone())
            .unwrap_or_default();

        for shard_id in 0..self.shards.len() {
            if shard_id == home_shard {
                continue;
            }

            if self.shards[shard_id].get_user(user_id).is_none() {
                let _ = self.shards[shard_id].add_user(user_id, &user_name);
            }

            for &target in &adjacency_list {
                let _ = self.shards[shard_id].add_follow_unchecked(user_id, target);
            }
        }

        self.replicated_users.insert(user_id);

        if let Some(caches) = self.caches.as_mut() {
            for (shard_id, _) in self.shards.iter().enumerate() {
                if shard_id == home_shard {
                    continue;
                }

                caches[shard_id].insert(user_id, adjacency_list.clone());
            }
        }

        Ok(self.replication_stats())
    }

    pub fn unreplicate_user(&mut self, user_id: u64) -> Result<()> {
        if !self.replicated_users.contains(&user_id) {
            return Ok(());
        }

        let home_shard = self
            .try_shard_for(user_id)
            .ok_or(GraphError::ShardNotFound(user_id))?;

        let adjacency_list = self.shards[home_shard].get_following_ids(user_id).to_vec();

        for shard_id in 0..self.shards.len() {
            if shard_id == home_shard {
                continue;
            }

            for &target in &adjacency_list {
                let _ = self.shards[shard_id].remove_follow_unchecked(user_id, target);
            }

            let _ = self.shards[shard_id].remove_follow_unchecked(user_id, user_id);

            let _ = self.shards[shard_id].remove_user(user_id);

            if let Some(caches) = self.caches.as_mut() {
                caches[shard_id].invalidate(user_id);
            }
        }

        self.replicated_users.remove(&user_id);

        Ok(())
    }

    pub fn is_replicated(&self, user_id: u64) -> bool {
        self.replicated_users.contains(&user_id)
    }

    pub fn replication_stats(&self) -> ReplicationStats {
        let total_replicated_edges: usize = self
            .replicated_users
            .iter()
            .map(|&user_id| {
                let home_shard = self.shard_for(user_id);
                self.shards[home_shard].get_following_ids(user_id).len() * (self.shards.len() - 1)
            })
            .sum();

        ReplicationStats {
            replicated_users: self.replicated_users.len(),
            total_replicated_edges,
        }
    }

    pub fn get_following_ids_replicated(&self, source: u64) -> &[u64] {
        let Some(shard_id) = self.try_shard_for(source) else {
            return &[];
        };

        self.shards[shard_id].get_following_ids(source)
    }

    pub fn add_follow_replicated(&mut self, source: u64, target: u64) -> Result<()> {
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

        if !edge_already_exists && self.replicated_users.contains(&source) {
            for shard_id in 0..self.shards.len() {
                if shard_id == source_shard {
                    continue;
                }

                let _ = self.shards[shard_id].add_follow_unchecked(source, target);

                if let Some(caches) = self.caches.as_mut() {
                    caches[shard_id].invalidate(source);
                }
            }
        }

        if !edge_already_exists {
            self.invalidate_cached_adjacency(source_shard, source);
        }

        Ok(())
    }

    pub fn remove_follow_replicated(&mut self, source: u64, target: u64) -> Result<bool> {
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

        if removed && self.replicated_users.contains(&source) {
            for shard_id in 0..self.shards.len() {
                if shard_id == source_shard {
                    continue;
                }

                let _ = self.shards[shard_id].remove_follow_unchecked(source, target);

                if let Some(caches) = self.caches.as_mut() {
                    caches[shard_id].invalidate(source);
                }
            }
        }

        if removed {
            self.invalidate_cached_adjacency(source_shard, source);
        }

        Ok(removed)
    }

    pub fn auto_replicate_hubs(&mut self, hub_ids: &[u64]) -> Result<ReplicationStats> {
        for &hub_id in hub_ids {
            if self.try_shard_for(hub_id).is_some() {
                self.replicate_user(hub_id)?;
            }
        }

        Ok(self.replication_stats())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_replicated_graph() -> ShardedGraph {
        let mut graph = ShardedGraph::new_with_cache(4, 100).unwrap();

        for id in 1..=16 {
            graph.add_user(id, &format!("user-{id}")).unwrap();
        }

        for source in 1..=16 {
            for offset in 1..=3 {
                let target = ((source + offset - 1) % 16) + 1;
                graph.add_follow(source, target).unwrap();
            }
        }

        graph
    }

    #[test]
    fn replicates_user_across_all_shards() {
        let mut graph = build_replicated_graph();

        let stats = graph.replicate_user(1).unwrap();

        assert_eq!(stats.replicated_users, 1);
        assert!(graph.is_replicated(1));

        for shard_id in 0..graph.shard_count() {
            assert!(graph.shards[shard_id].get_user(1).is_some());
        }
    }

    #[test]
    fn replicated_user_has_same_adjacency_on_all_shards() {
        let mut graph = build_replicated_graph();

        graph.replicate_user(1).unwrap();

        let home_shard = graph.shard_for(1);
        let home_adjacency = graph.shards[home_shard].get_following_ids(1).to_vec();

        for shard_id in 0..graph.shard_count() {
            let shard_adjacency = graph.shards[shard_id].get_following_ids(1).to_vec();

            assert_eq!(shard_adjacency, home_adjacency);
        }
    }

    #[test]
    fn does_not_replicate_twice() {
        let mut graph = build_replicated_graph();

        graph.replicate_user(1).unwrap();
        let stats = graph.replicate_user(1).unwrap();

        assert_eq!(stats.replicated_users, 1);
    }

    #[test]
    fn unreplicate_removes_replication() {
        let mut graph = build_replicated_graph();

        graph.replicate_user(1).unwrap();
        assert!(graph.is_replicated(1));

        graph.unreplicate_user(1).unwrap();
        assert!(!graph.is_replicated(1));

        let home_shard = graph.shard_for(1);
        let home_adjacency = graph.shards[home_shard].get_following_ids(1).to_vec();

        for shard_id in 0..graph.shard_count() {
            if shard_id == home_shard {
                continue;
            }
            assert!(
                graph.shards[shard_id].get_user(1).is_none(),
                "User 1 should be removed from shard {shard_id}"
            );
            assert!(
                graph.shards[shard_id].get_following_ids(1).is_empty(),
                "Adjacency list for user 1 should be empty on shard {shard_id}"
            );
        }

        assert_eq!(
            graph.shards[home_shard].get_following_ids(1),
            &home_adjacency
        );
    }

    #[test]
    fn add_follow_updates_replicas() {
        let mut graph = build_replicated_graph();

        graph.replicate_user(1).unwrap();

        graph.add_follow_replicated(1, 16).unwrap();

        for shard_id in 0..graph.shard_count() {
            assert!(graph.shards[shard_id].get_following_ids(1).contains(&16));
        }
    }

    #[test]
    fn remove_follow_updates_replicas() {
        let mut graph = build_replicated_graph();

        graph.replicate_user(1).unwrap();

        let home_shard = graph.shard_for(1);
        let targets = graph.shards[home_shard].get_following_ids(1).to_vec();
        let first_target = targets[0];

        graph.remove_follow_replicated(1, first_target).unwrap();

        for shard_id in 0..graph.shard_count() {
            assert!(
                !graph.shards[shard_id]
                    .get_following_ids(1)
                    .contains(&first_target)
            );
        }
    }

    #[test]
    fn auto_replicate_hubs_replicates_multiple() {
        let mut graph = build_replicated_graph();

        let stats = graph.auto_replicate_hubs(&[1, 5, 9]).unwrap();

        assert_eq!(stats.replicated_users, 3);
        assert!(graph.is_replicated(1));
        assert!(graph.is_replicated(5));
        assert!(graph.is_replicated(9));
    }

    #[test]
    fn replicated_query_returns_correct_results() {
        let mut graph = build_replicated_graph();

        graph.replicate_user(1).unwrap();

        let mut expected = graph.get_two_hop_ids(1);
        expected.sort_unstable();

        let mut actual = graph.get_two_hop_with_stats(1).user_ids;
        actual.sort_unstable();

        assert_eq!(actual, expected);
    }

    #[test]
    fn replication_stats_counts_edges() {
        let mut graph = build_replicated_graph();

        graph.replicate_user(1).unwrap();

        let stats = graph.replication_stats();

        assert_eq!(stats.replicated_users, 1);

        let home_shard = graph.shard_for(1);
        let edge_count = graph.shards[home_shard].get_following_ids(1).len();

        assert_eq!(
            stats.total_replicated_edges,
            edge_count * (graph.shard_count() - 1)
        );
    }
}
