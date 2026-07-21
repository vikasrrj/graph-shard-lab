pub mod balanced;
pub mod cache;
pub mod sharded;
pub mod uneven;
pub mod workload;

use cache::AdjacencyLruCache;
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct User {
    pub id: u64,
    pub name: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CachedTwoHopResult {
    pub user_ids: Vec<u64>,
    pub cache_hits: usize,
    pub cache_misses: usize,
}

#[derive(Debug)]
pub struct Graph {
    users: HashMap<u64, User>,
    follows: HashMap<u64, Vec<u64>>,
}

impl Graph {
    pub fn new() -> Self {
        Self {
            users: HashMap::new(),
            follows: HashMap::new(),
        }
    }

    pub fn add_user(&mut self, id: u64, name: &str) -> Result<(), String> {
        if self.users.contains_key(&id) {
            return Err(format!("User {id} already exists"));
        }

        self.users.insert(
            id,
            User {
                id,
                name: name.to_string(),
            },
        );

        Ok(())
    }

    pub fn add_follow(&mut self, source: u64, target: u64) -> Result<(), String> {
        if !self.users.contains_key(&source) {
            return Err(format!("Source user {source} does not exist"));
        }

        if !self.users.contains_key(&target) {
            return Err(format!("Target user {target} does not exist"));
        }

        self.add_follow_unchecked(source, target)
    }

    pub(crate) fn add_follow_unchecked(&mut self, source: u64, target: u64) -> Result<(), String> {
        if !self.users.contains_key(&source) {
            return Err(format!("Source user {source} does not exist"));
        }

        let targets = self.follows.entry(source).or_default();

        if !targets.contains(&target) {
            targets.push(target);
        }

        Ok(())
    }

    pub fn remove_follow(&mut self, source: u64, target: u64) -> Result<bool, String> {
        if !self.users.contains_key(&source) {
            return Err(format!("Source user {source} does not exist"));
        }

        if !self.users.contains_key(&target) {
            return Err(format!("Target user {target} does not exist"));
        }

        self.remove_follow_unchecked(source, target)
    }

    pub(crate) fn remove_follow_unchecked(
        &mut self,
        source: u64,
        target: u64,
    ) -> Result<bool, String> {
        if !self.users.contains_key(&source) {
            return Err(format!("Source user {source} does not exist"));
        }

        let Some(targets) = self.follows.get_mut(&source) else {
            return Ok(false);
        };

        let Some(position) = targets
            .iter()
            .position(|existing_target| *existing_target == target)
        else {
            return Ok(false);
        };

        targets.remove(position);

        Ok(true)
    }

    pub fn get_user(&self, id: u64) -> Option<&User> {
        self.users.get(&id)
    }

    pub fn get_following_ids(&self, source: u64) -> &[u64] {
        match self.follows.get(&source) {
            Some(targets) => targets,
            None => &[],
        }
    }

    pub fn get_two_hop_ids(&self, source: u64) -> Vec<u64> {
        let mut result = Vec::new();
        let mut seen = HashSet::new();

        for first_hop in self.get_following_ids(source) {
            for second_hop in self.get_following_ids(*first_hop) {
                if *second_hop != source && seen.insert(*second_hop) {
                    result.push(*second_hop);
                }
            }
        }

        result
    }

    pub fn get_two_hop_ids_with_cache(
        &self,
        source: u64,
        cache: &mut AdjacencyLruCache,
    ) -> CachedTwoHopResult {
        let mut result = Vec::new();
        let mut seen = HashSet::new();

        let mut cache_hits = 0;
        let mut cache_misses = 0;

        for first_hop in self.get_following_ids(source) {
            let second_hops = match cache.get(*first_hop) {
                Some(cached_adjacency_list) => {
                    cache_hits += 1;
                    cached_adjacency_list
                }

                None => {
                    cache_misses += 1;

                    let adjacency_list = self.get_following_ids(*first_hop).to_vec();

                    cache.insert(*first_hop, adjacency_list.clone());

                    adjacency_list
                }
            };

            for second_hop in second_hops {
                if second_hop != source && seen.insert(second_hop) {
                    result.push(second_hop);
                }
            }
        }

        CachedTwoHopResult {
            user_ids: result,
            cache_hits,
            cache_misses,
        }
    }

    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    pub fn edge_count(&self) -> usize {
        self.follows.values().map(Vec::len).sum()
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_sample_graph() -> Graph {
        let mut graph = Graph::new();

        graph.add_user(1, "Alice").unwrap();
        graph.add_user(2, "Bob").unwrap();
        graph.add_user(3, "Charlie").unwrap();
        graph.add_user(4, "Diana").unwrap();

        graph.add_follow(1, 2).unwrap();
        graph.add_follow(1, 3).unwrap();
        graph.add_follow(2, 3).unwrap();
        graph.add_follow(3, 4).unwrap();

        graph
    }

    #[test]
    fn removes_an_existing_edge() {
        let mut graph = Graph::new();

        graph.add_user(1, "Alice").unwrap();
        graph.add_user(2, "Bob").unwrap();
        graph.add_user(3, "Charlie").unwrap();

        graph.add_follow(1, 2).unwrap();
        graph.add_follow(1, 3).unwrap();

        assert_eq!(graph.get_following_ids(1), &[2, 3]);

        let removed = graph.remove_follow(1, 2).unwrap();

        assert!(removed);
        assert_eq!(graph.get_following_ids(1), &[3]);
    }

    #[test]
    fn removing_a_missing_edge_returns_false() {
        let mut graph = Graph::new();

        graph.add_user(1, "Alice").unwrap();
        graph.add_user(2, "Bob").unwrap();

        let removed = graph.remove_follow(1, 2).unwrap();

        assert!(!removed);
        assert!(graph.get_following_ids(1).is_empty());
    }

    #[test]
    fn returns_one_hop_users() {
        let graph = build_sample_graph();

        assert_eq!(graph.get_following_ids(1), &[2, 3]);
    }

    #[test]
    fn returns_two_hop_users() {
        let graph = build_sample_graph();

        assert_eq!(graph.get_two_hop_ids(1), vec![3, 4]);
    }

    #[test]
    fn rejects_missing_target_user() {
        let mut graph = Graph::new();

        graph.add_user(1, "Alice").unwrap();

        assert!(graph.add_follow(1, 999).is_err());
    }

    #[test]
    fn does_not_duplicate_edges() {
        let mut graph = Graph::new();

        graph.add_user(1, "Alice").unwrap();
        graph.add_user(2, "Bob").unwrap();

        graph.add_follow(1, 2).unwrap();
        graph.add_follow(1, 2).unwrap();

        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn rejects_duplicate_user_ids() {
        let mut graph = Graph::new();

        graph.add_user(1, "Alice").unwrap();

        assert!(graph.add_user(1, "Different Alice").is_err());
    }

    #[test]
    fn cached_two_hop_query_returns_same_users() {
        let graph = build_sample_graph();

        let expected = graph.get_two_hop_ids(1);

        let mut cache = AdjacencyLruCache::new(10).unwrap();

        let cached = graph.get_two_hop_ids_with_cache(1, &mut cache);

        assert_eq!(cached.user_ids, expected);
    }

    #[test]
    fn cached_two_hop_query_misses_then_hits() {
        let graph = build_sample_graph();

        let mut cache = AdjacencyLruCache::new(10).unwrap();

        let first = graph.get_two_hop_ids_with_cache(1, &mut cache);

        assert_eq!(first.user_ids, vec![3, 4]);
        assert_eq!(first.cache_hits, 0);
        assert_eq!(first.cache_misses, 2);

        let second = graph.get_two_hop_ids_with_cache(1, &mut cache);

        assert_eq!(second.user_ids, vec![3, 4]);
        assert_eq!(second.cache_hits, 2);
        assert_eq!(second.cache_misses, 0);
    }

    #[test]
    fn reports_graph_size() {
        let graph = build_sample_graph();

        assert_eq!(graph.user_count(), 4);
        assert_eq!(graph.edge_count(), 4);
    }
}
