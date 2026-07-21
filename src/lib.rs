pub mod balanced;
pub mod cache;
pub mod sharded;
pub mod uneven;
pub mod workload;

use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct User {
    pub id: u64,
    pub name: String,
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
    fn reports_graph_size() {
        let graph = build_sample_graph();

        assert_eq!(graph.user_count(), 4);
        assert_eq!(graph.edge_count(), 4);
    }
}
