use std::collections::VecDeque;

/// A lightweight LRU simulator.
///
/// This stores only user IDs and is used by the existing
/// logical cache-hit benchmarks.
#[derive(Debug)]
pub struct IdLruSimulator {
    capacity: usize,
    entries: VecDeque<u64>,
}

impl IdLruSimulator {
    pub fn new(capacity: usize) -> Result<Self, String> {
        if capacity == 0 {
            return Err("Cache capacity must be greater than zero".to_string());
        }

        Ok(Self {
            capacity,
            entries: VecDeque::with_capacity(capacity),
        })
    }

    /// Returns true for a cache hit and false for a cache miss.
    pub fn access(&mut self, user_id: u64) -> bool {
        if let Some(position) = self
            .entries
            .iter()
            .position(|cached_id| *cached_id == user_id)
        {
            // Cache hit:
            // move the entry to the most-recently-used position.
            self.entries.remove(position);
            self.entries.push_back(user_id);

            return true;
        }

        // Cache miss:
        // remove the least-recently-used entry when full.
        if self.entries.len() == self.capacity {
            self.entries.pop_front();
        }

        self.entries.push_back(user_id);

        false
    }

    pub fn contains(&self, user_id: u64) -> bool {
        self.entries.contains(&user_id)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// A real adjacency-list LRU cache.
///
/// Each entry stores:
///
/// user ID -> IDs of users that user follows
#[derive(Debug)]
pub struct AdjacencyLruCache {
    capacity: usize,
    entries: VecDeque<(u64, Vec<u64>)>,
}

impl AdjacencyLruCache {
    pub fn new(capacity: usize) -> Result<Self, String> {
        if capacity == 0 {
            return Err("Cache capacity must be greater than zero".to_string());
        }

        Ok(Self {
            capacity,
            entries: VecDeque::with_capacity(capacity),
        })
    }

    /// Returns the cached adjacency list.
    ///
    /// Reading an entry also makes it the most recently used entry.
    pub fn get(&mut self, user_id: u64) -> Option<Vec<u64>> {
        let position = self
            .entries
            .iter()
            .position(|(cached_id, _)| *cached_id == user_id)?;

        let (cached_id, adjacency_list) = self.entries.remove(position)?;

        // We return a copy because the original value must remain in the cache.
        let result = adjacency_list.clone();

        // Move the entry to the most-recently-used position.
        self.entries.push_back((cached_id, adjacency_list));

        Some(result)
    }

    /// Inserts or replaces one user's adjacency list.
    pub fn insert(&mut self, user_id: u64, adjacency_list: Vec<u64>) {
        // Remove an older copy when this user is already cached.
        if let Some(position) = self
            .entries
            .iter()
            .position(|(cached_id, _)| *cached_id == user_id)
        {
            self.entries.remove(position);
        }

        // Remove the least-recently-used entry when full.
        if self.entries.len() == self.capacity {
            self.entries.pop_front();
        }

        self.entries.push_back((user_id, adjacency_list));
    }

    /// Removes one user's cached adjacency list.
    ///
    /// Returns true when an entry existed and was removed.
    /// Returns false when the user was not cached.
    pub fn invalidate(&mut self, user_id: u64) -> bool {
        let Some(position) = self
            .entries
            .iter()
            .position(|(cached_id, _)| *cached_id == user_id)
        else {
            return false;
        };

        self.entries.remove(position);

        true
    }

    pub fn contains(&self, user_id: u64) -> bool {
        self.entries
            .iter()
            .any(|(cached_id, _)| *cached_id == user_id)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_access_is_a_miss_and_second_is_a_hit() {
        let mut cache = IdLruSimulator::new(3).unwrap();

        assert!(!cache.access(10));
        assert!(cache.access(10));
    }

    #[test]
    fn never_exceeds_capacity() {
        let mut cache = IdLruSimulator::new(2).unwrap();

        cache.access(10);
        cache.access(20);
        cache.access(30);

        assert_eq!(cache.len(), 2);
        assert!(!cache.contains(10));
        assert!(cache.contains(20));
        assert!(cache.contains(30));
    }

    #[test]
    fn adjacency_cache_can_invalidate_an_entry() {
        let mut cache = AdjacencyLruCache::new(2).unwrap();

        cache.insert(10, vec![20, 30]);
        cache.insert(11, vec![40, 50]);

        assert!(cache.contains(10));
        assert_eq!(cache.len(), 2);

        let removed = cache.invalidate(10);

        assert!(removed);
        assert!(!cache.contains(10));
        assert_eq!(cache.len(), 1);

        // Invalidating an absent entry should do nothing.
        let removed_again = cache.invalidate(10);

        assert!(!removed_again);
        assert_eq!(cache.len(), 1);

        // Other cached entries must remain untouched.
        assert!(cache.contains(11));
    }
    #[test]
    fn evicts_the_least_recently_used_entry() {
        let mut cache = IdLruSimulator::new(3).unwrap();

        cache.access(10);
        cache.access(20);
        cache.access(30);

        // Accessing 10 makes it recently used.
        assert!(cache.access(10));

        // 20 is now the least recently used entry.
        cache.access(40);

        assert!(cache.contains(10));
        assert!(!cache.contains(20));
        assert!(cache.contains(30));
        assert!(cache.contains(40));
    }

    #[test]
    fn rejects_zero_capacity() {
        assert!(IdLruSimulator::new(0).is_err());
    }

    #[test]
    fn adjacency_cache_stores_real_following_lists() {
        let mut cache = AdjacencyLruCache::new(3).unwrap();

        cache.insert(10, vec![20, 30, 40]);

        assert_eq!(cache.get(10), Some(vec![20, 30, 40]));
    }

    #[test]
    fn adjacency_cache_returns_none_for_a_miss() {
        let mut cache = AdjacencyLruCache::new(3).unwrap();

        assert_eq!(cache.get(999), None);
    }

    #[test]
    fn adjacency_cache_evicts_least_recently_used_entry() {
        let mut cache = AdjacencyLruCache::new(2).unwrap();

        cache.insert(10, vec![1]);
        cache.insert(20, vec![2]);

        // Reading 10 makes 20 the least recently used entry.
        assert_eq!(cache.get(10), Some(vec![1]));

        cache.insert(30, vec![3]);

        assert!(cache.contains(10));
        assert!(!cache.contains(20));
        assert!(cache.contains(30));
    }

    #[test]
    fn adjacency_cache_replaces_existing_data() {
        let mut cache = AdjacencyLruCache::new(2).unwrap();

        cache.insert(10, vec![1, 2]);
        cache.insert(10, vec![3, 4]);

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(10), Some(vec![3, 4]));
    }
}
