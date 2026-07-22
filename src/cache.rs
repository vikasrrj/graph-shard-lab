use std::collections::{HashMap, VecDeque};

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
///

#[derive(Debug)]
struct CacheNode {
    user_id: u64,
    adjacency_list: Vec<u64>,
    previous: Option<usize>,
    next: Option<usize>,
}

/// A real adjacency-list LRU cache.
///
/// Each entry stores:
///
/// user ID -> IDs of users that user follows
#[derive(Debug)]
pub struct AdjacencyLruCache {
    capacity: usize,

    // Finds a cached user without scanning every entry.
    locations: HashMap<u64, usize>,

    // Nodes form a doubly linked usage-order list.
    nodes: Vec<Option<CacheNode>>,

    // Empty node positions that can be reused.
    free_indices: Vec<usize>,

    // Least recently used entry.
    head: Option<usize>,

    // Most recently used entry.
    tail: Option<usize>,
}

impl AdjacencyLruCache {
    pub fn new(capacity: usize) -> Result<Self, String> {
        if capacity == 0 {
            return Err("Cache capacity must be greater than zero".to_string());
        }

        Ok(Self {
            capacity,
            locations: HashMap::with_capacity(capacity),
            nodes: Vec::with_capacity(capacity),
            free_indices: Vec::new(),
            head: None,
            tail: None,
        })
    }

    /// Removes a node from its current position in the LRU order.
    fn detach(&mut self, index: usize) {
        let (previous, next) = {
            let node = self.nodes[index].as_ref().expect("Cache node must exist");

            (node.previous, node.next)
        };

        match previous {
            Some(previous_index) => {
                self.nodes[previous_index]
                    .as_mut()
                    .expect("Previous cache node must exist")
                    .next = next;
            }

            None => {
                // This node was the least recently used entry.
                self.head = next;
            }
        }

        match next {
            Some(next_index) => {
                self.nodes[next_index]
                    .as_mut()
                    .expect("Next cache node must exist")
                    .previous = previous;
            }

            None => {
                // This node was the most recently used entry.
                self.tail = previous;
            }
        }

        let node = self.nodes[index].as_mut().expect("Cache node must exist");

        node.previous = None;
        node.next = None;
    }

    /// Places a node at the most-recently-used end.
    fn attach_as_most_recent(&mut self, index: usize) {
        let previous_tail = self.tail;

        {
            let node = self.nodes[index].as_mut().expect("Cache node must exist");

            node.previous = previous_tail;
            node.next = None;
        }

        match previous_tail {
            Some(tail_index) => {
                self.nodes[tail_index]
                    .as_mut()
                    .expect("Tail cache node must exist")
                    .next = Some(index);
            }

            None => {
                // The cache was empty, so this is also the head.
                self.head = Some(index);
            }
        }

        self.tail = Some(index);
    }

    /// Creates a new node, reusing a deleted slot when possible.
    fn allocate_node(&mut self, user_id: u64, adjacency_list: Vec<u64>) -> usize {
        let node = CacheNode {
            user_id,
            adjacency_list,
            previous: None,
            next: None,
        };

        let index = match self.free_indices.pop() {
            Some(index) => {
                self.nodes[index] = Some(node);
                index
            }

            None => {
                self.nodes.push(Some(node));
                self.nodes.len() - 1
            }
        };

        self.locations.insert(user_id, index);
        self.attach_as_most_recent(index);

        index
    }

    /// Completely removes a node from the cache.
    fn remove_node(&mut self, index: usize) {
        let user_id = self.nodes[index]
            .as_ref()
            .expect("Cache node must exist")
            .user_id;

        self.detach(index);
        self.locations.remove(&user_id);

        self.nodes[index] = None;
        self.free_indices.push(index);
    }

    /// Returns the cached adjacency list.
    ///
    /// Reading an entry also makes it the most recently used entry.
    pub fn get(&mut self, user_id: u64) -> Option<Vec<u64>> {
        let index = self.locations.get(&user_id).copied()?;

        let adjacency_list = self.nodes[index].as_ref()?.adjacency_list.clone();

        self.detach(index);
        self.attach_as_most_recent(index);

        Some(adjacency_list)
    }

    /// Inserts or replaces one user's adjacency list.
    pub fn insert(&mut self, user_id: u64, adjacency_list: Vec<u64>) {
        if let Some(index) = self.locations.get(&user_id).copied() {
            self.nodes[index]
                .as_mut()
                .expect("Cache node must exist")
                .adjacency_list = adjacency_list;

            self.detach(index);
            self.attach_as_most_recent(index);

            return;
        }

        if self.locations.len() == self.capacity {
            let least_recently_used = self.head.expect("A full cache must contain a head node");

            self.remove_node(least_recently_used);
        }

        self.allocate_node(user_id, adjacency_list);
    }

    /// Removes one user's cached adjacency list.
    pub fn invalidate(&mut self, user_id: u64) -> bool {
        let Some(index) = self.locations.get(&user_id).copied() else {
            return false;
        };

        self.remove_node(index);

        true
    }

    pub fn contains(&self, user_id: u64) -> bool {
        self.locations.contains_key(&user_id)
    }

    pub fn len(&self) -> usize {
        self.locations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.locations.is_empty()
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
    fn adjacency_cache_hit_moves_entry_to_most_recent() {
        let mut cache = AdjacencyLruCache::new(3).unwrap();

        cache.insert(1, vec![10]);
        cache.insert(2, vec![20]);
        cache.insert(3, vec![30]);

        // Order is now: 1, 2, 3.
        // Reading 1 should move it to the most-recent end:
        // 2, 3, 1.
        assert_eq!(cache.get(1), Some(vec![10]));

        // Cache is full, so inserting 4 should evict User 2.
        cache.insert(4, vec![40]);

        assert!(cache.contains(1));
        assert!(!cache.contains(2));
        assert!(cache.contains(3));
        assert!(cache.contains(4));
        assert_eq!(cache.len(), 3);
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
