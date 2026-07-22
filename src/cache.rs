use std::collections::{HashMap, VecDeque};
use std::mem::size_of;

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
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    Lru,
    Fifo,
    Lfu,
}

#[derive(Debug)]
struct CacheNode {
    user_id: u64,
    adjacency_list: Vec<u64>,
    estimated_size_bytes: usize,

    frequency: u64,
    inserted_at: u64,
    last_accessed_at: u64,

    previous: Option<usize>,
    next: Option<usize>,
}

/// A shard-local adjacency-list LRU cache.
#[derive(Debug)]
pub struct AdjacencyLruCache {
    capacity: usize,
    byte_capacity: Option<usize>,
    current_bytes: usize,
    policy: EvictionPolicy,
    sequence: u64,

    locations: HashMap<u64, usize>,
    nodes: Vec<Option<CacheNode>>,
    free_indices: Vec<usize>,

    // Least recently used.
    head: Option<usize>,

    // Most recently used.
    tail: Option<usize>,
}

impl AdjacencyLruCache {
    pub fn new(capacity: usize) -> Result<Self, String> {
        Self::build(capacity, None, EvictionPolicy::Lru)
    }

    pub fn new_with_byte_capacity(capacity: usize, byte_capacity: usize) -> Result<Self, String> {
        if byte_capacity == 0 {
            return Err("Cache byte capacity must be greater than zero".to_string());
        }

        Self::build(capacity, Some(byte_capacity), EvictionPolicy::Lru)
    }

    pub fn new_with_policy(capacity: usize, policy: EvictionPolicy) -> Result<Self, String> {
        Self::build(capacity, None, policy)
    }

    pub fn new_with_policy_and_byte_capacity(
        capacity: usize,
        byte_capacity: usize,
        policy: EvictionPolicy,
    ) -> Result<Self, String> {
        if byte_capacity == 0 {
            return Err("Cache byte capacity must be greater than zero".to_string());
        }

        Self::build(capacity, Some(byte_capacity), policy)
    }

    fn build(
        capacity: usize,
        byte_capacity: Option<usize>,
        policy: EvictionPolicy,
    ) -> Result<Self, String> {
        if capacity == 0 {
            return Err("Cache capacity must be greater than zero".to_string());
        }

        Ok(Self {
            capacity,
            byte_capacity,
            current_bytes: 0,
            policy,
            sequence: 0,
            locations: HashMap::with_capacity(capacity),
            nodes: Vec::with_capacity(capacity),
            free_indices: Vec::new(),
            head: None,
            tail: None,
        })
    }

    fn next_sequence(&mut self) -> u64 {
        let current = self.sequence;
        self.sequence = self.sequence.saturating_add(1);
        current
    }

    fn estimate_entry_size(adjacency_list: &[u64]) -> usize {
        size_of::<CacheNode>().saturating_add(adjacency_list.len().saturating_mul(size_of::<u64>()))
    }

    fn would_exceed_byte_capacity(&self, additional_bytes: usize) -> bool {
        match self.byte_capacity {
            Some(limit) => self.current_bytes.saturating_add(additional_bytes) > limit,
            None => false,
        }
    }

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
                self.tail = previous;
            }
        }

        let node = self.nodes[index].as_mut().expect("Cache node must exist");

        node.previous = None;
        node.next = None;
    }

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
                self.head = Some(index);
            }
        }

        self.tail = Some(index);
    }

    fn allocate_node(
        &mut self,
        user_id: u64,
        adjacency_list: Vec<u64>,
        estimated_size_bytes: usize,
    ) -> usize {
        let sequence = self.next_sequence();

        let node = CacheNode {
            user_id,
            adjacency_list,
            estimated_size_bytes,
            frequency: 1,
            inserted_at: sequence,
            last_accessed_at: sequence,
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

        self.current_bytes = self.current_bytes.saturating_add(estimated_size_bytes);

        self.attach_as_most_recent(index);

        index
    }

    fn remove_node(&mut self, index: usize) {
        let (user_id, estimated_size_bytes) = {
            let node = self.nodes[index].as_ref().expect("Cache node must exist");

            (node.user_id, node.estimated_size_bytes)
        };

        self.detach(index);
        self.locations.remove(&user_id);

        self.current_bytes = self.current_bytes.saturating_sub(estimated_size_bytes);

        self.nodes[index] = None;
        self.free_indices.push(index);
    }

    pub fn get(&mut self, user_id: u64) -> Option<Vec<u64>> {
        let index = self.locations.get(&user_id).copied()?;

        let adjacency_list = self.nodes[index].as_ref()?.adjacency_list.clone();

        let sequence = self.next_sequence();

        {
            let node = self.nodes[index].as_mut()?;

            node.frequency = node.frequency.saturating_add(1);
            node.last_accessed_at = sequence;
        }

        if self.policy == EvictionPolicy::Lru {
            self.detach(index);
            self.attach_as_most_recent(index);
        }

        Some(adjacency_list)
    }

    fn eviction_candidate(&self) -> usize {
        match self.policy {
            EvictionPolicy::Lru | EvictionPolicy::Fifo => {
                self.head.expect("Non-empty cache must contain a head node")
            }

            EvictionPolicy::Lfu => self
                .locations
                .values()
                .copied()
                .min_by_key(|index| {
                    let node = self.nodes[*index].as_ref().expect("Cache node must exist");

                    (node.frequency, node.last_accessed_at, node.inserted_at)
                })
                .expect("Non-empty cache must contain an LFU candidate"),
        }
    }

    pub fn insert(&mut self, user_id: u64, adjacency_list: Vec<u64>) {
        let estimated_size_bytes = Self::estimate_entry_size(&adjacency_list);

        // Remove the old version before inserting its replacement.
        if let Some(index) = self.locations.get(&user_id).copied() {
            self.remove_node(index);
        }

        // An individual entry larger than the total limit cannot be cached.
        if let Some(byte_capacity) = self.byte_capacity {
            if estimated_size_bytes > byte_capacity {
                return;
            }
        }

        // Evict LRU entries until both limits allow this insertion.
        while self.locations.len() >= self.capacity
            || self.would_exceed_byte_capacity(estimated_size_bytes)
        {
            let victim = self.eviction_candidate();
            self.remove_node(victim);
        }

        self.allocate_node(user_id, adjacency_list, estimated_size_bytes);
    }

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

    pub fn current_bytes(&self) -> usize {
        self.current_bytes
    }

    pub fn byte_capacity(&self) -> Option<usize> {
        self.byte_capacity
    }

    pub fn policy(&self) -> EvictionPolicy {
        self.policy
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
    fn byte_bounded_cache_evicts_lru_entry() {
        let entry_size = AdjacencyLruCache::estimate_entry_size(&[10]);

        let mut cache = AdjacencyLruCache::new_with_byte_capacity(10, entry_size * 2).unwrap();

        cache.insert(1, vec![10]);
        cache.insert(2, vec![20]);
        cache.insert(3, vec![30]);

        assert!(!cache.contains(1));
        assert!(cache.contains(2));
        assert!(cache.contains(3));
        assert_eq!(cache.len(), 2);
        assert!(cache.current_bytes() <= entry_size * 2);
    }

    #[test]
    fn oversized_entry_is_not_cached() {
        let byte_limit = AdjacencyLruCache::estimate_entry_size(&[10]);

        let mut cache = AdjacencyLruCache::new_with_byte_capacity(10, byte_limit).unwrap();

        cache.insert(1, vec![10, 20, 30, 40]);

        assert!(!cache.contains(1));
        assert_eq!(cache.current_bytes(), 0);
    }

    #[test]
    fn replacing_entry_updates_byte_accounting() {
        let large_adjacency = vec![10, 20, 30, 40];

        let large_size = AdjacencyLruCache::estimate_entry_size(&large_adjacency);

        let mut cache = AdjacencyLruCache::new_with_byte_capacity(10, large_size).unwrap();

        cache.insert(1, vec![10]);
        cache.insert(1, large_adjacency);

        assert!(cache.contains(1));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.current_bytes(), large_size);
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
    fn fifo_does_not_promote_entry_after_read() {
        let mut cache = AdjacencyLruCache::new_with_policy(2, EvictionPolicy::Fifo).unwrap();

        cache.insert(1, vec![10]);
        cache.insert(2, vec![20]);

        assert_eq!(cache.get(1), Some(vec![10]));

        cache.insert(3, vec![30]);

        assert!(!cache.contains(1));
        assert!(cache.contains(2));
        assert!(cache.contains(3));
    }

    #[test]
    fn lru_promotes_entry_after_read() {
        let mut cache = AdjacencyLruCache::new_with_policy(2, EvictionPolicy::Lru).unwrap();

        cache.insert(1, vec![10]);
        cache.insert(2, vec![20]);

        assert_eq!(cache.get(1), Some(vec![10]));

        cache.insert(3, vec![30]);

        assert!(cache.contains(1));
        assert!(!cache.contains(2));
        assert!(cache.contains(3));
    }

    #[test]
    fn lfu_evicts_least_frequently_used_entry() {
        let mut cache = AdjacencyLruCache::new_with_policy(2, EvictionPolicy::Lfu).unwrap();

        cache.insert(1, vec![10]);
        cache.insert(2, vec![20]);

        assert_eq!(cache.get(1), Some(vec![10]));
        assert_eq!(cache.get(1), Some(vec![10]));

        cache.insert(3, vec![30]);

        assert!(cache.contains(1));
        assert!(!cache.contains(2));
        assert!(cache.contains(3));
    }

    #[test]
    fn cache_policy_works_with_byte_capacity() {
        let entry_size = AdjacencyLruCache::estimate_entry_size(&[10]);

        let mut cache = AdjacencyLruCache::new_with_policy_and_byte_capacity(
            10,
            entry_size * 2,
            EvictionPolicy::Fifo,
        )
        .unwrap();

        cache.insert(1, vec![10]);
        cache.insert(2, vec![20]);

        assert_eq!(cache.get(1), Some(vec![10]));

        cache.insert(3, vec![30]);

        assert!(!cache.contains(1));
        assert!(cache.contains(2));
        assert!(cache.contains(3));
        assert!(cache.current_bytes() <= entry_size * 2);
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
