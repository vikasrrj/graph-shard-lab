use std::collections::VecDeque;

#[derive(Debug)]
pub struct LruCache {
    capacity: usize,
    entries: VecDeque<u64>,
}

impl LruCache {
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
            // remove the old position and move it to the newest position.
            self.entries.remove(position);
            self.entries.push_back(user_id);

            return true;
        }

        // Cache miss:
        // remove the least recently used entry when the cache is full.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_access_is_a_miss_and_second_is_a_hit() {
        let mut cache = LruCache::new(3).unwrap();

        assert!(!cache.access(10));
        assert!(cache.access(10));
    }

    #[test]
    fn never_exceeds_capacity() {
        let mut cache = LruCache::new(2).unwrap();

        cache.access(10);
        cache.access(20);
        cache.access(30);

        assert_eq!(cache.len(), 2);
        assert!(!cache.contains(10));
        assert!(cache.contains(20));
        assert!(cache.contains(30));
    }

    #[test]
    fn evicts_the_least_recently_used_entry() {
        let mut cache = LruCache::new(3).unwrap();

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
        assert!(LruCache::new(0).is_err());
    }
}
