// Time Window Container - Rolling time-based data window
// Generic container with automatic age-based pruning and capacity management

use std::collections::VecDeque;

/// Rolling time window that stores (timestamp, data) pairs
/// Automatically prunes old entries and enforces capacity limits
pub struct TimeWindow<T> {
    duration_ms: i64,           // Window duration in milliseconds
    max_capacity: usize,        // Maximum items to prevent unbounded growth
    data: VecDeque<(i64, T)>,   // (timestamp_ms, data) pairs
}

impl<T: Clone> TimeWindow<T> {
    /// Create a new time window
    /// duration_ms: How long to keep data (in milliseconds)
    /// max_capacity: Maximum number of items (prevents memory explosion)
    pub fn new(duration_ms: i64, max_capacity: usize) -> Self {
        Self {
            duration_ms,
            max_capacity,
            data: VecDeque::with_capacity(max_capacity.min(10000)),
        }
    }

    /// Add an item with its timestamp
    pub fn add(&mut self, timestamp: i64, item: T) {
        // Enforce capacity before adding
        if self.data.len() >= self.max_capacity {
            self.data.pop_front();
        }

        self.data.push_back((timestamp, item));
    }

    /// Remove items older than duration_ms from current_time
    pub fn prune(&mut self, current_time: i64) {
        let cutoff = current_time - self.duration_ms;

        while let Some((ts, _)) = self.data.front() {
            if *ts < cutoff {
                self.data.pop_front();
            } else {
                break;
            }
        }
    }

    /// Iterate over all items
    pub fn iter(&self) -> impl Iterator<Item = &(i64, T)> {
        self.data.iter()
    }

    /// Get number of items
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clear all items
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Get the most recent item
    pub fn last(&self) -> Option<&(i64, T)> {
        self.data.back()
    }

    /// Get the oldest item
    pub fn first(&self) -> Option<&(i64, T)> {
        self.data.front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut window: TimeWindow<i32> = TimeWindow::new(1000, 100);

        window.add(100, 1);
        window.add(200, 2);
        window.add(300, 3);

        assert_eq!(window.len(), 3);
        assert!(!window.is_empty());

        // Last should be (300, 3)
        let last = window.last().unwrap();
        assert_eq!(last.0, 300);
        assert_eq!(last.1, 3);
    }

    #[test]
    fn test_pruning() {
        let mut window: TimeWindow<i32> = TimeWindow::new(100, 100);

        window.add(0, 1);
        window.add(50, 2);
        window.add(100, 3);
        window.add(150, 4);
        window.add(200, 5);

        // Prune from time 200: items older than 100 should be removed
        window.prune(200);

        // Items with timestamp < 100 should be removed
        assert_eq!(window.len(), 3); // (100, 3), (150, 4), (200, 5)
    }

    #[test]
    fn test_capacity() {
        let mut window: TimeWindow<i32> = TimeWindow::new(10000, 3);

        window.add(100, 1);
        window.add(200, 2);
        window.add(300, 3);
        window.add(400, 4); // Should evict (100, 1)

        assert_eq!(window.len(), 3);
        let first = window.first().unwrap();
        assert_eq!(first.0, 200);
    }

    #[test]
    fn test_iteration() {
        let mut window: TimeWindow<i32> = TimeWindow::new(10000, 100);

        window.add(100, 1);
        window.add(200, 2);
        window.add(300, 3);

        let sum: i32 = window.iter().map(|(_, v)| v).sum();
        assert_eq!(sum, 6);
    }

    #[test]
    fn test_clear() {
        let mut window: TimeWindow<i32> = TimeWindow::new(10000, 100);

        window.add(100, 1);
        window.add(200, 2);

        window.clear();
        assert!(window.is_empty());
    }
}
