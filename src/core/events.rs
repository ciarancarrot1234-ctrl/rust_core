// Event System for Liquidity Hunt
// Pure Rust - central pub/sub event bus using tokio channels

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock};
use tokio::sync::broadcast;
use parking_lot::RwLock;
use uuid::Uuid;

// ============================================================================
// Event Priority
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EventPriority {
    Critical = 1,  // Cascades, systemic risk
    High = 2,      // Large liquidations, whale activity
    Medium = 3,    // Threshold crossings, regime changes
    Low = 4,       // Trend confirmations
    Info = 5,      // Minor updates
}

impl fmt::Display for EventPriority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

// ============================================================================
// Event
// ============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub event_type: String,
    pub timestamp: i64,
    pub data: HashMap<String, serde_json::Value>,
    pub source: String,
    pub priority: EventPriority,
    pub event_id: String,
}

impl Event {
    pub fn new(
        event_type: String,
        timestamp: i64,
        data: HashMap<String, serde_json::Value>,
        source: String,
        priority: EventPriority,
    ) -> Self {
        Self {
            event_type,
            timestamp,
            data,
            source,
            priority,
            event_id: Uuid::new_v4().to_string(),
        }
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Event(type={}, source={}, priority={:?}, id={})",
            self.event_type,
            self.source,
            self.priority,
            &self.event_id[..8]
        )
    }
}

// ============================================================================
// Event Bus
// ============================================================================

type EventCallback = Arc<dyn Fn(Event) + Send + Sync>;

pub struct EventBus {
    tx: broadcast::Sender<Event>,
    subscribers: Arc<RwLock<HashMap<String, Vec<EventCallback>>>>,
    wildcard_subscribers: Arc<RwLock<Vec<EventCallback>>>,
    event_history: Arc<RwLock<Vec<Event>>>,
    max_history: usize,
    stats: Arc<RwLock<EventBusStats>>,
}

#[derive(Debug, Clone, Default)]
struct EventBusStats {
    total_published: u64,
    total_delivered: u64,
    errors: u64,
}

impl EventBus {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(10000);

        Self {
            tx,
            subscribers: Arc::new(RwLock::new(HashMap::new())),
            wildcard_subscribers: Arc::new(RwLock::new(Vec::new())),
            event_history: Arc::new(RwLock::new(Vec::new())),
            max_history: 10000,
            stats: Arc::new(RwLock::new(EventBusStats::default())),
        }
    }

    /// Subscribe to events by type, or "*" for all events
    pub fn subscribe<F>(&self, event_type: &str, callback: F)
    where
        F: Fn(Event) + Send + Sync + 'static,
    {
        let callback = Arc::new(callback);

        if event_type == "*" {
            self.wildcard_subscribers.write().push(callback);
        } else {
            let mut subscribers = self.subscribers.write();
            subscribers
                .entry(event_type.to_string())
                .or_default()
                .push(callback);
        }

        tracing::info!(event_type = %event_type, "Subscribed to events");
    }

    /// Publish event to all subscribers
    pub fn publish(&self, event: Event) {
        // Update stats
        {
            let mut stats = self.stats.write();
            stats.total_published += 1;
        }

        // Store in history
        {
            let mut history = self.event_history.write();
            history.push(event.clone());
            if history.len() > self.max_history {
                history.remove(0);
            }
        }

        // Send to broadcast channel
        let _ = self.tx.send(event.clone());

        // Deliver to direct subscribers
        let subscribers = self.subscribers.read();
        if let Some(callbacks) = subscribers.get(&event.event_type) {
            let mut stats = self.stats.write();
            for callback in callbacks {
                callback(event.clone());
                stats.total_delivered += 1;
            }
        }

        // Deliver to wildcard subscribers
        let wildcard = self.wildcard_subscribers.read();
        let mut stats = self.stats.write();
        for callback in wildcard.iter() {
            callback(event.clone());
            stats.total_delivered += 1;
        }
    }

    /// Get a receiver for all events (broadcast channel)
    pub fn subscribe_channel(&self) -> broadcast::Receiver<Event> {
        self.tx.subscribe()
    }

    /// Get recent events from history
    pub fn get_recent_events(&self, event_type: Option<&str>, limit: Option<usize>) -> Vec<Event> {
        let history = self.event_history.read();
        let limit = limit.unwrap_or(100);

        let events: Vec<Event> = if let Some(et) = event_type {
            history
                .iter()
                .filter(|e| e.event_type == et)
                .cloned()
                .collect()
        } else {
            history.iter().cloned().collect()
        };

        events.into_iter().rev().take(limit).collect()
    }

    /// Get event bus statistics
    pub fn get_stats(&self) -> EventBusStatsSnapshot {
        let stats = self.stats.read();
        let subscribers = self.subscribers.read();
        let wildcard = self.wildcard_subscribers.read();
        let history = self.event_history.read();

        EventBusStatsSnapshot {
            total_published: stats.total_published,
            total_delivered: stats.total_delivered,
            errors: stats.errors,
            subscriber_count: subscribers.len() + wildcard.len(),
            event_types: subscribers.keys().cloned().collect(),
            history_size: history.len(),
        }
    }

    /// Clear event history
    pub fn clear_history(&self) {
        self.event_history.write().clear();
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of event bus statistics
#[derive(Debug, Clone)]
pub struct EventBusStatsSnapshot {
    pub total_published: u64,
    pub total_delivered: u64,
    pub errors: u64,
    pub subscriber_count: usize,
    pub event_types: Vec<String>,
    pub history_size: usize,
}

// ============================================================================
// Global Event Bus (thread-safe singleton)
// ============================================================================

static GLOBAL_EVENT_BUS: OnceLock<Arc<EventBus>> = OnceLock::new();

/// Get global event bus instance (singleton)
pub fn get_event_bus() -> Arc<EventBus> {
    Arc::clone(GLOBAL_EVENT_BUS.get_or_init(|| Arc::new(EventBus::new())))
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Publish event (convenience function)
pub fn publish_event(
    event_type: &str,
    timestamp: i64,
    data: HashMap<String, serde_json::Value>,
    source: &str,
    priority: EventPriority,
) {
    let bus = get_event_bus();
    let event = Event::new(
        event_type.to_string(),
        timestamp,
        data,
        source.to_string(),
        priority,
    );
    bus.publish(event);
}

/// Subscribe to events (convenience function)
pub fn subscribe_to_events<F>(event_type: &str, callback: F)
where
    F: Fn(Event) + Send + Sync + 'static,
{
    let bus = get_event_bus();
    bus.subscribe(event_type, callback);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_priority() {
        assert!(EventPriority::Critical < EventPriority::High);
        assert!(EventPriority::High < EventPriority::Medium);
    }

    #[test]
    fn test_event_creation() {
        let event = Event::new(
            "test".to_string(),
            123456789,
            HashMap::new(),
            "test_source".to_string(),
            EventPriority::Info,
        );
        assert_eq!(event.event_type, "test");
        assert_eq!(event.source, "test_source");
        assert!(!event.event_id.is_empty());
    }

    #[test]
    fn test_event_bus() {
        let bus = EventBus::new();
        assert!(bus.get_recent_events(None, Some(10)).is_empty());
    }

    #[test]
    fn test_event_bus_subscribe_and_publish() {
        let bus = EventBus::new();

        let received = Arc::new(RwLock::new(false));
        let received_clone = Arc::clone(&received);

        bus.subscribe("test", move |_event| {
            *received_clone.write() = true;
        });

        let event = Event::new(
            "test".to_string(),
            123456789,
            HashMap::new(),
            "test_source".to_string(),
            EventPriority::Info,
        );

        bus.publish(event);

        assert!(*received.read());
    }

    #[test]
    fn test_event_history() {
        let bus = EventBus::new();

        let event = Event::new(
            "test".to_string(),
            123456789,
            HashMap::new(),
            "test_source".to_string(),
            EventPriority::Info,
        );

        bus.publish(event);

        let history = bus.get_recent_events(None, Some(10));
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].event_type, "test");
    }

    #[test]
    fn test_event_bus_stats() {
        let bus = EventBus::new();
        let stats = bus.get_stats();
        assert_eq!(stats.total_published, 0);
    }
}
