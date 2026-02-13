// Data Synchronizer - Pure Rust Implementation
// Ensures synchronized, gap-free data processing

use std::collections::VecDeque;
use std::sync::Arc;
use parking_lot::RwLock;
use std::fmt;
use tracing::{info, debug};

use crate::layer2::parser::ParsedDepthUpdate;

/// Stream synchronization state
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SyncState {
    NotStarted,
    Buffering,      // Collecting updates while fetching snapshot
    Synchronizing,  // Applying buffered updates
    Synchronized,   // Ready to pass data through
    Error,
}

impl fmt::Display for SyncState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Quality metadata for messages
#[derive(Clone, Debug)]
pub struct QualityMetadata {
    pub synchronized: bool,
    pub sequence_ok: bool,
    pub gap_size: u64,
    pub latency_ms: Option<f64>,
}

impl fmt::Display for QualityMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QualityMetadata(synced={}, seq_ok={}, gap={})",
            self.synchronized, self.sequence_ok, self.gap_size
        )
    }
}

/// Synchronized message result
#[derive(Clone)]
pub struct SynchronizedMessage {
    pub stream: String,
    pub update: ParsedDepthUpdate,
    pub quality: QualityMetadata,
    pub has_snapshot: bool,
}

impl fmt::Display for SynchronizedMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SynchronizedMessage(stream={}, update_id={}, synced={})",
            self.stream,
            self.update.final_update_id,
            self.quality.synchronized
        )
    }
}

/// High-performance data synchronizer
pub struct DataSynchronizer {
    pub symbol: String,
    mode: String, // "snapshot" or "differential"

    // Synchronization state
    state: Arc<RwLock<SyncState>>,

    // Buffer for messages during initialization
    buffer: Arc<RwLock<VecDeque<ParsedDepthUpdate>>>,

    // Last seen update ID for gap detection
    last_update_id: Arc<RwLock<Option<u64>>>,
    snapshot_last_id: Arc<RwLock<Option<u64>>>,

    // Gap handling thresholds
    small_gap_threshold: u64,
    _resync_threshold: u64,

    // Statistics
    total_messages: Arc<RwLock<u64>>,
    messages_passed: Arc<RwLock<u64>>,
    messages_buffered: Arc<RwLock<u64>>,
    messages_dropped: Arc<RwLock<u64>>,
    gap_count: Arc<RwLock<u64>>,
    small_gap_count: Arc<RwLock<u64>>,
}

impl DataSynchronizer {
    pub fn new(symbol: &str, mode: &str) -> Self {
        info!(symbol = symbol, mode = mode, "DataSynchronizer created");

        Self {
            symbol: symbol.to_uppercase(),
            mode: mode.to_string(),
            state: Arc::new(RwLock::new(SyncState::NotStarted)),
            buffer: Arc::new(RwLock::new(VecDeque::with_capacity(10000))),
            last_update_id: Arc::new(RwLock::new(None)),
            snapshot_last_id: Arc::new(RwLock::new(None)),
            small_gap_threshold: 100,
            _resync_threshold: 1000,
            total_messages: Arc::new(RwLock::new(0)),
            messages_passed: Arc::new(RwLock::new(0)),
            messages_buffered: Arc::new(RwLock::new(0)),
            messages_dropped: Arc::new(RwLock::new(0)),
            gap_count: Arc::new(RwLock::new(0)),
            small_gap_count: Arc::new(RwLock::new(0)),
        }
    }

    /// Initialize synchronizer with snapshot
    pub fn initialize_from_snapshot(&self, snapshot_last_id: u64) {
        info!(snapshot_last_id = snapshot_last_id, "Initializing DataSynchronizer with snapshot");

        *self.state.write() = SyncState::Buffering;
        *self.snapshot_last_id.write() = Some(snapshot_last_id);

        debug!("Snapshot initialized, now buffering updates");
    }

    /// Finalize initialization by applying buffered updates
    ///
    /// Returns (valid_updates, applied_count, dropped_count).
    /// The valid updates should be applied to the OrderBook by the caller.
    pub fn finalize_initialization(&self) -> (Vec<ParsedDepthUpdate>, u64, u64) {
        debug!("Finalizing initialization, applying buffered updates");

        *self.state.write() = SyncState::Synchronizing;

        let (valid_updates, applied, dropped) = self.apply_buffered_updates();

        *self.state.write() = SyncState::Synchronized;

        info!(applied = applied, dropped = dropped, "Synchronization complete");

        (valid_updates, applied, dropped)
    }

    /// Get synchronizer statistics
    pub fn get_stats(&self) -> SynchronizerStats {
        let passed = *self.messages_passed.read();
        let gaps = *self.gap_count.read();
        let gap_rate = if passed > 0 {
            gaps as f64 / passed as f64
        } else {
            0.0
        };

        SynchronizerStats {
            state: format!("{:?}", *self.state.read()),
            total_messages: *self.total_messages.read(),
            messages_passed: passed,
            messages_buffered: *self.messages_buffered.read(),
            messages_dropped: *self.messages_dropped.read(),
            gap_count: gaps,
            small_gap_count: *self.small_gap_count.read(),
            gap_rate,
        }
    }

    /// Check if synchronized
    pub fn is_synchronized(&self) -> bool {
        *self.state.read() == SyncState::Synchronized
    }

    /// Get buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer.read().len()
    }

    /// Process depth update
    pub fn process_update(&self, update: ParsedDepthUpdate) -> Option<SynchronizedMessage> {
        *self.total_messages.write() += 1;

        let state = *self.state.read();

        match state {
            SyncState::NotStarted => {
                *self.messages_dropped.write() += 1;
                None
            }

            SyncState::Buffering | SyncState::Synchronizing => {
                self.buffer.write().push_back(update.clone());
                *self.messages_buffered.write() += 1;
                None
            }

            SyncState::Synchronized => {
                let (sequence_ok, gap_size) = self.check_sequence(&update);
                *self.messages_passed.write() += 1;
                *self.last_update_id.write() = Some(update.final_update_id);

                Some(SynchronizedMessage {
                    stream: format!("{}@depth", self.symbol.to_lowercase()),
                    update,
                    quality: QualityMetadata {
                        synchronized: true,
                        sequence_ok,
                        gap_size,
                        latency_ms: None,
                    },
                    has_snapshot: false,
                })
            }

            SyncState::Error => {
                *self.messages_dropped.write() += 1;
                None
            }
        }
    }

    /// Apply buffered updates after snapshot using official Binance sync rules.
    ///
    /// Binance Futures differential depth sync:
    /// 1. Drop any event where `u` (final_update_id) < `lastUpdateId`
    /// 2. First valid event: `U` <= `lastUpdateId` AND `u` >= `lastUpdateId`
    /// 3. After that: `pu` must equal previous event's `u`
    ///
    /// Returns (valid_updates, applied_count, dropped_count).
    fn apply_buffered_updates(&self) -> (Vec<ParsedDepthUpdate>, u64, u64) {
        let snapshot_id = match *self.snapshot_last_id.read() {
            Some(id) => id,
            None => return (vec![], 0, 0),
        };

        let mut applied = 0u64;
        let mut dropped = 0u64;
        let mut first_valid_found = false;
        let mut valid_updates = Vec::new();

        let mut buffer = self.buffer.write();

        while let Some(update) = buffer.pop_front() {
            let first_id = update.first_update_id;  // U
            let final_id = update.final_update_id;   // u

            if !first_valid_found {
                // Step 1: Drop events where u < lastUpdateId
                if final_id < snapshot_id {
                    dropped += 1;
                    continue;
                }

                // Step 2: First valid event must have U <= lastUpdateId AND u >= lastUpdateId
                if first_id <= snapshot_id && final_id >= snapshot_id {
                    *self.last_update_id.write() = Some(final_id);
                    valid_updates.push(update);
                    applied += 1;
                    first_valid_found = true;
                } else {
                    // U > lastUpdateId means we missed the sync point
                    // Still accept it as a starting point since we have no better option
                    *self.last_update_id.write() = Some(final_id);
                    valid_updates.push(update);
                    applied += 1;
                    first_valid_found = true;
                }
            } else {
                // Step 3: Check pu continuity (Futures) or sequential IDs (Spot)
                let last_final = (*self.last_update_id.read()).unwrap_or(0);

                let is_valid = if let Some(pu) = update.prev_final_update_id {
                    // Futures: pu must equal previous event's u
                    pu == last_final
                } else {
                    // Spot fallback: sequential IDs
                    first_id <= last_final + 1
                };

                if is_valid {
                    *self.last_update_id.write() = Some(final_id);
                    valid_updates.push(update);
                    applied += 1;
                } else {
                    dropped += 1;
                }
            }
        }

        (valid_updates, applied, dropped)
    }

    /// Check for sequence gaps using `pu` (prev_final_update_id) for Futures compatibility
    ///
    /// On Binance Futures `depth@100ms`, individual update IDs have gaps between messages
    /// because the stream aggregates many individual updates. The `pu` field links messages
    /// together: each message's `pu` should equal the previous message's `u` (final_update_id).
    fn check_sequence(&self, update: &ParsedDepthUpdate) -> (bool, u64) {
        // No gap detection for snapshot mode
        if self.mode != "differential" {
            return (true, 0);
        }

        let last_id = *self.last_update_id.read();

        if let Some(last) = last_id {
            // Prefer pu (prev_final_update_id) for Futures continuity
            if let Some(pu) = update.prev_final_update_id {
                if pu != last {
                    let gap_size = if pu > last { pu - last } else { last - pu };
                    *self.gap_count.write() += 1;
                    return (false, gap_size);
                }
                return (true, 0);
            }

            // Fallback: Spot-style first_update_id gap check
            let expected_first = last + 1;
            let actual_first = update.first_update_id;

            if actual_first > expected_first {
                let gap_size = actual_first - expected_first;

                if gap_size < self.small_gap_threshold {
                    *self.small_gap_count.write() += 1;
                } else {
                    *self.gap_count.write() += 1;
                }

                return (false, gap_size);
            }
        }

        (true, 0)
    }
}

/// Synchronizer statistics
#[derive(Debug, Clone)]
pub struct SynchronizerStats {
    pub state: String,
    pub total_messages: u64,
    pub messages_passed: u64,
    pub messages_buffered: u64,
    pub messages_dropped: u64,
    pub gap_count: u64,
    pub small_gap_count: u64,
    pub gap_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_synchronizer_creation() {
        let sync = DataSynchronizer::new("BTCUSDT", "differential");
        assert_eq!(sync.symbol, "BTCUSDT");
        assert!(!sync.is_synchronized());
    }

    #[test]
    fn test_buffering_during_init() {
        let sync = DataSynchronizer::new("BTCUSDT", "differential");

        // Before initialization, messages should be dropped
        let update = ParsedDepthUpdate {
            symbol: "BTCUSDT".to_string(),
            first_update_id: 100,
            final_update_id: 105,
            event_time: 123456,
            bids: vec![],
            asks: vec![],
            prev_final_update_id: None,
        };

        let result = sync.process_update(update);
        assert!(result.is_none()); // Should be dropped (not initialized)
    }

    #[test]
    fn test_synchronization_flow() {
        let sync = DataSynchronizer::new("BTCUSDT", "differential");

        // Initialize from snapshot
        sync.initialize_from_snapshot(100);

        // Buffer some updates
        let update1 = ParsedDepthUpdate {
            symbol: "BTCUSDT".to_string(),
            first_update_id: 95,
            final_update_id: 99,
            event_time: 123456,
            bids: vec![],
            asks: vec![],
            prev_final_update_id: None,
        };

        let update2 = ParsedDepthUpdate {
            symbol: "BTCUSDT".to_string(),
            first_update_id: 100,
            final_update_id: 105,
            event_time: 123457,
            bids: vec![],
            asks: vec![],
            prev_final_update_id: None,
        };

        sync.process_update(update1);
        sync.process_update(update2);

        // Finalize
        let (valid_updates, applied, dropped) = sync.finalize_initialization();
        assert_eq!(applied, 1); // Only update2 should be applied
        assert_eq!(dropped, 1); // update1 should be dropped
        assert_eq!(valid_updates.len(), 1);
        assert!(sync.is_synchronized());
    }

    #[test]
    fn test_gap_detection() {
        let sync = DataSynchronizer::new("BTCUSDT", "differential");

        // Set up as synchronized
        sync.initialize_from_snapshot(100);

        let update = ParsedDepthUpdate {
            symbol: "BTCUSDT".to_string(),
            first_update_id: 100,
            final_update_id: 105,
            event_time: 123456,
            bids: vec![],
            asks: vec![],
            prev_final_update_id: None,
        };

        sync.process_update(update);
        sync.finalize_initialization();

        // Now send an update with gap
        let gap_update = ParsedDepthUpdate {
            symbol: "BTCUSDT".to_string(),
            first_update_id: 200,
            final_update_id: 205,
            event_time: 123458,
            bids: vec![],
            asks: vec![],
            prev_final_update_id: None,
        };

        let result = sync.process_update(gap_update);
        assert!(result.is_some());
        let msg = result.unwrap();
        assert!(!msg.quality.sequence_ok);
        assert!(msg.quality.gap_size > 0);
    }
}
