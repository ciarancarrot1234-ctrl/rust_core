// Liquidity Tracker - Monitors liquidity distribution in the order book
// Generates 10 liquidity-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer2::orderbook::OrderBookState;
use crate::layer3::common::event_types::*;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Liquidity regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiquidityRegime {
    Abundant,     // Deep liquidity on both sides
    Normal,       // Standard liquidity
    Thin,         // Low liquidity
    OneSided,     // Liquidity concentrated on one side
    Depleted,     // Very low liquidity
}

/// LiquidityTracker monitors liquidity distribution and depth
pub struct LiquidityTracker {
    symbol: String,

    // Liquidity metrics
    bid_depth: f64,
    ask_depth: f64,
    total_depth: f64,
    depth_imbalance: f64,

    // Previous values
    prev_bid_depth: f64,
    prev_ask_depth: f64,
    prev_total_depth: f64,

    // History for analysis
    depth_history: Vec<f64>,
    max_history: usize,
    avg_depth: f64,

    // Hole detection (price levels with no liquidity)
    detected_holes: Vec<(f64, f64)>,  // (price_start, price_end)
    hole_threshold: f64,

    // Regime tracking
    current_regime: LiquidityRegime,
    prev_regime: LiquidityRegime,

    // Vacuum detection (rapid liquidity removal)
    vacuum_rate: f64,
    vacuum_threshold: f64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl LiquidityTracker {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            bid_depth: 0.0,
            ask_depth: 0.0,
            total_depth: 0.0,
            depth_imbalance: 0.5,
            prev_bid_depth: 0.0,
            prev_ask_depth: 0.0,
            prev_total_depth: 0.0,
            depth_history: Vec::with_capacity(500),
            max_history: 500,
            avg_depth: 0.0,
            detected_holes: Vec::new(),
            hole_threshold: 0.1,
            current_regime: LiquidityRegime::Normal,
            prev_regime: LiquidityRegime::Normal,
            vacuum_rate: 0.0,
            vacuum_threshold: 0.3,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 500,
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    /// Process order book state update
    pub fn update(&mut self, state: &OrderBookState, current_time: i64) {
        self.updates_processed += 1;

        // Store previous values
        self.prev_bid_depth = self.bid_depth;
        self.prev_ask_depth = self.ask_depth;
        self.prev_total_depth = self.total_depth;
        self.prev_regime = self.current_regime;

        // Update current depth
        self.bid_depth = state.total_bid_volume;
        self.ask_depth = state.total_ask_volume;
        self.total_depth = self.bid_depth + self.ask_depth;

        // Calculate depth imbalance
        if self.total_depth > 0.0 {
            self.depth_imbalance = self.bid_depth / self.total_depth;
        }

        // Update history
        self.depth_history.push(self.total_depth);
        if self.depth_history.len() > self.max_history {
            self.depth_history.remove(0);
        }

        // Calculate average depth
        if !self.depth_history.is_empty() {
            self.avg_depth = self.depth_history.iter().sum::<f64>() / self.depth_history.len() as f64;
        }

        // Calculate vacuum rate (rate of liquidity removal)
        if self.prev_total_depth > 0.0 {
            self.vacuum_rate = (self.prev_total_depth - self.total_depth) / self.prev_total_depth;
        }

        // Update regime
        self.current_regime = self.classify_regime();
    }

    fn classify_regime(&self) -> LiquidityRegime {
        // Use thresholds relative to average
        let depth_ratio = if self.avg_depth > 0.0 {
            self.total_depth / self.avg_depth
        } else {
            1.0
        };

        // Check one-sided
        if self.depth_imbalance > 0.8 || self.depth_imbalance < 0.2 {
            return LiquidityRegime::OneSided;
        }

        if depth_ratio > 1.5 {
            LiquidityRegime::Abundant
        } else if depth_ratio > 0.7 {
            LiquidityRegime::Normal
        } else if depth_ratio > 0.3 {
            LiquidityRegime::Thin
        } else {
            LiquidityRegime::Depleted
        }
    }

    /// Check all liquidity events
    pub fn check_events(&mut self, current_time: i64) {
        self.check_liquidity_hole(current_time);
        self.check_liquidity_regime(current_time);
        self.check_liquidity_swept(current_time);
        self.check_liquidity_depth_threshold(current_time);
        self.check_liquidity_vacuum(current_time);
        self.check_liquidity_restored(current_time);
        self.check_liquidity_one_sided(current_time);
        self.check_liquidity_trap(current_time);
        self.check_liquidity_reconstruction(current_time);
        self.check_liquidity_migration(current_time);
    }

    /// Event 1: Liquidity hole detected
    fn check_liquidity_hole(&mut self, timestamp: i64) {
        if !self.detected_holes.is_empty() {
            if self.should_fire(OB_LIQUIDITY_HOLE, timestamp) {
                self.publish_event(
                    OB_LIQUIDITY_HOLE,
                    timestamp,
                    json!({
                        "hole_count": self.detected_holes.len(),
                        "holes": self.detected_holes.iter()
                            .map(|(start, end)| json!({"start": start, "end": end}))
                            .collect::<Vec<_>>(),
                    }),
                );
            }
        }
    }

    /// Event 2: Liquidity regime change
    fn check_liquidity_regime(&mut self, timestamp: i64) {
        if self.current_regime != self.prev_regime {
            if self.should_fire(OB_LIQUIDITY_REGIME, timestamp) {
                self.publish_event(
                    OB_LIQUIDITY_REGIME,
                    timestamp,
                    json!({
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                        "total_depth": self.total_depth,
                    }),
                );
            }
        }
    }

    /// Event 3: Liquidity swept (rapid removal)
    fn check_liquidity_swept(&mut self, timestamp: i64) {
        let swept_pct = if self.prev_total_depth > 0.0 {
            (self.prev_total_depth - self.total_depth) / self.prev_total_depth * 100.0
        } else {
            0.0
        };

        if swept_pct > 30.0 {
            if self.should_fire(OB_LIQUIDITY_SWEPT, timestamp) {
                self.publish_event(
                    OB_LIQUIDITY_SWEPT,
                    timestamp,
                    json!({
                        "swept_pct": swept_pct,
                        "prev_depth": self.prev_total_depth,
                        "new_depth": self.total_depth,
                    }),
                );
            }
        }
    }

    /// Event 4: Depth threshold crossed
    fn check_liquidity_depth_threshold(&mut self, timestamp: i64) {
        // Alert when depth drops below configurable threshold
        let depth_threshold = self.avg_depth * 0.5;
        if self.total_depth < depth_threshold {
            if self.should_fire(OB_LIQUIDITY_DEPTH_THRESHOLD, timestamp) {
                self.publish_event(
                    OB_LIQUIDITY_DEPTH_THRESHOLD,
                    timestamp,
                    json!({
                        "total_depth": self.total_depth,
                        "threshold": depth_threshold,
                        "avg_depth": self.avg_depth,
                    }),
                );
            }
        }
    }

    /// Event 5: Liquidity vacuum (sustained removal)
    fn check_liquidity_vacuum(&mut self, timestamp: i64) {
        if self.vacuum_rate > self.vacuum_threshold {
            if self.should_fire(OB_LIQUIDITY_VACUUM, timestamp) {
                self.publish_event(
                    OB_LIQUIDITY_VACUUM,
                    timestamp,
                    json!({
                        "vacuum_rate": self.vacuum_rate,
                        "total_depth": self.total_depth,
                        "prev_depth": self.prev_total_depth,
                    }),
                );
            }
        }
    }

    /// Event 6: Liquidity restored
    fn check_liquidity_restored(&mut self, timestamp: i64) {
        let restored_pct = if self.prev_total_depth > 0.0 {
            (self.total_depth - self.prev_total_depth) / self.prev_total_depth * 100.0
        } else {
            0.0
        };

        // Only fire if we were in thin/depleted state and now recovering
        let was_low = matches!(self.prev_regime, LiquidityRegime::Thin | LiquidityRegime::Depleted);
        if restored_pct > 50.0 && was_low {
            if self.should_fire(OB_LIQUIDITY_RESTORED, timestamp) {
                self.publish_event(
                    OB_LIQUIDITY_RESTORED,
                    timestamp,
                    json!({
                        "restored_pct": restored_pct,
                        "prev_depth": self.prev_total_depth,
                        "new_depth": self.total_depth,
                    }),
                );
            }
        }
    }

    /// Event 7: One-sided liquidity
    fn check_liquidity_one_sided(&mut self, timestamp: i64) {
        if self.current_regime == LiquidityRegime::OneSided {
            if self.should_fire(OB_LIQUIDITY_ONE_SIDED, timestamp) {
                self.publish_event(
                    OB_LIQUIDITY_ONE_SIDED,
                    timestamp,
                    json!({
                        "bid_depth": self.bid_depth,
                        "ask_depth": self.ask_depth,
                        "dominant_side": if self.depth_imbalance > 0.5 { "bid" } else { "ask" },
                        "imbalance": self.depth_imbalance,
                    }),
                );
            }
        }
    }

    /// Event 8: Liquidity trap detected
    fn check_liquidity_trap(&mut self, timestamp: i64) {
        // Trap: thin liquidity near current price with walls further out
        // This can trap traders who expect liquidity that isn't there
        let is_trap = matches!(self.current_regime, LiquidityRegime::Thin)
            && self.detected_holes.len() > 2;

        if is_trap {
            if self.should_fire(OB_LIQUIDITY_TRAP, timestamp) {
                self.publish_event(
                    OB_LIQUIDITY_TRAP,
                    timestamp,
                    json!({
                        "total_depth": self.total_depth,
                        "hole_count": self.detected_holes.len(),
                    }),
                );
            }
        }
    }

    /// Event 9: Liquidity reconstruction (rebuilding after sweep)
    fn check_liquidity_reconstruction(&mut self, timestamp: i64) {
        let is_reconstructing = self.total_depth > self.prev_total_depth
            && self.prev_total_depth < self.avg_depth * 0.5
            && self.total_depth < self.avg_depth;

        if is_reconstructing {
            if self.should_fire(OB_LIQUIDITY_RECONSTRUCTION, timestamp) {
                self.publish_event(
                    OB_LIQUIDITY_RECONSTRUCTION,
                    timestamp,
                    json!({
                        "current_depth": self.total_depth,
                        "target_depth": self.avg_depth,
                        "recovery_pct": (self.total_depth - self.prev_total_depth) / self.prev_total_depth * 100.0,
                    }),
                );
            }
        }
    }

    /// Event 10: Liquidity migration (moving to different price levels)
    fn check_liquidity_migration(&mut self, timestamp: i64) {
        // Detect when liquidity shifts from one side to another
        let bid_change = self.bid_depth - self.prev_bid_depth;
        let ask_change = self.ask_depth - self.prev_ask_depth;

        let migration_detected = (bid_change > 0.0 && ask_change < 0.0 && ask_change.abs() > self.avg_depth * 0.2)
            || (ask_change > 0.0 && bid_change < 0.0 && bid_change.abs() > self.avg_depth * 0.2);

        if migration_detected {
            if self.should_fire(OB_LIQUIDITY_MIGRATION, timestamp) {
                self.publish_event(
                    OB_LIQUIDITY_MIGRATION,
                    timestamp,
                    json!({
                        "bid_change": bid_change,
                        "ask_change": ask_change,
                        "direction": if bid_change > 0.0 { "to_bid" } else { "to_ask" },
                    }),
                );
            }
        }
    }

    fn should_fire(&self, event_type: &str, current_time: i64) -> bool {
        if let Some(&last_time) = self.last_event_times.get(event_type) {
            current_time - last_time >= self.min_event_interval_ms
        } else {
            true
        }
    }

    fn publish_event(&mut self, event_type: &str, timestamp: i64, mut data: serde_json::Value) {
        self.events_fired += 1;

        if let Some(obj) = data.as_object_mut() {
            obj.insert("symbol".to_string(), json!(self.symbol));
        }

        publish_event(
            event_type,
            timestamp,
            serde_json::from_value(data).unwrap_or_default(),
            "orderbook_aggregator",
            EventPriority::Medium,
        );

        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(symbol = %self.symbol, event = event_type, "Liquidity event published");
    }

    pub fn updates_processed(&self) -> u64 {
        self.updates_processed
    }

    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    pub fn total_depth(&self) -> f64 {
        self.total_depth
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state(bid_vol: f64, ask_vol: f64) -> OrderBookState {
        OrderBookState {
            timestamp: 0,
            last_update_id: 0,
            best_bid: 100.0,
            best_bid_qty: bid_vol / 10.0,
            best_ask: 100.1,
            best_ask_qty: ask_vol / 10.0,
            mid_price: 100.05,
            spread: 0.1,
            spread_bps: 10.0,
            total_bid_volume: bid_vol,
            total_ask_volume: ask_vol,
            is_synced: true,
        }
    }

    #[test]
    fn test_liquidity_tracking() {
        let thresholds = AggregatorThresholds::default();
        let mut tracker = LiquidityTracker::new("BTCUSDT".to_string(), thresholds);

        tracker.update(&make_state(1000.0, 1000.0), 1000);
        tracker.update(&make_state(800.0, 800.0), 2000);

        assert!((tracker.total_depth() - 1600.0).abs() < 0.001);
    }

    #[test]
    fn test_regime_classification() {
        let thresholds = AggregatorThresholds::default();
        let mut tracker = LiquidityTracker::new("BTCUSDT".to_string(), thresholds);

        // Build up average
        for i in 0..10 {
            tracker.update(&make_state(100.0, 100.0), i as i64 * 1000);
        }

        // Normal depth
        tracker.update(&make_state(100.0, 100.0), 10000);
        assert_eq!(tracker.classify_regime(), LiquidityRegime::Normal);

        // One-sided
        tracker.update(&make_state(90.0, 10.0), 11000);
        assert_eq!(tracker.classify_regime(), LiquidityRegime::OneSided);
    }
}
