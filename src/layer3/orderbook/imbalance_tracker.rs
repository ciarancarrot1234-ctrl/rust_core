// Imbalance Tracker - Monitors order book bid/ask imbalances
// Generates 12 imbalance-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer2::orderbook::OrderBookState;
use crate::layer3::common::event_types::*;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Imbalance regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImbalanceRegime {
    StrongBidSide,   // > 70% bid volume
    BidSide,         // 60-70% bid volume
    SlightBidSide,   // 55-60% bid volume
    Balanced,        // 45-55%
    SlightAskSide,   // 40-45% ask volume
    AskSide,         // 30-40% ask volume
    StrongAskSide,   // < 30% bid volume
}

/// ImbalanceTracker monitors order book bid/ask imbalances across depths
pub struct ImbalanceTracker {
    symbol: String,

    // Current imbalance at multiple depths
    imbalance_top: f64,         // Best bid/ask only
    imbalance_depth_3: f64,     // Top 3 levels
    imbalance_depth_5: f64,     // Top 5 levels
    imbalance_depth_10: f64,    // Top 10 levels

    // Previous values for delta calculation
    prev_imbalance_top: f64,
    prev_imbalance_depth_5: f64,

    // Imbalance velocity and acceleration
    imbalance_velocity: f64,
    imbalance_acceleration: f64,
    prev_velocity: f64,

    // Regime tracking
    current_regime: ImbalanceRegime,
    prev_regime: ImbalanceRegime,

    // Oscillation tracking
    oscillation_count: u32,
    oscillation_window: Vec<f64>,
    oscillation_threshold: f64,

    // Persistence tracking
    persistence_count: u64,
    persistence_threshold: f64,

    // History for analysis
    imbalance_history: Vec<f64>,
    max_history: usize,

    // Cross-depth divergence
    depth_divergence: f64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl ImbalanceTracker {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            imbalance_top: 0.5,
            imbalance_depth_3: 0.5,
            imbalance_depth_5: 0.5,
            imbalance_depth_10: 0.5,
            prev_imbalance_top: 0.5,
            prev_imbalance_depth_5: 0.5,
            imbalance_velocity: 0.0,
            imbalance_acceleration: 0.0,
            prev_velocity: 0.0,
            current_regime: ImbalanceRegime::Balanced,
            prev_regime: ImbalanceRegime::Balanced,
            oscillation_count: 0,
            oscillation_window: Vec::with_capacity(20),
            oscillation_threshold: 0.3,
            persistence_count: 0,
            persistence_threshold: 0.1,
            imbalance_history: Vec::with_capacity(1000),
            max_history: 1000,
            depth_divergence: 0.0,
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
        self.prev_imbalance_top = self.imbalance_top;
        self.prev_imbalance_depth_5 = self.imbalance_depth_5;
        self.prev_velocity = self.imbalance_velocity;
        self.prev_regime = self.current_regime;

        // Calculate imbalance at top level
        // imbalance = bid_vol / (bid_vol + ask_vol)
        let total_top = state.best_bid_qty + state.best_ask_qty;
        if total_top > 0.0 {
            self.imbalance_top = state.best_bid_qty / total_top;
        }

        // For deeper levels, we approximate using total volumes
        let total_vol = state.total_bid_volume + state.total_ask_volume;
        if total_vol > 0.0 {
            self.imbalance_depth_10 = state.total_bid_volume / total_vol;
            // Approximate depth_3 and depth_5 as weighted averages
            self.imbalance_depth_3 = self.imbalance_top * 0.7 + self.imbalance_depth_10 * 0.3;
            self.imbalance_depth_5 = self.imbalance_top * 0.5 + self.imbalance_depth_10 * 0.5;
        }

        // Calculate velocity and acceleration
        self.imbalance_velocity = self.imbalance_top - self.prev_imbalance_top;
        self.imbalance_acceleration = self.imbalance_velocity - self.prev_velocity;

        // Update regime
        self.current_regime = self.classify_regime(self.imbalance_top);

        // Update history
        self.imbalance_history.push(self.imbalance_top);
        if self.imbalance_history.len() > self.max_history {
            self.imbalance_history.remove(0);
        }

        // Calculate cross-depth divergence
        self.depth_divergence = (self.imbalance_top - self.imbalance_depth_10).abs();

        // Track oscillation
        self.update_oscillation();

        // Track persistence
        self.update_persistence();
    }

    /// Check all imbalance events
    pub fn check_events(&mut self, current_time: i64) {
        self.check_imbalance_threshold(current_time);
        self.check_imbalance_shift(current_time);
        self.check_imbalance_regime_change(current_time);
        self.check_imbalance_divergence(current_time);
        self.check_imbalance_alignment(current_time);
        self.check_imbalance_reversal(current_time);
        self.check_imbalance_acceleration(current_time);
        self.check_imbalance_exhaustion(current_time);
        self.check_imbalance_oscillation(current_time);
        self.check_imbalance_hidden(current_time);
        self.check_imbalance_persistence(current_time);
        self.check_imbalance_breakdown(current_time);
    }

    fn update_oscillation(&mut self) {
        self.oscillation_window.push(self.imbalance_top);
        if self.oscillation_window.len() > 20 {
            self.oscillation_window.remove(0);
        }

        if self.oscillation_window.len() >= 3 {
            let len = self.oscillation_window.len();
            let prev = self.oscillation_window[len - 2];
            let prev_prev = self.oscillation_window[len - 3];
            let curr = self.imbalance_top;

            // Check for direction change (peak or valley)
            let was_rising = prev > prev_prev;
            let now_falling = curr < prev;

            if (was_rising && now_falling) || (!was_rising && !now_falling && curr > prev) {
                self.oscillation_count += 1;
            }
        }
    }

    fn update_persistence(&mut self) {
        if (self.imbalance_top - self.prev_imbalance_top).abs() < self.persistence_threshold {
            self.persistence_count += 1;
        } else {
            self.persistence_count = 0;
        }
    }

    fn classify_regime(&self, imbalance: f64) -> ImbalanceRegime {
        if imbalance > 0.7 {
            ImbalanceRegime::StrongBidSide
        } else if imbalance > 0.6 {
            ImbalanceRegime::BidSide
        } else if imbalance > 0.55 {
            ImbalanceRegime::SlightBidSide
        } else if imbalance >= 0.45 {
            ImbalanceRegime::Balanced
        } else if imbalance >= 0.4 {
            ImbalanceRegime::SlightAskSide
        } else if imbalance >= 0.3 {
            ImbalanceRegime::AskSide
        } else {
            ImbalanceRegime::StrongAskSide
        }
    }

    /// Event 1: Imbalance threshold exceeded
    fn check_imbalance_threshold(&mut self, timestamp: i64) {
        let imbalance_pct = (self.imbalance_top - 0.5).abs() * 2.0; // 0-1 scale
        if imbalance_pct > self.thresholds.imbalance_threshold {
            if self.should_fire(OB_IMBALANCE_THRESHOLD, timestamp) {
                self.publish_event(
                    OB_IMBALANCE_THRESHOLD,
                    timestamp,
                    json!({
                        "imbalance": self.imbalance_top,
                        "threshold": self.thresholds.imbalance_threshold,
                        "direction": if self.imbalance_top > 0.5 { "bid" } else { "ask" },
                    }),
                );
            }
        }
    }

    /// Event 2: Imbalance shift detected
    fn check_imbalance_shift(&mut self, timestamp: i64) {
        let shift = (self.imbalance_top - self.prev_imbalance_top).abs();
        if shift > 0.15 {
            if self.should_fire(OB_IMBALANCE_SHIFT, timestamp) {
                self.publish_event(
                    OB_IMBALANCE_SHIFT,
                    timestamp,
                    json!({
                        "prev_imbalance": self.prev_imbalance_top,
                        "new_imbalance": self.imbalance_top,
                        "shift": shift,
                    }),
                );
            }
        }
    }

    /// Event 3: Imbalance regime change
    fn check_imbalance_regime_change(&mut self, timestamp: i64) {
        if self.current_regime != self.prev_regime {
            if self.should_fire(OB_IMBALANCE_REGIME, timestamp) {
                self.publish_event(
                    OB_IMBALANCE_REGIME,
                    timestamp,
                    json!({
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                        "imbalance": self.imbalance_top,
                    }),
                );
            }
        }
    }

    /// Event 4: Cross-depth divergence
    fn check_imbalance_divergence(&mut self, timestamp: i64) {
        if self.depth_divergence > 0.2 {
            if self.should_fire(OB_IMBALANCE_DIVERGENCE, timestamp) {
                self.publish_event(
                    OB_IMBALANCE_DIVERGENCE,
                    timestamp,
                    json!({
                        "imbalance_top": self.imbalance_top,
                        "imbalance_depth_10": self.imbalance_depth_10,
                        "divergence": self.depth_divergence,
                    }),
                );
            }
        }
    }

    /// Event 5: Multi-depth alignment
    fn check_imbalance_alignment(&mut self, timestamp: i64) {
        let aligned = (self.imbalance_top - self.imbalance_depth_5).abs() < 0.05
            && (self.imbalance_depth_5 - self.imbalance_depth_10).abs() < 0.05
            && (self.imbalance_top - 0.5).abs() > 0.15;

        if aligned {
            if self.should_fire(OB_IMBALANCE_ALIGNMENT, timestamp) {
                self.publish_event(
                    OB_IMBALANCE_ALIGNMENT,
                    timestamp,
                    json!({
                        "imbalance_top": self.imbalance_top,
                        "imbalance_depth_5": self.imbalance_depth_5,
                        "imbalance_depth_10": self.imbalance_depth_10,
                        "direction": if self.imbalance_top > 0.5 { "bid" } else { "ask" },
                    }),
                );
            }
        }
    }

    /// Event 6: Imbalance reversal
    fn check_imbalance_reversal(&mut self, timestamp: i64) {
        let crossed_half = (self.prev_imbalance_top > 0.5 && self.imbalance_top < 0.5)
            || (self.prev_imbalance_top < 0.5 && self.imbalance_top > 0.5);

        if crossed_half {
            if self.should_fire(OB_IMBALANCE_REVERSAL, timestamp) {
                self.publish_event(
                    OB_IMBALANCE_REVERSAL,
                    timestamp,
                    json!({
                        "prev_imbalance": self.prev_imbalance_top,
                        "new_imbalance": self.imbalance_top,
                        "direction": if self.imbalance_top > 0.5 { "bid" } else { "ask" },
                    }),
                );
            }
        }
    }

    /// Event 7: Imbalance acceleration
    fn check_imbalance_acceleration(&mut self, timestamp: i64) {
        if self.imbalance_acceleration.abs() > 0.05 {
            if self.should_fire(OB_IMBALANCE_ACCELERATION, timestamp) {
                self.publish_event(
                    OB_IMBALANCE_ACCELERATION,
                    timestamp,
                    json!({
                        "acceleration": self.imbalance_acceleration,
                        "velocity": self.imbalance_velocity,
                        "imbalance": self.imbalance_top,
                    }),
                );
            }
        }
    }

    /// Event 8: Imbalance exhaustion (extreme + slowing)
    fn check_imbalance_exhaustion(&mut self, timestamp: i64) {
        let is_extreme = self.imbalance_top > 0.8 || self.imbalance_top < 0.2;
        let is_slowing = self.imbalance_velocity.abs() < self.prev_velocity.abs();

        if is_extreme && is_slowing {
            if self.should_fire(OB_IMBALANCE_EXHAUSTION, timestamp) {
                self.publish_event(
                    OB_IMBALANCE_EXHAUSTION,
                    timestamp,
                    json!({
                        "imbalance": self.imbalance_top,
                        "velocity": self.imbalance_velocity,
                        "direction": if self.imbalance_top > 0.5 { "bid" } else { "ask" },
                    }),
                );
            }
        }
    }

    /// Event 9: Imbalance oscillation
    fn check_imbalance_oscillation(&mut self, timestamp: i64) {
        if self.oscillation_count >= 3 {
            if self.should_fire(OB_IMBALANCE_OSCILLATION, timestamp) {
                self.publish_event(
                    OB_IMBALANCE_OSCILLATION,
                    timestamp,
                    json!({
                        "oscillation_count": self.oscillation_count,
                        "imbalance": self.imbalance_top,
                    }),
                );
                self.oscillation_count = 0;
            }
        }
    }

    /// Event 10: Hidden imbalance (opposite side larger at depth)
    fn check_imbalance_hidden(&mut self, timestamp: i64) {
        let top_bid = self.imbalance_top > 0.55;
        let deep_ask = self.imbalance_depth_10 < 0.45;
        let top_ask = self.imbalance_top < 0.45;
        let deep_bid = self.imbalance_depth_10 > 0.55;

        if (top_bid && deep_ask) || (top_ask && deep_bid) {
            if self.should_fire(OB_IMBALANCE_HIDDEN, timestamp) {
                self.publish_event(
                    OB_IMBALANCE_HIDDEN,
                    timestamp,
                    json!({
                        "imbalance_top": self.imbalance_top,
                        "imbalance_depth_10": self.imbalance_depth_10,
                        "hidden_direction": if deep_ask { "ask" } else { "bid" },
                    }),
                );
            }
        }
    }

    /// Event 11: Imbalance persistence
    fn check_imbalance_persistence(&mut self, timestamp: i64) {
        if self.persistence_count >= 50 && (self.imbalance_top - 0.5).abs() > 0.1 {
            if self.should_fire(OB_IMBALANCE_PERSISTENCE, timestamp) {
                self.publish_event(
                    OB_IMBALANCE_PERSISTENCE,
                    timestamp,
                    json!({
                        "imbalance": self.imbalance_top,
                        "persistence_count": self.persistence_count,
                        "direction": if self.imbalance_top > 0.5 { "bid" } else { "ask" },
                    }),
                );
            }
        }
    }

    /// Event 12: Imbalance breakdown (rapid collapse)
    fn check_imbalance_breakdown(&mut self, timestamp: i64) {
        let rapid_collapse = (self.imbalance_top - self.prev_imbalance_top).abs() > 0.25;
        let towards_neutral = (self.imbalance_top - 0.5).abs() < (self.prev_imbalance_top - 0.5).abs();

        if rapid_collapse && towards_neutral {
            if self.should_fire(OB_IMBALANCE_BREAKDOWN, timestamp) {
                self.publish_event(
                    OB_IMBALANCE_BREAKDOWN,
                    timestamp,
                    json!({
                        "prev_imbalance": self.prev_imbalance_top,
                        "new_imbalance": self.imbalance_top,
                        "change": self.imbalance_top - self.prev_imbalance_top,
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
        debug!(symbol = %self.symbol, event = event_type, "Imbalance event published");
    }

    pub fn updates_processed(&self) -> u64 {
        self.updates_processed
    }

    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    pub fn current_imbalance(&self) -> f64 {
        self.imbalance_top
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state(bid_qty: f64, ask_qty: f64, total_bid: f64, total_ask: f64) -> OrderBookState {
        OrderBookState {
            timestamp: 0,
            last_update_id: 0,
            best_bid: 100.0,
            best_bid_qty: bid_qty,
            best_ask: 100.1,
            best_ask_qty: ask_qty,
            mid_price: 100.05,
            spread: 0.1,
            spread_bps: 10.0,
            total_bid_volume: total_bid,
            total_ask_volume: total_ask,
            is_synced: true,
        }
    }

    #[test]
    fn test_imbalance_calculation() {
        let thresholds = AggregatorThresholds::default();
        let mut tracker = ImbalanceTracker::new("BTCUSDT".to_string(), thresholds);

        tracker.update(&make_state(70.0, 30.0, 700.0, 300.0), 1000);

        // Imbalance should be 70/(70+30) = 0.7
        assert!((tracker.current_imbalance() - 0.7).abs() < 0.01);
    }

    #[test]
    fn test_regime_classification() {
        let thresholds = AggregatorThresholds::default();
        let tracker = ImbalanceTracker::new("BTCUSDT".to_string(), thresholds);

        assert_eq!(tracker.classify_regime(0.8), ImbalanceRegime::StrongBidSide);
        assert_eq!(tracker.classify_regime(0.65), ImbalanceRegime::BidSide);
        assert_eq!(tracker.classify_regime(0.5), ImbalanceRegime::Balanced);
        assert_eq!(tracker.classify_regime(0.35), ImbalanceRegime::AskSide);
        assert_eq!(tracker.classify_regime(0.2), ImbalanceRegime::StrongAskSide);
    }
}
