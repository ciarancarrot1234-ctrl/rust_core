// Microprice Tracker - Monitors microprice and its relationship to mid price
// Generates 8 microprice-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer2::orderbook::OrderBookState;
use crate::layer3::common::event_types::*;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Microprice position relative to mid
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MicropricePosition {
    AboveMid,        // Microprice above mid (bid pressure)
    AtMid,           // Microprice at mid
    BelowMid,        // Microprice below mid (ask pressure)
    CrossedAbove,    // Crossed from below to above
    CrossedBelow,    // Crossed from above to below
}

/// MicropriceTracker monitors microprice and its relationship to mid price
pub struct MicropriceTracker {
    symbol: String,

    // Microprice calculations
    microprice: f64,
    prev_microprice: f64,
    mid_price: f64,

    // Microprice distance from mid
    microprice_distance: f64,      // In price units
    microprice_distance_bps: f64,  // In basis points

    // Velocity and acceleration
    microprice_velocity: f64,
    microprice_acceleration: f64,
    prev_velocity: f64,

    // Position tracking
    current_position: MicropricePosition,
    prev_position: MicropricePosition,

    // History for analysis
    microprice_history: Vec<f64>,
    distance_history: Vec<f64>,
    max_history: usize,
    avg_distance: f64,

    // Divergence tracking
    divergence_threshold_bps: f64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl MicropriceTracker {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            microprice: 0.0,
            prev_microprice: 0.0,
            mid_price: 0.0,
            microprice_distance: 0.0,
            microprice_distance_bps: 0.0,
            microprice_velocity: 0.0,
            microprice_acceleration: 0.0,
            prev_velocity: 0.0,
            current_position: MicropricePosition::AtMid,
            prev_position: MicropricePosition::AtMid,
            microprice_history: Vec::with_capacity(500),
            distance_history: Vec::with_capacity(500),
            max_history: 500,
            avg_distance: 0.0,
            divergence_threshold_bps: 1.0,
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
        self.prev_microprice = self.microprice;
        self.prev_velocity = self.microprice_velocity;
        self.prev_position = self.current_position;

        // Update mid price
        self.mid_price = state.mid_price;

        // Calculate microprice
        // microprice = (bid_price * ask_vol + ask_price * bid_vol) / (bid_vol + ask_vol)
        let total_qty = state.best_bid_qty + state.best_ask_qty;
        if total_qty > 0.0 {
            self.microprice = (state.best_bid * state.best_ask_qty
                + state.best_ask * state.best_bid_qty) / total_qty;
        } else {
            self.microprice = state.mid_price;
        }

        // Calculate distance from mid
        self.microprice_distance = self.microprice - state.mid_price;
        self.microprice_distance_bps = if state.mid_price > 0.0 {
            (self.microprice_distance / state.mid_price) * 10000.0
        } else {
            0.0
        };

        // Calculate velocity and acceleration
        self.microprice_velocity = self.microprice - self.prev_microprice;
        self.microprice_acceleration = self.microprice_velocity - self.prev_velocity;

        // Update position
        self.current_position = self.classify_position();

        // Update history
        self.microprice_history.push(self.microprice);
        self.distance_history.push(self.microprice_distance_bps.abs());

        if self.microprice_history.len() > self.max_history {
            self.microprice_history.remove(0);
            self.distance_history.remove(0);
        }

        // Calculate average distance
        if !self.distance_history.is_empty() {
            self.avg_distance = self.distance_history.iter().sum::<f64>() / self.distance_history.len() as f64;
        }
    }

    fn classify_position(&self) -> MicropricePosition {
        let threshold_bps = 0.1;

        if self.microprice_distance_bps > threshold_bps {
            if self.prev_position == MicropricePosition::BelowMid {
                MicropricePosition::CrossedAbove
            } else {
                MicropricePosition::AboveMid
            }
        } else if self.microprice_distance_bps < -threshold_bps {
            if self.prev_position == MicropricePosition::AboveMid {
                MicropricePosition::CrossedBelow
            } else {
                MicropricePosition::BelowMid
            }
        } else {
            MicropricePosition::AtMid
        }
    }

    /// Check all microprice events
    pub fn check_events(&mut self, current_time: i64) {
        self.check_microprice_divergence(current_time);
        self.check_microprice_alignment(current_time);
        self.check_microprice_crossed_mid(current_time);
        self.check_microprice_leading(current_time);
        self.check_microprice_acceleration(current_time);
        self.check_microprice_reversal(current_time);
        self.check_microprice_compression(current_time);
        self.check_microprice_expansion(current_time);
    }

    /// Event 1: Microprice divergence from mid
    fn check_microprice_divergence(&mut self, timestamp: i64) {
        if self.microprice_distance_bps.abs() > self.divergence_threshold_bps {
            if self.should_fire(OB_MICROPRICE_DIVERGENCE, timestamp) {
                self.publish_event(
                    OB_MICROPRICE_DIVERGENCE,
                    timestamp,
                    json!({
                        "microprice": self.microprice,
                        "mid_price": self.mid_price,
                        "distance_bps": self.microprice_distance_bps,
                        "direction": if self.microprice > self.mid_price { "bid" } else { "ask" },
                    }),
                );
            }
        }
    }

    /// Event 2: Microprice aligned with mid
    fn check_microprice_alignment(&mut self, timestamp: i64) {
        if self.microprice_distance_bps.abs() < 0.05 && self.avg_distance > 0.5 {
            if self.should_fire(OB_MICROPRICE_ALIGNMENT, timestamp) {
                self.publish_event(
                    OB_MICROPRICE_ALIGNMENT,
                    timestamp,
                    json!({
                        "microprice": self.microprice,
                        "mid_price": self.mid_price,
                        "distance_bps": self.microprice_distance_bps,
                    }),
                );
            }
        }
    }

    /// Event 3: Microprice crossed mid
    fn check_microprice_crossed_mid(&mut self, timestamp: i64) {
        match self.current_position {
            MicropricePosition::CrossedAbove => {
                if self.should_fire(OB_MICROPRICE_CROSSED_MID, timestamp) {
                    self.publish_event(
                        OB_MICROPRICE_CROSSED_MID,
                        timestamp,
                        json!({
                            "direction": "above",
                            "microprice": self.microprice,
                            "mid_price": self.mid_price,
                        }),
                    );
                }
            }
            MicropricePosition::CrossedBelow => {
                if self.should_fire(OB_MICROPRICE_CROSSED_MID, timestamp) {
                    self.publish_event(
                        OB_MICROPRICE_CROSSED_MID,
                        timestamp,
                        json!({
                            "direction": "below",
                            "microprice": self.microprice,
                            "mid_price": self.mid_price,
                        }),
                    );
                }
            }
            _ => {}
        }
    }

    /// Event 4: Microprice leading price movement
    fn check_microprice_leading(&mut self, timestamp: i64) {
        // Microprice is leading when velocity aligns with direction and is accelerating
        let is_leading = (self.microprice_velocity > 0.0 && self.microprice > self.mid_price)
            || (self.microprice_velocity < 0.0 && self.microprice < self.mid_price);

        if is_leading && self.microprice_acceleration.abs() > 0.01 {
            if self.should_fire(OB_MICROPRICE_LEADING, timestamp) {
                self.publish_event(
                    OB_MICROPRICE_LEADING,
                    timestamp,
                    json!({
                        "microprice": self.microprice,
                        "mid_price": self.mid_price,
                        "velocity": self.microprice_velocity,
                        "acceleration": self.microprice_acceleration,
                    }),
                );
            }
        }
    }

    /// Event 5: Microprice acceleration
    fn check_microprice_acceleration(&mut self, timestamp: i64) {
        if self.microprice_acceleration.abs() > 0.05 {
            if self.should_fire(OB_MICROPRICE_ACCELERATION, timestamp) {
                self.publish_event(
                    OB_MICROPRICE_ACCELERATION,
                    timestamp,
                    json!({
                        "acceleration": self.microprice_acceleration,
                        "velocity": self.microprice_velocity,
                        "microprice": self.microprice,
                    }),
                );
            }
        }
    }

    /// Event 6: Microprice reversal
    fn check_microprice_reversal(&mut self, timestamp: i64) {
        // Reversal: velocity changes sign
        let is_reversal = self.microprice_velocity * self.prev_velocity < 0.0
            && self.microprice_velocity.abs() > 0.01;

        if is_reversal {
            if self.should_fire(OB_MICROPRICE_REVERSAL, timestamp) {
                self.publish_event(
                    OB_MICROPRICE_REVERSAL,
                    timestamp,
                    json!({
                        "prev_velocity": self.prev_velocity,
                        "new_velocity": self.microprice_velocity,
                        "microprice": self.microprice,
                    }),
                );
            }
        }
    }

    /// Event 7: Microprice compression (moving towards mid)
    fn check_microprice_compression(&mut self, timestamp: i64) {
        let prev_distance = if !self.distance_history.is_empty() {
            self.distance_history[self.distance_history.len() - 1]
        } else {
            0.0
        };

        let compressing = self.microprice_distance_bps.abs() < prev_distance
            && self.microprice_distance_bps.abs() < self.avg_distance * 0.5;

        if compressing {
            if self.should_fire(OB_MICROPRICE_COMPRESSION, timestamp) {
                self.publish_event(
                    OB_MICROPRICE_COMPRESSION,
                    timestamp,
                    json!({
                        "distance_bps": self.microprice_distance_bps,
                        "prev_distance_bps": prev_distance,
                        "avg_distance_bps": self.avg_distance,
                    }),
                );
            }
        }
    }

    /// Event 8: Microprice expansion (moving away from mid)
    fn check_microprice_expansion(&mut self, timestamp: i64) {
        let prev_distance = if !self.distance_history.is_empty() {
            self.distance_history[self.distance_history.len() - 1]
        } else {
            0.0
        };

        let expanding = self.microprice_distance_bps.abs() > prev_distance
            && self.microprice_distance_bps.abs() > self.avg_distance * 1.5;

        if expanding {
            if self.should_fire(OB_MICROPRICE_EXPANSION, timestamp) {
                self.publish_event(
                    OB_MICROPRICE_EXPANSION,
                    timestamp,
                    json!({
                        "distance_bps": self.microprice_distance_bps,
                        "prev_distance_bps": prev_distance,
                        "avg_distance_bps": self.avg_distance,
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
        debug!(symbol = %self.symbol, event = event_type, "Microprice event published");
    }

    pub fn updates_processed(&self) -> u64 {
        self.updates_processed
    }

    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    pub fn current_microprice(&self) -> f64 {
        self.microprice
    }

    pub fn distance_bps(&self) -> f64 {
        self.microprice_distance_bps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state(bid: f64, bid_qty: f64, ask: f64, ask_qty: f64) -> OrderBookState {
        OrderBookState {
            timestamp: 0,
            last_update_id: 0,
            best_bid: bid,
            best_bid_qty: bid_qty,
            best_ask: ask,
            best_ask_qty: ask_qty,
            mid_price: (bid + ask) / 2.0,
            spread: ask - bid,
            spread_bps: (ask - bid) / ((bid + ask) / 2.0) * 10000.0,
            total_bid_volume: bid_qty * 10.0,
            total_ask_volume: ask_qty * 10.0,
            is_synced: true,
        }
    }

    #[test]
    fn test_microprice_calculation() {
        let thresholds = AggregatorThresholds::default();
        let mut tracker = MicropriceTracker::new("BTCUSDT".to_string(), thresholds);

        // Bid 100, qty 10; Ask 101, qty 5
        // Microprice = (100 * 5 + 101 * 10) / 15 = (500 + 1010) / 15 = 100.67
        tracker.update(&make_state(100.0, 10.0, 101.0, 5.0), 1000);

        let expected = (100.0 * 5.0 + 101.0 * 10.0) / 15.0;
        assert!((tracker.current_microprice() - expected).abs() < 0.01);
    }

    #[test]
    fn test_microprice_position() {
        let thresholds = AggregatorThresholds::default();
        let mut tracker = MicropriceTracker::new("BTCUSDT".to_string(), thresholds);

        // More ask volume = microprice closer to bid (below mid)
        tracker.update(&make_state(100.0, 5.0, 101.0, 20.0), 1000);
        assert!(tracker.distance_bps() > 0.0); // Microprice above mid due to ask pressure
    }
}
