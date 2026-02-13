// Spread Tracker - Monitors bid/ask spread patterns
// Generates 8 spread-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer2::orderbook::OrderBookState;
use crate::layer3::common::event_types::*;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Spread regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpreadRegime {
    Tight,      // < 2 bps
    Normal,     // 2-5 bps
    Wide,       // 5-10 bps
    VeryWide,   // 10-20 bps
    Extreme,    // > 20 bps
}

/// SpreadTracker monitors bid/ask spread patterns over time
pub struct SpreadTracker {
    symbol: String,

    // Current spread metrics
    current_spread_bps: f64,
    prev_spread_bps: f64,

    // Spread history for analysis
    spread_history: Vec<f64>,
    max_history: usize,

    // Moving averages
    avg_spread_bps: f64,
    min_spread_bps: f64,
    max_spread_bps: f64,

    // Velocity
    spread_velocity: f64,
    prev_velocity: f64,
    spread_acceleration: f64,

    // Regime tracking
    current_regime: SpreadRegime,
    prev_regime: SpreadRegime,

    // Stability tracking
    spread_stable_count: u64,
    stability_threshold_bps: f64,

    // Volatility tracking
    spread_volatility: f64,

    // Asymmetry tracking (bid/ask quantity ratio)
    prev_bid_qty: f64,
    prev_ask_qty: f64,
    asymmetry_score: f64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl SpreadTracker {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            current_spread_bps: 0.0,
            prev_spread_bps: 0.0,
            spread_history: Vec::with_capacity(1000),
            max_history: 1000,
            avg_spread_bps: 0.0,
            min_spread_bps: f64::MAX,
            max_spread_bps: 0.0,
            spread_velocity: 0.0,
            prev_velocity: 0.0,
            spread_acceleration: 0.0,
            current_regime: SpreadRegime::Normal,
            prev_regime: SpreadRegime::Normal,
            spread_stable_count: 0,
            stability_threshold_bps: 0.5,
            spread_volatility: 0.0,
            prev_bid_qty: 0.0,
            prev_ask_qty: 0.0,
            asymmetry_score: 0.0,
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
        self.prev_spread_bps = self.current_spread_bps;
        self.prev_velocity = self.spread_velocity;
        self.prev_regime = self.current_regime;
        self.prev_bid_qty = state.best_bid_qty;
        self.prev_ask_qty = state.best_ask_qty;

        // Update current spread
        self.current_spread_bps = state.spread_bps;

        // Update history
        self.spread_history.push(state.spread_bps);
        if self.spread_history.len() > self.max_history {
            self.spread_history.remove(0);
        }

        // Calculate statistics
        self.calculate_statistics();

        // Calculate velocity and acceleration
        self.spread_velocity = self.current_spread_bps - self.prev_spread_bps;
        self.spread_acceleration = self.spread_velocity - self.prev_velocity;

        // Update regime
        self.current_regime = self.classify_regime(self.current_spread_bps);

        // Calculate asymmetry
        let total_qty = state.best_bid_qty + state.best_ask_qty;
        if total_qty > 0.0 {
            self.asymmetry_score = ((state.best_bid_qty - state.best_ask_qty) / total_qty).abs();
        }

        // Check for stability
        if (self.current_spread_bps - self.prev_spread_bps).abs() < self.stability_threshold_bps {
            self.spread_stable_count += 1;
        } else {
            self.spread_stable_count = 0;
        }
    }

    /// Check all spread events
    pub fn check_events(&mut self, current_time: i64) {
        self.check_spread_threshold(current_time);
        self.check_spread_regime_change(current_time);
        self.check_spread_compressed(current_time);
        self.check_spread_expanded(current_time);
        self.check_spread_volatility(current_time);
        self.check_spread_stabilized(current_time);
        self.check_spread_asymmetry(current_time);
        self.check_spread_anomaly(current_time);
    }

    fn calculate_statistics(&mut self) {
        if self.spread_history.is_empty() {
            return;
        }

        // Calculate average
        self.avg_spread_bps = self.spread_history.iter().sum::<f64>() / self.spread_history.len() as f64;

        // Update min/max
        self.min_spread_bps = self.min_spread_bps.min(self.current_spread_bps);
        self.max_spread_bps = self.max_spread_bps.max(self.current_spread_bps);

        // Calculate volatility (standard deviation)
        if self.spread_history.len() > 1 {
            let mean = self.avg_spread_bps;
            let variance: f64 = self.spread_history.iter()
                .map(|x| (x - mean).powi(2))
                .sum::<f64>() / self.spread_history.len() as f64;
            self.spread_volatility = variance.sqrt();
        }
    }

    fn classify_regime(&self, spread_bps: f64) -> SpreadRegime {
        if spread_bps < 2.0 {
            SpreadRegime::Tight
        } else if spread_bps < 5.0 {
            SpreadRegime::Normal
        } else if spread_bps < 10.0 {
            SpreadRegime::Wide
        } else if spread_bps < 20.0 {
            SpreadRegime::VeryWide
        } else {
            SpreadRegime::Extreme
        }
    }

    /// Event 1: Spread threshold exceeded
    fn check_spread_threshold(&mut self, timestamp: i64) {
        if self.current_spread_bps > self.thresholds.spread_threshold_bps {
            if self.should_fire(OB_SPREAD_THRESHOLD, timestamp) {
                self.publish_event(
                    OB_SPREAD_THRESHOLD,
                    timestamp,
                    json!({
                        "spread_bps": self.current_spread_bps,
                        "threshold": self.thresholds.spread_threshold_bps,
                        "avg_spread_bps": self.avg_spread_bps,
                    }),
                );
            }
        }
    }

    /// Event 2: Spread regime change
    fn check_spread_regime_change(&mut self, timestamp: i64) {
        if self.current_regime != self.prev_regime {
            if self.should_fire(OB_SPREAD_REGIME, timestamp) {
                self.publish_event(
                    OB_SPREAD_REGIME,
                    timestamp,
                    json!({
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                        "spread_bps": self.current_spread_bps,
                    }),
                );
            }
        }
    }

    /// Event 3: Spread compressed (tightening)
    fn check_spread_compressed(&mut self, timestamp: i64) {
        let compression_pct = if self.avg_spread_bps > 0.0 {
            ((self.avg_spread_bps - self.current_spread_bps) / self.avg_spread_bps) * 100.0
        } else {
            0.0
        };

        if compression_pct > 30.0 && self.spread_velocity < 0.0 {
            if self.should_fire(OB_SPREAD_COMPRESSED, timestamp) {
                self.publish_event(
                    OB_SPREAD_COMPRESSED,
                    timestamp,
                    json!({
                        "spread_bps": self.current_spread_bps,
                        "avg_spread_bps": self.avg_spread_bps,
                        "compression_pct": compression_pct,
                    }),
                );
            }
        }
    }

    /// Event 4: Spread expanded (widening)
    fn check_spread_expanded(&mut self, timestamp: i64) {
        let expansion_pct = if self.avg_spread_bps > 0.0 {
            ((self.current_spread_bps - self.avg_spread_bps) / self.avg_spread_bps) * 100.0
        } else {
            0.0
        };

        if expansion_pct > 50.0 && self.spread_velocity > 0.0 {
            if self.should_fire(OB_SPREAD_EXPANDED, timestamp) {
                self.publish_event(
                    OB_SPREAD_EXPANDED,
                    timestamp,
                    json!({
                        "spread_bps": self.current_spread_bps,
                        "avg_spread_bps": self.avg_spread_bps,
                        "expansion_pct": expansion_pct,
                    }),
                );
            }
        }
    }

    /// Event 5: Spread volatility spike
    fn check_spread_volatility(&mut self, timestamp: i64) {
        if self.spread_volatility > self.avg_spread_bps * 0.5 {
            if self.should_fire(OB_SPREAD_VOLATILITY, timestamp) {
                self.publish_event(
                    OB_SPREAD_VOLATILITY,
                    timestamp,
                    json!({
                        "spread_volatility": self.spread_volatility,
                        "avg_spread_bps": self.avg_spread_bps,
                        "volatility_ratio": self.spread_volatility / self.avg_spread_bps,
                    }),
                );
            }
        }
    }

    /// Event 6: Spread stabilized
    fn check_spread_stabilized(&mut self, timestamp: i64) {
        if self.spread_stable_count >= 100 {
            if self.should_fire(OB_SPREAD_STABILIZED, timestamp) {
                self.publish_event(
                    OB_SPREAD_STABILIZED,
                    timestamp,
                    json!({
                        "spread_bps": self.current_spread_bps,
                        "stable_count": self.spread_stable_count,
                    }),
                );
            }
        }
    }

    /// Event 7: Spread asymmetry detected
    fn check_spread_asymmetry(&mut self, timestamp: i64) {
        if self.asymmetry_score > 0.7 {
            if self.should_fire(OB_SPREAD_ASYMMETRY, timestamp) {
                self.publish_event(
                    OB_SPREAD_ASYMMETRY,
                    timestamp,
                    json!({
                        "asymmetry_score": self.asymmetry_score,
                        "bid_qty": self.prev_bid_qty,
                        "ask_qty": self.prev_ask_qty,
                    }),
                );
            }
        }
    }

    /// Event 8: Spread anomaly (unusual pattern)
    fn check_spread_anomaly(&mut self, timestamp: i64) {
        // Anomaly: spread is more than 3 standard deviations from mean
        if self.spread_volatility > 0.0 {
            let z_score = (self.current_spread_bps - self.avg_spread_bps).abs() / self.spread_volatility;
            if z_score > 3.0 {
                if self.should_fire(OB_SPREAD_ANOMALY, timestamp) {
                    self.publish_event(
                        OB_SPREAD_ANOMALY,
                        timestamp,
                        json!({
                            "spread_bps": self.current_spread_bps,
                            "avg_spread_bps": self.avg_spread_bps,
                            "z_score": z_score,
                        }),
                    );
                }
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
        debug!(symbol = %self.symbol, event = event_type, "Spread event published");
    }

    pub fn updates_processed(&self) -> u64 {
        self.updates_processed
    }

    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    /// Get current spread in bps
    pub fn current_spread_bps(&self) -> f64 {
        self.current_spread_bps
    }

    /// Get average spread in bps
    pub fn avg_spread_bps(&self) -> f64 {
        self.avg_spread_bps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state(spread_bps: f64, bid_qty: f64, ask_qty: f64) -> OrderBookState {
        OrderBookState {
            timestamp: 0,
            last_update_id: 0,
            best_bid: 100.0,
            best_bid_qty: bid_qty,
            best_ask: 100.0 + spread_bps * 100.0 / 10000.0,
            best_ask_qty: ask_qty,
            mid_price: 100.0,
            spread: spread_bps * 100.0 / 10000.0,
            spread_bps,
            total_bid_volume: bid_qty,
            total_ask_volume: ask_qty,
            is_synced: true,
        }
    }

    #[test]
    fn test_spread_regime_classification() {
        let thresholds = AggregatorThresholds::default();
        let tracker = SpreadTracker::new("BTCUSDT".to_string(), thresholds);

        assert_eq!(tracker.classify_regime(1.0), SpreadRegime::Tight);
        assert_eq!(tracker.classify_regime(3.0), SpreadRegime::Normal);
        assert_eq!(tracker.classify_regime(7.0), SpreadRegime::Wide);
        assert_eq!(tracker.classify_regime(15.0), SpreadRegime::VeryWide);
        assert_eq!(tracker.classify_regime(25.0), SpreadRegime::Extreme);
    }

    #[test]
    fn test_spread_tracking() {
        let thresholds = AggregatorThresholds::default();
        let mut tracker = SpreadTracker::new("BTCUSDT".to_string(), thresholds);

        tracker.update(&make_state(3.0, 10.0, 10.0), 1000);
        tracker.update(&make_state(4.0, 10.0, 10.0), 2000);
        tracker.update(&make_state(5.0, 10.0, 10.0), 3000);

        assert_eq!(tracker.current_spread_bps(), 5.0);
        assert!((tracker.avg_spread_bps() - 4.0).abs() < 0.001);
    }
}
