// Order Flow Tracker - Monitors order flow patterns in the order book
// Generates 8 order flow-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer2::orderbook::OrderBookState;
use crate::layer3::common::event_types::*;
use crate::layer3::common::time_windows::TimeWindow;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Order flow regime
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderFlowRegime {
    Aggressive,      // Large market orders dominating
    Passive,         // Limit orders dominating
    Mixed,           // Balanced
    Absorbing,       // Passive side absorbing
}

/// Order flow event for tracking
#[derive(Debug, Clone)]
struct FlowEvent {
    timestamp: i64,
    is_aggressive: bool,
    is_bid: bool,
    quantity: f64,
    price_movement: f64,
}

/// OrderFlowTracker monitors order flow patterns
pub struct OrderFlowTracker {
    symbol: String,

    // Flow tracking
    aggressive_buy_volume: f64,
    aggressive_sell_volume: f64,
    passive_buy_volume: f64,
    passive_sell_volume: f64,

    // Recent flow events
    flow_events: TimeWindow<FlowEvent>,

    // Aggression metrics
    aggression_ratio: f64,        // aggressive / total
    buy_aggression: f64,
    sell_aggression: f64,

    // Quote stuffing detection
    rapid_updates: u32,
    last_update_time: i64,
    update_interval_samples: Vec<i64>,

    // Iceberg detection
    iceberg_candidates: Vec<(f64, u32)>,  // (price, repeat_count)

    // Stop hunt detection
    price_swings: Vec<(i64, f64, f64)>,  // (time, low, high)
    stop_hunt_threshold_pct: f64,

    // Momentum ignition
    consecutive_aggressive: u32,
    momentum_threshold: u32,

    // Regime tracking
    current_regime: OrderFlowRegime,
    prev_regime: OrderFlowRegime,

    // Absorption tracking
    absorption_price: Option<f64>,
    absorption_volume: f64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,

    // Previous state for delta calculation
    prev_mid_price: f64,
    prev_bid_qty: f64,
    prev_ask_qty: f64,
}

impl OrderFlowTracker {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            aggressive_buy_volume: 0.0,
            aggressive_sell_volume: 0.0,
            passive_buy_volume: 0.0,
            passive_sell_volume: 0.0,
            flow_events: TimeWindow::new(60_000, 10_000),
            aggression_ratio: 0.5,
            buy_aggression: 0.5,
            sell_aggression: 0.5,
            rapid_updates: 0,
            last_update_time: 0,
            update_interval_samples: Vec::with_capacity(100),
            iceberg_candidates: Vec::new(),
            price_swings: Vec::new(),
            stop_hunt_threshold_pct: 0.2,
            consecutive_aggressive: 0,
            momentum_threshold: 5,
            current_regime: OrderFlowRegime::Mixed,
            prev_regime: OrderFlowRegime::Mixed,
            absorption_price: None,
            absorption_volume: 0.0,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 500,
            thresholds,
            updates_processed: 0,
            events_fired: 0,
            prev_mid_price: 0.0,
            prev_bid_qty: 0.0,
            prev_ask_qty: 0.0,
        }
    }

    /// Process order book state update
    pub fn update(&mut self, state: &OrderBookState, current_time: i64) {
        self.updates_processed += 1;

        // Calculate changes
        let mid_change = state.mid_price - self.prev_mid_price;
        let bid_qty_change = state.best_bid_qty - self.prev_bid_qty;
        let ask_qty_change = state.best_ask_qty - self.prev_ask_qty;

        // Detect aggressive vs passive flow
        // Aggressive buy: price moves up, ask side decreases
        // Aggressive sell: price moves down, bid side decreases
        if mid_change > 0.0 && ask_qty_change < 0.0 {
            // Aggressive buying
            self.aggressive_buy_volume += ask_qty_change.abs();
            self.record_flow_event(current_time, true, true, ask_qty_change.abs(), mid_change);
        } else if mid_change < 0.0 && bid_qty_change < 0.0 {
            // Aggressive selling
            self.aggressive_sell_volume += bid_qty_change.abs();
            self.record_flow_event(current_time, true, false, bid_qty_change.abs(), mid_change);
        }

        // Passive flow
        if bid_qty_change > 0.0 {
            self.passive_buy_volume += bid_qty_change;
        }
        if ask_qty_change > 0.0 {
            self.passive_sell_volume += ask_qty_change;
        }

        // Calculate aggression ratios
        let total_aggressive = self.aggressive_buy_volume + self.aggressive_sell_volume;
        let total = total_aggressive + self.passive_buy_volume + self.passive_sell_volume;

        if total > 0.0 {
            self.aggression_ratio = total_aggressive / total;
        }
        if self.aggressive_buy_volume + self.passive_buy_volume > 0.0 {
            self.buy_aggression = self.aggressive_buy_volume
                / (self.aggressive_buy_volume + self.passive_buy_volume);
        }
        if self.aggressive_sell_volume + self.passive_sell_volume > 0.0 {
            self.sell_aggression = self.aggressive_sell_volume
                / (self.aggressive_sell_volume + self.passive_sell_volume);
        }

        // Track rapid updates for quote stuffing detection
        self.track_update_frequency(current_time);

        // Update regime
        self.prev_regime = self.current_regime;
        self.current_regime = self.classify_regime();

        // Store previous state
        self.prev_mid_price = state.mid_price;
        self.prev_bid_qty = state.best_bid_qty;
        self.prev_ask_qty = state.best_ask_qty;
    }

    fn record_flow_event(&mut self, timestamp: i64, is_aggressive: bool, is_bid: bool, quantity: f64, price_movement: f64) {
        let event = FlowEvent {
            timestamp,
            is_aggressive,
            is_bid,
            quantity,
            price_movement,
        };
        self.flow_events.add(timestamp, event);
        self.flow_events.prune(timestamp);
    }

    fn track_update_frequency(&mut self, current_time: i64) {
        if self.last_update_time > 0 {
            let interval = current_time - self.last_update_time;
            self.update_interval_samples.push(interval);

            if self.update_interval_samples.len() > 100 {
                self.update_interval_samples.remove(0);
            }

            // Count rapid updates (under 10ms)
            if interval < 10 {
                self.rapid_updates += 1;
            } else {
                self.rapid_updates = 0;
            }
        }
        self.last_update_time = current_time;
    }

    fn classify_regime(&self) -> OrderFlowRegime {
        if self.aggression_ratio > 0.7 {
            OrderFlowRegime::Aggressive
        } else if self.aggression_ratio < 0.3 {
            OrderFlowRegime::Passive
        } else if self.absorption_volume > 0.0 {
            OrderFlowRegime::Absorbing
        } else {
            OrderFlowRegime::Mixed
        }
    }

    /// Check all order flow events
    pub fn check_events(&mut self, current_time: i64) {
        self.check_orderflow_aggressive(current_time);
        self.check_orderflow_passive_accumulation(current_time);
        self.check_orderflow_reversal(current_time);
        self.check_orderflow_iceberg(current_time);
        self.check_orderflow_quote_stuffing(current_time);
        self.check_orderflow_momentum_ignition(current_time);
        self.check_orderflow_stop_hunt(current_time);
        self.check_orderflow_absorption(current_time);
    }

    /// Event 1: Aggressive order flow detected
    fn check_orderflow_aggressive(&mut self, timestamp: i64) {
        if self.aggression_ratio > 0.7 {
            if self.should_fire(OB_ORDERFLOW_AGGRESSIVE, timestamp) {
                self.publish_event(
                    OB_ORDERFLOW_AGGRESSIVE,
                    timestamp,
                    json!({
                        "aggression_ratio": self.aggression_ratio,
                        "aggressive_buy_volume": self.aggressive_buy_volume,
                        "aggressive_sell_volume": self.aggressive_sell_volume,
                        "dominant_side": if self.aggressive_buy_volume > self.aggressive_sell_volume { "buy" } else { "sell" },
                    }),
                );
            }
        }
    }

    /// Event 2: Passive accumulation detected
    fn check_orderflow_passive_accumulation(&mut self, timestamp: i64) {
        // Passive accumulation: passive orders building while aggressive decreases
        let passive_ratio = (self.passive_buy_volume + self.passive_sell_volume)
            / (self.aggressive_buy_volume + self.aggressive_sell_volume + self.passive_buy_volume + self.passive_sell_volume + 0.001);

        if passive_ratio > 0.7 && self.passive_buy_volume > self.passive_sell_volume * 1.5 {
            if self.should_fire(OB_ORDERFLOW_PASSIVE_ACCUMULATION, timestamp) {
                self.publish_event(
                    OB_ORDERFLOW_PASSIVE_ACCUMULATION,
                    timestamp,
                    json!({
                        "passive_ratio": passive_ratio,
                        "passive_buy_volume": self.passive_buy_volume,
                        "passive_sell_volume": self.passive_sell_volume,
                    }),
                );
            }
        }
    }

    /// Event 3: Order flow reversal
    fn check_orderflow_reversal(&mut self, timestamp: i64) {
        // Reversal: dominant aggression switches sides
        let prev_buy_dominant = self.aggressive_buy_volume > self.aggressive_sell_volume;
        let now_sell_dominant = self.aggressive_sell_volume > self.aggressive_buy_volume;

        // This is simplified - would need historical tracking
        if self.current_regime != self.prev_regime {
            if self.should_fire(OB_ORDERFLOW_REVERSAL, timestamp) {
                self.publish_event(
                    OB_ORDERFLOW_REVERSAL,
                    timestamp,
                    json!({
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                    }),
                );
            }
        }
    }

    /// Event 4: Iceberg order detected
    fn check_orderflow_iceberg(&mut self, timestamp: i64) {
        // Iceberg: same price level keeps getting refilled
        let icebergs: Vec<_> = self.iceberg_candidates.iter()
            .filter(|(_, count)| *count >= 3)
            .collect();

        if !icebergs.is_empty() {
            if self.should_fire(OB_ORDERFLOW_ICEBERG, timestamp) {
                self.publish_event(
                    OB_ORDERFLOW_ICEBERG,
                    timestamp,
                    json!({
                        "iceberg_count": icebergs.len(),
                        "candidates": icebergs.iter()
                            .map(|(price, count)| json!({"price": price, "repeat_count": count}))
                            .collect::<Vec<_>>(),
                    }),
                );
            }
        }
    }

    /// Event 5: Quote stuffing detected
    fn check_orderflow_quote_stuffing(&mut self, timestamp: i64) {
        // Quote stuffing: rapid updates with no price movement
        if self.rapid_updates >= 20 {
            let avg_interval = if !self.update_interval_samples.is_empty() {
                self.update_interval_samples.iter().sum::<i64>() / self.update_interval_samples.len() as i64
            } else {
                0
            };

            if self.should_fire(OB_ORDERFLOW_QUOTE_STUFFING, timestamp) {
                self.publish_event(
                    OB_ORDERFLOW_QUOTE_STUFFING,
                    timestamp,
                    json!({
                        "rapid_updates": self.rapid_updates,
                        "avg_interval_ms": avg_interval,
                    }),
                );
            }
        }
    }

    /// Event 6: Momentum ignition detected
    fn check_orderflow_momentum_ignition(&mut self, timestamp: i64) {
        // Momentum ignition: series of aggressive orders in same direction
        if self.consecutive_aggressive >= self.momentum_threshold {
            if self.should_fire(OB_ORDERFLOW_MOMENTUM_IGNITION, timestamp) {
                self.publish_event(
                    OB_ORDERFLOW_MOMENTUM_IGNITION,
                    timestamp,
                    json!({
                        "consecutive_aggressive": self.consecutive_aggressive,
                        "direction": if self.aggressive_buy_volume > self.aggressive_sell_volume { "buy" } else { "sell" },
                    }),
                );
            }
        }
    }

    /// Event 7: Stop hunt detected
    fn check_orderflow_stop_hunt(&mut self, timestamp: i64) {
        // Stop hunt: price quickly moves to liquidity zone then reverses
        // Simplified detection based on price swings
        let recent_swings: Vec<_> = self.price_swings.iter()
            .filter(|(t, _, _)| timestamp - t < 30_000)
            .collect();

        if recent_swings.len() >= 3 {
            // Check for pattern: swing down, then up (or vice versa)
            if self.should_fire(OB_ORDERFLOW_STOP_HUNT, timestamp) {
                self.publish_event(
                    OB_ORDERFLOW_STOP_HUNT,
                    timestamp,
                    json!({
                        "swing_count": recent_swings.len(),
                    }),
                );
            }
        }
    }

    /// Event 8: Absorption detected
    fn check_orderflow_absorption(&mut self, timestamp: i64) {
        if self.absorption_volume > 0.0 && self.absorption_price.is_some() {
            if self.should_fire(OB_ORDERFLOW_ABSORPTION, timestamp) {
                self.publish_event(
                    OB_ORDERFLOW_ABSORPTION,
                    timestamp,
                    json!({
                        "absorption_price": self.absorption_price,
                        "absorption_volume": self.absorption_volume,
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
        debug!(symbol = %self.symbol, event = event_type, "OrderFlow event published");
    }

    pub fn updates_processed(&self) -> u64 {
        self.updates_processed
    }

    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    pub fn aggression_ratio(&self) -> f64 {
        self.aggression_ratio
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orderflow_tracker_creation() {
        let thresholds = AggregatorThresholds::default();
        let tracker = OrderFlowTracker::new("BTCUSDT".to_string(), thresholds);
        assert_eq!(tracker.aggression_ratio(), 0.5);
    }

    #[test]
    fn test_orderflow_update() {
        let thresholds = AggregatorThresholds::default();
        let mut tracker = OrderFlowTracker::new("BTCUSDT".to_string(), thresholds);

        let state = OrderBookState {
            timestamp: 0,
            last_update_id: 0,
            best_bid: 100.0,
            best_bid_qty: 10.0,
            best_ask: 101.0,
            best_ask_qty: 8.0,  // Decreased from previous
            mid_price: 100.5,
            spread: 1.0,
            spread_bps: 99.5,
            total_bid_volume: 100.0,
            total_ask_volume: 80.0,
            is_synced: true,
        };

        tracker.update(&state, 1000);
        assert!(tracker.updates_processed() > 0);
    }
}
