// Divergence Tracker - Monitors price divergences across different price types
// Generates 6 divergence-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::core::types::MarkPrice;
use crate::layer3::common::event_types::*;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// DivergenceTracker monitors divergences between mark, index, and last price
pub struct DivergenceTracker {
    symbol: String,

    // Current prices
    mark_price: f64,
    index_price: f64,
    last_price: f64,          // Estimated from mark/index
    estimated_settle_price: f64,

    // Divergence metrics (in bps)
    mark_last_divergence_bps: f64,
    mark_index_divergence_bps: f64,
    index_settle_divergence_bps: f64,

    // Previous values
    prev_mark_last_divergence_bps: f64,
    prev_mark_index_divergence_bps: f64,

    // History for analysis
    mark_index_history: Vec<f64>,
    mark_last_history: Vec<f64>,
    max_history: usize,

    // Statistics
    avg_mark_index_divergence: f64,
    avg_mark_last_divergence: f64,

    // Cross-exchange tracking (simulated)
    cross_exchange_spread_bps: f64,
    spot_futures_spread_bps: f64,

    // Perp/Quarterly tracking (simulated)
    perp_quarterly_spread_bps: f64,

    // Divergence regimes
    high_divergence_count: u64,
    extended_divergence_duration: u64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl DivergenceTracker {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            mark_price: 0.0,
            index_price: 0.0,
            last_price: 0.0,
            estimated_settle_price: 0.0,
            mark_last_divergence_bps: 0.0,
            mark_index_divergence_bps: 0.0,
            index_settle_divergence_bps: 0.0,
            prev_mark_last_divergence_bps: 0.0,
            prev_mark_index_divergence_bps: 0.0,
            mark_index_history: Vec::with_capacity(1000),
            mark_last_history: Vec::with_capacity(1000),
            max_history: 1000,
            avg_mark_index_divergence: 0.0,
            avg_mark_last_divergence: 0.0,
            cross_exchange_spread_bps: 0.0,
            spot_futures_spread_bps: 0.0,
            perp_quarterly_spread_bps: 0.0,
            high_divergence_count: 0,
            extended_divergence_duration: 0,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 500,
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    /// Process mark price update
    pub fn update(&mut self, data: &MarkPrice, current_time: i64) {
        self.updates_processed += 1;

        // Store previous values
        self.prev_mark_last_divergence_bps = self.mark_last_divergence_bps;
        self.prev_mark_index_divergence_bps = self.mark_index_divergence_bps;

        // Update current prices
        self.mark_price = data.mark_price;
        self.index_price = data.index_price;
        self.estimated_settle_price = data.estimated_settle_price;
        self.last_price = (data.mark_price + data.index_price) / 2.0; // Estimate

        // Calculate divergences in basis points
        if data.index_price > 0.0 {
            self.mark_index_divergence_bps = ((data.mark_price - data.index_price) / data.index_price) * 10000.0;
        }

        if self.last_price > 0.0 {
            self.mark_last_divergence_bps = ((data.mark_price - self.last_price) / self.last_price) * 10000.0;
        }

        if data.estimated_settle_price > 0.0 {
            self.index_settle_divergence_bps = ((data.index_price - data.estimated_settle_price) / data.estimated_settle_price) * 10000.0;
        }

        // Update history
        self.mark_index_history.push(self.mark_index_divergence_bps);
        self.mark_last_history.push(self.mark_last_divergence_bps);
        
        if self.mark_index_history.len() > self.max_history {
            self.mark_index_history.remove(0);
        }
        if self.mark_last_history.len() > self.max_history {
            self.mark_last_history.remove(0);
        }

        // Calculate statistics
        self.calculate_statistics();

        // Update derived spreads (simulated/estimated)
        self.update_derived_spreads();

        // Track high divergence
        self.update_divergence_tracking();
    }

    /// Check all divergence events
    pub fn check_events(&mut self, current_time: i64) {
        self.check_divergence_mark_last(current_time);
        self.check_divergence_index_composition(current_time);
        self.check_divergence_price_anchor(current_time);
        self.check_divergence_cross_exchange(current_time);
        self.check_divergence_spot_futures(current_time);
        self.check_divergence_perp_quarterly(current_time);
    }

    fn calculate_statistics(&mut self) {
        if !self.mark_index_history.is_empty() {
            self.avg_mark_index_divergence = self.mark_index_history.iter().sum::<f64>() 
                / self.mark_index_history.len() as f64;
        }

        if !self.mark_last_history.is_empty() {
            self.avg_mark_last_divergence = self.mark_last_history.iter().sum::<f64>() 
                / self.mark_last_history.len() as f64;
        }
    }

    fn update_derived_spreads(&mut self) {
        // Estimate cross-exchange spread based on index composition deviation
        // In production, this would compare with actual prices from other exchanges
        self.cross_exchange_spread_bps = self.mark_index_divergence_bps.abs() * 0.3;

        // Estimate spot-futures spread (mark price vs estimated last price)
        self.spot_futures_spread_bps = self.mark_last_divergence_bps.abs() * 0.5;

        // Estimate perp-quarterly spread (based on settlement price)
        self.perp_quarterly_spread_bps = self.index_settle_divergence_bps.abs() * 0.4;
    }

    fn update_divergence_tracking(&mut self) {
        // Track high divergence periods
        if self.mark_index_divergence_bps.abs() > 10.0 {
            self.high_divergence_count += 1;
            self.extended_divergence_duration += 1;
        } else {
            self.extended_divergence_duration = 0;
        }
    }

    /// Event 1: Mark vs Last price divergence
    fn check_divergence_mark_last(&mut self, timestamp: i64) {
        if self.mark_last_divergence_bps.abs() > 15.0 {
            if self.should_fire(MP_DIVERGENCE_MARK_LAST, timestamp) {
                self.publish_event(
                    MP_DIVERGENCE_MARK_LAST,
                    timestamp,
                    json!({
                        "mark_price": self.mark_price,
                        "last_price": self.last_price,
                        "divergence_bps": self.mark_last_divergence_bps,
                        "avg_divergence_bps": self.avg_mark_last_divergence,
                    }),
                );
            }
        }
    }

    /// Event 2: Index composition divergence (mark vs index)
    fn check_divergence_index_composition(&mut self, timestamp: i64) {
        if self.mark_index_divergence_bps.abs() > 20.0 {
            if self.should_fire(MP_DIVERGENCE_INDEX_COMPOSITION, timestamp) {
                self.publish_event(
                    MP_DIVERGENCE_INDEX_COMPOSITION,
                    timestamp,
                    json!({
                        "mark_price": self.mark_price,
                        "index_price": self.index_price,
                        "divergence_bps": self.mark_index_divergence_bps,
                        "avg_divergence_bps": self.avg_mark_index_divergence,
                    }),
                );
            }
        }
    }

    /// Event 3: Price anchor divergence (index vs settlement)
    fn check_divergence_price_anchor(&mut self, timestamp: i64) {
        if self.index_settle_divergence_bps.abs() > 25.0 {
            if self.should_fire(MP_DIVERGENCE_PRICE_ANCHOR, timestamp) {
                self.publish_event(
                    MP_DIVERGENCE_PRICE_ANCHOR,
                    timestamp,
                    json!({
                        "index_price": self.index_price,
                        "settle_price": self.estimated_settle_price,
                        "divergence_bps": self.index_settle_divergence_bps,
                    }),
                );
            }
        }
    }

    /// Event 4: Cross-exchange divergence detected
    fn check_divergence_cross_exchange(&mut self, timestamp: i64) {
        if self.cross_exchange_spread_bps > 15.0 {
            if self.should_fire(MP_DIVERGENCE_CROSS_EXCHANGE, timestamp) {
                self.publish_event(
                    MP_DIVERGENCE_CROSS_EXCHANGE,
                    timestamp,
                    json!({
                        "estimated_spread_bps": self.cross_exchange_spread_bps,
                        "mark_index_divergence_bps": self.mark_index_divergence_bps,
                        "note": "estimated_from_mark_index_divergence",
                    }),
                );
            }
        }
    }

    /// Event 5: Spot-futures divergence detected
    fn check_divergence_spot_futures(&mut self, timestamp: i64) {
        if self.spot_futures_spread_bps > 20.0 {
            if self.should_fire(MP_DIVERGENCE_SPOT_FUTURES, timestamp) {
                self.publish_event(
                    MP_DIVERGENCE_SPOT_FUTURES,
                    timestamp,
                    json!({
                        "estimated_spread_bps": self.spot_futures_spread_bps,
                        "mark_price": self.mark_price,
                        "estimated_last_price": self.last_price,
                    }),
                );
            }
        }
    }

    /// Event 6: Perp-quarterly divergence detected
    fn check_divergence_perp_quarterly(&mut self, timestamp: i64) {
        if self.perp_quarterly_spread_bps > 15.0 {
            if self.should_fire(MP_DIVERGENCE_PERP_QUARTERLY, timestamp) {
                self.publish_event(
                    MP_DIVERGENCE_PERP_QUARTERLY,
                    timestamp,
                    json!({
                        "estimated_spread_bps": self.perp_quarterly_spread_bps,
                        "index_price": self.index_price,
                        "settle_price": self.estimated_settle_price,
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
            "markprice_aggregator",
            EventPriority::Medium,
        );

        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(symbol = %self.symbol, event = event_type, "Divergence event published");
    }

    pub fn updates_processed(&self) -> u64 {
        self.updates_processed
    }

    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    /// Get mark-index divergence in bps
    pub fn mark_index_divergence_bps(&self) -> f64 {
        self.mark_index_divergence_bps
    }

    /// Get extended divergence duration
    pub fn extended_divergence_duration(&self) -> u64 {
        self.extended_divergence_duration
    }
}
