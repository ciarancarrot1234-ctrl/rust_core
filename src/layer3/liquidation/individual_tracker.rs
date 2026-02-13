// Individual Tracker - Monitors individual liquidation events
// Generates 8 liquidation-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer2::parser::ParsedLiquidation;
use crate::layer3::common::event_types::*;
use crate::layer3::common::time_windows::TimeWindow;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// IndividualTracker tracks individual liquidation events and basic metrics
pub struct IndividualTracker {
    symbol: String,

    // Rolling windows for timeframes
    liquidations_1m: TimeWindow<ParsedLiquidation>,
    liquidations_5m: TimeWindow<ParsedLiquidation>,
    liquidations_15m: TimeWindow<ParsedLiquidation>,

    // Cumulative metrics
    total_volume_1m: f64,
    total_volume_5m: f64,
    total_volume_15m: f64,
    total_count_1m: u64,
    total_count_5m: u64,
    total_count_15m: u64,

    // Rate calculations
    liquidation_rate_1m: f64,       // Liquidations per second
    prev_rate_1m: f64,
    rate_velocity: f64,             // Change in rate

    // Price tracking
    last_price: f64,
    prev_price: f64,
    price_levels_breached: HashMap<f64, i64>,  // price level -> timestamp

    // Sequential tracking
    sequential_count: u64,
    last_side: String,
    last_liquidation_time: i64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    liquidations_processed: u64,
    events_fired: u64,
}

impl IndividualTracker {
    /// Create a new IndividualTracker
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            liquidations_1m: TimeWindow::new(60_000, 1_000),
            liquidations_5m: TimeWindow::new(300_000, 5_000),
            liquidations_15m: TimeWindow::new(900_000, 15_000),
            total_volume_1m: 0.0,
            total_volume_5m: 0.0,
            total_volume_15m: 0.0,
            total_count_1m: 0,
            total_count_5m: 0,
            total_count_15m: 0,
            liquidation_rate_1m: 0.0,
            prev_rate_1m: 0.0,
            rate_velocity: 0.0,
            last_price: 0.0,
            prev_price: 0.0,
            price_levels_breached: HashMap::new(),
            sequential_count: 0,
            last_side: String::new(),
            last_liquidation_time: 0,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 1000,
            thresholds,
            liquidations_processed: 0,
            events_fired: 0,
        }
    }

    /// Add a liquidation to all windows
    pub fn add_liquidation(&mut self, liq: &ParsedLiquidation, current_time: i64) {
        self.liquidations_processed += 1;
        self.prev_price = self.last_price;
        self.last_price = liq.average_price;

        // Track sequential liquidations
        if self.last_side == liq.side && (current_time - self.last_liquidation_time) < 5000 {
            self.sequential_count += 1;
        } else {
            self.sequential_count = 1;
        }
        self.last_side = liq.side.clone();
        self.last_liquidation_time = current_time;

        self.liquidations_1m.add(current_time, liq.clone());
        self.liquidations_5m.add(current_time, liq.clone());
        self.liquidations_15m.add(current_time, liq.clone());

        self.liquidations_1m.prune(current_time);
        self.liquidations_5m.prune(current_time);
        self.liquidations_15m.prune(current_time);
    }

    /// Check all individual liquidation events
    pub fn check_events(&mut self, current_time: i64) {
        self.recalculate_metrics();

        // Event 1: Liquidation detected
        self.check_liq_detected(current_time);

        // Event 2: Large liquidation
        self.check_liq_large(current_time);

        // Event 3: Rate threshold
        self.check_liq_rate_threshold(current_time);

        // Event 4: Volume threshold
        self.check_liq_volume_threshold(current_time);

        // Event 5: Size anomaly
        self.check_liq_size_anomaly(current_time);

        // Event 6: Whale liquidation
        self.check_liq_whale(current_time);

        // Event 7: Sequential liquidations
        self.check_liq_sequential(current_time);

        // Event 8: Price level breached
        self.check_liq_price_level_breached(current_time);
    }

    /// Recalculate all metrics
    fn recalculate_metrics(&mut self) {
        self.prev_rate_1m = self.liquidation_rate_1m;

        // Calculate 1m metrics
        let (volume_1m, count_1m) = self.calculate_window_metrics(&self.liquidations_1m);
        self.total_volume_1m = volume_1m;
        self.total_count_1m = count_1m;
        self.liquidation_rate_1m = count_1m as f64 / 60.0;  // Per second

        // Calculate rate velocity
        self.rate_velocity = self.liquidation_rate_1m - self.prev_rate_1m;

        // Calculate 5m metrics
        let (volume_5m, count_5m) = self.calculate_window_metrics(&self.liquidations_5m);
        self.total_volume_5m = volume_5m;
        self.total_count_5m = count_5m;

        // Calculate 15m metrics
        let (volume_15m, count_15m) = self.calculate_window_metrics(&self.liquidations_15m);
        self.total_volume_15m = volume_15m;
        self.total_count_15m = count_15m;
    }

    /// Calculate volume and count from a time window
    fn calculate_window_metrics(&self, window: &TimeWindow<ParsedLiquidation>) -> (f64, u64) {
        let mut total_volume = 0.0;
        let mut count = 0u64;

        for (_, liq) in window.iter() {
            total_volume += liq.notional_usd();
            count += 1;
        }

        (total_volume, count)
    }

    /// Event 1: Liquidation detected (any liquidation)
    fn check_liq_detected(&mut self, timestamp: i64) {
        if self.total_count_1m > 0 {
            if self.should_fire(LIQ_DETECTED, timestamp) {
                self.publish_event(
                    LIQ_DETECTED,
                    timestamp,
                    json!({
                        "count_1m": self.total_count_1m,
                        "volume_1m": self.total_volume_1m,
                        "rate_per_sec": self.liquidation_rate_1m,
                    }),
                );
            }
        }
    }

    /// Event 2: Large liquidation (> threshold)
    fn check_liq_large(&mut self, timestamp: i64) {
        if let Some((_, last_liq)) = self.liquidations_1m.last() {
            let notional = last_liq.notional_usd();
            if notional > self.thresholds.large_liquidation_usd {
                if self.should_fire(LIQ_LARGE, timestamp) {
                    self.publish_event(
                        LIQ_LARGE,
                        timestamp,
                        json!({
                            "notional_usd": notional,
                            "threshold": self.thresholds.large_liquidation_usd,
                            "side": last_liq.side,
                            "price": last_liq.average_price,
                        }),
                    );
                }
            }
        }
    }

    /// Event 3: Liquidation rate threshold exceeded
    fn check_liq_rate_threshold(&mut self, timestamp: i64) {
        let rate_threshold = 0.5;  // 0.5 liquidations per second
        if self.liquidation_rate_1m > rate_threshold {
            if self.should_fire(LIQ_RATE_THRESHOLD, timestamp) {
                self.publish_event(
                    LIQ_RATE_THRESHOLD,
                    timestamp,
                    json!({
                        "rate_per_sec": self.liquidation_rate_1m,
                        "threshold": rate_threshold,
                        "rate_velocity": self.rate_velocity,
                    }),
                );
            }
        }
    }

    /// Event 4: Cumulative volume threshold exceeded
    fn check_liq_volume_threshold(&mut self, timestamp: i64) {
        if self.total_volume_1m > self.thresholds.cascade_threshold_usd {
            if self.should_fire(LIQ_VOLUME_THRESHOLD, timestamp) {
                self.publish_event(
                    LIQ_VOLUME_THRESHOLD,
                    timestamp,
                    json!({
                        "volume_1m": self.total_volume_1m,
                        "volume_5m": self.total_volume_5m,
                        "volume_15m": self.total_volume_15m,
                        "threshold": self.thresholds.cascade_threshold_usd,
                    }),
                );
            }
        }
    }

    /// Event 5: Size anomaly (unusually large single liquidation)
    fn check_liq_size_anomaly(&mut self, timestamp: i64) {
        if let Some((_, last_liq)) = self.liquidations_1m.last() {
            let notional = last_liq.notional_usd();
            // Anomaly threshold is 5x the large liquidation threshold
            let anomaly_threshold = self.thresholds.large_liquidation_usd * 5.0;
            if notional > anomaly_threshold {
                if self.should_fire(LIQ_SIZE_ANOMALY, timestamp) {
                    self.publish_event(
                        LIQ_SIZE_ANOMALY,
                        timestamp,
                        json!({
                            "notional_usd": notional,
                            "threshold": anomaly_threshold,
                            "side": last_liq.side,
                        }),
                    );
                }
            }
        }
    }

    /// Event 6: Whale liquidation (> 100k USD)
    fn check_liq_whale(&mut self, timestamp: i64) {
        if let Some((_, last_liq)) = self.liquidations_1m.last() {
            let notional = last_liq.notional_usd();
            let whale_threshold = self.thresholds.whale_trade_usd;
            if notional > whale_threshold {
                if self.should_fire(LIQ_WHALE, timestamp) {
                    self.publish_event(
                        LIQ_WHALE,
                        timestamp,
                        json!({
                            "notional_usd": notional,
                            "threshold": whale_threshold,
                            "side": last_liq.side,
                            "quantity": last_liq.filled_accumulated_quantity,
                        }),
                    );
                }
            }
        }
    }

    /// Event 7: Sequential liquidations (same direction in short time)
    fn check_liq_sequential(&mut self, timestamp: i64) {
        if self.sequential_count >= 3 {
            if self.should_fire(LIQ_SEQUENTIAL, timestamp) {
                self.publish_event(
                    LIQ_SEQUENTIAL,
                    timestamp,
                    json!({
                        "sequential_count": self.sequential_count,
                        "side": self.last_side,
                    }),
                );
            }
        }
    }

    /// Event 8: Price level breached by liquidations
    fn check_liq_price_level_breached(&mut self, timestamp: i64) {
        // Check if price changed significantly
        if self.prev_price > 0.0 && self.last_price > 0.0 {
            let price_change_pct = ((self.last_price - self.prev_price) / self.prev_price).abs() * 100.0;
            if price_change_pct > 0.1 && self.total_count_1m > 0 {
                // Round to significant price level
                let level = (self.last_price / 100.0).round() * 100.0;
                self.price_levels_breached.insert(level, timestamp);

                if self.should_fire(LIQ_PRICE_LEVEL_BREACHED, timestamp) {
                    self.publish_event(
                        LIQ_PRICE_LEVEL_BREACHED,
                        timestamp,
                        json!({
                            "price": self.last_price,
                            "price_level": level,
                            "price_change_pct": price_change_pct,
                            "liquidation_count": self.total_count_1m,
                        }),
                    );
                }
            }
        }

        // Clean old price levels
        let cutoff = timestamp - 300_000;  // 5 minutes
        self.price_levels_breached.retain(|_, &mut t| t > cutoff);
    }

    /// Check if event should be fired (debouncing)
    fn should_fire(&self, event_type: &str, current_time: i64) -> bool {
        if let Some(&last_time) = self.last_event_times.get(event_type) {
            current_time - last_time >= self.min_event_interval_ms
        } else {
            true
        }
    }

    /// Publish event to EventBus
    fn publish_event(&mut self, event_type: &str, timestamp: i64, mut data: serde_json::Value) {
        self.events_fired += 1;

        if let Some(obj) = data.as_object_mut() {
            obj.insert("symbol".to_string(), json!(self.symbol));
        }

        publish_event(
            event_type,
            timestamp,
            serde_json::from_value(data).unwrap_or_default(),
            "liquidation_aggregator",
            EventPriority::High,
        );

        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(
            symbol = %self.symbol,
            event = event_type,
            "Individual liquidation event published"
        );
    }

    /// Get liquidations processed
    pub fn liquidations_processed(&self) -> u64 {
        self.liquidations_processed
    }

    /// Get events fired
    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    /// Get current liquidation rate
    #[allow(dead_code)]
    pub fn get_rate_1m(&self) -> f64 {
        self.liquidation_rate_1m
    }

    /// Get total 1m volume
    #[allow(dead_code)]
    pub fn get_volume_1m(&self) -> f64 {
        self.total_volume_1m
    }
}
