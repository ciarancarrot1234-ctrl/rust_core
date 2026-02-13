// Trend Tracker - Monitors trend direction and strength
// Generates 8 trend-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer3::common::event_types::*;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Trend direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrendDirection {
    Up,
    Down,
    Sideways,
}

/// TrendTracker monitors trend direction and strength
pub struct TrendTracker {
    symbol: String,
    interval: String,

    // Trend state
    current_trend: TrendDirection,
    prev_trend: TrendDirection,

    // Trend strength (0-100)
    trend_strength: f64,
    prev_strength: f64,

    // Price swing tracking
    last_high: f64,
    last_low: f64,
    current_high: f64,
    current_low: f64,

    // Swing history
    higher_highs: u32,
    lower_lows: u32,
    higher_lows: u32,
    lower_highs: u32,

    // Price history
    price_history: Vec<f64>,
    max_history: usize,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl TrendTracker {
    pub fn new(symbol: String, interval: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            interval,
            current_trend: TrendDirection::Sideways,
            prev_trend: TrendDirection::Sideways,
            trend_strength: 50.0,
            prev_strength: 50.0,
            last_high: 0.0,
            last_low: f64::MAX,
            current_high: 0.0,
            current_low: f64::MAX,
            higher_highs: 0,
            lower_lows: 0,
            higher_lows: 0,
            lower_highs: 0,
            price_history: Vec::with_capacity(100),
            max_history: 100,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 1000,
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    pub fn update(&mut self, high: f64, low: f64, close: f64, timestamp: i64) {
        self.updates_processed += 1;

        self.prev_trend = self.current_trend;
        self.prev_strength = self.trend_strength;

        // Update swings
        self.last_high = self.current_high;
        self.last_low = self.current_low;
        self.current_high = high;
        self.current_low = low;

        // Track swing patterns
        if high > self.last_high && self.last_high > 0.0 {
            self.higher_highs += 1;
            self.lower_highs = 0;
        } else if high < self.last_high && self.last_high > 0.0 {
            self.lower_highs += 1;
            self.higher_highs = 0;
        }

        if low > self.last_low && self.last_low < f64::MAX {
            self.higher_lows += 1;
            self.lower_lows = 0;
        } else if low < self.last_low && self.last_low < f64::MAX {
            self.lower_lows += 1;
            self.higher_lows = 0;
        }

        // Update price history
        self.price_history.push(close);
        if self.price_history.len() > self.max_history {
            self.price_history.remove(0);
        }

        // Calculate trend
        self.current_trend = self.determine_trend();
        self.trend_strength = self.calculate_strength();
    }

    fn determine_trend(&self) -> TrendDirection {
        if self.higher_highs >= 3 && self.higher_lows >= 2 {
            TrendDirection::Up
        } else if self.lower_lows >= 3 && self.lower_highs >= 2 {
            TrendDirection::Down
        } else {
            TrendDirection::Sideways
        }
    }

    fn calculate_strength(&self) -> f64 {
        if self.price_history.len() < 10 { return 50.0; }

        // Simple trend strength based on price direction consistency
        let mut up_moves = 0u32;
        let mut down_moves = 0u32;

        for i in 1..self.price_history.len() {
            if self.price_history[i] > self.price_history[i-1] {
                up_moves += 1;
            } else if self.price_history[i] < self.price_history[i-1] {
                down_moves += 1;
            }
        }

        let total = up_moves + down_moves;
        if total == 0 { return 50.0; }

        (up_moves as f64 / total as f64) * 100.0
    }

    pub fn check_events(&mut self, timestamp: i64) {
        self.check_trend_changed(timestamp);
        self.check_trend_strength_increased(timestamp);
        self.check_trend_strength_decreased(timestamp);
        self.check_trend_higher_high(timestamp);
        self.check_trend_lower_low(timestamp);
        self.check_trend_higher_low(timestamp);
        self.check_trend_lower_high(timestamp);
        self.check_trend_continuation(timestamp);
    }

    fn check_trend_changed(&mut self, timestamp: i64) {
        if self.current_trend != self.prev_trend && self.should_fire(KLINE_TREND_CHANGED, timestamp) {
            self.publish_event(KLINE_TREND_CHANGED, timestamp, json!({
                "prev_trend": format!("{:?}", self.prev_trend),
                "new_trend": format!("{:?}", self.current_trend),
                "strength": self.trend_strength,
            }));
        }
    }

    fn check_trend_strength_increased(&mut self, timestamp: i64) {
        let strength_change = self.trend_strength - self.prev_strength;

        if strength_change > 10.0 && self.should_fire(KLINE_TREND_STRENGTH_INCREASED, timestamp) {
            self.publish_event(KLINE_TREND_STRENGTH_INCREASED, timestamp, json!({
                "prev_strength": self.prev_strength,
                "new_strength": self.trend_strength,
                "change": strength_change,
            }));
        }
    }

    fn check_trend_strength_decreased(&mut self, timestamp: i64) {
        let strength_change = self.prev_strength - self.trend_strength;

        if strength_change > 10.0 && self.should_fire(KLINE_TREND_STRENGTH_DECREASED, timestamp) {
            self.publish_event(KLINE_TREND_STRENGTH_DECREASED, timestamp, json!({
                "prev_strength": self.prev_strength,
                "new_strength": self.trend_strength,
                "change": strength_change,
            }));
        }
    }

    fn check_trend_higher_high(&mut self, timestamp: i64) {
        if self.current_high > self.last_high && self.last_high > 0.0
            && self.should_fire(KLINE_TREND_HIGHER_HIGH, timestamp) {
            self.publish_event(KLINE_TREND_HIGHER_HIGH, timestamp, json!({
                "current_high": self.current_high,
                "last_high": self.last_high,
                "higher_by": self.current_high - self.last_high,
            }));
        }
    }

    fn check_trend_lower_low(&mut self, timestamp: i64) {
        if self.current_low < self.last_low && self.last_low < f64::MAX
            && self.should_fire(KLINE_TREND_LOWER_LOW, timestamp) {
            self.publish_event(KLINE_TREND_LOWER_LOW, timestamp, json!({
                "current_low": self.current_low,
                "last_low": self.last_low,
                "lower_by": self.last_low - self.current_low,
            }));
        }
    }

    fn check_trend_higher_low(&mut self, timestamp: i64) {
        if self.current_low > self.last_low && self.last_low < f64::MAX
            && self.should_fire(KLINE_TREND_HIGHER_LOW, timestamp) {
            self.publish_event(KLINE_TREND_HIGHER_LOW, timestamp, json!({
                "current_low": self.current_low,
                "last_low": self.last_low,
                "higher_by": self.current_low - self.last_low,
            }));
        }
    }

    fn check_trend_lower_high(&mut self, timestamp: i64) {
        if self.current_high < self.last_high && self.last_high > 0.0
            && self.should_fire(KLINE_TREND_LOWER_HIGH, timestamp) {
            self.publish_event(KLINE_TREND_LOWER_HIGH, timestamp, json!({
                "current_high": self.current_high,
                "last_high": self.last_high,
                "lower_by": self.last_high - self.current_high,
            }));
        }
    }

    fn check_trend_continuation(&mut self, timestamp: i64) {
        // Trend continuation: same trend for multiple periods with increasing strength
        if self.current_trend == self.prev_trend
            && self.trend_strength > self.prev_strength
            && self.trend_strength > 60.0
            && self.should_fire(KLINE_TREND_CONTINUATION, timestamp) {
            self.publish_event(KLINE_TREND_CONTINUATION, timestamp, json!({
                "trend": format!("{:?}", self.current_trend),
                "strength": self.trend_strength,
            }));
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
            obj.insert("interval".to_string(), json!(self.interval));
        }

        publish_event(event_type, timestamp, serde_json::from_value(data).unwrap_or_default(),
            "kline_aggregator", EventPriority::Medium);

        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(symbol = %self.symbol, event = event_type, "Trend event published");
    }

    pub fn updates_processed(&self) -> u64 { self.updates_processed }
    pub fn events_fired(&self) -> u64 { self.events_fired }
}
