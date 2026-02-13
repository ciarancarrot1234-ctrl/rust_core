// Candle Tracker - Monitors candlestick patterns
// Generates 12 candle-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer3::common::event_types::*;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Candle data
#[derive(Debug, Clone)]
pub struct Candle {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub timestamp: i64,
}

impl Candle {
    pub fn body(&self) -> f64 { (self.close - self.open).abs() }
    pub fn upper_wick(&self) -> f64 { self.high - self.open.max(self.close) }
    pub fn lower_wick(&self) -> f64 { self.open.min(self.close) - self.low }
    pub fn range(&self) -> f64 { self.high - self.low }
    pub fn is_bullish(&self) -> bool { self.close > self.open }
    pub fn is_bearish(&self) -> bool { self.close < self.open }
    pub fn body_pct(&self) -> f64 { if self.range() > 0.0 { self.body() / self.range() } else { 0.0 } }
}

/// CandleTracker monitors candlestick patterns
pub struct CandleTracker {
    symbol: String,
    interval: String,

    // Recent candles
    candles: Vec<Candle>,
    max_candles: usize,

    // Consecutive tracking
    consecutive_bullish: u32,
    consecutive_bearish: u32,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl CandleTracker {
    pub fn new(symbol: String, interval: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            interval,
            candles: Vec::with_capacity(20),
            max_candles: 20,
            consecutive_bullish: 0,
            consecutive_bearish: 0,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 500,
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    pub fn update(&mut self, candle: Candle, timestamp: i64) {
        self.updates_processed += 1;

        // Update consecutive counts
        if candle.is_bullish() {
            self.consecutive_bullish += 1;
            self.consecutive_bearish = 0;
        } else if candle.is_bearish() {
            self.consecutive_bearish += 1;
            self.consecutive_bullish = 0;
        }

        // Store candle
        self.candles.push(candle);
        if self.candles.len() > self.max_candles {
            self.candles.remove(0);
        }
    }

    pub fn check_events(&mut self, timestamp: i64) {
        self.check_candle_pattern(timestamp);
        self.check_candle_doji(timestamp);
        self.check_candle_engulfing_bullish(timestamp);
        self.check_candle_engulfing_bearish(timestamp);
        self.check_candle_hammer(timestamp);
        self.check_candle_shooting_star(timestamp);
        self.check_candle_size_anomaly(timestamp);
        self.check_candle_wick_anomaly(timestamp);
        self.check_candle_consecutive(timestamp);
        self.check_candle_inside_bar(timestamp);
        self.check_candle_outside_bar(timestamp);
        self.check_candle_pin_bar(timestamp);
    }

    fn current_candle(&self) -> Option<&Candle> {
        self.candles.last()
    }

    fn prev_candle(&self) -> Option<&Candle> {
        if self.candles.len() >= 2 {
            self.candles.get(self.candles.len() - 2)
        } else {
            None
        }
    }

    fn check_candle_pattern(&mut self, timestamp: i64) {
        // General pattern detection
        if let (Some(curr), Some(prev)) = (self.current_candle(), self.prev_candle()) {
            let pattern_detected = self.detect_pattern(curr, prev);

            if pattern_detected.is_some() && self.should_fire(KLINE_CANDLE_PATTERN, timestamp) {
                self.publish_event(KLINE_CANDLE_PATTERN, timestamp, json!({
                    "pattern": pattern_detected.unwrap(),
                    "open": curr.open,
                    "high": curr.high,
                    "low": curr.low,
                    "close": curr.close,
                }));
            }
        }
    }

    fn detect_pattern(&self, curr: &Candle, prev: &Candle) -> Option<String> {
        // Check various patterns
        if curr.body_pct() < 0.1 {
            return Some("doji".to_string());
        }
        None
    }

    fn check_candle_doji(&mut self, timestamp: i64) {
        if let Some(curr) = self.current_candle() {
            if curr.body_pct() < 0.1 && curr.range() > 0.0 && self.should_fire(KLINE_CANDLE_DOJI, timestamp) {
                self.publish_event(KLINE_CANDLE_DOJI, timestamp, json!({
                    "body_pct": curr.body_pct(),
                    "range": curr.range(),
                }));
            }
        }
    }

    fn check_candle_engulfing_bullish(&mut self, timestamp: i64) {
        if let (Some(curr), Some(prev)) = (self.current_candle(), self.prev_candle()) {
            let is_engulfing = curr.is_bullish()
                && prev.is_bearish()
                && curr.open < prev.close
                && curr.close > prev.open;

            if is_engulfing && self.should_fire(KLINE_CANDLE_ENGULFING_BULLISH, timestamp) {
                self.publish_event(KLINE_CANDLE_ENGULFING_BULLISH, timestamp, json!({
                    "curr_open": curr.open,
                    "curr_close": curr.close,
                    "prev_open": prev.open,
                    "prev_close": prev.close,
                }));
            }
        }
    }

    fn check_candle_engulfing_bearish(&mut self, timestamp: i64) {
        if let (Some(curr), Some(prev)) = (self.current_candle(), self.prev_candle()) {
            let is_engulfing = curr.is_bearish()
                && prev.is_bullish()
                && curr.open > prev.close
                && curr.close < prev.open;

            if is_engulfing && self.should_fire(KLINE_CANDLE_ENGULFING_BEARISH, timestamp) {
                self.publish_event(KLINE_CANDLE_ENGULFING_BEARISH, timestamp, json!({
                    "curr_open": curr.open,
                    "curr_close": curr.close,
                    "prev_open": prev.open,
                    "prev_close": prev.close,
                }));
            }
        }
    }

    fn check_candle_hammer(&mut self, timestamp: i64) {
        if let Some(curr) = self.current_candle() {
            let is_hammer = curr.lower_wick() > curr.body() * 2.0
                && curr.upper_wick() < curr.body() * 0.5
                && curr.is_bullish();

            if is_hammer && self.should_fire(KLINE_CANDLE_HAMMER, timestamp) {
                self.publish_event(KLINE_CANDLE_HAMMER, timestamp, json!({
                    "lower_wick": curr.lower_wick(),
                    "body": curr.body(),
                }));
            }
        }
    }

    fn check_candle_shooting_star(&mut self, timestamp: i64) {
        if let Some(curr) = self.current_candle() {
            let is_shooting_star = curr.upper_wick() > curr.body() * 2.0
                && curr.lower_wick() < curr.body() * 0.5
                && curr.is_bearish();

            if is_shooting_star && self.should_fire(KLINE_CANDLE_SHOOTING_STAR, timestamp) {
                self.publish_event(KLINE_CANDLE_SHOOTING_STAR, timestamp, json!({
                    "upper_wick": curr.upper_wick(),
                    "body": curr.body(),
                }));
            }
        }
    }

    fn check_candle_size_anomaly(&mut self, timestamp: i64) {
        if self.candles.len() < 5 { return; }

        if let Some(curr) = self.current_candle() {
            let avg_range: f64 = self.candles.iter().take(self.candles.len()-1).map(|c| c.range()).sum::<f64>()
                / (self.candles.len() - 1) as f64;

            if curr.range() > avg_range * 2.0 && self.should_fire(KLINE_CANDLE_SIZE_ANOMALY, timestamp) {
                self.publish_event(KLINE_CANDLE_SIZE_ANOMALY, timestamp, json!({
                    "range": curr.range(),
                    "avg_range": avg_range,
                    "ratio": curr.range() / avg_range,
                }));
            }
        }
    }

    fn check_candle_wick_anomaly(&mut self, timestamp: i64) {
        if let Some(curr) = self.current_candle() {
            let wick_ratio = (curr.upper_wick() + curr.lower_wick()) / curr.range().max(0.001);

            if wick_ratio > 0.7 && self.should_fire(KLINE_CANDLE_WICK_ANOMALY, timestamp) {
                self.publish_event(KLINE_CANDLE_WICK_ANOMALY, timestamp, json!({
                    "upper_wick": curr.upper_wick(),
                    "lower_wick": curr.lower_wick(),
                    "wick_ratio": wick_ratio,
                }));
            }
        }
    }

    fn check_candle_consecutive(&mut self, timestamp: i64) {
        if (self.consecutive_bullish >= 5 || self.consecutive_bearish >= 5)
            && self.should_fire(KLINE_CANDLE_CONSECUTIVE, timestamp) {
            self.publish_event(KLINE_CANDLE_CONSECUTIVE, timestamp, json!({
                "direction": if self.consecutive_bullish >= 5 { "bullish" } else { "bearish" },
                "count": self.consecutive_bullish.max(self.consecutive_bearish),
            }));
        }
    }

    fn check_candle_inside_bar(&mut self, timestamp: i64) {
        if let (Some(curr), Some(prev)) = (self.current_candle(), self.prev_candle()) {
            let is_inside = curr.high < prev.high && curr.low > prev.low;

            if is_inside && self.should_fire(KLINE_CANDLE_INSIDE_BAR, timestamp) {
                self.publish_event(KLINE_CANDLE_INSIDE_BAR, timestamp, json!({
                    "curr_high": curr.high,
                    "curr_low": curr.low,
                    "prev_high": prev.high,
                    "prev_low": prev.low,
                }));
            }
        }
    }

    fn check_candle_outside_bar(&mut self, timestamp: i64) {
        if let (Some(curr), Some(prev)) = (self.current_candle(), self.prev_candle()) {
            let is_outside = curr.high > prev.high && curr.low < prev.low;

            if is_outside && self.should_fire(KLINE_CANDLE_OUTSIDE_BAR, timestamp) {
                self.publish_event(KLINE_CANDLE_OUTSIDE_BAR, timestamp, json!({
                    "curr_high": curr.high,
                    "curr_low": curr.low,
                    "prev_high": prev.high,
                    "prev_low": prev.low,
                }));
            }
        }
    }

    fn check_candle_pin_bar(&mut self, timestamp: i64) {
        if let Some(curr) = self.current_candle() {
            let total_wick = curr.upper_wick() + curr.lower_wick();
            let is_pin_bar = total_wick > curr.body() * 3.0 && curr.body_pct() < 0.3;

            if is_pin_bar && self.should_fire(KLINE_CANDLE_PIN_BAR, timestamp) {
                self.publish_event(KLINE_CANDLE_PIN_BAR, timestamp, json!({
                    "upper_wick": curr.upper_wick(),
                    "lower_wick": curr.lower_wick(),
                    "body": curr.body(),
                }));
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
            obj.insert("interval".to_string(), json!(self.interval));
        }

        publish_event(event_type, timestamp, serde_json::from_value(data).unwrap_or_default(),
            "kline_aggregator", EventPriority::Medium);

        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(symbol = %self.symbol, event = event_type, "Candle event published");
    }

    pub fn updates_processed(&self) -> u64 { self.updates_processed }
    pub fn events_fired(&self) -> u64 { self.events_fired }
}
