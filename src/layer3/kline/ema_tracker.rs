// EMA Tracker - Monitors EMA crossovers and price relationships
// Generates 10 EMA-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer3::common::event_types::*;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// EMA alignment state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EMAAlignment {
    BullishStack,   // Price > EMA9 > EMA21 > EMA50 > EMA200
    BearishStack,   // Price < EMA9 < EMA21 < EMA50 < EMA200
    Mixed,          // No clear alignment
}

/// EMATracker monitors EMA values and their relationships
pub struct EMATracker {
    symbol: String,
    interval: String,

    // EMA values
    ema9: f64,
    ema21: f64,
    ema50: f64,
    ema200: f64,

    // Previous values for crossover detection
    prev_ema9: f64,
    prev_ema21: f64,
    prev_ema50: f64,
    prev_ema200: f64,
    prev_price: f64,

    // Current price
    current_price: f64,

    // Alignment state
    current_alignment: EMAAlignment,
    prev_alignment: EMAAlignment,

    // Slope tracking
    ema9_slope: f64,
    ema21_slope: f64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl EMATracker {
    pub fn new(symbol: String, interval: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            interval,
            ema9: 0.0, ema21: 0.0, ema50: 0.0, ema200: 0.0,
            prev_ema9: 0.0, prev_ema21: 0.0, prev_ema50: 0.0, prev_ema200: 0.0,
            prev_price: 0.0,
            current_price: 0.0,
            current_alignment: EMAAlignment::Mixed,
            prev_alignment: EMAAlignment::Mixed,
            ema9_slope: 0.0,
            ema21_slope: 0.0,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 1000,
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    pub fn update(&mut self, price: f64, ema9: f64, ema21: f64, ema50: f64, ema200: f64, timestamp: i64) {
        self.updates_processed += 1;

        // Store previous values
        self.prev_ema9 = self.ema9;
        self.prev_ema21 = self.ema21;
        self.prev_ema50 = self.ema50;
        self.prev_ema200 = self.ema200;
        self.prev_price = self.current_price;
        self.prev_alignment = self.current_alignment;

        // Update current values
        self.current_price = price;
        self.ema9 = ema9;
        self.ema21 = ema21;
        self.ema50 = ema50;
        self.ema200 = ema200;

        // Calculate slopes
        if self.prev_ema9 > 0.0 {
            self.ema9_slope = (self.ema9 - self.prev_ema9) / self.prev_ema9 * 100.0;
        }
        if self.prev_ema21 > 0.0 {
            self.ema21_slope = (self.ema21 - self.prev_ema21) / self.prev_ema21 * 100.0;
        }

        // Update alignment
        self.current_alignment = self.classify_alignment();
    }

    fn classify_alignment(&self) -> EMAAlignment {
        if self.current_price > self.ema9 && self.ema9 > self.ema21
            && self.ema21 > self.ema50 && self.ema50 > self.ema200 {
            EMAAlignment::BullishStack
        } else if self.current_price < self.ema9 && self.ema9 < self.ema21
            && self.ema21 < self.ema50 && self.ema50 < self.ema200 {
            EMAAlignment::BearishStack
        } else {
            EMAAlignment::Mixed
        }
    }

    pub fn check_events(&mut self, timestamp: i64) {
        self.check_ema_crossed(timestamp);
        self.check_ema_crossover(timestamp);
        self.check_ema_stack_bullish(timestamp);
        self.check_ema_stack_bearish(timestamp);
        self.check_ema_alignment(timestamp);
        self.check_ema_compression(timestamp);
        self.check_ema_expansion(timestamp);
        self.check_ema_slope(timestamp);
        self.check_ema_distance(timestamp);
        self.check_ema_sr_test(timestamp);
    }

    fn check_ema_crossed(&mut self, timestamp: i64) {
        // Price crossed EMA
        let crossed_ema9 = (self.prev_price < self.prev_ema9 && self.current_price > self.ema9)
            || (self.prev_price > self.prev_ema9 && self.current_price < self.ema9);

        if crossed_ema9 && self.should_fire(KLINE_EMA_CROSSED, timestamp) {
            self.publish_event(KLINE_EMA_CROSSED, timestamp, json!({
                "ema": "ema9",
                "direction": if self.current_price > self.ema9 { "above" } else { "below" },
                "price": self.current_price,
                "ema_value": self.ema9,
            }));
        }
    }

    fn check_ema_crossover(&mut self, timestamp: i64) {
        // EMA crossover (e.g., EMA9 crosses EMA21)
        let bullish_cross = self.prev_ema9 <= self.prev_ema21 && self.ema9 > self.ema21;
        let bearish_cross = self.prev_ema9 >= self.prev_ema21 && self.ema9 < self.ema21;

        if (bullish_cross || bearish_cross) && self.should_fire(KLINE_EMA_CROSSOVER, timestamp) {
            self.publish_event(KLINE_EMA_CROSSOVER, timestamp, json!({
                "type": if bullish_cross { "bullish" } else { "bearish" },
                "ema_fast": 9,
                "ema_slow": 21,
                "ema9": self.ema9,
                "ema21": self.ema21,
            }));
        }
    }

    fn check_ema_stack_bullish(&mut self, timestamp: i64) {
        if self.current_alignment == EMAAlignment::BullishStack
            && self.prev_alignment != EMAAlignment::BullishStack
            && self.should_fire(KLINE_EMA_STACK_BULLISH, timestamp) {
            self.publish_event(KLINE_EMA_STACK_BULLISH, timestamp, json!({
                "price": self.current_price,
                "ema9": self.ema9,
                "ema21": self.ema21,
                "ema50": self.ema50,
                "ema200": self.ema200,
            }));
        }
    }

    fn check_ema_stack_bearish(&mut self, timestamp: i64) {
        if self.current_alignment == EMAAlignment::BearishStack
            && self.prev_alignment != EMAAlignment::BearishStack
            && self.should_fire(KLINE_EMA_STACK_BEARISH, timestamp) {
            self.publish_event(KLINE_EMA_STACK_BEARISH, timestamp, json!({
                "price": self.current_price,
                "ema9": self.ema9,
                "ema21": self.ema21,
                "ema50": self.ema50,
                "ema200": self.ema200,
            }));
        }
    }

    fn check_ema_alignment(&mut self, timestamp: i64) {
        if self.current_alignment != self.prev_alignment
            && self.should_fire(KLINE_EMA_ALIGNMENT, timestamp) {
            self.publish_event(KLINE_EMA_ALIGNMENT, timestamp, json!({
                "prev_alignment": format!("{:?}", self.prev_alignment),
                "new_alignment": format!("{:?}", self.current_alignment),
            }));
        }
    }

    fn check_ema_compression(&mut self, timestamp: i64) {
        // EMA compression (EMAs converging)
        let spread_9_21 = (self.ema9 - self.ema21).abs() / self.ema21 * 100.0;
        let spread_21_50 = (self.ema21 - self.ema50).abs() / self.ema50 * 100.0;

        if spread_9_21 < 0.1 && spread_21_50 < 0.2 && self.should_fire(KLINE_EMA_COMPRESSION, timestamp) {
            self.publish_event(KLINE_EMA_COMPRESSION, timestamp, json!({
                "spread_9_21_pct": spread_9_21,
                "spread_21_50_pct": spread_21_50,
            }));
        }
    }

    fn check_ema_expansion(&mut self, timestamp: i64) {
        // EMA expansion (EMAs diverging)
        let spread_9_21 = (self.ema9 - self.ema21).abs() / self.ema21 * 100.0;

        if spread_9_21 > 1.0 && self.should_fire(KLINE_EMA_EXPANSION, timestamp) {
            self.publish_event(KLINE_EMA_EXPANSION, timestamp, json!({
                "spread_9_21_pct": spread_9_21,
            }));
        }
    }

    fn check_ema_slope(&mut self, timestamp: i64) {
        if self.ema9_slope.abs() > 0.5 && self.should_fire(KLINE_EMA_SLOPE, timestamp) {
            self.publish_event(KLINE_EMA_SLOPE, timestamp, json!({
                "ema9_slope_pct": self.ema9_slope,
                "ema21_slope_pct": self.ema21_slope,
            }));
        }
    }

    fn check_ema_distance(&mut self, timestamp: i64) {
        let distance_from_ema200 = (self.current_price - self.ema200).abs() / self.ema200 * 100.0;

        if distance_from_ema200 > 10.0 && self.should_fire(KLINE_EMA_DISTANCE, timestamp) {
            self.publish_event(KLINE_EMA_DISTANCE, timestamp, json!({
                "distance_from_ema200_pct": distance_from_ema200,
                "direction": if self.current_price > self.ema200 { "above" } else { "below" },
            }));
        }
    }

    fn check_ema_sr_test(&mut self, timestamp: i64) {
        // Price testing EMA as support/resistance
        let distance_to_ema21 = (self.current_price - self.ema21).abs() / self.ema21 * 100.0;

        if distance_to_ema21 < 0.2 && self.should_fire(KLINE_EMA_SR_TEST, timestamp) {
            self.publish_event(KLINE_EMA_SR_TEST, timestamp, json!({
                "ema": 21,
                "ema_value": self.ema21,
                "price": self.current_price,
                "distance_pct": distance_to_ema21,
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
        debug!(symbol = %self.symbol, event = event_type, "EMA event published");
    }

    pub fn updates_processed(&self) -> u64 { self.updates_processed }
    pub fn events_fired(&self) -> u64 { self.events_fired }
}
