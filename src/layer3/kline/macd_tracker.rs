// MACD Tracker - Monitors MACD indicator
// Generates 8 MACD-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer3::common::event_types::*;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// MACD histogram regime
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MACDRegime {
    PositiveIncreasing,
    PositiveDecreasing,
    NegativeIncreasing,
    NegativeDecreasing,
}

/// MACDTracker monitors MACD values and signals
pub struct MACDTracker {
    symbol: String,
    interval: String,

    // MACD values
    macd_line: f64,
    signal_line: f64,
    histogram: f64,

    // Previous values
    prev_macd: f64,
    prev_signal: f64,
    prev_histogram: f64,

    // Histogram history
    histogram_history: Vec<f64>,
    max_history: usize,

    // Regime
    current_regime: MACDRegime,
    prev_regime: MACDRegime,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl MACDTracker {
    pub fn new(symbol: String, interval: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            interval,
            macd_line: 0.0,
            signal_line: 0.0,
            histogram: 0.0,
            prev_macd: 0.0,
            prev_signal: 0.0,
            prev_histogram: 0.0,
            histogram_history: Vec::with_capacity(50),
            max_history: 50,
            current_regime: MACDRegime::PositiveIncreasing,
            prev_regime: MACDRegime::PositiveIncreasing,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 1000,
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    pub fn update(&mut self, macd: f64, signal: f64, timestamp: i64) {
        self.updates_processed += 1;

        self.prev_macd = self.macd_line;
        self.prev_signal = self.signal_line;
        self.prev_histogram = self.histogram;
        self.prev_regime = self.current_regime;

        self.macd_line = macd;
        self.signal_line = signal;
        self.histogram = macd - signal;

        self.current_regime = self.classify_regime();

        self.histogram_history.push(self.histogram);
        if self.histogram_history.len() > self.max_history {
            self.histogram_history.remove(0);
        }
    }

    fn classify_regime(&self) -> MACDRegime {
        if self.histogram >= 0.0 {
            if self.histogram >= self.prev_histogram {
                MACDRegime::PositiveIncreasing
            } else {
                MACDRegime::PositiveDecreasing
            }
        } else {
            if self.histogram >= self.prev_histogram {
                MACDRegime::NegativeIncreasing
            } else {
                MACDRegime::NegativeDecreasing
            }
        }
    }

    pub fn check_events(&mut self, timestamp: i64) {
        self.check_macd_crossover(timestamp);
        self.check_macd_zero_crossed(timestamp);
        self.check_macd_histogram_regime(timestamp);
        self.check_macd_divergence(timestamp);
        self.check_macd_histogram_peak(timestamp);
        self.check_macd_acceleration(timestamp);
        self.check_macd_deceleration(timestamp);
        self.check_macd_hidden_divergence(timestamp);
    }

    fn check_macd_crossover(&mut self, timestamp: i64) {
        let bullish_cross = self.prev_macd <= self.prev_signal && self.macd_line > self.signal_line;
        let bearish_cross = self.prev_macd >= self.prev_signal && self.macd_line < self.signal_line;

        if (bullish_cross || bearish_cross) && self.should_fire(KLINE_MACD_CROSSOVER, timestamp) {
            self.publish_event(KLINE_MACD_CROSSOVER, timestamp, json!({
                "type": if bullish_cross { "bullish" } else { "bearish" },
                "macd": self.macd_line,
                "signal": self.signal_line,
            }));
        }
    }

    fn check_macd_zero_crossed(&mut self, timestamp: i64) {
        let crossed_above = self.prev_macd <= 0.0 && self.macd_line > 0.0;
        let crossed_below = self.prev_macd >= 0.0 && self.macd_line < 0.0;

        if (crossed_above || crossed_below) && self.should_fire(KLINE_MACD_ZERO_CROSSED, timestamp) {
            self.publish_event(KLINE_MACD_ZERO_CROSSED, timestamp, json!({
                "direction": if crossed_above { "above" } else { "below" },
                "macd": self.macd_line,
            }));
        }
    }

    fn check_macd_histogram_regime(&mut self, timestamp: i64) {
        if self.current_regime != self.prev_regime && self.should_fire(KLINE_MACD_HISTOGRAM_REGIME, timestamp) {
            self.publish_event(KLINE_MACD_HISTOGRAM_REGIME, timestamp, json!({
                "prev_regime": format!("{:?}", self.prev_regime),
                "new_regime": format!("{:?}", self.current_regime),
                "histogram": self.histogram,
            }));
        }
    }

    fn check_macd_divergence(&mut self, timestamp: i64) {
        // Simplified divergence detection
        if self.histogram_history.len() < 10 { return; }

        let len = self.histogram_history.len();
        let hist_trend = self.histogram_history[len-1] - self.histogram_history[len-5];

        // Bearish divergence: histogram decreasing while price potentially rising
        if hist_trend < 0.0 && self.histogram > 0.0 && self.should_fire(KLINE_MACD_DIVERGENCE, timestamp) {
            self.publish_event(KLINE_MACD_DIVERGENCE, timestamp, json!({
                "type": "bearish",
                "histogram": self.histogram,
            }));
        }
    }

    fn check_macd_histogram_peak(&mut self, timestamp: i64) {
        // Detect histogram peak (local maximum/minimum)
        if self.histogram_history.len() < 3 { return; }

        let len = self.histogram_history.len();
        let prev = self.histogram_history[len-2];
        let curr = self.histogram_history[len-1];
        let prev_prev = self.histogram_history[len-3];

        let is_peak = prev > prev_prev && prev > curr;
        let is_valley = prev < prev_prev && prev < curr;

        if (is_peak || is_valley) && self.should_fire(KLINE_MACD_HISTOGRAM_PEAK, timestamp) {
            self.publish_event(KLINE_MACD_HISTOGRAM_PEAK, timestamp, json!({
                "type": if is_peak { "peak" } else { "valley" },
                "histogram": prev,
            }));
        }
    }

    fn check_macd_acceleration(&mut self, timestamp: i64) {
        let acceleration = self.histogram - self.prev_histogram;

        if acceleration > 0.0 && acceleration.abs() > 0.01 && self.should_fire(KLINE_MACD_ACCELERATION, timestamp) {
            self.publish_event(KLINE_MACD_ACCELERATION, timestamp, json!({
                "acceleration": acceleration,
                "histogram": self.histogram,
            }));
        }
    }

    fn check_macd_deceleration(&mut self, timestamp: i64) {
        let acceleration = self.histogram - self.prev_histogram;

        if acceleration < 0.0 && acceleration.abs() > 0.01 && self.should_fire(KLINE_MACD_DECELERATION, timestamp) {
            self.publish_event(KLINE_MACD_DECELERATION, timestamp, json!({
                "deceleration": acceleration.abs(),
                "histogram": self.histogram,
            }));
        }
    }

    fn check_macd_hidden_divergence(&mut self, timestamp: i64) {
        // Hidden divergence detection (continuation signal)
        if self.histogram_history.len() < 10 { return; }

        // Simplified: histogram making higher lows while in positive territory
        let len = self.histogram_history.len();
        if self.histogram > 0.0 && self.histogram_history[len-5] > 0.0 {
            if self.histogram > self.histogram_history[len-5]
                && self.should_fire(KLINE_MACD_HIDDEN_DIVERGENCE, timestamp) {
                self.publish_event(KLINE_MACD_HIDDEN_DIVERGENCE, timestamp, json!({
                    "type": "bullish",
                    "histogram": self.histogram,
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
        debug!(symbol = %self.symbol, event = event_type, "MACD event published");
    }

    pub fn updates_processed(&self) -> u64 { self.updates_processed }
    pub fn events_fired(&self) -> u64 { self.events_fired }
}
