// RSI Tracker - Monitors Relative Strength Index
// Generates 8 RSI-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer3::common::event_types::*;
use crate::layer3::common::time_windows::TimeWindow;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// RSI regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RSIRegime {
    Overbought,     // > 70
    Bullish,        // 50-70
    Neutral,        // 40-60
    Bearish,        // 30-50
    Oversold,       // < 30
}

/// RSITracker monitors RSI values and patterns
pub struct RSITracker {
    symbol: String,
    interval: String,

    // RSI values
    rsi: f64,
    prev_rsi: f64,

    // RSI history for divergence detection
    rsi_history: Vec<f64>,
    price_history: Vec<f64>,
    max_history: usize,

    // Regime tracking
    current_regime: RSIRegime,
    prev_regime: RSIRegime,

    // Failure swing detection
    failure_swing_point: Option<f64>,

    // Time window for divergence
    divergence_window: TimeWindow<(f64, f64)>,  // (rsi, price)

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl RSITracker {
    pub fn new(symbol: String, interval: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            interval,
            rsi: 50.0,
            prev_rsi: 50.0,
            rsi_history: Vec::with_capacity(100),
            price_history: Vec::with_capacity(100),
            max_history: 100,
            current_regime: RSIRegime::Neutral,
            prev_regime: RSIRegime::Neutral,
            failure_swing_point: None,
            divergence_window: TimeWindow::new(300_000, 500),  // 5 min
            last_event_times: HashMap::new(),
            min_event_interval_ms: 1000,
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    pub fn update(&mut self, rsi: f64, price: f64, timestamp: i64) {
        self.updates_processed += 1;

        self.prev_rsi = self.rsi;
        self.prev_regime = self.current_regime;

        self.rsi = rsi;
        self.current_regime = self.classify_regime(rsi);

        // Update history
        self.rsi_history.push(rsi);
        self.price_history.push(price);

        if self.rsi_history.len() > self.max_history {
            self.rsi_history.remove(0);
            self.price_history.remove(0);
        }

        // Update divergence window
        self.divergence_window.add(timestamp, (rsi, price));
        self.divergence_window.prune(timestamp);
    }

    fn classify_regime(&self, rsi: f64) -> RSIRegime {
        if rsi >= self.thresholds.rsi_overbought {
            RSIRegime::Overbought
        } else if rsi >= 50.0 {
            RSIRegime::Bullish
        } else if rsi >= 40.0 {
            RSIRegime::Neutral
        } else if rsi >= self.thresholds.rsi_oversold {
            RSIRegime::Bearish
        } else {
            RSIRegime::Oversold
        }
    }

    pub fn check_events(&mut self, timestamp: i64) {
        self.check_rsi_threshold(timestamp);
        self.check_rsi_divergence(timestamp);
        self.check_rsi_regime(timestamp);
        self.check_rsi_centerline(timestamp);
        self.check_rsi_extreme(timestamp);
        self.check_rsi_reversal(timestamp);
        self.check_rsi_failure_swing(timestamp);
        self.check_rsi_hidden_divergence(timestamp);
    }

    fn check_rsi_threshold(&mut self, timestamp: i64) {
        if self.rsi >= self.thresholds.rsi_overbought || self.rsi <= self.thresholds.rsi_oversold {
            if self.should_fire(KLINE_RSI_THRESHOLD, timestamp) {
                self.publish_event(KLINE_RSI_THRESHOLD, timestamp, json!({
                    "rsi": self.rsi,
                    "overbought": self.thresholds.rsi_overbought,
                    "oversold": self.thresholds.rsi_oversold,
                    "zone": if self.rsi >= self.thresholds.rsi_overbought { "overbought" } else { "oversold" },
                }));
            }
        }
    }

    fn check_rsi_divergence(&mut self, timestamp: i64) {
        // Detect regular divergence: price makes new high/low but RSI doesn't
        if self.rsi_history.len() < 10 { return; }

        let len = self.rsi_history.len();
        let recent_prices: Vec<_> = self.price_history.iter().rev().take(10).collect();
        let recent_rsi: Vec<_> = self.rsi_history.iter().rev().take(10).collect();

        // Bullish divergence: price lower low, RSI higher low
        let price_lower_low = recent_prices[0] < recent_prices.iter().skip(1).min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
        let rsi_higher_low = recent_rsi[0] > recent_rsi.iter().skip(1).min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();

        if price_lower_low && rsi_higher_low && self.should_fire(KLINE_RSI_DIVERGENCE, timestamp) {
            self.publish_event(KLINE_RSI_DIVERGENCE, timestamp, json!({
                "type": "bullish",
                "rsi": self.rsi,
                "price": self.price_history.last(),
            }));
        }

        // Bearish divergence: price higher high, RSI lower high
        let price_higher_high = recent_prices[0] > recent_prices.iter().skip(1).max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
        let rsi_lower_high = recent_rsi[0] < recent_rsi.iter().skip(1).max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();

        if price_higher_high && rsi_lower_high && self.should_fire(KLINE_RSI_DIVERGENCE, timestamp) {
            self.publish_event(KLINE_RSI_DIVERGENCE, timestamp, json!({
                "type": "bearish",
                "rsi": self.rsi,
                "price": self.price_history.last(),
            }));
        }
    }

    fn check_rsi_regime(&mut self, timestamp: i64) {
        if self.current_regime != self.prev_regime && self.should_fire(KLINE_RSI_REGIME, timestamp) {
            self.publish_event(KLINE_RSI_REGIME, timestamp, json!({
                "prev_regime": format!("{:?}", self.prev_regime),
                "new_regime": format!("{:?}", self.current_regime),
                "rsi": self.rsi,
            }));
        }
    }

    fn check_rsi_centerline(&mut self, timestamp: i64) {
        // RSI crossed 50 (centerline)
        let crossed_above = self.prev_rsi <= 50.0 && self.rsi > 50.0;
        let crossed_below = self.prev_rsi >= 50.0 && self.rsi < 50.0;

        if (crossed_above || crossed_below) && self.should_fire(KLINE_RSI_CENTERLINE, timestamp) {
            self.publish_event(KLINE_RSI_CENTERLINE, timestamp, json!({
                "direction": if crossed_above { "above" } else { "below" },
                "rsi": self.rsi,
            }));
        }
    }

    fn check_rsi_extreme(&mut self, timestamp: i64) {
        // RSI at extreme levels (>80 or <20)
        if (self.rsi > 80.0 || self.rsi < 20.0) && self.should_fire(KLINE_RSI_EXTREME, timestamp) {
            self.publish_event(KLINE_RSI_EXTREME, timestamp, json!({
                "rsi": self.rsi,
                "extreme": if self.rsi > 80.0 { "high" } else { "low" },
            }));
        }
    }

    fn check_rsi_reversal(&mut self, timestamp: i64) {
        // RSI reversal from extreme
        let reversed_from_overbought = self.prev_rsi >= 70.0 && self.rsi < 70.0;
        let reversed_from_oversold = self.prev_rsi <= 30.0 && self.rsi > 30.0;

        if (reversed_from_overbought || reversed_from_oversold) && self.should_fire(KLINE_RSI_REVERSAL, timestamp) {
            self.publish_event(KLINE_RSI_REVERSAL, timestamp, json!({
                "type": if reversed_from_oversold { "bullish" } else { "bearish" },
                "rsi": self.rsi,
            }));
        }
    }

    fn check_rsi_failure_sing(&mut self, timestamp: i64) {
        // Failure swing: RSI makes higher low in oversold or lower high in overbought
        if self.rsi_history.len() < 5 { return; }

        let len = self.rsi_history.len();

        // Bullish failure swing in oversold
        if self.rsi > 30.0 && self.rsi_history[len-2] < 30.0 {
            if let Some(&prev_low) = self.rsi_history.iter().rev().skip(2).find(|&&r| r < 30.0) {
                if self.rsi_history[len-2] > prev_low && self.should_fire(KLINE_RSI_FAILURE_SWING, timestamp) {
                    self.publish_event(KLINE_RSI_FAILURE_SWING, timestamp, json!({
                        "type": "bullish",
                        "rsi": self.rsi,
                    }));
                }
            }
        }
    }

    fn check_rsi_hidden_divergence(&mut self, timestamp: i64) {
        // Hidden divergence: price makes higher high but RSI makes lower high (continuation)
        if self.rsi_history.len() < 15 { return; }

        let len = self.rsi_history.len();

        // Hidden bullish: price higher low, RSI lower low
        // This signals continuation of uptrend
        if self.price_history[len-1] > self.price_history[len-10]
            && self.rsi_history[len-1] < self.rsi_history[len-10]
            && self.should_fire(KLINE_RSI_HIDDEN_DIVERGENCE, timestamp) {
            self.publish_event(KLINE_RSI_HIDDEN_DIVERGENCE, timestamp, json!({
                "type": "bullish",
                "rsi": self.rsi,
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
        debug!(symbol = %self.symbol, event = event_type, "RSI event published");
    }

    pub fn updates_processed(&self) -> u64 { self.updates_processed }
    pub fn events_fired(&self) -> u64 { self.events_fired }
}
