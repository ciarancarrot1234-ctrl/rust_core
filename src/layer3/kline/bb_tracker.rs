// BB Tracker - Monitors Bollinger Bands and Volatility
// Generates 16 events (8 BB + 8 volatility)

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer3::common::event_types::*;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// BB position relative to bands
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BBPosition {
    AboveUpper,
    AtUpper,
    InsideUpper,
    AtMiddle,
    InsideLower,
    AtLower,
    BelowLower,
}

/// Volatility regime
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolatilityRegime {
    Low,
    Normal,
    High,
    Extreme,
}

/// BBTracker monitors Bollinger Bands
pub struct BBTracker {
    symbol: String,
    interval: String,

    // BB values
    middle_band: f64,
    upper_band: f64,
    lower_band: f64,
    bandwidth: f64,  // (upper - lower) / middle * 100

    // Previous values
    prev_middle: f64,
    prev_upper: f64,
    prev_lower: f64,
    prev_bandwidth: f64,
    prev_price: f64,

    // Current price
    current_price: f64,

    // Position tracking
    current_position: BBPosition,
    prev_position: BBPosition,

    // Bandwidth history for squeeze detection
    bandwidth_history: Vec<f64>,
    max_history: usize,

    // Volatility regime
    volatility_regime: VolatilityRegime,
    prev_volatility_regime: VolatilityRegime,

    // ATR
    atr: f64,
    atr_period: u32,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl BBTracker {
    pub fn new(symbol: String, interval: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            interval,
            middle_band: 0.0,
            upper_band: 0.0,
            lower_band: 0.0,
            bandwidth: 0.0,
            prev_middle: 0.0,
            prev_upper: 0.0,
            prev_lower: 0.0,
            prev_bandwidth: 0.0,
            prev_price: 0.0,
            current_price: 0.0,
            current_position: BBPosition::AtMiddle,
            prev_position: BBPosition::AtMiddle,
            bandwidth_history: Vec::with_capacity(50),
            max_history: 50,
            volatility_regime: VolatilityRegime::Normal,
            prev_volatility_regime: VolatilityRegime::Normal,
            atr: 0.0,
            atr_period: thresholds.atr_period,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 1000,
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    pub fn update(&mut self, price: f64, middle: f64, upper: f64, lower: f64, atr: f64, timestamp: i64) {
        self.updates_processed += 1;

        self.prev_middle = self.middle_band;
        self.prev_upper = self.upper_band;
        self.prev_lower = self.lower_band;
        self.prev_bandwidth = self.bandwidth;
        self.prev_price = self.current_price;
        self.prev_position = self.current_position;
        self.prev_volatility_regime = self.volatility_regime;

        self.current_price = price;
        self.middle_band = middle;
        self.upper_band = upper;
        self.lower_band = lower;
        self.atr = atr;

        // Calculate bandwidth
        if middle > 0.0 {
            self.bandwidth = (upper - lower) / middle * 100.0;
        }

        self.current_position = self.classify_position();
        self.volatility_regime = self.classify_volatility();

        self.bandwidth_history.push(self.bandwidth);
        if self.bandwidth_history.len() > self.max_history {
            self.bandwidth_history.remove(0);
        }
    }

    fn classify_position(&self) -> BBPosition {
        let range = self.upper_band - self.lower_band;
        if range <= 0.0 { return BBPosition::AtMiddle; }

        if self.current_price > self.upper_band {
            BBPosition::AboveUpper
        } else if self.current_price > self.upper_band - range * 0.1 {
            BBPosition::AtUpper
        } else if self.current_price > self.middle_band + range * 0.1 {
            BBPosition::InsideUpper
        } else if self.current_price > self.middle_band - range * 0.1 {
            BBPosition::AtMiddle
        } else if self.current_price > self.lower_band + range * 0.1 {
            BBPosition::InsideLower
        } else if self.current_price > self.lower_band {
            BBPosition::AtLower
        } else {
            BBPosition::BelowLower
        }
    }

    fn classify_volatility(&self) -> VolatilityRegime {
        if self.bandwidth < 2.0 {
            VolatilityRegime::Low
        } else if self.bandwidth < 5.0 {
            VolatilityRegime::Normal
        } else if self.bandwidth < 10.0 {
            VolatilityRegime::High
        } else {
            VolatilityRegime::Extreme
        }
    }

    pub fn check_events(&mut self, timestamp: i64) {
        // BB events
        self.check_bb_touched(timestamp);
        self.check_bb_exited(timestamp);
        self.check_bb_returned(timestamp);
        self.check_bb_walk(timestamp);
        self.check_bb_middle_crossed(timestamp);
        self.check_bb_reversal(timestamp);
        self.check_bb_breakout(timestamp);
        self.check_bb_failed_breakout(timestamp);

        // Volatility events
        self.check_volatility_threshold(timestamp);
        self.check_volatility_regime(timestamp);
        self.check_volatility_contraction(timestamp);
        self.check_volatility_expansion(timestamp);
        self.check_bb_width_extreme(timestamp);
        self.check_bb_squeeze(timestamp);
        self.check_bb_expansion_detected(timestamp);
        self.check_volatility_spike(timestamp);
    }

    fn check_bb_touched(&mut self, timestamp: i64) {
        let touched_upper = self.current_price >= self.upper_band * 0.99;
        let touched_lower = self.current_price <= self.lower_band * 1.01;

        if (touched_upper || touched_lower) && self.should_fire(KLINE_BB_TOUCHED, timestamp) {
            self.publish_event(KLINE_BB_TOUCHED, timestamp, json!({
                "band": if touched_upper { "upper" } else { "lower" },
                "price": self.current_price,
                "band_value": if touched_upper { self.upper_band } else { self.lower_band },
            }));
        }
    }

    fn check_bb_exited(&mut self, timestamp: i64) {
        let exited_above = self.prev_price > self.upper_band && self.current_price < self.upper_band;
        let exited_below = self.prev_price < self.lower_band && self.current_price > self.lower_band;

        if (exited_above || exited_below) && self.should_fire(KLINE_BB_EXITED, timestamp) {
            self.publish_event(KLINE_BB_EXITED, timestamp, json!({
                "direction": if exited_above { "from_above" } else { "from_below" },
                "price": self.current_price,
            }));
        }
    }

    fn check_bb_returned(&mut self, timestamp: i64) {
        let returned_from_above = self.prev_position == BBPosition::AboveUpper && self.current_position != BBPosition::AboveUpper;
        let returned_from_below = self.prev_position == BBPosition::BelowLower && self.current_position != BBPosition::BelowLower;

        if (returned_from_above || returned_from_below) && self.should_fire(KLINE_BB_RETURNED, timestamp) {
            self.publish_event(KLINE_BB_RETURNED, timestamp, json!({
                "direction": if returned_from_above { "from_above" } else { "from_below" },
                "price": self.current_price,
            }));
        }
    }

    fn check_bb_walk(&mut self, timestamp: i64) {
        // Walking the bands (staying near band for multiple periods)
        let walking_upper = self.current_position == BBPosition::AtUpper
            && self.prev_position == BBPosition::AtUpper;
        let walking_lower = self.current_position == BBPosition::AtLower
            && self.prev_position == BBPosition::AtLower;

        if (walking_upper || walking_lower) && self.should_fire(KLINE_BB_WALK, timestamp) {
            self.publish_event(KLINE_BB_WALK, timestamp, json!({
                "band": if walking_upper { "upper" } else { "lower" },
                "price": self.current_price,
            }));
        }
    }

    fn check_bb_middle_crossed(&mut self, timestamp: i64) {
        let crossed_above = self.prev_price <= self.prev_middle && self.current_price > self.middle_band;
        let crossed_below = self.prev_price >= self.prev_middle && self.current_price < self.middle_band;

        if (crossed_above || crossed_below) && self.should_fire(KLINE_BB_MIDDLE_CROSSED, timestamp) {
            self.publish_event(KLINE_BB_MIDDLE_CROSSED, timestamp, json!({
                "direction": if crossed_above { "above" } else { "below" },
                "price": self.current_price,
                "middle": self.middle_band,
            }));
        }
    }

    fn check_bb_reversal(&mut self, timestamp: i64) {
        // Reversal after touching band
        let reversal_up = self.prev_position == BBPosition::AtLower
            && self.current_position == BBPosition::InsideLower;
        let reversal_down = self.prev_position == BBPosition::AtUpper
            && self.current_position == BBPosition::InsideUpper;

        if (reversal_up || reversal_down) && self.should_fire(KLINE_BB_REVERSAL, timestamp) {
            self.publish_event(KLINE_BB_REVERSAL, timestamp, json!({
                "direction": if reversal_up { "bullish" } else { "bearish" },
                "price": self.current_price,
            }));
        }
    }

    fn check_bb_breakout(&mut self, timestamp: i64) {
        let breakout_up = self.prev_price <= self.prev_upper && self.current_price > self.upper_band;
        let breakout_down = self.prev_price >= self.prev_lower && self.current_price < self.lower_band;

        if (breakout_up || breakout_down) && self.should_fire(KLINE_BB_BREAKOUT, timestamp) {
            self.publish_event(KLINE_BB_BREAKOUT, timestamp, json!({
                "direction": if breakout_up { "up" } else { "down" },
                "price": self.current_price,
                "band": if breakout_up { self.upper_band } else { self.lower_band },
            }));
        }
    }

    fn check_bb_failed_breakout(&mut self, timestamp: i64) {
        let failed_up = self.prev_position == BBPosition::AboveUpper
            && self.current_position != BBPosition::AboveUpper;
        let failed_down = self.prev_position == BBPosition::BelowLower
            && self.current_position != BBPosition::BelowLower;

        if (failed_up || failed_down) && self.should_fire(KLINE_BB_FAILED_BREAKOUT, timestamp) {
            self.publish_event(KLINE_BB_FAILED_BREAKOUT, timestamp, json!({
                "direction": if failed_up { "up" } else { "down" },
                "price": self.current_price,
            }));
        }
    }

    fn check_volatility_threshold(&mut self, timestamp: i64) {
        if self.atr > 0.0 && self.should_fire(KLINE_VOLATILITY_THRESHOLD, timestamp) {
            self.publish_event(KLINE_VOLATILITY_THRESHOLD, timestamp, json!({
                "atr": self.atr,
                "bandwidth": self.bandwidth,
            }));
        }
    }

    fn check_volatility_regime(&mut self, timestamp: i64) {
        if self.volatility_regime != self.prev_volatility_regime
            && self.should_fire(KLINE_VOLATILITY_REGIME, timestamp) {
            self.publish_event(KLINE_VOLATILITY_REGIME, timestamp, json!({
                "prev_regime": format!("{:?}", self.prev_volatility_regime),
                "new_regime": format!("{:?}", self.volatility_regime),
                "bandwidth": self.bandwidth,
            }));
        }
    }

    fn check_volatility_contraction(&mut self, timestamp: i64) {
        if self.bandwidth_history.len() < 10 { return; }

        let recent_avg: f64 = self.bandwidth_history.iter().rev().take(5).sum::<f64>() / 5.0;
        let older_avg: f64 = self.bandwidth_history.iter().take(5).sum::<f64>() / 5.0;

        if recent_avg < older_avg * 0.7 && self.should_fire(KLINE_VOLATILITY_CONTRACTION, timestamp) {
            self.publish_event(KLINE_VOLATILITY_CONTRACTION, timestamp, json!({
                "recent_avg": recent_avg,
                "older_avg": older_avg,
                "contraction_pct": (older_avg - recent_avg) / older_avg * 100.0,
            }));
        }
    }

    fn check_volatility_expansion(&mut self, timestamp: i64) {
        if self.bandwidth_history.len() < 10 { return; }

        let recent_avg: f64 = self.bandwidth_history.iter().rev().take(5).sum::<f64>() / 5.0;
        let older_avg: f64 = self.bandwidth_history.iter().take(5).sum::<f64>() / 5.0;

        if recent_avg > older_avg * 1.5 && self.should_fire(KLINE_VOLATILITY_EXPANSION, timestamp) {
            self.publish_event(KLINE_VOLATILITY_EXPANSION, timestamp, json!({
                "recent_avg": recent_avg,
                "older_avg": older_avg,
                "expansion_pct": (recent_avg - older_avg) / older_avg * 100.0,
            }));
        }
    }

    fn check_bb_width_extreme(&mut self, timestamp: i64) {
        if (self.bandwidth > 15.0 || self.bandwidth < 1.0) && self.should_fire(KLINE_BB_WIDTH_EXTREME, timestamp) {
            self.publish_event(KLINE_BB_WIDTH_EXTREME, timestamp, json!({
                "bandwidth": self.bandwidth,
                "type": if self.bandwidth > 15.0 { "wide" } else { "narrow" },
            }));
        }
    }

    fn check_bb_squeeze(&mut self, timestamp: i64) {
        // Squeeze: bandwidth at 6-month low
        if self.bandwidth_history.len() < 20 { return; }

        let min_bandwidth = self.bandwidth_history.iter().cloned().fold(f64::INFINITY, f64::min);

        if self.bandwidth <= min_bandwidth && self.bandwidth < 3.0
            && self.should_fire(KLINE_BB_SQUEEZE, timestamp) {
            self.publish_event(KLINE_BB_SQUEEZE, timestamp, json!({
                "bandwidth": self.bandwidth,
                "min_bandwidth": min_bandwidth,
            }));
        }
    }

    fn check_bb_expansion_detected(&mut self, timestamp: i64) {
        // Expansion after squeeze
        if self.prev_bandwidth < 3.0 && self.bandwidth > 5.0
            && self.should_fire(KLINE_BB_EXPANSION_DETECTED, timestamp) {
            self.publish_event(KLINE_BB_EXPANSION_DETECTED, timestamp, json!({
                "prev_bandwidth": self.prev_bandwidth,
                "current_bandwidth": self.bandwidth,
            }));
        }
    }

    fn check_volatility_spike(&mut self, timestamp: i64) {
        if self.bandwidth_history.len() < 5 { return; }

        let avg: f64 = self.bandwidth_history.iter().cloned().sum::<f64>() / self.bandwidth_history.len() as f64;

        if self.bandwidth > avg * 2.0 && self.should_fire(KLINE_VOLATILITY_SPIKE, timestamp) {
            self.publish_event(KLINE_VOLATILITY_SPIKE, timestamp, json!({
                "bandwidth": self.bandwidth,
                "avg_bandwidth": avg,
                "spike_ratio": self.bandwidth / avg,
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
        debug!(symbol = %self.symbol, event = event_type, "BB event published");
    }

    pub fn updates_processed(&self) -> u64 { self.updates_processed }
    pub fn events_fired(&self) -> u64 { self.events_fired }
}
