// Long/Short Ratio Tracker - Monitors market sentiment from Binance
// Generates 15 long/short-related condition events
// Input: REST API polling (5 min interval)

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer3::common::event_types::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Long/Short regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LSRegime {
    ExtremeLong,     // Heavily long biased (>70%)
    LongBiased,      // Moderately long biased (55-70%)
    Balanced,        // Roughly equal (45-55%)
    ShortBiased,     // Moderately short biased (30-45%)
    ExtremeShort,    // Heavily short biased (<30%)
}

/// Long/Short data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LSDataPoint {
    pub symbol: String,
    pub long_short_ratio: f64,    // Ratio: >1 = more longs, <1 = more shorts
    pub long_account: f64,        // % of accounts with long positions
    pub short_account: f64,       // % of accounts with short positions
    pub timestamp: i64,
}

/// Long/Short ratio history entry
#[derive(Debug, Clone)]
struct LSHistoryEntry {
    ratio: f64,
    long_account: f64,
    short_account: f64,
    timestamp: i64,
}

/// LongShortTracker monitors market positioning and sentiment
pub struct LongShortTracker {
    symbol: String,

    // Current values
    current_ratio: f64,
    long_account: f64,
    short_account: f64,

    // Previous values for change detection
    prev_ratio: f64,
    prev_long_account: f64,
    prev_short_account: f64,

    // History for trend analysis
    history: Vec<LSHistoryEntry>,
    max_history: usize,

    // Calculated metrics
    ratio_change: f64,
    ratio_velocity: f64,
    ratio_avg: f64,
    ratio_std: f64,

    // Historical extremes
    ratio_high: f64,
    ratio_low: f64,

    // Regime tracking
    current_regime: LSRegime,
    prev_regime: LSRegime,

    // Trend tracking
    trend_direction: i32,  // 1 = more long, -1 = more short, 0 = neutral
    trend_duration: i64,   // Duration of current trend in ms

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl LongShortTracker {
    /// Create a new LongShortTracker
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            current_ratio: 1.0,
            long_account: 50.0,
            short_account: 50.0,
            prev_ratio: 1.0,
            prev_long_account: 50.0,
            prev_short_account: 50.0,
            history: Vec::with_capacity(100),
            max_history: 100,
            ratio_change: 0.0,
            ratio_velocity: 0.0,
            ratio_avg: 1.0,
            ratio_std: 0.0,
            ratio_high: 1.0,
            ratio_low: 1.0,
            current_regime: LSRegime::Balanced,
            prev_regime: LSRegime::Balanced,
            trend_direction: 0,
            trend_duration: 0,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 300_000,  // 5 minute debounce
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    /// Update with new L/S data from REST API
    pub fn update_ls(&mut self, data: &LSDataPoint, current_time: i64) {
        self.updates_processed += 1;

        self.prev_ratio = self.current_ratio;
        self.prev_long_account = self.long_account;
        self.prev_short_account = self.short_account;

        self.current_ratio = data.long_short_ratio;
        self.long_account = data.long_account;
        self.short_account = data.short_account;

        // Calculate change
        self.ratio_change = self.current_ratio - self.prev_ratio;

        // Update history
        self.history.push(LSHistoryEntry {
            ratio: data.long_short_ratio,
            long_account: data.long_account,
            short_account: data.short_account,
            timestamp: data.timestamp,
        });

        if self.history.len() > self.max_history {
            self.history.remove(0);
        }

        // Update statistics
        self.update_statistics();

        // Update regime
        self.prev_regime = self.current_regime;
        self.current_regime = self.classify_regime();

        // Update trend
        self.update_trend(current_time);
    }

    /// Check all L/S events
    pub fn check_events(&mut self, current_time: i64) {
        // Event 1: L/S threshold exceeded
        self.check_ls_threshold(current_time);

        // Event 2: Extreme long positioning
        self.check_extreme_long(current_time);

        // Event 3: Extreme short positioning
        self.check_extreme_short(current_time);

        // Event 4: Regime change
        self.check_ls_regime(current_time);

        // Event 5: Reversal
        self.check_ls_reversal(current_time);

        // Event 6: Divergence from trend
        self.check_ls_divergence(current_time);

        // Event 7: Convergence
        self.check_ls_convergence(current_time);

        // Event 8: Trend detection
        self.check_ls_trend(current_time);

        // Event 9: Counter-trend positioning
        self.check_ls_counter_trend(current_time);

        // Event 10: Squeeze setup
        self.check_squeeze_setup(current_time);

        // Event 11: Historical extreme
        self.check_historical_extreme(current_time);

        // Event 12: Account ratio divergence
        self.check_account_top_divergence(current_time);

        // Event 13: Sentiment shift
        self.check_sentiment_shift(current_time);

        // Event 14: Contrarian signal
        self.check_contrarian(current_time);

        // Event 15: Positioning extreme
        self.check_positioning_extreme(current_time);
    }

    /// Update internal statistics
    fn update_statistics(&mut self) {
        if self.history.is_empty() {
            return;
        }

        let sum: f64 = self.history.iter().map(|h| h.ratio).sum();
        self.ratio_avg = sum / self.history.len() as f64;

        let variance: f64 = self.history
            .iter()
            .map(|h| (h.ratio - self.ratio_avg).powi(2))
            .sum::<f64>() / self.history.len() as f64;
        self.ratio_std = variance.sqrt();

        self.ratio_high = self.history.iter().map(|h| h.ratio).fold(f64::MIN, f64::max);
        self.ratio_low = self.history.iter().map(|h| h.ratio).fold(f64::MAX, f64::min);

        // Calculate velocity
        if self.history.len() >= 2 {
            self.ratio_velocity = self.current_ratio - self.history[self.history.len() - 2].ratio;
        }
    }

    /// Classify L/S regime
    fn classify_regime(&self) -> LSRegime {
        if self.long_account > 70.0 {
            LSRegime::ExtremeLong
        } else if self.long_account > 55.0 {
            LSRegime::LongBiased
        } else if self.long_account >= 45.0 {
            LSRegime::Balanced
        } else if self.long_account >= 30.0 {
            LSRegime::ShortBiased
        } else {
            LSRegime::ExtremeShort
        }
    }

    /// Update trend tracking
    fn update_trend(&mut self, current_time: i64) {
        let new_direction = if self.ratio_change > 0.01 { 1 }
                           else if self.ratio_change < -0.01 { -1 }
                           else { 0 };

        if new_direction != 0 && new_direction == self.trend_direction {
            self.trend_duration += 300_000;  // Add 5 minutes
        } else {
            self.trend_direction = new_direction;
            self.trend_duration = 0;
        }
    }

    /// Event 1: L/S threshold exceeded
    fn check_ls_threshold(&mut self, timestamp: i64) {
        let threshold = self.thresholds.long_short_ratio_threshold;
        if self.current_ratio > 1.0 + threshold || self.current_ratio < 1.0 - threshold {
            if self.should_fire(BD_LS_THRESHOLD, timestamp) {
                self.publish_event(
                    BD_LS_THRESHOLD,
                    timestamp,
                    json!({
                        "ratio": self.current_ratio,
                        "long_pct": self.long_account,
                        "short_pct": self.short_account,
                        "threshold": threshold,
                    }),
                );
            }
        }
    }

    /// Event 2: Extreme long positioning
    fn check_extreme_long(&mut self, timestamp: i64) {
        if matches!(self.current_regime, LSRegime::ExtremeLong) {
            if self.should_fire(BD_LS_EXTREME_LONG, timestamp) {
                self.publish_event(
                    BD_LS_EXTREME_LONG,
                    timestamp,
                    json!({
                        "long_pct": self.long_account,
                        "short_pct": self.short_account,
                        "ratio": self.current_ratio,
                        "crowded_trade_risk": true,
                    }),
                );
            }
        }
    }

    /// Event 3: Extreme short positioning
    fn check_extreme_short(&mut self, timestamp: i64) {
        if matches!(self.current_regime, LSRegime::ExtremeShort) {
            if self.should_fire(BD_LS_EXTREME_SHORT, timestamp) {
                self.publish_event(
                    BD_LS_EXTREME_SHORT,
                    timestamp,
                    json!({
                        "long_pct": self.long_account,
                        "short_pct": self.short_account,
                        "ratio": self.current_ratio,
                        "crowded_trade_risk": true,
                    }),
                );
            }
        }
    }

    /// Event 4: Regime change
    fn check_ls_regime(&mut self, timestamp: i64) {
        if self.current_regime != self.prev_regime {
            if self.should_fire(BD_LS_REGIME, timestamp) {
                self.publish_event(
                    BD_LS_REGIME,
                    timestamp,
                    json!({
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                        "long_pct": self.long_account,
                    }),
                );
            }
        }
    }

    /// Event 5: Reversal (extreme flip)
    fn check_ls_reversal(&mut self, timestamp: i64) {
        let is_long_to_short = matches!(self.prev_regime, LSRegime::ExtremeLong | LSRegime::LongBiased)
            && matches!(self.current_regime, LSRegime::ExtremeShort | LSRegime::ShortBiased);

        let is_short_to_long = matches!(self.prev_regime, LSRegime::ExtremeShort | LSRegime::ShortBiased)
            && matches!(self.current_regime, LSRegime::ExtremeLong | LSRegime::LongBiased);

        if is_long_to_short || is_short_to_long {
            if self.should_fire(BD_LS_REVERSAL, timestamp) {
                self.publish_event(
                    BD_LS_REVERSAL,
                    timestamp,
                    json!({
                        "reversal_type": if is_long_to_short { "long_to_short" } else { "short_to_long" },
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                    }),
                );
            }
        }
    }

    /// Event 6: Divergence from price trend
    fn check_ls_divergence(&mut self, timestamp: i64) {
        // Positioning moving against the recent trend
        if self.ratio_velocity.abs() > 0.05 && self.history.len() >= 5 {
            if self.should_fire(BD_LS_DIVERGENCE, timestamp) {
                self.publish_event(
                    BD_LS_DIVERGENCE,
                    timestamp,
                    json!({
                        "ratio_velocity": self.ratio_velocity,
                        "trend_direction": self.trend_direction,
                    }),
                );
            }
        }
    }

    /// Event 7: Convergence toward balance
    fn check_ls_convergence(&mut self, timestamp: i64) {
        let moving_toward_balance = (self.current_ratio - 1.0).abs() < (self.prev_ratio - 1.0).abs();

        if moving_toward_balance && self.current_ratio > 0.8 && self.current_ratio < 1.2 {
            if self.should_fire(BD_LS_CONVERGENCE, timestamp) {
                self.publish_event(
                    BD_LS_CONVERGENCE,
                    timestamp,
                    json!({
                        "ratio": self.current_ratio,
                        "prev_ratio": self.prev_ratio,
                    }),
                );
            }
        }
    }

    /// Event 8: Trend detection
    fn check_ls_trend(&mut self, timestamp: i64) {
        if self.trend_direction != 0 && self.trend_duration >= 900_000 {  // 15+ minutes
            if self.should_fire(BD_LS_TREND, timestamp) {
                self.publish_event(
                    BD_LS_TREND,
                    timestamp,
                    json!({
                        "trend_direction": if self.trend_direction > 0 { "more_long" } else { "more_short" },
                        "trend_duration_ms": self.trend_duration,
                        "ratio_change": self.ratio_change,
                    }),
                );
            }
        }
    }

    /// Event 9: Counter-trend positioning
    fn check_ls_counter_trend(&mut self, timestamp: i64) {
        // Large players positioning against crowd
        if matches!(self.current_regime, LSRegime::ExtremeLong) && self.ratio_velocity < 0.0 {
            if self.should_fire(BD_LS_COUNTER_TREND, timestamp) {
                self.publish_event(
                    BD_LS_COUNTER_TREND,
                    timestamp,
                    json!({
                        "crowd_position": "long",
                        "shift_direction": "shorts_increasing",
                        "potential_squeeze": "short",
                    }),
                );
            }
        } else if matches!(self.current_regime, LSRegime::ExtremeShort) && self.ratio_velocity > 0.0 {
            if self.should_fire(BD_LS_COUNTER_TREND, timestamp) {
                self.publish_event(
                    BD_LS_COUNTER_TREND,
                    timestamp,
                    json!({
                        "crowd_position": "short",
                        "shift_direction": "longs_increasing",
                        "potential_squeeze": "long",
                    }),
                );
            }
        }
    }

    /// Event 10: Squeeze setup
    fn check_squeeze_setup(&mut self, timestamp: i64) {
        let long_squeeze_setup = matches!(self.current_regime, LSRegime::ExtremeLong)
            && self.ratio_velocity < 0.0;

        let short_squeeze_setup = matches!(self.current_regime, LSRegime::ExtremeShort)
            && self.ratio_velocity > 0.0;

        if long_squeeze_setup || short_squeeze_setup {
            if self.should_fire(BD_LS_SQUEEZE_SETUP, timestamp) {
                self.publish_event(
                    BD_LS_SQUEEZE_SETUP,
                    timestamp,
                    json!({
                        "squeeze_type": if long_squeeze_setup { "long" } else { "short" },
                        "positioning_extreme": true,
                        "velocity_reversing": true,
                    }),
                );
            }
        }
    }

    /// Event 11: Historical extreme
    fn check_historical_extreme(&mut self, timestamp: i64) {
        let near_high = self.current_ratio >= self.ratio_high * 0.95;
        let near_low = self.current_ratio <= self.ratio_low * 1.05;

        if near_high || near_low {
            if self.should_fire(BD_LS_HISTORICAL_EXTREME, timestamp) {
                self.publish_event(
                    BD_LS_HISTORICAL_EXTREME,
                    timestamp,
                    json!({
                        "ratio": self.current_ratio,
                        "ratio_high": self.ratio_high,
                        "ratio_low": self.ratio_low,
                        "extreme_type": if near_high { "high" } else { "low" },
                    }),
                );
            }
        }
    }

    /// Event 12: Account vs top trader divergence
    fn check_account_top_divergence(&mut self, timestamp: i64) {
        // Placeholder - would need top trader data to compare
        // For now, detect unusual ratio shifts
        if self.ratio_change.abs() > 0.1 {
            if self.should_fire(BD_LS_ACCOUNT_TOP_DIVERGENCE, timestamp) {
                self.publish_event(
                    BD_LS_ACCOUNT_TOP_DIVERGENCE,
                    timestamp,
                    json!({
                        "ratio_change": self.ratio_change,
                        "potential_retail_sweep": true,
                    }),
                );
            }
        }
    }

    /// Event 13: Sentiment shift
    fn check_sentiment_shift(&mut self, timestamp: i64) {
        // Large shift in positioning within short timeframe
        if self.ratio_change.abs() > 0.15 {
            if self.should_fire(BD_LS_SENTIMENT_SHIFT, timestamp) {
                self.publish_event(
                    BD_LS_SENTIMENT_SHIFT,
                    timestamp,
                    json!({
                        "ratio_change": self.ratio_change,
                        "direction": if self.ratio_change > 0.0 { "more_long" } else { "more_short" },
                        "magnitude": "significant",
                    }),
                );
            }
        }
    }

    /// Event 14: Contrarian signal
    fn check_contrarian(&mut self, timestamp: i64) {
        // Contrarian: extreme positioning often reverses
        if matches!(self.current_regime, LSRegime::ExtremeLong | LSRegime::ExtremeShort) {
            if self.should_fire(BD_LS_CONTRARIAN, timestamp) {
                let contrarian_direction = if matches!(self.current_regime, LSRegime::ExtremeLong) {
                    "short"
                } else {
                    "long"
                };
                self.publish_event(
                    BD_LS_CONTRARIAN,
                    timestamp,
                    json!({
                        "current_positioning": format!("{:?}", self.current_regime),
                        "contrarian_direction": contrarian_direction,
                        "confidence": "medium",
                    }),
                );
            }
        }
    }

    /// Event 15: Positioning extreme (sustained)
    fn check_positioning_extreme(&mut self, timestamp: i64) {
        // Extreme positioning sustained for 30+ minutes
        if matches!(self.current_regime, LSRegime::ExtremeLong | LSRegime::ExtremeShort)
            && self.trend_duration >= 1_800_000 {
            if self.should_fire(BD_LS_POSITIONING_EXTREME, timestamp) {
                self.publish_event(
                    BD_LS_POSITIONING_EXTREME,
                    timestamp,
                    json!({
                        "regime": format!("{:?}", self.current_regime),
                        "duration_ms": self.trend_duration,
                        "sustained": true,
                    }),
                );
            }
        }
    }

    /// Check if event should fire (debouncing)
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
            "binance_data_aggregator",
            EventPriority::Medium,
        );

        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(
            symbol = %self.symbol,
            event = event_type,
            "Long/Short event published"
        );
    }

    /// Get updates processed count
    pub fn updates_processed(&self) -> u64 {
        self.updates_processed
    }

    /// Get events fired count
    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    /// Get current ratio
    pub fn current_ratio(&self) -> f64 {
        self.current_ratio
    }
}
