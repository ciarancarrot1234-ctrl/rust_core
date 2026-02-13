// Top Trader Tracker - Monitors top trader positions from Binance
// Generates 15 top trader-related condition events
// Input: REST API polling (5 min interval)

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer3::common::event_types::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Top trader positioning regime
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopTraderRegime {
    HeavyLong,        // Top traders heavily long
    ModerateLong,     // Top traders moderately long
    Balanced,         // Top traders neutral
    ModerateShort,    // Top traders moderately short
    HeavyShort,       // Top traders heavily short
}

/// Top trader data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopTraderDataPoint {
    pub symbol: String,
    pub long_short_ratio: f64,
    pub long_position: f64,      // % of top traders long
    pub short_position: f64,     // % of top traders short
    pub timestamp: i64,
}

/// Top trader history entry
#[derive(Debug, Clone)]
struct TopTraderHistoryEntry {
    ratio: f64,
    long_position: f64,
    short_position: f64,
    timestamp: i64,
}

/// TopTraderTracker monitors whale and institutional positioning
pub struct TopTraderTracker {
    symbol: String,

    // Current values
    current_ratio: f64,
    long_position: f64,
    short_position: f64,

    // Previous values
    prev_ratio: f64,
    prev_long_position: f64,
    prev_short_position: f64,

    // History
    history: Vec<TopTraderHistoryEntry>,
    max_history: usize,

    // Calculated metrics
    ratio_change: f64,
    ratio_velocity: f64,
    position_change: f64,

    // Statistics
    ratio_avg: f64,
    ratio_std: f64,
    ratio_high: f64,
    ratio_low: f64,

    // Regime
    current_regime: TopTraderRegime,
    prev_regime: TopTraderRegime,

    // Momentum tracking
    momentum_direction: i32,   // 1 = more long, -1 = more short
    momentum_strength: f64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl TopTraderTracker {
    /// Create a new TopTraderTracker
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            current_ratio: 1.0,
            long_position: 50.0,
            short_position: 50.0,
            prev_ratio: 1.0,
            prev_long_position: 50.0,
            prev_short_position: 50.0,
            history: Vec::with_capacity(100),
            max_history: 100,
            ratio_change: 0.0,
            ratio_velocity: 0.0,
            position_change: 0.0,
            ratio_avg: 1.0,
            ratio_std: 0.0,
            ratio_high: 1.0,
            ratio_low: 1.0,
            current_regime: TopTraderRegime::Balanced,
            prev_regime: TopTraderRegime::Balanced,
            momentum_direction: 0,
            momentum_strength: 0.0,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 300_000,  // 5 minute debounce
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    /// Update with new top trader data from REST API
    pub fn update_top_trader(&mut self, data: &TopTraderDataPoint, current_time: i64) {
        self.updates_processed += 1;

        self.prev_ratio = self.current_ratio;
        self.prev_long_position = self.long_position;
        self.prev_short_position = self.short_position;

        self.current_ratio = data.long_short_ratio;
        self.long_position = data.long_position;
        self.short_position = data.short_position;

        // Calculate changes
        self.ratio_change = self.current_ratio - self.prev_ratio;
        self.position_change = self.long_position - self.prev_long_position;

        // Update history
        self.history.push(TopTraderHistoryEntry {
            ratio: data.long_short_ratio,
            long_position: data.long_position,
            short_position: data.short_position,
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

        // Update momentum
        self.update_momentum();
    }

    /// Check all top trader events
    pub fn check_events(&mut self, current_time: i64) {
        // Event 1: Position spike
        self.check_position_spike(current_time);

        // Event 2: Position drop
        self.check_position_drop(current_time);

        // Event 3: Long extreme
        self.check_long_extreme(current_time);

        // Event 4: Short extreme
        self.check_short_extreme(current_time);

        // Event 5: Reversal
        self.check_reversal(current_time);

        // Event 6: Divergence from L/S ratio
        self.check_divergence(current_time);

        // Event 7: Convergence
        self.check_convergence(current_time);

        // Event 8: Accumulation pattern
        self.check_accumulation(current_time);

        // Event 9: Distribution pattern
        self.check_distribution(current_time);

        // Event 10: Sentiment shift
        self.check_sentiment_shift(current_time);

        // Event 11: Liquidation risk
        self.check_liquidation_risk(current_time);

        // Event 12: Correlation breakdown
        self.check_correlation_breakdown(current_time);

        // Event 13: Momentum building
        self.check_momentum_building(current_time);

        // Event 14: Momentum fading
        self.check_momentum_fading(current_time);

        // Event 15: Historical extreme
        self.check_historical_extreme(current_time);
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

    /// Classify regime
    fn classify_regime(&self) -> TopTraderRegime {
        if self.long_position > 70.0 {
            TopTraderRegime::HeavyLong
        } else if self.long_position > 55.0 {
            TopTraderRegime::ModerateLong
        } else if self.long_position >= 45.0 {
            TopTraderRegime::Balanced
        } else if self.long_position >= 30.0 {
            TopTraderRegime::ModerateShort
        } else {
            TopTraderRegime::HeavyShort
        }
    }

    /// Update momentum tracking
    fn update_momentum(&mut self) {
        let new_direction = if self.ratio_velocity > 0.05 { 1 }
                           else if self.ratio_velocity < -0.05 { -1 }
                           else { 0 };

        if new_direction != 0 && new_direction == self.momentum_direction {
            self.momentum_strength += self.ratio_velocity.abs();
        } else {
            self.momentum_direction = new_direction;
            self.momentum_strength = self.ratio_velocity.abs();
        }
    }

    /// Event 1: Position spike
    fn check_position_spike(&mut self, timestamp: i64) {
        if self.ratio_change > self.thresholds.long_short_ratio_threshold {
            if self.should_fire(BD_TOP_POSITION_SPIKE, timestamp) {
                self.publish_event(
                    BD_TOP_POSITION_SPIKE,
                    timestamp,
                    json!({
                        "ratio_change": self.ratio_change,
                        "current_ratio": self.current_ratio,
                        "long_position": self.long_position,
                        "direction": "longs_increasing",
                    }),
                );
            }
        }
    }

    /// Event 2: Position drop
    fn check_position_drop(&mut self, timestamp: i64) {
        if self.ratio_change < -self.thresholds.long_short_ratio_threshold {
            if self.should_fire(BD_TOP_POSITION_DROP, timestamp) {
                self.publish_event(
                    BD_TOP_POSITION_DROP,
                    timestamp,
                    json!({
                        "ratio_change": self.ratio_change,
                        "current_ratio": self.current_ratio,
                        "short_position": self.short_position,
                        "direction": "shorts_increasing",
                    }),
                );
            }
        }
    }

    /// Event 3: Long extreme
    fn check_long_extreme(&mut self, timestamp: i64) {
        if matches!(self.current_regime, TopTraderRegime::HeavyLong) {
            if self.should_fire(BD_TOP_LONG_EXTREME, timestamp) {
                self.publish_event(
                    BD_TOP_LONG_EXTREME,
                    timestamp,
                    json!({
                        "long_position": self.long_position,
                        "ratio": self.current_ratio,
                        "whale_sentiment": "extreme_bullish",
                    }),
                );
            }
        }
    }

    /// Event 4: Short extreme
    fn check_short_extreme(&mut self, timestamp: i64) {
        if matches!(self.current_regime, TopTraderRegime::HeavyShort) {
            if self.should_fire(BD_TOP_SHORT_EXTREME, timestamp) {
                self.publish_event(
                    BD_TOP_SHORT_EXTREME,
                    timestamp,
                    json!({
                        "short_position": self.short_position,
                        "ratio": self.current_ratio,
                        "whale_sentiment": "extreme_bearish",
                    }),
                );
            }
        }
    }

    /// Event 5: Reversal
    fn check_reversal(&mut self, timestamp: i64) {
        let long_to_short = matches!(self.prev_regime, TopTraderRegime::HeavyLong | TopTraderRegime::ModerateLong)
            && matches!(self.current_regime, TopTraderRegime::HeavyShort | TopTraderRegime::ModerateShort);

        let short_to_long = matches!(self.prev_regime, TopTraderRegime::HeavyShort | TopTraderRegime::ModerateShort)
            && matches!(self.current_regime, TopTraderRegime::HeavyLong | TopTraderRegime::ModerateLong);

        if long_to_short || short_to_long {
            if self.should_fire(BD_TOP_REVERSAL, timestamp) {
                self.publish_event(
                    BD_TOP_REVERSAL,
                    timestamp,
                    json!({
                        "reversal_type": if long_to_short { "long_to_short" } else { "short_to_long" },
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                    }),
                );
            }
        }
    }

    /// Event 6: Divergence from overall L/S ratio
    fn check_divergence(&mut self, timestamp: i64) {
        // Top traders moving opposite to overall market
        if self.ratio_velocity.abs() > 0.1 {
            if self.should_fire(BD_TOP_DIVERGENCE, timestamp) {
                self.publish_event(
                    BD_TOP_DIVERGENCE,
                    timestamp,
                    json!({
                        "ratio_velocity": self.ratio_velocity,
                        "direction": if self.ratio_velocity > 0.0 { "toward_long" } else { "toward_short" },
                        "signal": "whale_divergence_from_crowd",
                    }),
                );
            }
        }
    }

    /// Event 7: Convergence
    fn check_convergence(&mut self, timestamp: i64) {
        // Top traders lining up with market
        let moving_toward_balance = (self.current_ratio - 1.0).abs() < (self.prev_ratio - 1.0).abs();

        if moving_toward_balance {
            if self.should_fire(BD_TOP_CONVERGENCE, timestamp) {
                self.publish_event(
                    BD_TOP_CONVERGENCE,
                    timestamp,
                    json!({
                        "ratio": self.current_ratio,
                        "prev_ratio": self.prev_ratio,
                        "signal": "alignment_with_market",
                    }),
                );
            }
        }
    }

    /// Event 8: Accumulation pattern
    fn check_accumulation(&mut self, timestamp: i64) {
        // Gradual increase in long positions
        if self.history.len() >= 3 {
            let recent: Vec<_> = self.history.iter().rev().take(3).collect();
            let increasing = recent.windows(2).all(|w| w[0].long_position > w[1].long_position);

            if increasing && self.long_position > 50.0 {
                if self.should_fire(BD_TOP_ACCUMULATION, timestamp) {
                    self.publish_event(
                        BD_TOP_ACCUMULATION,
                        timestamp,
                        json!({
                            "long_position": self.long_position,
                            "trend": "gradual_long_buildup",
                        }),
                    );
                }
            }
        }
    }

    /// Event 9: Distribution pattern
    fn check_distribution(&mut self, timestamp: i64) {
        // Gradual increase in short positions
        if self.history.len() >= 3 {
            let recent: Vec<_> = self.history.iter().rev().take(3).collect();
            let increasing = recent.windows(2).all(|w| w[0].short_position > w[1].short_position);

            if increasing && self.short_position > 50.0 {
                if self.should_fire(BD_TOP_DISTRIBUTION, timestamp) {
                    self.publish_event(
                        BD_TOP_DISTRIBUTION,
                        timestamp,
                        json!({
                            "short_position": self.short_position,
                            "trend": "gradual_short_buildup",
                        }),
                    );
                }
            }
        }
    }

    /// Event 10: Sentiment shift
    fn check_sentiment_shift(&mut self, timestamp: i64) {
        // Large position change
        if self.position_change.abs() > 10.0 {
            if self.should_fire(BD_TOP_SENTIMENT_SHIFT, timestamp) {
                self.publish_event(
                    BD_TOP_SENTIMENT_SHIFT,
                    timestamp,
                    json!({
                        "position_change": self.position_change,
                        "direction": if self.position_change > 0.0 { "toward_long" } else { "toward_short" },
                        "magnitude": "significant",
                    }),
                );
            }
        }
    }

    /// Event 11: Liquidation risk
    fn check_liquidation_risk(&mut self, timestamp: i64) {
        // Extreme positioning = liquidation risk
        if matches!(self.current_regime, TopTraderRegime::HeavyLong | TopTraderRegime::HeavyShort) {
            if self.should_fire(BD_TOP_LIQUIDATION_RISK, timestamp) {
                self.publish_event(
                    BD_TOP_LIQUIDATION_RISK,
                    timestamp,
                    json!({
                        "regime": format!("{:?}", self.current_regime),
                        "risk_type": if matches!(self.current_regime, TopTraderRegime::HeavyLong) {
                            "long_liquidations"
                        } else {
                            "short_liquidations"
                        },
                    }),
                );
            }
        }
    }

    /// Event 12: Correlation breakdown
    fn check_correlation_breakdown(&mut self, timestamp: i64) {
        // Top trader positioning no longer follows patterns
        if self.ratio_std > 0.5 {
            if self.should_fire(BD_TOP_CORRELATION_BREAKDOWN, timestamp) {
                self.publish_event(
                    BD_TOP_CORRELATION_BREAKDOWN,
                    timestamp,
                    json!({
                        "ratio_std": self.ratio_std,
                        "signal": "unpredictable_whale_behavior",
                    }),
                );
            }
        }
    }

    /// Event 13: Momentum building
    fn check_momentum_building(&mut self, timestamp: i64) {
        if self.momentum_strength > 0.3 && self.momentum_direction != 0 {
            if self.should_fire(BD_TOP_MOMENTUM_BUILDING, timestamp) {
                self.publish_event(
                    BD_TOP_MOMENTUM_BUILDING,
                    timestamp,
                    json!({
                        "momentum_direction": if self.momentum_direction > 0 { "long" } else { "short" },
                        "momentum_strength": self.momentum_strength,
                    }),
                );
            }
        }
    }

    /// Event 14: Momentum fading
    fn check_momentum_fading(&mut self, timestamp: i64) {
        // Momentum decreasing
        if self.ratio_velocity.abs() < 0.02 && self.momentum_strength > 0.1 {
            if self.should_fire(BD_TOP_MOMENTUM_FADING, timestamp) {
                self.publish_event(
                    BD_TOP_MOMENTUM_FADING,
                    timestamp,
                    json!({
                        "prev_momentum": self.momentum_strength,
                        "current_velocity": self.ratio_velocity,
                        "signal": "positioning_stabilizing",
                    }),
                );
            }
        }
    }

    /// Event 15: Historical extreme
    fn check_historical_extreme(&mut self, timestamp: i64) {
        let near_high = self.current_ratio >= self.ratio_high * 0.95;
        let near_low = self.current_ratio <= self.ratio_low * 1.05;

        if near_high || near_low {
            if self.should_fire(BD_TOP_HISTORICAL_EXTREME, timestamp) {
                self.publish_event(
                    BD_TOP_HISTORICAL_EXTREME,
                    timestamp,
                    json!({
                        "ratio": self.current_ratio,
                        "ratio_high": self.ratio_high,
                        "ratio_low": self.ratio_low,
                        "extreme_type": if near_high { "long_extreme" } else { "short_extreme" },
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
            "Top Trader event published"
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
