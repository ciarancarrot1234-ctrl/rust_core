// Taker Volume Tracker - Monitors taker buy/sell volume from Binance
// Generates 15 taker volume-related condition events
// Input: REST API polling (5 min interval)

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer3::common::event_types::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Taker volume regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakerRegime {
    AggressiveBuying,    // Strong taker buy dominance
    ModerateBuying,      // Slight taker buy dominance
    Balanced,            // Roughly equal
    ModerateSelling,     // Slight taker sell dominance
    AggressiveSelling,   // Strong taker sell dominance
}

/// Taker volume data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TakerVolumeDataPoint {
    pub symbol: String,
    pub buy_volume: f64,
    pub sell_volume: f64,
    pub buy_sell_ratio: f64,
    pub timestamp: i64,
}

/// Taker volume history entry
#[derive(Debug, Clone)]
struct TakerHistoryEntry {
    buy_volume: f64,
    sell_volume: f64,
    ratio: f64,
    timestamp: i64,
}

/// TakerVolumeTracker monitors aggressive market order flow
pub struct TakerVolumeTracker {
    symbol: String,

    // Current values
    buy_volume: f64,
    sell_volume: f64,
    buy_sell_ratio: f64,

    // Previous values
    prev_buy_volume: f64,
    prev_sell_volume: f64,
    prev_ratio: f64,

    // History for trend analysis
    history: Vec<TakerHistoryEntry>,
    max_history: usize,

    // Calculated metrics
    taker_ratio_pct: f64,      // Buy volume as percentage
    ratio_change: f64,
    ratio_velocity: f64,

    // Statistical measures
    buy_avg: f64,
    sell_avg: f64,
    ratio_avg: f64,
    ratio_std: f64,

    // Extremes
    ratio_high: f64,
    ratio_low: f64,

    // Regime tracking
    current_regime: TakerRegime,
    prev_regime: TakerRegime,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl TakerVolumeTracker {
    /// Create a new TakerVolumeTracker
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            buy_volume: 0.0,
            sell_volume: 0.0,
            buy_sell_ratio: 1.0,
            prev_buy_volume: 0.0,
            prev_sell_volume: 0.0,
            prev_ratio: 1.0,
            history: Vec::with_capacity(100),
            max_history: 100,
            taker_ratio_pct: 50.0,
            ratio_change: 0.0,
            ratio_velocity: 0.0,
            buy_avg: 0.0,
            sell_avg: 0.0,
            ratio_avg: 1.0,
            ratio_std: 0.0,
            ratio_high: 1.0,
            ratio_low: 1.0,
            current_regime: TakerRegime::Balanced,
            prev_regime: TakerRegime::Balanced,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 300_000,  // 5 minute debounce
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    /// Update with new taker volume data from REST API
    pub fn update_taker_volume(&mut self, data: &TakerVolumeDataPoint, current_time: i64) {
        self.updates_processed += 1;

        self.prev_buy_volume = self.buy_volume;
        self.prev_sell_volume = self.sell_volume;
        self.prev_ratio = self.buy_sell_ratio;

        self.buy_volume = data.buy_volume;
        self.sell_volume = data.sell_volume;
        self.buy_sell_ratio = data.buy_sell_ratio;

        // Calculate buy percentage
        let total = self.buy_volume + self.sell_volume;
        self.taker_ratio_pct = if total > 0.0 {
            (self.buy_volume / total) * 100.0
        } else {
            50.0
        };

        // Calculate change
        self.ratio_change = self.buy_sell_ratio - self.prev_ratio;

        // Update history
        self.history.push(TakerHistoryEntry {
            buy_volume: data.buy_volume,
            sell_volume: data.sell_volume,
            ratio: data.buy_sell_ratio,
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
    }

    /// Check all taker volume events
    pub fn check_events(&mut self, current_time: i64) {
        // Event 1: Taker buy spike
        self.check_taker_buy_spike(current_time);

        // Event 2: Taker sell spike
        self.check_taker_sell_spike(current_time);

        // Event 3: Taker ratio extreme
        self.check_taker_ratio_extreme(current_time);

        // Event 4: Regime change
        self.check_taker_regime(current_time);

        // Event 5: Trend detection
        self.check_taker_trend(current_time);

        // Event 6: Reversal
        self.check_taker_reversal(current_time);

        // Event 7: Price correlation
        self.check_price_correlation(current_time);

        // Event 8: OI correlation
        self.check_oi_correlation(current_time);

        // Event 9: Funding correlation
        self.check_funding_correlation(current_time);

        // Event 10: Historical extreme
        self.check_historical_extreme(current_time);

        // Event 11: Accumulation pattern
        self.check_accumulation(current_time);

        // Event 12: Distribution pattern
        self.check_distribution(current_time);

        // Event 13: Climax detection
        self.check_climax(current_time);

        // Event 14: Exhaustion detection
        self.check_exhaustion(current_time);

        // Event 15: Smart money detection
        self.check_smart_money(current_time);
    }

    /// Update internal statistics
    fn update_statistics(&mut self) {
        if self.history.is_empty() {
            return;
        }

        let sum_buy: f64 = self.history.iter().map(|h| h.buy_volume).sum();
        let sum_sell: f64 = self.history.iter().map(|h| h.sell_volume).sum();

        self.buy_avg = sum_buy / self.history.len() as f64;
        self.sell_avg = sum_sell / self.history.len() as f64;

        let sum_ratio: f64 = self.history.iter().map(|h| h.ratio).sum();
        self.ratio_avg = sum_ratio / self.history.len() as f64;

        let variance: f64 = self.history
            .iter()
            .map(|h| (h.ratio - self.ratio_avg).powi(2))
            .sum::<f64>() / self.history.len() as f64;
        self.ratio_std = variance.sqrt();

        self.ratio_high = self.history.iter().map(|h| h.ratio).fold(f64::MIN, f64::max);
        self.ratio_low = self.history.iter().map(|h| h.ratio).fold(f64::MAX, f64::min);

        // Calculate velocity
        if self.history.len() >= 2 {
            self.ratio_velocity = self.buy_sell_ratio - self.history[self.history.len() - 2].ratio;
        }
    }

    /// Classify taker regime
    fn classify_regime(&self) -> TakerRegime {
        let threshold = self.thresholds.taker_volume_threshold_pct;

        if self.taker_ratio_pct > threshold + 15.0 {
            TakerRegime::AggressiveBuying
        } else if self.taker_ratio_pct > threshold {
            TakerRegime::ModerateBuying
        } else if self.taker_ratio_pct >= 45.0 {
            TakerRegime::Balanced
        } else if self.taker_ratio_pct >= 30.0 {
            TakerRegime::ModerateSelling
        } else {
            TakerRegime::AggressiveSelling
        }
    }

    /// Event 1: Taker buy spike
    fn check_taker_buy_spike(&mut self, timestamp: i64) {
        let spike_threshold = self.buy_avg * 2.0;
        if self.buy_volume > spike_threshold && self.buy_avg > 0.0 {
            if self.should_fire(BD_TAKER_BUY_SPIKE, timestamp) {
                self.publish_event(
                    BD_TAKER_BUY_SPIKE,
                    timestamp,
                    json!({
                        "buy_volume": self.buy_volume,
                        "buy_avg": self.buy_avg,
                        "spike_multiple": self.buy_volume / self.buy_avg,
                        "ratio": self.buy_sell_ratio,
                    }),
                );
            }
        }
    }

    /// Event 2: Taker sell spike
    fn check_taker_sell_spike(&mut self, timestamp: i64) {
        let spike_threshold = self.sell_avg * 2.0;
        if self.sell_volume > spike_threshold && self.sell_avg > 0.0 {
            if self.should_fire(BD_TAKER_SELL_SPIKE, timestamp) {
                self.publish_event(
                    BD_TAKER_SELL_SPIKE,
                    timestamp,
                    json!({
                        "sell_volume": self.sell_volume,
                        "sell_avg": self.sell_avg,
                        "spike_multiple": self.sell_volume / self.sell_avg,
                        "ratio": self.buy_sell_ratio,
                    }),
                );
            }
        }
    }

    /// Event 3: Taker ratio extreme
    fn check_taker_ratio_extreme(&mut self, timestamp: i64) {
        if self.taker_ratio_pct > 70.0 || self.taker_ratio_pct < 30.0 {
            if self.should_fire(BD_TAKER_RATIO_EXTREME, timestamp) {
                self.publish_event(
                    BD_TAKER_RATIO_EXTREME,
                    timestamp,
                    json!({
                        "buy_pct": self.taker_ratio_pct,
                        "ratio": self.buy_sell_ratio,
                        "direction": if self.taker_ratio_pct > 70.0 { "buy" } else { "sell" },
                    }),
                );
            }
        }
    }

    /// Event 4: Regime change
    fn check_taker_regime(&mut self, timestamp: i64) {
        if self.current_regime != self.prev_regime {
            if self.should_fire(BD_TAKER_REGIME, timestamp) {
                self.publish_event(
                    BD_TAKER_REGIME,
                    timestamp,
                    json!({
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                        "buy_pct": self.taker_ratio_pct,
                    }),
                );
            }
        }
    }

    /// Event 5: Trend detection
    fn check_taker_trend(&mut self, timestamp: i64) {
        // Sustained directional taker flow
        if self.history.len() >= 3 {
            let recent: Vec<_> = self.history.iter().rev().take(3).collect();
            let mut buy_increasing = true;
            let mut sell_increasing = true;

            for i in 0..recent.len() - 1 {
                if recent[i].buy_volume <= recent[i + 1].buy_volume {
                    buy_increasing = false;
                }
                if recent[i].sell_volume <= recent[i + 1].sell_volume {
                    sell_increasing = false;
                }
            }

            if buy_increasing || sell_increasing {
                if self.should_fire(BD_TAKER_TREND, timestamp) {
                    self.publish_event(
                        BD_TAKER_TREND,
                        timestamp,
                        json!({
                            "trend_type": if buy_increasing { "buy_pressure" } else { "sell_pressure" },
                            "buy_volume": self.buy_volume,
                            "sell_volume": self.sell_volume,
                        }),
                    );
                }
            }
        }
    }

    /// Event 6: Reversal
    fn check_taker_reversal(&mut self, timestamp: i64) {
        let is_buy_reversal = matches!(self.prev_regime, TakerRegime::AggressiveSelling | TakerRegime::ModerateSelling)
            && matches!(self.current_regime, TakerRegime::AggressiveBuying | TakerRegime::ModerateBuying);

        let is_sell_reversal = matches!(self.prev_regime, TakerRegime::AggressiveBuying | TakerRegime::ModerateBuying)
            && matches!(self.current_regime, TakerRegime::AggressiveSelling | TakerRegime::ModerateSelling);

        if is_buy_reversal || is_sell_reversal {
            if self.should_fire(BD_TAKER_REVERSAL, timestamp) {
                self.publish_event(
                    BD_TAKER_REVERSAL,
                    timestamp,
                    json!({
                        "reversal_type": if is_buy_reversal { "sell_to_buy" } else { "buy_to_sell" },
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                    }),
                );
            }
        }
    }

    /// Event 7: Price correlation
    fn check_price_correlation(&mut self, timestamp: i64) {
        // Taker flow confirming or diverging from price
        let ratio_direction = self.ratio_change.signum();

        if ratio_direction != 0.0 && self.ratio_change.abs() > 0.2 {
            if self.should_fire(BD_TAKER_PRICE_CORRELATION, timestamp) {
                self.publish_event(
                    BD_TAKER_PRICE_CORRELATION,
                    timestamp,
                    json!({
                        "ratio_change": self.ratio_change,
                        "flow_direction": if ratio_direction > 0.0 { "buying" } else { "selling" },
                        "correlation": "watch_price_action",
                    }),
                );
            }
        }
    }

    /// Event 8: OI correlation
    fn check_oi_correlation(&mut self, timestamp: i64) {
        // High taker volume + OI changes
        if self.buy_volume + self.sell_volume > (self.buy_avg + self.sell_avg) * 2.0 {
            if self.should_fire(BD_TAKER_OI_CORRELATION, timestamp) {
                self.publish_event(
                    BD_TAKER_OI_CORRELATION,
                    timestamp,
                    json!({
                        "total_volume": self.buy_volume + self.sell_volume,
                        "avg_total": self.buy_avg + self.sell_avg,
                        "watch_oi_changes": true,
                    }),
                );
            }
        }
    }

    /// Event 9: Funding correlation
    fn check_funding_correlation(&mut self, timestamp: i64) {
        // Persistent aggressive buying/selling affects funding
        if matches!(self.current_regime, TakerRegime::AggressiveBuying | TakerRegime::AggressiveSelling) {
            if self.should_fire(BD_TAKER_FUNDING_CORRELATION, timestamp) {
                self.publish_event(
                    BD_TAKER_FUNDING_CORRELATION,
                    timestamp,
                    json!({
                        "regime": format!("{:?}", self.current_regime),
                        "funding_implication": if matches!(self.current_regime, TakerRegime::AggressiveBuying) {
                            "potential_higher_funding"
                        } else {
                            "potential_lower_funding"
                        },
                    }),
                );
            }
        }
    }

    /// Event 10: Historical extreme
    fn check_historical_extreme(&mut self, timestamp: i64) {
        let near_high = self.buy_sell_ratio >= self.ratio_high * 0.95;
        let near_low = self.buy_sell_ratio <= self.ratio_low * 1.05;

        if near_high || near_low {
            if self.should_fire(BD_TAKER_HISTORICAL_EXTREME, timestamp) {
                self.publish_event(
                    BD_TAKER_HISTORICAL_EXTREME,
                    timestamp,
                    json!({
                        "ratio": self.buy_sell_ratio,
                        "ratio_high": self.ratio_high,
                        "ratio_low": self.ratio_low,
                        "extreme_type": if near_high { "buy_extreme" } else { "sell_extreme" },
                    }),
                );
            }
        }
    }

    /// Event 11: Accumulation pattern
    fn check_accumulation(&mut self, timestamp: i64) {
        // Steady buying with low selling
        if self.taker_ratio_pct > 55.0 && self.sell_volume < self.sell_avg {
            if self.should_fire(BD_TAKER_ACCUMULATION, timestamp) {
                self.publish_event(
                    BD_TAKER_ACCUMULATION,
                    timestamp,
                    json!({
                        "buy_pct": self.taker_ratio_pct,
                        "sell_vs_avg": self.sell_volume / self.sell_avg.max(1.0),
                        "pattern": "steady_buying_low_selling",
                    }),
                );
            }
        }
    }

    /// Event 12: Distribution pattern
    fn check_distribution(&mut self, timestamp: i64) {
        // Steady selling with low buying
        if self.taker_ratio_pct < 45.0 && self.buy_volume < self.buy_avg {
            if self.should_fire(BD_TAKER_DISTRIBUTION, timestamp) {
                self.publish_event(
                    BD_TAKER_DISTRIBUTION,
                    timestamp,
                    json!({
                        "buy_pct": self.taker_ratio_pct,
                        "buy_vs_avg": self.buy_volume / self.buy_avg.max(1.0),
                        "pattern": "steady_selling_low_buying",
                    }),
                );
            }
        }
    }

    /// Event 13: Climax detection
    fn check_climax(&mut self, timestamp: i64) {
        // Extreme volume on both sides (battle)
        let total = self.buy_volume + self.sell_volume;
        let avg_total = self.buy_avg + self.sell_avg;

        if total > avg_total * 3.0 && avg_total > 0.0 {
            if self.should_fire(BD_TAKER_CLIMAX, timestamp) {
                self.publish_event(
                    BD_TAKER_CLIMAX,
                    timestamp,
                    json!({
                        "total_volume": total,
                        "avg_total": avg_total,
                        "buy_pct": self.taker_ratio_pct,
                        "climax_type": "high_volume_battle",
                    }),
                );
            }
        }
    }

    /// Event 14: Exhaustion detection
    fn check_exhaustion(&mut self, timestamp: i64) {
        // Declining volume after extreme
        if self.history.len() >= 3 {
            let recent: Vec<_> = self.history.iter().rev().take(3).collect();
            let total_volumes: Vec<f64> = recent.iter()
                .map(|r| r.buy_volume + r.sell_volume)
                .collect();

            if total_volumes[0] < total_volumes[1] && total_volumes[1] < total_volumes[2] {
                if self.should_fire(BD_TAKER_EXHAUSTION, timestamp) {
                    self.publish_event(
                        BD_TAKER_EXHAUSTION,
                        timestamp,
                        json!({
                            "current_total": total_volumes[0],
                            "prev_total": total_volumes[1],
                            "older_total": total_volumes[2],
                            "pattern": "declining_volume",
                        }),
                    );
                }
            }
        }
    }

    /// Event 15: Smart money detection
    fn check_smart_money(&mut self, timestamp: i64) {
        // Large taker orders against the crowd
        if self.history.len() >= 5 {
            let recent_ratio = self.buy_sell_ratio;
            let older_ratios: f64 = self.history.iter()
                .take(self.history.len() - 1)
                .map(|h| h.ratio)
                .sum::<f64>() / (self.history.len() - 1) as f64;

            // Trading against the recent trend
            if (recent_ratio - older_ratios).abs() > 0.5 {
                if self.should_fire(BD_TAKER_SMART_MONEY, timestamp) {
                    self.publish_event(
                        BD_TAKER_SMART_MONEY,
                        timestamp,
                        json!({
                            "current_ratio": recent_ratio,
                            "avg_ratio": older_ratios,
                            "divergence": "potential_smart_money_flow",
                        }),
                    );
                }
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
            "Taker Volume event published"
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

    /// Get buy/sell ratio
    pub fn current_ratio(&self) -> f64 {
        self.buy_sell_ratio
    }
}
