// Open Interest Tracker - Monitors open interest changes from Binance
// Generates 15 open interest-related condition events
// Input: REST API polling (1-3 min interval)

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer3::common::event_types::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Open Interest regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OIRegime {
    Accumulating,    // OI increasing, potential trend building
    Distributing,    // OI decreasing, positions closing
    Stable,          // OI relatively unchanged
    RapidGrowth,     // OI spiking rapidly
    RapidDecline,    // OI dropping rapidly
}

/// Open Interest data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OIDataPoint {
    pub symbol: String,
    pub open_interest: f64,
    pub open_interest_value: f64,
    pub timestamp: i64,
    pub notional_value: f64,
}

/// OpenInterestTracker monitors open interest changes across timeframes
pub struct OpenInterestTracker {
    symbol: String,

    // Current and historical OI values
    current_oi: f64,
    prev_oi: f64,
    prev_oi_value: f64,

    // OI history for trend analysis
    oi_history: Vec<OIDataPoint>,
    max_history: usize,

    // Calculated metrics
    oi_change_pct: f64,
    oi_velocity: f64,        // Rate of change
    oi_acceleration: f64,    // Acceleration of change

    // Historical statistics
    oi_avg: f64,
    oi_std: f64,
    oi_high: f64,
    oi_low: f64,

    // Price correlation tracking
    last_price: f64,
    price_oi_correlation: f64,

    // Regime tracking
    current_regime: OIRegime,
    prev_regime: OIRegime,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl OpenInterestTracker {
    /// Create a new OpenInterestTracker
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            current_oi: 0.0,
            prev_oi: 0.0,
            prev_oi_value: 0.0,
            oi_history: Vec::with_capacity(100),
            max_history: 100,
            oi_change_pct: 0.0,
            oi_velocity: 0.0,
            oi_acceleration: 0.0,
            oi_avg: 0.0,
            oi_std: 0.0,
            oi_high: 0.0,
            oi_low: f64::MAX,
            last_price: 0.0,
            price_oi_correlation: 0.0,
            current_regime: OIRegime::Stable,
            prev_regime: OIRegime::Stable,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 60_000,  // 1 minute debounce
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    /// Update with new OI data from REST API
    pub fn update_oi(&mut self, data: &OIDataPoint, current_time: i64) {
        self.updates_processed += 1;

        self.prev_oi = self.current_oi;
        self.current_oi = data.open_interest;

        // Calculate change percentage
        if self.prev_oi > 0.0 {
            self.oi_change_pct = ((self.current_oi - self.prev_oi) / self.prev_oi) * 100.0;
        }

        // Update history
        self.oi_history.push(data.clone());
        if self.oi_history.len() > self.max_history {
            self.oi_history.remove(0);
        }

        // Update statistics
        self.update_statistics();

        // Detect regime
        self.prev_regime = self.current_regime;
        self.current_regime = self.classify_regime();
    }

    /// Update price for correlation tracking
    pub fn update_price(&mut self, price: f64) {
        self.last_price = price;
    }

    /// Check all OI events
    pub fn check_events(&mut self, current_time: i64) {
        // Event 1: OI threshold exceeded
        self.check_oi_threshold(current_time);

        // Event 2: OI regime change
        self.check_oi_regime(current_time);

        // Event 3: OI spike
        self.check_oi_spike(current_time);

        // Event 4: OI drop
        self.check_oi_drop(current_time);

        // Event 5: Price-OI correlation
        self.check_price_oi_correlation(current_time);

        // Event 6: OI delta positive
        self.check_oi_delta_positive(current_time);

        // Event 7: OI delta negative
        self.check_oi_delta_negative(current_time);

        // Event 8: OI concentration
        self.check_oi_concentration(current_time);

        // Event 9: Liquidation correlation
        self.check_liquidation_correlation(current_time);

        // Event 10: Funding correlation
        self.check_funding_correlation(current_time);

        // Event 11: Premium correlation
        self.check_premium_correlation(current_time);

        // Event 12: Trend acceleration
        self.check_trend_acceleration(current_time);

        // Event 13: Trend deceleration
        self.check_trend_deceleration(current_time);

        // Event 14: Historical extreme
        self.check_historical_extreme(current_time);

        // Event 15: Mean reversion
        self.check_mean_reversion(current_time);
    }

    /// Update internal statistics
    fn update_statistics(&mut self) {
        if self.oi_history.is_empty() {
            return;
        }

        let sum: f64 = self.oi_history.iter().map(|d| d.open_interest).sum();
        self.oi_avg = sum / self.oi_history.len() as f64;

        let variance: f64 = self.oi_history
            .iter()
            .map(|d| (d.open_interest - self.oi_avg).powi(2))
            .sum::<f64>() / self.oi_history.len() as f64;
        self.oi_std = variance.sqrt();

        self.oi_high = self.oi_history.iter().map(|d| d.open_interest).fold(f64::MIN, f64::max);
        self.oi_low = self.oi_history.iter().map(|d| d.open_interest).fold(f64::MAX, f64::min);

        // Calculate velocity and acceleration
        if self.oi_history.len() >= 2 {
            let current = self.oi_history.last().unwrap().open_interest;
            let prev = self.oi_history[self.oi_history.len() - 2].open_interest;
            let new_velocity = current - prev;

            self.oi_acceleration = new_velocity - self.oi_velocity;
            self.oi_velocity = new_velocity;
        }
    }

    /// Classify OI regime
    fn classify_regime(&self) -> OIRegime {
        let threshold = self.thresholds.oi_change_threshold_pct;

        if self.oi_change_pct > threshold * 3.0 {
            OIRegime::RapidGrowth
        } else if self.oi_change_pct < -threshold * 3.0 {
            OIRegime::RapidDecline
        } else if self.oi_change_pct > threshold {
            OIRegime::Accumulating
        } else if self.oi_change_pct < -threshold {
            OIRegime::Distributing
        } else {
            OIRegime::Stable
        }
    }

    /// Event 1: OI threshold exceeded
    fn check_oi_threshold(&mut self, timestamp: i64) {
        if self.oi_change_pct.abs() > self.thresholds.oi_change_threshold_pct {
            if self.should_fire(BD_OI_THRESHOLD, timestamp) {
                self.publish_event(
                    BD_OI_THRESHOLD,
                    timestamp,
                    json!({
                        "oi_change_pct": self.oi_change_pct,
                        "current_oi": self.current_oi,
                        "threshold": self.thresholds.oi_change_threshold_pct,
                    }),
                );
            }
        }
    }

    /// Event 2: OI regime change
    fn check_oi_regime(&mut self, timestamp: i64) {
        if self.current_regime != self.prev_regime {
            if self.should_fire(BD_OI_REGIME, timestamp) {
                self.publish_event(
                    BD_OI_REGIME,
                    timestamp,
                    json!({
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                        "oi_change_pct": self.oi_change_pct,
                    }),
                );
            }
        }
    }

    /// Event 3: OI spike (rapid increase)
    fn check_oi_spike(&mut self, timestamp: i64) {
        if self.oi_change_pct > self.thresholds.oi_change_threshold_pct * 2.0 {
            if self.should_fire(BD_OI_SPIKE, timestamp) {
                self.publish_event(
                    BD_OI_SPIKE,
                    timestamp,
                    json!({
                        "oi_change_pct": self.oi_change_pct,
                        "current_oi": self.current_oi,
                        "velocity": self.oi_velocity,
                    }),
                );
            }
        }
    }

    /// Event 4: OI drop (rapid decrease)
    fn check_oi_drop(&mut self, timestamp: i64) {
        if self.oi_change_pct < -self.thresholds.oi_change_threshold_pct * 2.0 {
            if self.should_fire(BD_OI_DROP, timestamp) {
                self.publish_event(
                    BD_OI_DROP,
                    timestamp,
                    json!({
                        "oi_change_pct": self.oi_change_pct,
                        "current_oi": self.current_oi,
                        "velocity": self.oi_velocity,
                    }),
                );
            }
        }
    }

    /// Event 5: Price-OI correlation
    fn check_price_oi_correlation(&mut self, timestamp: i64) {
        // Detect when OI and price move together or diverge
        let oi_direction = self.oi_change_pct.signum();
        let price_direction = if self.last_price > 0.0 { 1.0 } else if self.last_price < 0.0 { -1.0 } else { 0.0 };

        if oi_direction != 0.0 && price_direction != 0.0 && oi_direction != price_direction {
            if self.oi_change_pct.abs() > self.thresholds.oi_change_threshold_pct {
                if self.should_fire(BD_OI_PRICE_CORRELATION, timestamp) {
                    self.publish_event(
                        BD_OI_PRICE_CORRELATION,
                        timestamp,
                        json!({
                            "oi_change_pct": self.oi_change_pct,
                            "oi_direction": oi_direction,
                            "price_direction": price_direction,
                            "correlation_type": "divergence",
                        }),
                    );
                }
            }
        }
    }

    /// Event 6: OI delta positive (new positions opening)
    fn check_oi_delta_positive(&mut self, timestamp: i64) {
        if self.oi_velocity > 0.0 && self.oi_acceleration > 0.0 {
            if self.should_fire(BD_OI_DELTA_POSITIVE, timestamp) {
                self.publish_event(
                    BD_OI_DELTA_POSITIVE,
                    timestamp,
                    json!({
                        "velocity": self.oi_velocity,
                        "acceleration": self.oi_acceleration,
                        "current_oi": self.current_oi,
                    }),
                );
            }
        }
    }

    /// Event 7: OI delta negative (positions closing)
    fn check_oi_delta_negative(&mut self, timestamp: i64) {
        if self.oi_velocity < 0.0 && self.oi_acceleration < 0.0 {
            if self.should_fire(BD_OI_DELTA_NEGATIVE, timestamp) {
                self.publish_event(
                    BD_OI_DELTA_NEGATIVE,
                    timestamp,
                    json!({
                        "velocity": self.oi_velocity,
                        "acceleration": self.oi_acceleration,
                        "current_oi": self.current_oi,
                    }),
                );
            }
        }
    }

    /// Event 8: OI concentration (approaching historical extremes)
    fn check_oi_concentration(&mut self, timestamp: i64) {
        if self.oi_avg > 0.0 {
            let z_score = (self.current_oi - self.oi_avg) / self.oi_std.max(1.0);
            if z_score.abs() > 2.0 {
                if self.should_fire(BD_OI_CONCENTRATION, timestamp) {
                    self.publish_event(
                        BD_OI_CONCENTRATION,
                        timestamp,
                        json!({
                            "z_score": z_score,
                            "current_oi": self.current_oi,
                            "oi_avg": self.oi_avg,
                            "oi_std": self.oi_std,
                        }),
                    );
                }
            }
        }
    }

    /// Event 9: Liquidation correlation (placeholder for integration)
    fn check_liquidation_correlation(&mut self, timestamp: i64) {
        // Triggered when OI drops rapidly - likely liquidations
        if self.oi_change_pct < -self.thresholds.oi_change_threshold_pct * 3.0 {
            if self.should_fire(BD_OI_LIQUIDATION_CORRELATION, timestamp) {
                self.publish_event(
                    BD_OI_LIQUIDATION_CORRELATION,
                    timestamp,
                    json!({
                        "oi_drop_pct": self.oi_change_pct,
                        "current_oi": self.current_oi,
                        "potential_liquidations": true,
                    }),
                );
            }
        }
    }

    /// Event 10: Funding correlation
    fn check_funding_correlation(&mut self, timestamp: i64) {
        // High OI growth often precedes funding extremes
        if matches!(self.current_regime, OIRegime::RapidGrowth | OIRegime::Accumulating) {
            if self.should_fire(BD_OI_FUNDING_CORRELATION, timestamp) {
                self.publish_event(
                    BD_OI_FUNDING_CORRELATION,
                    timestamp,
                    json!({
                        "regime": format!("{:?}", self.current_regime),
                        "oi_change_pct": self.oi_change_pct,
                        "funding_implication": if self.oi_change_pct > 0.0 { "potential_increase" } else { "potential_decrease" },
                    }),
                );
            }
        }
    }

    /// Event 11: Premium correlation
    fn check_premium_correlation(&mut self, timestamp: i64) {
        // OI changes can affect futures premium
        if self.oi_change_pct.abs() > self.thresholds.oi_change_threshold_pct * 1.5 {
            if self.should_fire(BD_OI_PREMIUM_CORRELATION, timestamp) {
                self.publish_event(
                    BD_OI_PREMIUM_CORRELATION,
                    timestamp,
                    json!({
                        "oi_change_pct": self.oi_change_pct,
                        "premium_implication": "watch_for_premium_shift",
                    }),
                );
            }
        }
    }

    /// Event 12: Trend acceleration
    fn check_trend_acceleration(&mut self, timestamp: i64) {
        if self.oi_acceleration.abs() > self.oi_velocity.abs() * 0.5 && self.oi_acceleration > 0.0 {
            if self.should_fire(BD_OI_TREND_ACCELERATION, timestamp) {
                self.publish_event(
                    BD_OI_TREND_ACCELERATION,
                    timestamp,
                    json!({
                        "acceleration": self.oi_acceleration,
                        "velocity": self.oi_velocity,
                        "current_oi": self.current_oi,
                    }),
                );
            }
        }
    }

    /// Event 13: Trend deceleration
    fn check_trend_deceleration(&mut self, timestamp: i64) {
        if self.oi_acceleration < 0.0 && self.oi_velocity.abs() > 0.0 {
            if self.should_fire(BD_OI_TREND_DECELERATION, timestamp) {
                self.publish_event(
                    BD_OI_TREND_DECELERATION,
                    timestamp,
                    json!({
                        "acceleration": self.oi_acceleration,
                        "velocity": self.oi_velocity,
                        "current_oi": self.current_oi,
                    }),
                );
            }
        }
    }

    /// Event 14: Historical extreme
    fn check_historical_extreme(&mut self, timestamp: i64) {
        let near_high = self.current_oi >= self.oi_high * 0.95;
        let near_low = self.current_oi <= self.oi_low * 1.05 && self.oi_low > 0.0;

        if near_high || near_low {
            if self.should_fire(BD_OI_HISTORICAL_EXTREME, timestamp) {
                self.publish_event(
                    BD_OI_HISTORICAL_EXTREME,
                    timestamp,
                    json!({
                        "current_oi": self.current_oi,
                        "oi_high": self.oi_high,
                        "oi_low": self.oi_low,
                        "extreme_type": if near_high { "high" } else { "low" },
                    }),
                );
            }
        }
    }

    /// Event 15: Mean reversion
    fn check_mean_reversion(&mut self, timestamp: i64) {
        if self.oi_avg > 0.0 && self.current_oi > 0.0 {
            let deviation_pct = ((self.current_oi - self.oi_avg) / self.oi_avg) * 100.0;
            let prev_deviation_pct = ((self.prev_oi - self.oi_avg) / self.oi_avg) * 100.0;

            // Check if returning towards mean from extreme
            if deviation_pct.abs() < prev_deviation_pct.abs() && deviation_pct.abs() < 20.0 {
                if self.should_fire(BD_OI_MEAN_REVERSION, timestamp) {
                    self.publish_event(
                        BD_OI_MEAN_REVERSION,
                        timestamp,
                        json!({
                            "current_oi": self.current_oi,
                            "oi_avg": self.oi_avg,
                            "deviation_pct": deviation_pct,
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
            "Open Interest event published"
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

    /// Get current OI
    pub fn current_oi(&self) -> f64 {
        self.current_oi
    }
}
