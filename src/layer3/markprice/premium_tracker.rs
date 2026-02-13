// Premium Tracker - Monitors mark price premium over index price
// Generates 10 premium-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::core::types::MarkPrice;
use crate::layer3::common::event_types::*;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Premium regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PremiumRegime {
    DeepDiscount,   // Premium < -20 bps
    Discount,       // Premium -20 to -5 bps
    Neutral,        // Premium -5 to +5 bps
    Premium,        // Premium +5 to +20 bps
    DeepPremium,    // Premium > +20 bps
}

/// PremiumTracker monitors mark price premium patterns over time
pub struct PremiumTracker {
    symbol: String,

    // Current premium metrics
    current_premium: f64,           // mark_price - index_price
    current_premium_bps: f64,       // premium in basis points
    prev_premium_bps: f64,

    // Premium history for analysis
    premium_history: Vec<f64>,
    max_history: usize,

    // Moving averages
    avg_premium_bps: f64,
    min_premium_bps: f64,
    max_premium_bps: f64,

    // Velocity and acceleration
    premium_velocity: f64,
    prev_velocity: f64,
    premium_acceleration: f64,

    // Regime tracking
    current_regime: PremiumRegime,
    prev_regime: PremiumRegime,

    // Persistence tracking
    premium_persistence_count: u64,
    persistence_threshold_bps: f64,

    // Volatility tracking
    premium_volatility: f64,

    // Convergence/Divergence tracking
    convergence_count: u64,
    divergence_count: u64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl PremiumTracker {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            current_premium: 0.0,
            current_premium_bps: 0.0,
            prev_premium_bps: 0.0,
            premium_history: Vec::with_capacity(1000),
            max_history: 1000,
            avg_premium_bps: 0.0,
            min_premium_bps: f64::MAX,
            max_premium_bps: f64::MIN,
            premium_velocity: 0.0,
            prev_velocity: 0.0,
            premium_acceleration: 0.0,
            current_regime: PremiumRegime::Neutral,
            prev_regime: PremiumRegime::Neutral,
            premium_persistence_count: 0,
            persistence_threshold_bps: 1.0,
            premium_volatility: 0.0,
            convergence_count: 0,
            divergence_count: 0,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 1000,
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    /// Process mark price update
    pub fn update(&mut self, data: &MarkPrice, current_time: i64) {
        self.updates_processed += 1;

        // Store previous values
        self.prev_premium_bps = self.current_premium_bps;
        self.prev_velocity = self.premium_velocity;
        self.prev_regime = self.current_regime;

        // Calculate premium
        self.current_premium = data.mark_price - data.index_price;
        
        // Calculate premium in basis points
        if data.index_price > 0.0 {
            self.current_premium_bps = (self.current_premium / data.index_price) * 10000.0;
        } else {
            self.current_premium_bps = 0.0;
        }

        // Update history
        self.premium_history.push(self.current_premium_bps);
        if self.premium_history.len() > self.max_history {
            self.premium_history.remove(0);
        }

        // Calculate statistics
        self.calculate_statistics();

        // Calculate velocity and acceleration
        self.premium_velocity = self.current_premium_bps - self.prev_premium_bps;
        self.premium_acceleration = self.premium_velocity - self.prev_velocity;

        // Update regime
        self.current_regime = self.classify_regime(self.current_premium_bps);

        // Track persistence
        self.update_persistence();

        // Track convergence/divergence
        self.update_convergence_divergence();
    }

    /// Check all premium events
    pub fn check_events(&mut self, current_time: i64) {
        self.check_premium_threshold(current_time);
        self.check_premium_regime_change(current_time);
        self.check_premium_flipped(current_time);
        self.check_premium_spike(current_time);
        self.check_premium_convergence(current_time);
        self.check_premium_divergence(current_time);
        self.check_premium_extreme(current_time);
        self.check_premium_mean_reversion(current_time);
        self.check_premium_acceleration(current_time);
        self.check_premium_persistence(current_time);
    }

    fn calculate_statistics(&mut self) {
        if self.premium_history.is_empty() {
            return;
        }

        // Calculate average
        self.avg_premium_bps = self.premium_history.iter().sum::<f64>() / self.premium_history.len() as f64;

        // Update min/max
        self.min_premium_bps = self.min_premium_bps.min(self.current_premium_bps);
        self.max_premium_bps = self.max_premium_bps.max(self.current_premium_bps);

        // Calculate volatility (standard deviation)
        if self.premium_history.len() > 1 {
            let mean = self.avg_premium_bps;
            let variance: f64 = self.premium_history.iter()
                .map(|x| (x - mean).powi(2))
                .sum::<f64>() / self.premium_history.len() as f64;
            self.premium_volatility = variance.sqrt();
        }
    }

    fn classify_regime(&self, premium_bps: f64) -> PremiumRegime {
        if premium_bps < -20.0 {
            PremiumRegime::DeepDiscount
        } else if premium_bps < -5.0 {
            PremiumRegime::Discount
        } else if premium_bps <= 5.0 {
            PremiumRegime::Neutral
        } else if premium_bps <= 20.0 {
            PremiumRegime::Premium
        } else {
            PremiumRegime::DeepPremium
        }
    }

    fn update_persistence(&mut self) {
        if (self.current_premium_bps - self.prev_premium_bps).abs() < self.persistence_threshold_bps {
            self.premium_persistence_count += 1;
        } else {
            self.premium_persistence_count = 0;
        }
    }

    fn update_convergence_divergence(&mut self) {
        let dist_from_mean = (self.current_premium_bps - self.avg_premium_bps).abs();
        let prev_dist_from_mean = (self.prev_premium_bps - self.avg_premium_bps).abs();

        if dist_from_mean < prev_dist_from_mean {
            self.convergence_count += 1;
            self.divergence_count = 0;
        } else if dist_from_mean > prev_dist_from_mean {
            self.divergence_count += 1;
            self.convergence_count = 0;
        }
    }

    /// Event 1: Premium threshold exceeded
    fn check_premium_threshold(&mut self, timestamp: i64) {
        let threshold_bps = self.thresholds.premium_threshold_pct * 100.0; // Convert to bps
        if self.current_premium_bps.abs() > threshold_bps {
            if self.should_fire(MP_PREMIUM_THRESHOLD, timestamp) {
                self.publish_event(
                    MP_PREMIUM_THRESHOLD,
                    timestamp,
                    json!({
                        "premium_bps": self.current_premium_bps,
                        "threshold_bps": threshold_bps,
                        "direction": if self.current_premium_bps > 0.0 { "premium" } else { "discount" },
                    }),
                );
            }
        }
    }

    /// Event 2: Premium regime change
    fn check_premium_regime_change(&mut self, timestamp: i64) {
        if self.current_regime != self.prev_regime {
            if self.should_fire(MP_PREMIUM_REGIME, timestamp) {
                self.publish_event(
                    MP_PREMIUM_REGIME,
                    timestamp,
                    json!({
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                        "premium_bps": self.current_premium_bps,
                    }),
                );
            }
        }
    }

    /// Event 3: Premium flipped (changed sign)
    fn check_premium_flipped(&mut self, timestamp: i64) {
        let flipped = (self.prev_premium_bps > 0.0 && self.current_premium_bps < 0.0)
            || (self.prev_premium_bps < 0.0 && self.current_premium_bps > 0.0);

        if flipped {
            if self.should_fire(MP_PREMIUM_FLIPPED, timestamp) {
                self.publish_event(
                    MP_PREMIUM_FLIPPED,
                    timestamp,
                    json!({
                        "prev_premium_bps": self.prev_premium_bps,
                        "new_premium_bps": self.current_premium_bps,
                        "direction": if self.current_premium_bps > 0.0 { "to_premium" } else { "to_discount" },
                    }),
                );
            }
        }
    }

    /// Event 4: Premium spike detected
    fn check_premium_spike(&mut self, timestamp: i64) {
        if self.premium_velocity.abs() > 5.0 { // 5 bps change in one update
            if self.should_fire(MP_PREMIUM_SPIKE, timestamp) {
                self.publish_event(
                    MP_PREMIUM_SPIKE,
                    timestamp,
                    json!({
                        "premium_bps": self.current_premium_bps,
                        "velocity": self.premium_velocity,
                        "prev_premium_bps": self.prev_premium_bps,
                    }),
                );
            }
        }
    }

    /// Event 5: Premium converging to mean
    fn check_premium_convergence(&mut self, timestamp: i64) {
        if self.convergence_count >= 10 && self.premium_volatility > 0.0 {
            let z_score = (self.current_premium_bps - self.avg_premium_bps).abs() / self.premium_volatility;
            if z_score < 1.0 {
                if self.should_fire(MP_PREMIUM_CONVERGENCE, timestamp) {
                    self.publish_event(
                        MP_PREMIUM_CONVERGENCE,
                        timestamp,
                        json!({
                            "premium_bps": self.current_premium_bps,
                            "avg_premium_bps": self.avg_premium_bps,
                            "convergence_count": self.convergence_count,
                        }),
                    );
                }
            }
        }
    }

    /// Event 6: Premium diverging from mean
    fn check_premium_divergence(&mut self, timestamp: i64) {
        if self.divergence_count >= 5 {
            if self.should_fire(MP_PREMIUM_DIVERGENCE, timestamp) {
                self.publish_event(
                    MP_PREMIUM_DIVERGENCE,
                    timestamp,
                    json!({
                        "premium_bps": self.current_premium_bps,
                        "avg_premium_bps": self.avg_premium_bps,
                        "divergence_count": self.divergence_count,
                    }),
                );
            }
        }
    }

    /// Event 7: Premium at extreme level
    fn check_premium_extreme(&mut self, timestamp: i64) {
        let is_extreme = self.current_premium_bps > 50.0 || self.current_premium_bps < -50.0;
        
        if is_extreme {
            if self.should_fire(MP_PREMIUM_EXTREME, timestamp) {
                self.publish_event(
                    MP_PREMIUM_EXTREME,
                    timestamp,
                    json!({
                        "premium_bps": self.current_premium_bps,
                        "min_premium_bps": self.min_premium_bps,
                        "max_premium_bps": self.max_premium_bps,
                        "direction": if self.current_premium_bps > 0.0 { "premium" } else { "discount" },
                    }),
                );
            }
        }
    }

    /// Event 8: Premium mean reversion signal
    fn check_premium_mean_reversion(&mut self, timestamp: i64) {
        if self.premium_volatility > 0.0 && (self.current_premium_bps - self.avg_premium_bps).abs() > 2.0 * self.premium_volatility {
            let is_reversing = self.premium_velocity.abs() > 0.0 
                && ((self.current_premium_bps > self.avg_premium_bps && self.premium_velocity < 0.0)
                    || (self.current_premium_bps < self.avg_premium_bps && self.premium_velocity > 0.0));

            if is_reversing {
                if self.should_fire(MP_PREMIUM_MEAN_REVERSION, timestamp) {
                    self.publish_event(
                        MP_PREMIUM_MEAN_REVERSION,
                        timestamp,
                        json!({
                            "premium_bps": self.current_premium_bps,
                            "avg_premium_bps": self.avg_premium_bps,
                            "velocity": self.premium_velocity,
                        }),
                    );
                }
            }
        }
    }

    /// Event 9: Premium acceleration detected
    fn check_premium_acceleration(&mut self, timestamp: i64) {
        if self.premium_acceleration.abs() > 2.0 { // Acceleration > 2 bps/update^2
            if self.should_fire(MP_PREMIUM_ACCELERATION, timestamp) {
                self.publish_event(
                    MP_PREMIUM_ACCELERATION,
                    timestamp,
                    json!({
                        "acceleration": self.premium_acceleration,
                        "velocity": self.premium_velocity,
                        "premium_bps": self.current_premium_bps,
                    }),
                );
            }
        }
    }

    /// Event 10: Premium persistence detected
    fn check_premium_persistence(&mut self, timestamp: i64) {
        if self.premium_persistence_count >= 30 && self.current_premium_bps.abs() > 5.0 {
            if self.should_fire(MP_PREMIUM_PERSISTENCE, timestamp) {
                self.publish_event(
                    MP_PREMIUM_PERSISTENCE,
                    timestamp,
                    json!({
                        "premium_bps": self.current_premium_bps,
                        "persistence_count": self.premium_persistence_count,
                        "direction": if self.current_premium_bps > 0.0 { "premium" } else { "discount" },
                    }),
                );
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
        }

        publish_event(
            event_type,
            timestamp,
            serde_json::from_value(data).unwrap_or_default(),
            "markprice_aggregator",
            EventPriority::Medium,
        );

        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(symbol = %self.symbol, event = event_type, "Premium event published");
    }

    pub fn updates_processed(&self) -> u64 {
        self.updates_processed
    }

    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    /// Get current premium in bps
    pub fn current_premium_bps(&self) -> f64 {
        self.current_premium_bps
    }

    /// Get average premium in bps
    pub fn avg_premium_bps(&self) -> f64 {
        self.avg_premium_bps
    }

    /// Get current regime
    pub fn current_regime(&self) -> PremiumRegime {
        self.current_regime
    }
}
