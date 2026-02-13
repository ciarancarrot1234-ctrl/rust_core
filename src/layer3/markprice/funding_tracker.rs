// Funding Tracker - Monitors funding rate patterns
// Generates 10 funding-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::core::types::MarkPrice;
use crate::layer3::common::event_types::*;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Funding regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FundingRegime {
    StrongShort,    // Funding < -0.05% (strong short pressure)
    Short,          // Funding -0.05% to -0.01%
    Neutral,        // Funding -0.01% to +0.01%
    Long,           // Funding +0.01% to +0.05%
    StrongLong,     // Funding > +0.05% (strong long pressure)
}

/// FundingTracker monitors funding rate patterns over time
pub struct FundingTracker {
    symbol: String,

    // Current funding metrics
    current_funding_rate: f64,
    prev_funding_rate: f64,
    
    // Annualized funding (funding_rate * 3 * 365 * 100)
    annualized_funding: f64,

    // Next funding time
    next_funding_time: i64,
    time_to_funding_ms: i64,

    // Funding history for analysis
    funding_history: Vec<f64>,
    max_history: usize,

    // Moving averages
    avg_funding_rate: f64,
    min_funding_rate: f64,
    max_funding_rate: f64,

    // Velocity and trend
    funding_velocity: f64,
    funding_trend: f64, // Sum of last N changes

    // Regime tracking
    current_regime: FundingRegime,
    prev_regime: FundingRegime,

    // Cumulative tracking
    cumulative_funding: f64,
    funding_count: u64,

    // Squeeze detection
    squeeze_signals: u32,
    high_funding_duration: u64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl FundingTracker {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            current_funding_rate: 0.0,
            prev_funding_rate: 0.0,
            annualized_funding: 0.0,
            next_funding_time: 0,
            time_to_funding_ms: 0,
            funding_history: Vec::with_capacity(1000),
            max_history: 1000,
            avg_funding_rate: 0.0,
            min_funding_rate: f64::MAX,
            max_funding_rate: f64::MIN,
            funding_velocity: 0.0,
            funding_trend: 0.0,
            current_regime: FundingRegime::Neutral,
            prev_regime: FundingRegime::Neutral,
            cumulative_funding: 0.0,
            funding_count: 0,
            squeeze_signals: 0,
            high_funding_duration: 0,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 5000, // 5 seconds for funding events
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    /// Process mark price update
    pub fn update(&mut self, data: &MarkPrice, current_time: i64) {
        self.updates_processed += 1;

        // Store previous values
        self.prev_funding_rate = self.current_funding_rate;
        self.prev_regime = self.current_regime;

        // Update current funding rate
        self.current_funding_rate = data.funding_rate;
        self.next_funding_time = data.next_funding_time;

        // Calculate annualized funding: funding_rate * 3 * 365 * 100 (as percentage)
        self.annualized_funding = data.funding_rate * 3.0 * 365.0 * 100.0;

        // Calculate time to funding
        self.time_to_funding_ms = (data.next_funding_time - current_time).max(0);

        // Update history
        self.funding_history.push(self.current_funding_rate);
        if self.funding_history.len() > self.max_history {
            self.funding_history.remove(0);
        }

        // Calculate statistics
        self.calculate_statistics();

        // Calculate velocity and trend
        self.funding_velocity = self.current_funding_rate - self.prev_funding_rate;
        self.calculate_trend();

        // Update regime
        self.current_regime = self.classify_regime(self.current_funding_rate);

        // Track cumulative funding
        self.cumulative_funding += self.current_funding_rate;
        self.funding_count += 1;

        // Track squeeze conditions
        self.update_squeeze_tracking();
    }

    /// Check all funding events
    pub fn check_events(&mut self, current_time: i64) {
        self.check_funding_threshold(current_time);
        self.check_funding_regime_change(current_time);
        self.check_funding_flipped(current_time);
        self.check_funding_extreme(current_time);
        self.check_funding_approaching(current_time);
        self.check_funding_trend(current_time);
        self.check_funding_reversal(current_time);
        self.check_funding_squeeze_conditions(current_time);
        self.check_funding_arb_opportunity(current_time);
        self.check_funding_positioning(current_time);
    }

    fn calculate_statistics(&mut self) {
        if self.funding_history.is_empty() {
            return;
        }

        // Calculate average
        self.avg_funding_rate = self.funding_history.iter().sum::<f64>() / self.funding_history.len() as f64;

        // Update min/max
        self.min_funding_rate = self.min_funding_rate.min(self.current_funding_rate);
        self.max_funding_rate = self.max_funding_rate.max(self.current_funding_rate);
    }

    fn calculate_trend(&mut self) {
        // Sum of last 5 funding changes
        let recent_changes: Vec<f64> = self.funding_history.iter()
            .rev()
            .take(5)
            .copied()
            .collect();
        
        self.funding_trend = recent_changes.iter()
            .zip(recent_changes.iter().skip(1))
            .map(|(curr, prev)| curr - prev)
            .sum();
    }

    fn classify_regime(&self, funding_rate: f64) -> FundingRegime {
        if funding_rate < -0.0005 { // -0.05%
            FundingRegime::StrongShort
        } else if funding_rate < -0.0001 { // -0.01%
            FundingRegime::Short
        } else if funding_rate <= 0.0001 { // +0.01%
            FundingRegime::Neutral
        } else if funding_rate <= 0.0005 { // +0.05%
            FundingRegime::Long
        } else {
            FundingRegime::StrongLong
        }
    }

    fn update_squeeze_tracking(&mut self) {
        // High funding indicates potential squeeze
        if self.current_funding_rate.abs() > 0.0003 { // 0.03%
            self.high_funding_duration += 1;
            self.squeeze_signals += 1;
        } else {
            self.high_funding_duration = 0;
        }
    }

    /// Event 1: Funding threshold exceeded
    fn check_funding_threshold(&mut self, timestamp: i64) {
        let threshold = self.thresholds.funding_rate_threshold_pct / 100.0; // Convert from percentage
        if self.current_funding_rate.abs() > threshold {
            if self.should_fire(MP_FUNDING_THRESHOLD, timestamp) {
                self.publish_event(
                    MP_FUNDING_THRESHOLD,
                    timestamp,
                    json!({
                        "funding_rate": self.current_funding_rate,
                        "annualized_pct": self.annualized_funding,
                        "threshold": threshold,
                        "direction": if self.current_funding_rate > 0.0 { "longs_pay_shorts" } else { "shorts_pay_longs" },
                    }),
                );
            }
        }
    }

    /// Event 2: Funding regime change
    fn check_funding_regime_change(&mut self, timestamp: i64) {
        if self.current_regime != self.prev_regime {
            if self.should_fire(MP_FUNDING_REGIME, timestamp) {
                self.publish_event(
                    MP_FUNDING_REGIME,
                    timestamp,
                    json!({
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                        "funding_rate": self.current_funding_rate,
                    }),
                );
            }
        }
    }

    /// Event 3: Funding flipped (changed sign)
    fn check_funding_flipped(&mut self, timestamp: i64) {
        let flipped = (self.prev_funding_rate > 0.0 && self.current_funding_rate < 0.0)
            || (self.prev_funding_rate < 0.0 && self.current_funding_rate > 0.0);

        if flipped {
            if self.should_fire(MP_FUNDING_FLIPPED, timestamp) {
                self.publish_event(
                    MP_FUNDING_FLIPPED,
                    timestamp,
                    json!({
                        "prev_funding_rate": self.prev_funding_rate,
                        "new_funding_rate": self.current_funding_rate,
                        "direction": if self.current_funding_rate > 0.0 { "longs_pay_shorts" } else { "shorts_pay_longs" },
                    }),
                );
            }
        }
    }

    /// Event 4: Funding at extreme level
    fn check_funding_extreme(&mut self, timestamp: i64) {
        let is_extreme = self.current_funding_rate.abs() > 0.001; // 0.1%
        
        if is_extreme {
            if self.should_fire(MP_FUNDING_EXTREME, timestamp) {
                self.publish_event(
                    MP_FUNDING_EXTREME,
                    timestamp,
                    json!({
                        "funding_rate": self.current_funding_rate,
                        "annualized_pct": self.annualized_funding,
                        "direction": if self.current_funding_rate > 0.0 { "long_pressure" } else { "short_pressure" },
                    }),
                );
            }
        }
    }

    /// Event 5: Funding settlement approaching
    fn check_funding_approaching(&mut self, timestamp: i64) {
        // Alert when funding is within 5 minutes
        if self.time_to_funding_ms > 0 && self.time_to_funding_ms < 300000 {
            if self.should_fire(MP_FUNDING_APPROACHING, timestamp) {
                self.publish_event(
                    MP_FUNDING_APPROACHING,
                    timestamp,
                    json!({
                        "funding_rate": self.current_funding_rate,
                        "time_to_funding_ms": self.time_to_funding_ms,
                        "next_funding_time": self.next_funding_time,
                    }),
                );
            }
        }
    }

    /// Event 6: Funding trend detected
    fn check_funding_trend(&mut self, timestamp: i64) {
        if self.funding_trend.abs() > 0.0002 {
            if self.should_fire(MP_FUNDING_TREND, timestamp) {
                self.publish_event(
                    MP_FUNDING_TREND,
                    timestamp,
                    json!({
                        "funding_rate": self.current_funding_rate,
                        "trend": self.funding_trend,
                        "direction": if self.funding_trend > 0.0 { "increasing" } else { "decreasing" },
                    }),
                );
            }
        }
    }

    /// Event 7: Funding reversal detected
    fn check_funding_reversal(&mut self, timestamp: i64) {
        let reversal = self.funding_velocity.abs() > 0.0001
            && ((self.funding_trend > 0.0 && self.funding_velocity < 0.0)
                || (self.funding_trend < 0.0 && self.funding_velocity > 0.0));

        if reversal {
            if self.should_fire(MP_FUNDING_REVERSAL, timestamp) {
                self.publish_event(
                    MP_FUNDING_REVERSAL,
                    timestamp,
                    json!({
                        "funding_rate": self.current_funding_rate,
                        "prev_funding_rate": self.prev_funding_rate,
                        "velocity": self.funding_velocity,
                    }),
                );
            }
        }
    }

    /// Event 8: Squeeze conditions detected
    fn check_funding_squeeze_conditions(&mut self, timestamp: i64) {
        // Squeeze conditions: sustained high funding
        if self.high_funding_duration >= 10 && self.current_funding_rate.abs() > 0.0005 {
            if self.should_fire(MP_FUNDING_SQUEEZE_CONDITIONS, timestamp) {
                self.publish_event(
                    MP_FUNDING_SQUEEZE_CONDITIONS,
                    timestamp,
                    json!({
                        "funding_rate": self.current_funding_rate,
                        "annualized_pct": self.annualized_funding,
                        "high_funding_duration": self.high_funding_duration,
                        "squeeze_type": if self.current_funding_rate > 0.0 { "short_squeeze_risk" } else { "long_squeeze_risk" },
                    }),
                );
            }
        }
    }

    /// Event 9: Arbitrage opportunity detected
    fn check_funding_arb_opportunity(&mut self, timestamp: i64) {
        // Annualized funding > 20% is typically considered attractive for arb
        if self.annualized_funding.abs() > 20.0 {
            if self.should_fire(MP_FUNDING_ARB_OPPORTUNITY, timestamp) {
                self.publish_event(
                    MP_FUNDING_ARB_OPPORTUNITY,
                    timestamp,
                    json!({
                        "funding_rate": self.current_funding_rate,
                        "annualized_pct": self.annualized_funding,
                        "strategy": if self.current_funding_rate > 0.0 { "short_perp_long_spot" } else { "long_perp_short_spot" },
                    }),
                );
            }
        }
    }

    /// Event 10: Funding-based positioning signal
    fn check_funding_positioning(&mut self, timestamp: i64) {
        // Contrarian positioning signal based on extreme funding
        if self.current_funding_rate.abs() > 0.0003 {
            let avg_funding = if self.avg_funding_rate.abs() > 0.0 {
                self.current_funding_rate / self.avg_funding_rate
            } else {
                0.0
            };
            
            if avg_funding.abs() > 1.5 {
                if self.should_fire(MP_FUNDING_POSITIONING, timestamp) {
                    self.publish_event(
                        MP_FUNDING_POSITIONING,
                        timestamp,
                        json!({
                            "funding_rate": self.current_funding_rate,
                            "avg_funding_rate": self.avg_funding_rate,
                            "ratio": avg_funding,
                            "signal": if self.current_funding_rate > 0.0 { "crowded_long" } else { "crowded_short" },
                        }),
                    );
                }
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
        debug!(symbol = %self.symbol, event = event_type, "Funding event published");
    }

    pub fn updates_processed(&self) -> u64 {
        self.updates_processed
    }

    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    /// Get current funding rate
    pub fn current_funding_rate(&self) -> f64 {
        self.current_funding_rate
    }

    /// Get annualized funding rate
    pub fn annualized_funding(&self) -> f64 {
        self.annualized_funding
    }

    /// Get current regime
    pub fn current_regime(&self) -> FundingRegime {
        self.current_regime
    }

    /// Get time to next funding
    pub fn time_to_funding_ms(&self) -> i64 {
        self.time_to_funding_ms
    }
}
