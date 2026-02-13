// Insurance Fund Tracker - Monitors Binance insurance fund health
// Generates 15 insurance fund-related condition events
// Input: REST API polling (15 min interval)

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer3::common::event_types::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Insurance fund health regime
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsuranceRegime {
    Healthy,        // Well-funded, low risk
    Adequate,       // Normal operation
    Stressed,       // Drawdown detected, watch closely
    Critical,       // Significant depletion
    Recovering,     // Fund recovering from drawdown
}

/// Insurance fund data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsuranceFundDataPoint {
    pub symbol: String,
    pub fund_balance: f64,      // Current insurance fund balance (USDT)
    pub timestamp: i64,
    pub daily_change: f64,      // Change in last 24h
    pub weekly_change: f64,     // Change in last 7 days
}

/// Insurance fund history entry
#[derive(Debug, Clone)]
struct InsuranceHistoryEntry {
    balance: f64,
    timestamp: i64,
}

/// InsuranceFundTracker monitors exchange insurance fund health
pub struct InsuranceFundTracker {
    symbol: String,

    // Current values
    current_balance: f64,
    daily_change: f64,
    weekly_change: f64,

    // Previous values
    prev_balance: f64,

    // History
    history: Vec<InsuranceHistoryEntry>,
    max_history: usize,

    // Calculated metrics
    balance_change_pct: f64,
    balance_velocity: f64,

    // Statistics
    balance_avg: f64,
    balance_std: f64,
    balance_high: f64,
    balance_low: f64,

    // Drawdown tracking
    current_drawdown_pct: f64,
    max_drawdown_pct: f64,
    recovery_since_low: f64,

    // Liquidation correlation
    recent_liquidations_usd: f64,
    liquidation_to_fund_ratio: f64,

    // Regime
    current_regime: InsuranceRegime,
    prev_regime: InsuranceRegime,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl InsuranceFundTracker {
    /// Create a new InsuranceFundTracker
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            current_balance: 0.0,
            daily_change: 0.0,
            weekly_change: 0.0,
            prev_balance: 0.0,
            history: Vec::with_capacity(100),
            max_history: 100,
            balance_change_pct: 0.0,
            balance_velocity: 0.0,
            balance_avg: 0.0,
            balance_std: 0.0,
            balance_high: 0.0,
            balance_low: f64::MAX,
            current_drawdown_pct: 0.0,
            max_drawdown_pct: 0.0,
            recovery_since_low: 0.0,
            recent_liquidations_usd: 0.0,
            liquidation_to_fund_ratio: 0.0,
            current_regime: InsuranceRegime::Adequate,
            prev_regime: InsuranceRegime::Adequate,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 900_000,  // 15 minute debounce
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    /// Update with new insurance fund data from REST API
    pub fn update_insurance(&mut self, data: &InsuranceFundDataPoint, current_time: i64) {
        self.updates_processed += 1;

        self.prev_balance = self.current_balance;
        self.current_balance = data.fund_balance;
        self.daily_change = data.daily_change;
        self.weekly_change = data.weekly_change;

        // Calculate change percentage
        if self.prev_balance > 0.0 {
            self.balance_change_pct = ((self.current_balance - self.prev_balance) / self.prev_balance) * 100.0;
        }

        // Update history
        self.history.push(InsuranceHistoryEntry {
            balance: data.fund_balance,
            timestamp: data.timestamp,
        });

        if self.history.len() > self.max_history {
            self.history.remove(0);
        }

        // Update statistics
        self.update_statistics();

        // Update drawdown tracking
        self.update_drawdown();

        // Update regime
        self.prev_regime = self.current_regime;
        self.current_regime = self.classify_regime();
    }

    /// Update liquidation data for correlation
    pub fn update_liquidations(&mut self, liquidation_usd: f64) {
        self.recent_liquidations_usd = liquidation_usd;
        if self.current_balance > 0.0 {
            self.liquidation_to_fund_ratio = liquidation_usd / self.current_balance;
        }
    }

    /// Check all insurance fund events
    pub fn check_events(&mut self, current_time: i64) {
        // Event 1: Fund depletion
        self.check_depletion(current_time);

        // Event 2: Fund replenishment
        self.check_replenishment(current_time);

        // Event 3: Historical low
        self.check_historical_low(current_time);

        // Event 4: Historical high
        self.check_historical_high(current_time);

        // Event 5: Liquidation correlation
        self.check_liquidation_correlation(current_time);

        // Event 6: Systemic risk indicator
        self.check_systemic_risk(current_time);

        // Event 7: ADL indicator
        self.check_adl_indicator(current_time);

        // Event 8: Drawdown extreme
        self.check_drawdown_extreme(current_time);

        // Event 9: Recovery
        self.check_recovery(current_time);

        // Event 10: Stress test
        self.check_stress_test(current_time);

        // Event 11: Volatility correlation
        self.check_volatility_correlation(current_time);

        // Event 12: Market regime
        self.check_market_regime(current_time);

        // Event 13: Predictive model
        self.check_predictive_model(current_time);

        // Event 14: Risk adjustment
        self.check_risk_adjustment(current_time);

        // Event 15: Exchange health
        self.check_exchange_health(current_time);
    }

    /// Update internal statistics
    fn update_statistics(&mut self) {
        if self.history.is_empty() {
            return;
        }

        let sum: f64 = self.history.iter().map(|h| h.balance).sum();
        self.balance_avg = sum / self.history.len() as f64;

        let variance: f64 = self.history
            .iter()
            .map(|h| (h.balance - self.balance_avg).powi(2))
            .sum::<f64>() / self.history.len() as f64;
        self.balance_std = variance.sqrt();

        self.balance_high = self.history.iter().map(|h| h.balance).fold(f64::MIN, f64::max);
        self.balance_low = self.history.iter().map(|h| h.balance).fold(f64::MAX, f64::min);

        // Calculate velocity
        if self.history.len() >= 2 {
            self.balance_velocity = self.current_balance - self.history[self.history.len() - 2].balance;
        }
    }

    /// Update drawdown tracking
    fn update_drawdown(&mut self) {
        if self.balance_high > 0.0 {
            self.current_drawdown_pct = ((self.balance_high - self.current_balance) / self.balance_high) * 100.0;
            self.max_drawdown_pct = self.max_drawdown_pct.max(self.current_drawdown_pct);
        }

        if self.balance_low > 0.0 && self.current_balance > 0.0 {
            self.recovery_since_low = ((self.current_balance - self.balance_low) / self.balance_low) * 100.0;
        }
    }

    /// Classify regime
    fn classify_regime(&self) -> InsuranceRegime {
        // Check for recovery
        if self.recovery_since_low > 10.0 && self.current_drawdown_pct < self.max_drawdown_pct {
            return InsuranceRegime::Recovering;
        }

        // Check drawdown levels
        if self.current_drawdown_pct > 30.0 {
            InsuranceRegime::Critical
        } else if self.current_drawdown_pct > 15.0 {
            InsuranceRegime::Stressed
        } else if self.daily_change < -5.0 {
            InsuranceRegime::Stressed
        } else if self.current_balance > self.balance_avg * 1.2 {
            InsuranceRegime::Healthy
        } else {
            InsuranceRegime::Adequate
        }
    }

    /// Event 1: Fund depletion
    fn check_depletion(&mut self, timestamp: i64) {
        if self.daily_change < -5.0 || self.balance_change_pct < -2.0 {
            if self.should_fire(BD_INSURANCE_DEPLETION, timestamp) {
                self.publish_event(
                    BD_INSURANCE_DEPLETION,
                    timestamp,
                    json!({
                        "current_balance": self.current_balance,
                        "daily_change": self.daily_change,
                        "balance_change_pct": self.balance_change_pct,
                        "severity": if self.daily_change < -10.0 { "high" } else { "moderate" },
                    }),
                );
            }
        }
    }

    /// Event 2: Fund replenishment
    fn check_replenishment(&mut self, timestamp: i64) {
        if self.daily_change > 2.0 || self.balance_change_pct > 1.0 {
            if self.should_fire(BD_INSURANCE_REPLENISHMENT, timestamp) {
                self.publish_event(
                    BD_INSURANCE_REPLENISHMENT,
                    timestamp,
                    json!({
                        "current_balance": self.current_balance,
                        "daily_change": self.daily_change,
                        "balance_change_pct": self.balance_change_pct,
                    }),
                );
            }
        }
    }

    /// Event 3: Historical low
    fn check_historical_low(&mut self, timestamp: i64) {
        if self.current_balance <= self.balance_low * 1.01 && self.balance_low > 0.0 {
            if self.should_fire(BD_INSURANCE_HISTORICAL_LOW, timestamp) {
                self.publish_event(
                    BD_INSURANCE_HISTORICAL_LOW,
                    timestamp,
                    json!({
                        "current_balance": self.current_balance,
                        "historical_low": self.balance_low,
                        "drawdown_pct": self.current_drawdown_pct,
                        "risk_level": "elevated",
                    }),
                );
            }
        }
    }

    /// Event 4: Historical high
    fn check_historical_high(&mut self, timestamp: i64) {
        if self.current_balance >= self.balance_high * 0.99 {
            if self.should_fire(BD_INSURANCE_HISTORICAL_HIGH, timestamp) {
                self.publish_event(
                    BD_INSURANCE_HISTORICAL_HIGH,
                    timestamp,
                    json!({
                        "current_balance": self.current_balance,
                        "historical_high": self.balance_high,
                        "risk_level": "low",
                    }),
                );
            }
        }
    }

    /// Event 5: Liquidation correlation
    fn check_liquidation_correlation(&mut self, timestamp: i64) {
        if self.liquidation_to_fund_ratio > 0.01 {  // Liquidations > 1% of fund
            if self.should_fire(BD_INSURANCE_LIQUIDATION_CORRELATION, timestamp) {
                self.publish_event(
                    BD_INSURANCE_LIQUIDATION_CORRELATION,
                    timestamp,
                    json!({
                        "liquidation_usd": self.recent_liquidations_usd,
                        "fund_balance": self.current_balance,
                        "ratio": self.liquidation_to_fund_ratio,
                        "impact": if self.liquidation_to_fund_ratio > 0.05 { "significant" } else { "moderate" },
                    }),
                );
            }
        }
    }

    /// Event 6: Systemic risk indicator
    fn check_systemic_risk(&mut self, timestamp: i64) {
        if matches!(self.current_regime, InsuranceRegime::Critical | InsuranceRegime::Stressed) {
            if self.should_fire(BD_INSURANCE_SYSTEMIC_RISK, timestamp) {
                self.publish_event(
                    BD_INSURANCE_SYSTEMIC_RISK,
                    timestamp,
                    json!({
                        "regime": format!("{:?}", self.current_regime),
                        "drawdown_pct": self.current_drawdown_pct,
                        "weekly_change": self.weekly_change,
                        "risk_level": if matches!(self.current_regime, InsuranceRegime::Critical) { "high" } else { "elevated" },
                    }),
                );
            }
        }
    }

    /// Event 7: ADL indicator
    fn check_adl_indicator(&mut self, timestamp: i64) {
        // Low insurance fund increases ADL probability
        if self.current_drawdown_pct > 20.0 || self.liquidation_to_fund_ratio > 0.05 {
            if self.should_fire(BD_INSURANCE_ADL_INDICATOR, timestamp) {
                self.publish_event(
                    BD_INSURANCE_ADL_INDICATOR,
                    timestamp,
                    json!({
                        "drawdown_pct": self.current_drawdown_pct,
                        "liquidation_fund_ratio": self.liquidation_to_fund_ratio,
                        "adl_probability": "elevated",
                    }),
                );
            }
        }
    }

    /// Event 8: Drawdown extreme
    fn check_drawdown_extreme(&mut self, timestamp: i64) {
        if self.current_drawdown_pct > 25.0 {
            if self.should_fire(BD_INSURANCE_DRAWDOWN_EXTREME, timestamp) {
                self.publish_event(
                    BD_INSURANCE_DRAWDOWN_EXTREME,
                    timestamp,
                    json!({
                        "current_drawdown_pct": self.current_drawdown_pct,
                        "max_drawdown_pct": self.max_drawdown_pct,
                        "balance_high": self.balance_high,
                        "current_balance": self.current_balance,
                    }),
                );
            }
        }
    }

    /// Event 9: Recovery
    fn check_recovery(&mut self, timestamp: i64) {
        if matches!(self.current_regime, InsuranceRegime::Recovering) {
            if self.should_fire(BD_INSURANCE_RECOVERY, timestamp) {
                self.publish_event(
                    BD_INSURANCE_RECOVERY,
                    timestamp,
                    json!({
                        "recovery_since_low_pct": self.recovery_since_low,
                        "current_balance": self.current_balance,
                        "balance_low": self.balance_low,
                    }),
                );
            }
        }
    }

    /// Event 10: Stress test
    fn check_stress_test(&mut self, timestamp: i64) {
        // Simulate stress scenarios
        let stress_scenario_liquidation = self.current_balance * 0.3;  // 30% stress test
        let can_handle_stress = self.current_balance > stress_scenario_liquidation;

        if !can_handle_stress || self.current_drawdown_pct > 30.0 {
            if self.should_fire(BD_INSURANCE_STRESS_TEST, timestamp) {
                self.publish_event(
                    BD_INSURANCE_STRESS_TEST,
                    timestamp,
                    json!({
                        "current_balance": self.current_balance,
                        "stress_capacity": stress_scenario_liquidation,
                        "passed": can_handle_stress,
                        "drawdown_pct": self.current_drawdown_pct,
                    }),
                );
            }
        }
    }

    /// Event 11: Volatility correlation
    fn check_volatility_correlation(&mut self, timestamp: i64) {
        // High volatility + low fund = dangerous combination
        if self.balance_std > self.balance_avg * 0.2 && self.current_drawdown_pct > 10.0 {
            if self.should_fire(BD_INSURANCE_VOLATILITY_CORRELATION, timestamp) {
                self.publish_event(
                    BD_INSURANCE_VOLATILITY_CORRELATION,
                    timestamp,
                    json!({
                        "balance_volatility": self.balance_std / self.balance_avg,
                        "drawdown_pct": self.current_drawdown_pct,
                        "correlation": "high_volatility_stressed_fund",
                    }),
                );
            }
        }
    }

    /// Event 12: Market regime indicator
    fn check_market_regime(&mut self, timestamp: i64) {
        // Insurance fund changes indicate market stress
        if self.weekly_change < -10.0 {
            if self.should_fire(BD_INSURANCE_MARKET_REGIME, timestamp) {
                self.publish_event(
                    BD_INSURANCE_MARKET_REGIME,
                    timestamp,
                    json!({
                        "weekly_change": self.weekly_change,
                        "regime": "stressed_market_conditions",
                        "significance": "elevated_liquidation_activity",
                    }),
                );
            }
        }
    }

    /// Event 13: Predictive model
    fn check_predictive_model(&mut self, timestamp: i64) {
        // Simple trend extrapolation
        if self.history.len() >= 10 {
            let recent_trend: f64 = self.history.iter().rev().take(5)
                .map(|h| h.balance)
                .sum::<f64>() / 5.0;
            let older_trend: f64 = self.history.iter().take(5)
                .map(|h| h.balance)
                .sum::<f64>() / 5.0;

            let trend_velocity = recent_trend - older_trend;

            if trend_velocity < -self.balance_avg * 0.05 {
                if self.should_fire(BD_INSURANCE_PREDICTIVE_MODEL, timestamp) {
                    self.publish_event(
                        BD_INSURANCE_PREDICTIVE_MODEL,
                        timestamp,
                        json!({
                            "trend_direction": "declining",
                            "trend_velocity": trend_velocity,
                            "predicted_drawdown_7d": (self.current_balance + trend_velocity) / self.balance_high * 100.0,
                        }),
                    );
                }
            }
        }
    }

    /// Event 14: Risk adjustment
    fn check_risk_adjustment(&mut self, timestamp: i64) {
        // Suggest risk adjustments based on fund health
        if matches!(self.current_regime, InsuranceRegime::Critical | InsuranceRegime::Stressed) {
            if self.should_fire(BD_INSURANCE_RISK_ADJUSTMENT, timestamp) {
                self.publish_event(
                    BD_INSURANCE_RISK_ADJUSTMENT,
                    timestamp,
                    json!({
                        "regime": format!("{:?}", self.current_regime),
                        "recommendation": "reduce_leverage",
                        "current_drawdown_pct": self.current_drawdown_pct,
                    }),
                );
            }
        }
    }

    /// Event 15: Exchange health indicator
    fn check_exchange_health(&mut self, timestamp: i64) {
        // Overall exchange health score
        let health_score = 100.0 - self.current_drawdown_pct.min(100.0);

        if self.should_fire(BD_INSURANCE_EXCHANGE_HEALTH, timestamp) {
            self.publish_event(
                BD_INSURANCE_EXCHANGE_HEALTH,
                timestamp,
                json!({
                    "health_score": health_score,
                    "regime": format!("{:?}", self.current_regime),
                    "fund_balance": self.current_balance,
                    "weekly_change": self.weekly_change,
                }),
            );
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
            "Insurance Fund event published"
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

    /// Get current balance
    pub fn current_balance(&self) -> f64 {
        self.current_balance
    }
}
