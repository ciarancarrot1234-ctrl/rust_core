// Impact Tracker - Monitors liquidation market impact and systemic effects
// Generates 8 liquidation impact-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer2::parser::ParsedLiquidation;
use crate::layer3::common::event_types::*;
use crate::layer3::common::time_windows::TimeWindow;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Impact level classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImpactLevel {
    Low,        // < $10k per minute
    Moderate,   // $10k - $50k per minute
    High,       // $50k - $100k per minute
    Extreme,    // > $100k per minute
    Critical,   // Systemic risk level
}

/// ImpactTracker monitors the market impact of liquidations
pub struct ImpactTracker {
    symbol: String,

    // Rolling windows
    liquidations_1m: TimeWindow<ParsedLiquidation>,
    liquidations_5m: TimeWindow<ParsedLiquidation>,

    // Volume metrics
    volume_1m: f64,
    volume_5m: f64,
    volume_velocity: f64,      // Change in volume rate
    prev_volume_1m: f64,

    // Price impact metrics
    price_before_impact: f64,
    price_after_impact: f64,
    price_impact_pct: f64,
    price_impact_velocity: f64,

    // Impact level tracking
    current_impact_level: ImpactLevel,
    prev_impact_level: ImpactLevel,
    high_impact_duration_ms: i64,
    high_impact_start_time: i64,

    // Absorption metrics
    absorption_ratio: f64,     // Volume absorbed vs price impact
    absorption_detected: bool,

    // Feedback loop detection
    feedback_loop_active: bool,
    feedback_strength: f64,    // 0-100
    feedback_iterations: u64,

    // Position unwinding tracking
    unwinding_volume: f64,
    unwinding_count: u64,
    unwinding_rate: f64,       // Volume per second

    // Margin call cascade detection
    margin_call_score: f64,    // 0-100
    margin_call_threshold: f64,

    // Systemic risk
    systemic_risk_score: f64,  // 0-100
    systemic_indicators: HashMap<String, f64>,

    // Impact exhaustion
    impact_exhaustion_score: f64,
    rebound_detected: bool,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    liquidations_processed: u64,
    events_fired: u64,
}

impl ImpactTracker {
    /// Create a new ImpactTracker
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            liquidations_1m: TimeWindow::new(60_000, 1_000),
            liquidations_5m: TimeWindow::new(300_000, 5_000),
            volume_1m: 0.0,
            volume_5m: 0.0,
            volume_velocity: 0.0,
            prev_volume_1m: 0.0,
            price_before_impact: 0.0,
            price_after_impact: 0.0,
            price_impact_pct: 0.0,
            price_impact_velocity: 0.0,
            current_impact_level: ImpactLevel::Low,
            prev_impact_level: ImpactLevel::Low,
            high_impact_duration_ms: 0,
            high_impact_start_time: 0,
            absorption_ratio: 0.0,
            absorption_detected: false,
            feedback_loop_active: false,
            feedback_strength: 0.0,
            feedback_iterations: 0,
            unwinding_volume: 0.0,
            unwinding_count: 0,
            unwinding_rate: 0.0,
            margin_call_score: 0.0,
            margin_call_threshold: 70.0,
            systemic_risk_score: 0.0,
            systemic_indicators: HashMap::new(),
            impact_exhaustion_score: 0.0,
            rebound_detected: false,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 1000,
            thresholds,
            liquidations_processed: 0,
            events_fired: 0,
        }
    }

    /// Add a liquidation
    pub fn add_liquidation(&mut self, liq: &ParsedLiquidation, current_time: i64) {
        self.liquidations_processed += 1;

        let notional = liq.notional_usd();

        // Track price changes
        self.price_before_impact = self.price_after_impact;
        self.price_after_impact = liq.average_price;

        // Add to windows
        self.liquidations_1m.add(current_time, liq.clone());
        self.liquidations_5m.add(current_time, liq.clone());

        self.liquidations_1m.prune(current_time);
        self.liquidations_5m.prune(current_time);

        // Update unwinding metrics
        self.unwinding_volume += notional;
        self.unwinding_count += 1;
    }

    /// Check all impact events
    pub fn check_events(&mut self, current_time: i64) {
        self.recalculate_metrics();
        self.update_impact_level();
        self.update_feedback_loop();
        self.update_systemic_risk();

        // Event 1: High impact
        self.check_liq_high_impact(current_time);

        // Event 2: Impact absorption
        self.check_liq_impact_absorption(current_time);

        // Event 3: Impact exhaustion
        self.check_liq_impact_exhaustion(current_time);

        // Event 4: Impact rebound
        self.check_liq_impact_rebound(current_time);

        // Event 5: Feedback loop
        self.check_liq_feedback_loop(current_time);

        // Event 6: Position unwinding
        self.check_liq_position_unwinding(current_time);

        // Event 7: Margin call cascade
        self.check_liq_margin_call_cascade(current_time);

        // Event 8: Systemic risk
        self.check_liq_systemic_risk(current_time);
    }

    /// Recalculate all metrics
    fn recalculate_metrics(&mut self) {
        // Store previous values
        self.prev_volume_1m = self.volume_1m;

        // Calculate volumes
        self.volume_1m = self.calculate_window_volume(&self.liquidations_1m);
        self.volume_5m = self.calculate_window_volume(&self.liquidations_5m);

        // Calculate velocity
        self.volume_velocity = self.volume_1m - self.prev_volume_1m;

        // Calculate unwinding rate
        let window_duration = 60.0;  // seconds
        self.unwinding_rate = self.unwinding_volume / window_duration;

        // Calculate price impact
        if self.price_before_impact > 0.0 && self.price_after_impact > 0.0 {
            let new_impact = ((self.price_after_impact - self.price_before_impact) /
                self.price_before_impact) * 100.0;
            self.price_impact_velocity = new_impact - self.price_impact_pct;
            self.price_impact_pct = new_impact;
        }

        // Calculate absorption ratio
        if self.price_impact_pct.abs() > 0.01 {
            self.absorption_ratio = self.volume_1m / self.price_impact_pct.abs();
        }

        // Check for absorption (high volume, low price impact)
        self.absorption_detected = self.volume_1m > self.thresholds.cascade_threshold_usd
            && self.price_impact_pct.abs() < 0.1;

        // Calculate exhaustion score
        let volume_declining = self.volume_velocity < 0.0;
        let impact_stabilizing = self.price_impact_velocity.abs() < 0.01;
        let high_volume_history = self.volume_5m > self.thresholds.cascade_threshold_usd * 2.0;

        self.impact_exhaustion_score = if volume_declining && impact_stabilizing && high_volume_history {
            80.0
        } else if volume_declining && high_volume_history {
            60.0
        } else {
            0.0
        };
    }

    /// Calculate volume from window
    fn calculate_window_volume(&self, window: &TimeWindow<ParsedLiquidation>) -> f64 {
        let mut total = 0.0;
        for (_, liq) in window.iter() {
            total += liq.notional_usd();
        }
        total
    }

    /// Update impact level based on volume
    fn update_impact_level(&mut self) {
        self.prev_impact_level = self.current_impact_level;

        self.current_impact_level = if self.volume_1m > 100_000.0 {
            ImpactLevel::Critical
        } else if self.volume_1m > 50_000.0 {
            ImpactLevel::Extreme
        } else if self.volume_1m > 10_000.0 {
            ImpactLevel::High
        } else if self.volume_1m > 5_000.0 {
            ImpactLevel::Moderate
        } else {
            ImpactLevel::Low
        };
    }

    /// Update feedback loop detection
    fn update_feedback_loop(&mut self) {
        // Feedback loop = accelerating liquidations + accelerating price impact
        let volume_accelerating = self.volume_velocity > 1_000.0;
        let price_accelerating = self.price_impact_velocity.abs() > 0.05;

        if volume_accelerating && price_accelerating {
            if !self.feedback_loop_active {
                self.feedback_loop_active = true;
                self.feedback_iterations = 1;
            } else {
                self.feedback_iterations += 1;
            }
            self.feedback_strength = (50.0 + self.feedback_iterations as f64 * 10.0).min(100.0);
        } else {
            self.feedback_strength *= 0.8;  // Decay
            if self.feedback_strength < 20.0 {
                self.feedback_loop_active = false;
                self.feedback_iterations = 0;
            }
        }
    }

    /// Update systemic risk assessment
    fn update_systemic_risk(&mut self) {
        let mut risk_score = 0.0;

        // High volume liquidations
        if self.volume_1m > self.thresholds.cascade_threshold_usd {
            risk_score += 20.0;
            self.systemic_indicators.insert("high_volume".to_string(), 20.0);
        }

        // Rapid price movement
        if self.price_impact_pct.abs() > 1.0 {
            risk_score += 25.0;
            self.systemic_indicators.insert("price_movement".to_string(), 25.0);
        }

        // Feedback loop active
        if self.feedback_loop_active {
            risk_score += 20.0;
            self.systemic_indicators.insert("feedback_loop".to_string(), 20.0);
        }

        // Impact level
        if self.current_impact_level == ImpactLevel::Critical {
            risk_score += 25.0;
            self.systemic_indicators.insert("critical_impact".to_string(), 25.0);
        } else if self.current_impact_level == ImpactLevel::Extreme {
            risk_score += 15.0;
            self.systemic_indicators.insert("extreme_impact".to_string(), 15.0);
        }

        // Sustained high impact
        if self.high_impact_duration_ms > 120_000 {  // 2 minutes
            risk_score += 10.0;
            self.systemic_indicators.insert("sustained_impact".to_string(), 10.0);
        }

        self.systemic_risk_score = risk_score.min(100.0);

        // Update margin call score
        self.margin_call_score = (self.volume_1m / 50_000.0 * 30.0
            + self.price_impact_pct.abs() * 3.0
            + self.feedback_strength * 0.4).min(100.0);
    }

    /// Event 1: High impact detected
    fn check_liq_high_impact(&mut self, timestamp: i64) {
        if self.current_impact_level == ImpactLevel::High ||
           self.current_impact_level == ImpactLevel::Extreme ||
           self.current_impact_level == ImpactLevel::Critical {

            if self.high_impact_start_time == 0 {
                self.high_impact_start_time = timestamp;
            }
            self.high_impact_duration_ms = timestamp - self.high_impact_start_time;

            if self.should_fire(LIQ_HIGH_IMPACT, timestamp) {
                self.publish_event(
                    LIQ_HIGH_IMPACT,
                    timestamp,
                    json!({
                        "impact_level": format!("{:?}", self.current_impact_level),
                        "volume_1m": self.volume_1m,
                        "price_impact_pct": self.price_impact_pct,
                        "duration_ms": self.high_impact_duration_ms,
                        "volume_velocity": self.volume_velocity,
                    }),
                );
            }
        } else {
            self.high_impact_start_time = 0;
            self.high_impact_duration_ms = 0;
        }
    }

    /// Event 2: Impact absorption (volume absorbed without price impact)
    fn check_liq_impact_absorption(&mut self, timestamp: i64) {
        if self.absorption_detected {
            if self.should_fire(LIQ_IMPACT_ABSORPTION, timestamp) {
                self.publish_event(
                    LIQ_IMPACT_ABSORPTION,
                    timestamp,
                    json!({
                        "volume_1m": self.volume_1m,
                        "price_impact_pct": self.price_impact_pct,
                        "absorption_ratio": self.absorption_ratio,
                    }),
                );
            }
        }
    }

    /// Event 3: Impact exhaustion
    fn check_liq_impact_exhaustion(&mut self, timestamp: i64) {
        if self.impact_exhaustion_score > 60.0 {
            if self.should_fire(LIQ_IMPACT_EXHAUSTION, timestamp) {
                self.publish_event(
                    LIQ_IMPACT_EXHAUSTION,
                    timestamp,
                    json!({
                        "exhaustion_score": self.impact_exhaustion_score,
                        "volume_velocity": self.volume_velocity,
                        "price_impact_velocity": self.price_impact_velocity,
                        "volume_5m": self.volume_5m,
                    }),
                );
            }
        }
    }

    /// Event 4: Impact rebound (price reversal after impact)
    fn check_liq_impact_rebound(&mut self, timestamp: i64) {
        // Rebound = opposite price movement after liquidation impact
        let had_impact = self.price_impact_pct.abs() > 0.3;
        let is_reversing = self.price_impact_velocity.signum() != self.price_impact_pct.signum()
            && self.price_impact_velocity.abs() > 0.02;

        if had_impact && is_reversing {
            self.rebound_detected = true;
            if self.should_fire(LIQ_IMPACT_REBOUND, timestamp) {
                self.publish_event(
                    LIQ_IMPACT_REBOUND,
                    timestamp,
                    json!({
                        "original_impact": self.price_impact_pct,
                        "rebound_velocity": self.price_impact_velocity,
                        "volume_1m": self.volume_1m,
                    }),
                );
            }
        } else {
            self.rebound_detected = false;
        }
    }

    /// Event 5: Feedback loop active
    fn check_liq_feedback_loop(&mut self, timestamp: i64) {
        if self.feedback_loop_active && self.feedback_strength > 50.0 {
            if self.should_fire(LIQ_FEEDBACK_LOOP, timestamp) {
                self.publish_event(
                    LIQ_FEEDBACK_LOOP,
                    timestamp,
                    json!({
                        "feedback_strength": self.feedback_strength,
                        "iterations": self.feedback_iterations,
                        "volume_velocity": self.volume_velocity,
                        "price_impact_velocity": self.price_impact_velocity,
                    }),
                );
            }
        }
    }

    /// Event 6: Position unwinding (sustained liquidations)
    fn check_liq_position_unwinding(&mut self, timestamp: i64) {
        let unwinding_threshold = self.thresholds.cascade_threshold_usd / 60.0;  // Per second
        if self.unwinding_rate > unwinding_threshold && self.unwinding_count > 5 {
            if self.should_fire(LIQ_POSITION_UNWINDING, timestamp) {
                self.publish_event(
                    LIQ_POSITION_UNWINDING,
                    timestamp,
                    json!({
                        "unwinding_volume": self.unwinding_volume,
                        "unwinding_count": self.unwinding_count,
                        "unwinding_rate": self.unwinding_rate,
                        "volume_1m": self.volume_1m,
                    }),
                );
            }
        }
    }

    /// Event 7: Margin call cascade conditions
    fn check_liq_margin_call_cascade(&mut self, timestamp: i64) {
        if self.margin_call_score > self.margin_call_threshold {
            if self.should_fire(LIQ_MARGIN_CALL_CASCADE, timestamp) {
                self.publish_event(
                    LIQ_MARGIN_CALL_CASCADE,
                    timestamp,
                    json!({
                        "margin_call_score": self.margin_call_score,
                        "volume_1m": self.volume_1m,
                        "price_impact_pct": self.price_impact_pct,
                        "feedback_strength": self.feedback_strength,
                    }),
                );
            }
        }
    }

    /// Event 8: Systemic risk detected
    fn check_liq_systemic_risk(&mut self, timestamp: i64) {
        if self.systemic_risk_score > 70.0 {
            if self.should_fire(LIQ_SYSTEMIC_RISK, timestamp) {
                self.publish_event(
                    LIQ_SYSTEMIC_RISK,
                    timestamp,
                    json!({
                        "systemic_risk_score": self.systemic_risk_score,
                        "indicators": serde_json::to_value(&self.systemic_indicators).unwrap_or_default(),
                        "impact_level": format!("{:?}", self.current_impact_level),
                        "volume_1m": self.volume_1m,
                        "price_impact_pct": self.price_impact_pct,
                    }),
                );
            }
        }
    }

    /// Check if event should be fired (debouncing)
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
            "liquidation_aggregator",
            EventPriority::High,
        );

        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(
            symbol = %self.symbol,
            event = event_type,
            "Impact liquidation event published"
        );
    }

    /// Get liquidations processed
    pub fn liquidations_processed(&self) -> u64 {
        self.liquidations_processed
    }

    /// Get events fired
    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    /// Get impact level
    #[allow(dead_code)]
    pub fn get_impact_level(&self) -> ImpactLevel {
        self.current_impact_level
    }

    /// Get systemic risk score
    #[allow(dead_code)]
    pub fn get_systemic_risk_score(&self) -> f64 {
        self.systemic_risk_score
    }
}
