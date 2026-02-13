// Cascade Tracker - Monitors liquidation cascades (chain reaction liquidations)
// Generates 10 liquidation cascade-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer2::parser::ParsedLiquidation;
use crate::layer3::common::event_types::*;
use crate::layer3::common::time_windows::TimeWindow;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Cascade phase
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CascadePhase {
    Dormant,
    Potential,       // Conditions forming for cascade
    Initiating,      // Cascade starting
    Accelerating,    // Cascade accelerating
    Peak,            // Maximum intensity
    Decelerating,    // Slowing down
    Exhausted,       // Cascade exhausted
}

/// CascadeTracker detects chain reaction liquidations with price feedback loops
pub struct CascadeTracker {
    symbol: String,

    // Rolling windows
    liquidations_30s: TimeWindow<ParsedLiquidation>,
    liquidations_2m: TimeWindow<ParsedLiquidation>,

    // Cascade metrics
    cascade_phase: CascadePhase,
    cascade_start_time: i64,
    cascade_peak_time: i64,

    // Acceleration tracking
    velocity: f64,           // Rate of change in liquidation rate
    acceleration: f64,       // Second derivative
    prev_velocity: f64,

    // Cumulative metrics
    total_volume: f64,
    total_count: u64,

    // Price tracking
    start_price: f64,
    current_price: f64,
    price_movement_pct: f64,
    prev_price: f64,

    // Phase tracking
    potential_score: f64,    // 0-100 score for cascade potential
    peak_volume_rate: f64,
    deceleration_count: u64,

    // Historical
    cascade_count: u64,
    avg_cascade_volume: f64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    liquidations_processed: u64,
    events_fired: u64,
}

impl CascadeTracker {
    /// Create a new CascadeTracker
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            liquidations_30s: TimeWindow::new(30_000, 500),
            liquidations_2m: TimeWindow::new(120_000, 2_000),
            cascade_phase: CascadePhase::Dormant,
            cascade_start_time: 0,
            cascade_peak_time: 0,
            velocity: 0.0,
            acceleration: 0.0,
            prev_velocity: 0.0,
            total_volume: 0.0,
            total_count: 0,
            start_price: 0.0,
            current_price: 0.0,
            price_movement_pct: 0.0,
            prev_price: 0.0,
            potential_score: 0.0,
            peak_volume_rate: 0.0,
            deceleration_count: 0,
            cascade_count: 0,
            avg_cascade_volume: 50_000.0,
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
        self.prev_price = self.current_price;
        self.current_price = liq.average_price;

        // Add to windows
        self.liquidations_30s.add(current_time, liq.clone());
        self.liquidations_2m.add(current_time, liq.clone());

        self.liquidations_30s.prune(current_time);
        self.liquidations_2m.prune(current_time);

        // Update metrics
        self.total_volume += notional;
        self.total_count += 1;

        // Calculate price movement
        if self.start_price > 0.0 {
            self.price_movement_pct = ((self.current_price - self.start_price) / self.start_price) * 100.0;
        }

        // Update cascade phase based on metrics
        self.update_cascade_phase(current_time);
    }

    /// Check all cascade events
    pub fn check_events(&mut self, current_time: i64) {
        self.calculate_derivatives();

        // Event 1: Cascade potential
        self.check_liq_cascade_potential(current_time);

        // Event 2: Cascade acceleration
        self.check_liq_cascade_acceleration(current_time);

        // Event 3: Cascade deceleration
        self.check_liq_cascade_deceleration(current_time);

        // Event 4: Cascade reversal
        self.check_liq_cascade_reversal(current_time);

        // Event 5: Cascade exhaustion
        self.check_liq_cascade_exhaustion(current_time);

        // Event 6: Cascade completion
        self.check_liq_cascade_completion(current_time);

        // Event 7: Cascade continuation
        self.check_liq_cascade_continuation(current_time);

        // Event 8: Cascade absorption
        self.check_liq_cascade_absorption(current_time);

        // Event 9: Cascade imminent warning
        self.check_liq_cascade_imminent_warning(current_time);

        // Event 10: Cascade spiral
        self.check_liq_cascade_spiral(current_time);
    }

    /// Calculate velocity and acceleration of liquidation rate
    fn calculate_derivatives(&mut self) {
        let count_30s = self.liquidations_30s.len() as f64;
        let rate_30s = count_30s / 30.0;  // Liquidations per second

        self.prev_velocity = self.velocity;
        self.velocity = rate_30s;
        self.acceleration = self.velocity - self.prev_velocity;

        if rate_30s > self.peak_volume_rate {
            self.peak_volume_rate = rate_30s;
        }
    }

    /// Update cascade phase based on metrics
    fn update_cascade_phase(&mut self, current_time: i64) {
        let count_30s = self.liquidations_30s.len() as u64;
        let count_2m = self.liquidations_2m.len() as u64;

        // Calculate potential score based on conditions
        let mut score = 0.0;

        // High volume in short window
        if count_30s >= 5 { score += 20.0; }
        if count_30s >= 10 { score += 15.0; }

        // Price movement
        if self.price_movement_pct.abs() > 0.3 { score += 15.0; }
        if self.price_movement_pct.abs() > 0.5 { score += 10.0; }

        // Acceleration
        if self.acceleration > 0.1 { score += 20.0; }
        if self.acceleration > 0.3 { score += 15.0; }

        // Persistent liquidations
        if count_2m >= 10 { score += 10.0; }

        // Volume threshold
        if self.total_volume > self.thresholds.cascade_threshold_usd { score += 10.0; }

        self.potential_score = score.min(100.0);

        // Update phase
        let prev_phase = self.cascade_phase;

        match self.cascade_phase {
            CascadePhase::Dormant => {
                if score >= 40.0 {
                    self.cascade_phase = CascadePhase::Potential;
                }
            }
            CascadePhase::Potential => {
                if score >= 60.0 && self.acceleration > 0.1 {
                    self.cascade_phase = CascadePhase::Initiating;
                    self.cascade_start_time = current_time;
                    self.start_price = self.current_price;
                } else if score < 30.0 {
                    self.cascade_phase = CascadePhase::Dormant;
                }
            }
            CascadePhase::Initiating => {
                if self.acceleration > 0.2 {
                    self.cascade_phase = CascadePhase::Accelerating;
                    self.cascade_count += 1;
                } else if self.acceleration < 0.0 {
                    self.cascade_phase = CascadePhase::Potential;
                }
            }
            CascadePhase::Accelerating => {
                if self.acceleration <= 0.0 {
                    self.cascade_phase = CascadePhase::Peak;
                    self.cascade_peak_time = current_time;
                    self.avg_cascade_volume = (self.avg_cascade_volume + self.total_volume) / 2.0;
                }
            }
            CascadePhase::Peak => {
                if self.acceleration < -0.1 {
                    self.cascade_phase = CascadePhase::Decelerating;
                    self.deceleration_count += 1;
                }
            }
            CascadePhase::Decelerating => {
                if self.velocity < 0.1 {
                    self.cascade_phase = CascadePhase::Exhausted;
                } else if self.acceleration > 0.1 {
                    self.cascade_phase = CascadePhase::Accelerating;
                }
            }
            CascadePhase::Exhausted => {
                if self.velocity < 0.05 {
                    self.cascade_phase = CascadePhase::Dormant;
                    self.reset_cascade_metrics();
                }
            }
        }

        let _ = prev_phase; // Used for logging if needed
    }

    /// Reset cascade metrics after completion
    fn reset_cascade_metrics(&mut self) {
        self.total_volume = 0.0;
        self.total_count = 0;
        self.start_price = self.current_price;
        self.velocity = 0.0;
        self.acceleration = 0.0;
        self.peak_volume_rate = 0.0;
        self.deceleration_count = 0;
    }

    /// Event 1: Cascade potential detected
    fn check_liq_cascade_potential(&mut self, timestamp: i64) {
        if self.cascade_phase == CascadePhase::Potential && self.potential_score >= 50.0 {
            if self.should_fire(LIQ_CASCADE_POTENTIAL, timestamp) {
                self.publish_event(
                    LIQ_CASCADE_POTENTIAL,
                    timestamp,
                    json!({
                        "potential_score": self.potential_score,
                        "liquidation_count_30s": self.liquidations_30s.len(),
                        "price_movement_pct": self.price_movement_pct,
                        "velocity": self.velocity,
                    }),
                );
            }
        }
    }

    /// Event 2: Cascade accelerating
    fn check_liq_cascade_acceleration(&mut self, timestamp: i64) {
        if self.cascade_phase == CascadePhase::Accelerating {
            if self.should_fire(LIQ_CASCADE_ACCELERATION, timestamp) {
                self.publish_event(
                    LIQ_CASCADE_ACCELERATION,
                    timestamp,
                    json!({
                        "acceleration": self.acceleration,
                        "velocity": self.velocity,
                        "price_movement_pct": self.price_movement_pct,
                        "total_volume": self.total_volume,
                        "liquidation_count": self.total_count,
                    }),
                );
            }
        }
    }

    /// Event 3: Cascade decelerating
    fn check_liq_cascade_deceleration(&mut self, timestamp: i64) {
        if self.cascade_phase == CascadePhase::Decelerating {
            if self.should_fire(LIQ_CASCADE_DECELERATION, timestamp) {
                self.publish_event(
                    LIQ_CASCADE_DECELERATION,
                    timestamp,
                    json!({
                        "deceleration": -self.acceleration,
                        "velocity": self.velocity,
                        "peak_velocity": self.peak_volume_rate,
                        "deceleration_count": self.deceleration_count,
                    }),
                );
            }
        }
    }

    /// Event 4: Cascade reversal (price reversal during cascade)
    fn check_liq_cascade_reversal(&mut self, timestamp: i64) {
        if self.cascade_phase == CascadePhase::Accelerating ||
           self.cascade_phase == CascadePhase::Peak {
            // Check if price reversed direction
            let price_reversed = (self.current_price - self.prev_price).signum()
                != self.price_movement_pct.signum() && self.price_movement_pct.abs() > 0.2;

            if price_reversed {
                if self.should_fire(LIQ_CASCADE_REVERSAL, timestamp) {
                    self.publish_event(
                        LIQ_CASCADE_REVERSAL,
                        timestamp,
                        json!({
                            "price_before": self.prev_price,
                            "price_current": self.current_price,
                            "cascade_price_movement": self.price_movement_pct,
                        }),
                    );
                }
            }
        }
    }

    /// Event 5: Cascade exhaustion
    fn check_liq_cascade_exhaustion(&mut self, timestamp: i64) {
        if self.cascade_phase == CascadePhase::Exhausted {
            if self.should_fire(LIQ_CASCADE_EXHAUSTION, timestamp) {
                self.publish_event(
                    LIQ_CASCADE_EXHAUSTION,
                    timestamp,
                    json!({
                        "total_volume": self.total_volume,
                        "total_count": self.total_count,
                        "price_movement_pct": self.price_movement_pct,
                        "duration_ms": timestamp - self.cascade_start_time,
                    }),
                );
            }
        }
    }

    /// Event 6: Cascade completion
    fn check_liq_cascade_completion(&mut self, timestamp: i64) {
        // Cascade completed when transitioning from Exhausted to Dormant
        if self.cascade_phase == CascadePhase::Dormant && self.cascade_start_time > 0 {
            let cascade_ended = timestamp - self.cascade_start_time < 5000; // Recently ended

            if cascade_ended {
                if self.should_fire(LIQ_CASCADE_COMPLETION, timestamp) {
                    self.publish_event(
                        LIQ_CASCADE_COMPLETION,
                        timestamp,
                        json!({
                            "cascade_count": self.cascade_count,
                            "final_volume": self.total_volume,
                            "final_price_movement": self.price_movement_pct,
                        }),
                    );
                }
            }
        }
    }

    /// Event 7: Cascade continuation (new acceleration after deceleration)
    fn check_liq_cascade_continuation(&mut self, timestamp: i64) {
        let was_decelerating = self.deceleration_count > 0;
        let now_accelerating = self.cascade_phase == CascadePhase::Accelerating;

        if was_decelerating && now_accelerating {
            if self.should_fire(LIQ_CASCADE_CONTINUATION, timestamp) {
                self.publish_event(
                    LIQ_CASCADE_CONTINUATION,
                    timestamp,
                    json!({
                        "acceleration": self.acceleration,
                        "velocity": self.velocity,
                        "total_volume": self.total_volume,
                        "deceleration_count": self.deceleration_count,
                    }),
                );
            }
        }
    }

    /// Event 8: Cascade absorption (high volume but price stabilizing)
    fn check_liq_cascade_absorption(&mut self, timestamp: i64) {
        let has_high_volume = self.total_volume > self.thresholds.cascade_threshold_usd;
        let price_stabilizing = (self.current_price - self.prev_price).abs() / self.current_price < 0.001;
        let in_cascade = matches!(self.cascade_phase,
            CascadePhase::Accelerating | CascadePhase::Peak | CascadePhase::Decelerating);

        if has_high_volume && price_stabilizing && in_cascade {
            if self.should_fire(LIQ_CASCADE_ABSORPTION, timestamp) {
                self.publish_event(
                    LIQ_CASCADE_ABSORPTION,
                    timestamp,
                    json!({
                        "total_volume": self.total_volume,
                        "current_price": self.current_price,
                        "velocity": self.velocity,
                    }),
                );
            }
        }
    }

    /// Event 9: Cascade imminent warning (all conditions met)
    fn check_liq_cascade_imminent_warning(&mut self, timestamp: i64) {
        if self.cascade_phase == CascadePhase::Initiating {
            if self.should_fire(LIQ_CASCADE_IMMINENT_WARNING, timestamp) {
                self.publish_event(
                    LIQ_CASCADE_IMMINENT_WARNING,
                    timestamp,
                    json!({
                        "potential_score": self.potential_score,
                        "velocity": self.velocity,
                        "acceleration": self.acceleration,
                        "price_movement_pct": self.price_movement_pct,
                        "liquidation_count": self.total_count,
                    }),
                );
            }
        }
    }

    /// Event 10: Cascade spiral (self-reinforcing feedback loop)
    fn check_liq_cascade_spiral(&mut self, timestamp: i64) {
        // Spiral = high acceleration + high price impact + increasing rate
        let is_spiral = self.cascade_phase == CascadePhase::Accelerating
            && self.acceleration > 0.3
            && self.price_movement_pct.abs() > 0.5
            && self.velocity > 1.0;

        if is_spiral {
            if self.should_fire(LIQ_CASCADE_SPIRAL, timestamp) {
                self.publish_event(
                    LIQ_CASCADE_SPIRAL,
                    timestamp,
                    json!({
                        "acceleration": self.acceleration,
                        "velocity": self.velocity,
                        "price_movement_pct": self.price_movement_pct,
                        "total_volume": self.total_volume,
                        "direction": if self.price_movement_pct > 0.0 { "UP" } else { "DOWN" },
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
            "Cascade liquidation event published"
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

    /// Get cascade phase
    #[allow(dead_code)]
    pub fn get_cascade_phase(&self) -> CascadePhase {
        self.cascade_phase
    }

    /// Get potential score
    #[allow(dead_code)]
    pub fn get_potential_score(&self) -> f64 {
        self.potential_score
    }
}
