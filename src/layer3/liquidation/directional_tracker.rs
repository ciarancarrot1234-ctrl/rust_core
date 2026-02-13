// Directional Tracker - Monitors liquidation direction and squeezes
// Generates 6 liquidation directional condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer2::parser::ParsedLiquidation;
use crate::layer3::common::event_types::*;
use crate::layer3::common::time_windows::TimeWindow;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Imbalance regime
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImbalanceRegime {
    Neutral,
    LongBiased,      // More long liquidations
    ShortBiased,     // More short liquidations
    ExtremeLong,     // Extreme long liquidation dominance
    ExtremeShort,    // Extreme short liquidation dominance
}

/// DirectionalTracker tracks liquidation direction imbalances and squeezes
pub struct DirectionalTracker {
    symbol: String,

    // Rolling windows
    liquidations_1m: TimeWindow<ParsedLiquidation>,
    liquidations_5m: TimeWindow<ParsedLiquidation>,

    // Side counts and volumes
    long_count_1m: u64,
    short_count_1m: u64,
    long_volume_1m: f64,
    short_volume_1m: f64,

    long_count_5m: u64,
    short_count_5m: u64,
    long_volume_5m: f64,
    short_volume_5m: f64,

    // Imbalance metrics
    imbalance_ratio: f64,          // -100 to 100
    prev_imbalance_ratio: f64,
    imbalance_velocity: f64,
    current_regime: ImbalanceRegime,
    prev_regime: ImbalanceRegime,

    // Squeeze detection
    squeeze_active: Option<String>,  // "LONG" or "SHORT"
    squeeze_start_time: i64,
    squeeze_volume: f64,
    squeeze_count: u64,
    squeeze_peak_volume: f64,

    // Price tracking for squeeze
    squeeze_start_price: f64,
    squeeze_current_price: f64,
    squeeze_price_impact: f64,

    // Flip tracking
    last_flip_time: i64,
    flips_count: u64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    liquidations_processed: u64,
    events_fired: u64,
}

impl DirectionalTracker {
    /// Create a new DirectionalTracker
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            liquidations_1m: TimeWindow::new(60_000, 1_000),
            liquidations_5m: TimeWindow::new(300_000, 5_000),
            long_count_1m: 0,
            short_count_1m: 0,
            long_volume_1m: 0.0,
            short_volume_1m: 0.0,
            long_count_5m: 0,
            short_count_5m: 0,
            long_volume_5m: 0.0,
            short_volume_5m: 0.0,
            imbalance_ratio: 0.0,
            prev_imbalance_ratio: 0.0,
            imbalance_velocity: 0.0,
            current_regime: ImbalanceRegime::Neutral,
            prev_regime: ImbalanceRegime::Neutral,
            squeeze_active: None,
            squeeze_start_time: 0,
            squeeze_volume: 0.0,
            squeeze_count: 0,
            squeeze_peak_volume: 0.0,
            squeeze_start_price: 0.0,
            squeeze_current_price: 0.0,
            squeeze_price_impact: 0.0,
            last_flip_time: 0,
            flips_count: 0,
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
        let is_long = liq.side == "BUY";

        // Update squeeze tracking
        self.squeeze_current_price = liq.average_price;

        // Add to windows
        self.liquidations_1m.add(current_time, liq.clone());
        self.liquidations_5m.add(current_time, liq.clone());

        self.liquidations_1m.prune(current_time);
        self.liquidations_5m.prune(current_time);

        // Update squeeze metrics if active
        if let Some(ref squeeze) = self.squeeze_active {
            let matches_squeeze = (squeeze == "LONG" && !is_long) || (squeeze == "SHORT" && is_long);
            if matches_squeeze {
                self.squeeze_volume += notional;
                self.squeeze_count += 1;
                if notional > self.squeeze_peak_volume {
                    self.squeeze_peak_volume = notional;
                }
                if self.squeeze_start_price > 0.0 {
                    self.squeeze_price_impact = ((self.squeeze_current_price - self.squeeze_start_price)
                        / self.squeeze_start_price) * 100.0;
                }
            }
        }
    }

    /// Check all directional events
    pub fn check_events(&mut self, current_time: i64) {
        self.recalculate_metrics();

        // Event 1: Side imbalance
        self.check_liq_side_imbalance(current_time);

        // Event 2: Side flipped
        self.check_liq_side_flipped(current_time);

        // Event 3: Balanced
        self.check_liq_balanced(current_time);

        // Event 4: Long squeeze
        self.check_liq_long_squeeze(current_time);

        // Event 5: Short squeeze
        self.check_liq_short_squeeze(current_time);

        // Event 6: Squeeze exhaustion
        self.check_liq_squeeze_exhaustion(current_time);
    }

    /// Recalculate all metrics
    fn recalculate_metrics(&mut self) {
        // Calculate 1m metrics
        let (longs, shorts, long_vol, short_vol) = self.calculate_side_metrics(&self.liquidations_1m);
        self.long_count_1m = longs;
        self.short_count_1m = shorts;
        self.long_volume_1m = long_vol;
        self.short_volume_1m = short_vol;

        // Calculate 5m metrics
        let (longs_5m, shorts_5m, long_vol_5m, short_vol_5m) = self.calculate_side_metrics(&self.liquidations_5m);
        self.long_count_5m = longs_5m;
        self.short_count_5m = shorts_5m;
        self.long_volume_5m = long_vol_5m;
        self.short_volume_5m = short_vol_5m;

        // Calculate imbalance ratio (-100 to 100)
        let total = self.long_count_1m + self.short_count_1m;
        self.prev_imbalance_ratio = self.imbalance_ratio;
        if total > 0 {
            // Positive = more long liquidations, Negative = more short liquidations
            self.imbalance_ratio = ((self.long_count_1m as f64 - self.short_count_1m as f64) / total as f64) * 100.0;
        } else {
            self.imbalance_ratio = 0.0;
        }

        // Calculate velocity
        self.imbalance_velocity = self.imbalance_ratio - self.prev_imbalance_ratio;

        // Update regime
        self.prev_regime = self.current_regime;
        self.current_regime = self.classify_regime(self.imbalance_ratio);
    }

    /// Calculate side metrics from window
    fn calculate_side_metrics(&self, window: &TimeWindow<ParsedLiquidation>) -> (u64, u64, f64, f64) {
        let mut longs = 0u64;
        let mut shorts = 0u64;
        let mut long_vol = 0.0;
        let mut short_vol = 0.0;

        for (_, liq) in window.iter() {
            if liq.side == "BUY" {
                longs += 1;
                long_vol += liq.notional_usd();
            } else {
                shorts += 1;
                short_vol += liq.notional_usd();
            }
        }

        (longs, shorts, long_vol, short_vol)
    }

    /// Classify imbalance into regime
    fn classify_regime(&self, ratio: f64) -> ImbalanceRegime {
        if ratio > 70.0 {
            ImbalanceRegime::ExtremeLong
        } else if ratio > 30.0 {
            ImbalanceRegime::LongBiased
        } else if ratio < -70.0 {
            ImbalanceRegime::ExtremeShort
        } else if ratio < -30.0 {
            ImbalanceRegime::ShortBiased
        } else {
            ImbalanceRegime::Neutral
        }
    }

    /// Event 1: Side imbalance detected
    fn check_liq_side_imbalance(&mut self, timestamp: i64) {
        let is_biased = matches!(self.current_regime,
            ImbalanceRegime::LongBiased | ImbalanceRegime::ShortBiased |
            ImbalanceRegime::ExtremeLong | ImbalanceRegime::ExtremeShort);

        if is_biased && (self.long_count_1m + self.short_count_1m) >= 3 {
            if self.should_fire(LIQ_SIDE_IMBALANCE, timestamp) {
                self.publish_event(
                    LIQ_SIDE_IMBALANCE,
                    timestamp,
                    json!({
                        "imbalance_ratio": self.imbalance_ratio,
                        "regime": format!("{:?}", self.current_regime),
                        "long_count": self.long_count_1m,
                        "short_count": self.short_count_1m,
                        "long_volume": self.long_volume_1m,
                        "short_volume": self.short_volume_1m,
                    }),
                );
            }
        }
    }

    /// Event 2: Side flipped
    fn check_liq_side_flipped(&mut self, timestamp: i64) {
        // Check for regime flip
        let flipped = match (self.prev_regime, self.current_regime) {
            (ImbalanceRegime::LongBiased | ImbalanceRegime::ExtremeLong,
             ImbalanceRegime::ShortBiased | ImbalanceRegime::ExtremeShort) => true,
            (ImbalanceRegime::ShortBiased | ImbalanceRegime::ExtremeShort,
             ImbalanceRegime::LongBiased | ImbalanceRegime::ExtremeLong) => true,
            _ => false,
        };

        if flipped {
            self.last_flip_time = timestamp;
            self.flips_count += 1;

            if self.should_fire(LIQ_SIDE_FLIPPED, timestamp) {
                self.publish_event(
                    LIQ_SIDE_FLIPPED,
                    timestamp,
                    json!({
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                        "prev_ratio": self.prev_imbalance_ratio,
                        "new_ratio": self.imbalance_ratio,
                        "flip_count": self.flips_count,
                    }),
                );
            }
        }
    }

    /// Event 3: Balanced liquidations
    fn check_liq_balanced(&mut self, timestamp: i64) {
        let total = self.long_count_1m + self.short_count_1m;
        if total >= 5 && self.imbalance_ratio.abs() < 20.0 {
            if self.should_fire(LIQ_BALANCED, timestamp) {
                self.publish_event(
                    LIQ_BALANCED,
                    timestamp,
                    json!({
                        "imbalance_ratio": self.imbalance_ratio,
                        "long_count": self.long_count_1m,
                        "short_count": self.short_count_1m,
                        "total_count": total,
                    }),
                );
            }
        }
    }

    /// Event 4: Long squeeze (short liquidations driving price up)
    fn check_liq_long_squeeze(&mut self, timestamp: i64) {
        // Long squeeze = many SHORT liquidations + price going up
        let is_short_dominated = self.current_regime == ImbalanceRegime::ShortBiased ||
            self.current_regime == ImbalanceRegime::ExtremeShort;
        let has_volume = self.short_volume_1m > self.thresholds.large_liquidation_usd;

        if is_short_dominated && has_volume {
            // Start or continue squeeze
            if self.squeeze_active.is_none() || self.squeeze_active.as_ref().unwrap() != "LONG" {
                self.squeeze_active = Some("LONG".to_string());
                self.squeeze_start_time = timestamp;
                self.squeeze_volume = 0.0;
                self.squeeze_count = 0;
                self.squeeze_peak_volume = 0.0;
                self.squeeze_start_price = self.squeeze_current_price;
            }

            if self.should_fire(LIQ_LONG_SQUEEZE, timestamp) {
                self.publish_event(
                    LIQ_LONG_SQUEEZE,
                    timestamp,
                    json!({
                        "short_count": self.short_count_1m,
                        "short_volume": self.short_volume_1m,
                        "squeeze_volume": self.squeeze_volume,
                        "price_impact": self.squeeze_price_impact,
                        "duration_ms": timestamp - self.squeeze_start_time,
                    }),
                );
            }
        }
    }

    /// Event 5: Short squeeze (long liquidations driving price down)
    fn check_liq_short_squeeze(&mut self, timestamp: i64) {
        // Short squeeze = many LONG liquidations + price going down
        let is_long_dominated = self.current_regime == ImbalanceRegime::LongBiased ||
            self.current_regime == ImbalanceRegime::ExtremeLong;
        let has_volume = self.long_volume_1m > self.thresholds.large_liquidation_usd;

        if is_long_dominated && has_volume {
            // Start or continue squeeze
            if self.squeeze_active.is_none() || self.squeeze_active.as_ref().unwrap() != "SHORT" {
                self.squeeze_active = Some("SHORT".to_string());
                self.squeeze_start_time = timestamp;
                self.squeeze_volume = 0.0;
                self.squeeze_count = 0;
                self.squeeze_peak_volume = 0.0;
                self.squeeze_start_price = self.squeeze_current_price;
            }

            if self.should_fire(LIQ_SHORT_SQUEEZE, timestamp) {
                self.publish_event(
                    LIQ_SHORT_SQUEEZE,
                    timestamp,
                    json!({
                        "long_count": self.long_count_1m,
                        "long_volume": self.long_volume_1m,
                        "squeeze_volume": self.squeeze_volume,
                        "price_impact": self.squeeze_price_impact,
                        "duration_ms": timestamp - self.squeeze_start_time,
                    }),
                );
            }
        }
    }

    /// Event 6: Squeeze exhaustion
    fn check_liq_squeeze_exhaustion(&mut self, timestamp: i64) {
        if self.squeeze_active.is_some() {
            // Check for exhaustion conditions
            let count_declining = (self.long_count_1m + self.short_count_1m) < 2;
            let velocity_declining = self.imbalance_velocity.abs() < 5.0 && self.imbalance_ratio.abs() < 50.0;
            let had_squeeze = self.squeeze_volume > self.thresholds.large_liquidation_usd;

            if count_declining && velocity_declining && had_squeeze {
                if self.should_fire(LIQ_SQUEEZE_EXHAUSTION, timestamp) {
                    let squeeze_type = self.squeeze_active.clone().unwrap_or_default();
                    self.publish_event(
                        LIQ_SQUEEZE_EXHAUSTION,
                        timestamp,
                        json!({
                            "squeeze_type": squeeze_type,
                            "total_volume": self.squeeze_volume,
                            "total_count": self.squeeze_count,
                            "price_impact": self.squeeze_price_impact,
                            "duration_ms": timestamp - self.squeeze_start_time,
                        }),
                    );

                    // Reset squeeze
                    self.squeeze_active = None;
                    self.squeeze_volume = 0.0;
                    self.squeeze_count = 0;
                }
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
            "Directional liquidation event published"
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

    /// Get imbalance ratio
    #[allow(dead_code)]
    pub fn get_imbalance_ratio(&self) -> f64 {
        self.imbalance_ratio
    }

    /// Get current regime
    #[allow(dead_code)]
    pub fn get_regime(&self) -> ImbalanceRegime {
        self.current_regime
    }
}
