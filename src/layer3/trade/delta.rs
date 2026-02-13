// Delta Tracker - Monitors buy/sell volume imbalances
// Generates 12 delta-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::core::types::Direction;
use crate::layer2::parser::ParsedAggTrade;
use crate::layer3::common::event_types::*;
use crate::layer3::common::time_windows::TimeWindow;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Delta regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaRegime {
    StrongBullish,   // > 50%
    Bullish,         // 30-50%
    WeakBullish,     // 10-30%
    Neutral,         // -10 to 10%
    WeakBearish,     // -30 to -10%
    Bearish,         // -50 to -30%
    StrongBearish,   // < -50%
}

/// DeltaTracker tracks buy/sell volume imbalances across multiple timeframes
pub struct DeltaTracker {
    symbol: String,

    // Rolling windows for 3 timeframes
    trades_1m: TimeWindow<ParsedAggTrade>,
    trades_5m: TimeWindow<ParsedAggTrade>,
    trades_15m: TimeWindow<ParsedAggTrade>,

    // Calculated metrics (buy_volume - sell_volume)
    delta_1m: f64,
    delta_5m: f64,
    delta_15m: f64,

    // Delta as percentage of total volume
    delta_pct_1m: f64,
    delta_pct_5m: f64,
    delta_pct_15m: f64,

    // Derivatives for slope/acceleration detection
    prev_delta_1m: f64,
    prev_delta_pct_1m: f64,
    delta_velocity_1m: f64,       // Change in delta_pct per calculation
    prev_velocity_1m: f64,
    delta_acceleration_1m: f64,   // Change in velocity

    // State tracking
    current_regime: DeltaRegime,
    prev_regime: DeltaRegime,
    last_zero_cross_time: i64,
    last_positive_to_negative: i64,
    last_negative_to_positive: i64,

    // Event debouncing (prevent spam)
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics tracking
    trades_processed: u64,
    events_fired: u64,
}

impl DeltaTracker {
    /// Create a new DeltaTracker
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            trades_1m: TimeWindow::new(60_000, 10_000),      // 1m, max 10k trades
            trades_5m: TimeWindow::new(300_000, 50_000),     // 5m, max 50k trades
            trades_15m: TimeWindow::new(900_000, 150_000),   // 15m, max 150k trades
            delta_1m: 0.0,
            delta_5m: 0.0,
            delta_15m: 0.0,
            delta_pct_1m: 0.0,
            delta_pct_5m: 0.0,
            delta_pct_15m: 0.0,
            prev_delta_1m: 0.0,
            prev_delta_pct_1m: 0.0,
            delta_velocity_1m: 0.0,
            prev_velocity_1m: 0.0,
            delta_acceleration_1m: 0.0,
            current_regime: DeltaRegime::Neutral,
            prev_regime: DeltaRegime::Neutral,
            last_zero_cross_time: 0,
            last_positive_to_negative: 0,
            last_negative_to_positive: 0,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 1000,  // 1 second debounce
            thresholds,
            trades_processed: 0,
            events_fired: 0,
        }
    }

    /// Add a trade to all windows
    pub fn add_trade(&mut self, trade: &ParsedAggTrade, current_time: i64) {
        self.trades_processed += 1;

        self.trades_1m.add(current_time, trade.clone());
        self.trades_5m.add(current_time, trade.clone());
        self.trades_15m.add(current_time, trade.clone());

        // Prune old data
        self.trades_1m.prune(current_time);
        self.trades_5m.prune(current_time);
        self.trades_15m.prune(current_time);
    }

    /// Check all delta events and publish to EventBus
    pub fn check_events(&mut self, current_time: i64) {
        // Recalculate all metrics
        self.recalculate_delta();

        // Event 1: Delta threshold exceeded
        self.check_delta_threshold(current_time);

        // Event 2: Delta slope (velocity)
        self.check_delta_slope(current_time);

        // Event 3: Delta acceleration
        self.check_delta_acceleration(current_time);

        // Event 4: Delta regime change
        self.check_delta_regime_change(current_time);

        // Event 5: Delta reversal
        self.check_delta_reversal(current_time);

        // Event 6: Momentum building
        self.check_delta_momentum_building(current_time);

        // Event 7: Delta exhaustion
        self.check_delta_exhaustion(current_time);

        // Event 8: Delta divergence (1m vs 15m)
        self.check_delta_divergence(current_time);

        // Event 9: Zero cross
        self.check_delta_zero_cross(current_time);

        // Event 10: Delta extreme
        self.check_delta_extreme(current_time);

        // Event 11: Delta neutral
        self.check_delta_neutral(current_time);

        // Event 12: Imbalance flip
        self.check_delta_imbalance_flip(current_time);
    }

    /// Recalculate delta metrics for all timeframes
    fn recalculate_delta(&mut self) {
        // Calculate 1m delta
        let (buy_1m, sell_1m) = self.calculate_buy_sell_volume(&self.trades_1m);
        self.prev_delta_1m = self.delta_1m;
        self.prev_delta_pct_1m = self.delta_pct_1m;
        self.delta_1m = buy_1m - sell_1m;
        let total_1m = buy_1m + sell_1m;
        self.delta_pct_1m = if total_1m > 0.0 {
            (self.delta_1m / total_1m) * 100.0
        } else {
            0.0
        };

        // Calculate velocity and acceleration
        self.prev_velocity_1m = self.delta_velocity_1m;
        self.delta_velocity_1m = self.delta_pct_1m - self.prev_delta_pct_1m;
        self.delta_acceleration_1m = self.delta_velocity_1m - self.prev_velocity_1m;

        // Calculate 5m delta
        let (buy_5m, sell_5m) = self.calculate_buy_sell_volume(&self.trades_5m);
        self.delta_5m = buy_5m - sell_5m;
        let total_5m = buy_5m + sell_5m;
        self.delta_pct_5m = if total_5m > 0.0 {
            (self.delta_5m / total_5m) * 100.0
        } else {
            0.0
        };

        // Calculate 15m delta
        let (buy_15m, sell_15m) = self.calculate_buy_sell_volume(&self.trades_15m);
        self.delta_15m = buy_15m - sell_15m;
        let total_15m = buy_15m + sell_15m;
        self.delta_pct_15m = if total_15m > 0.0 {
            (self.delta_15m / total_15m) * 100.0
        } else {
            0.0
        };

        // Update regime
        self.prev_regime = self.current_regime;
        self.current_regime = self.classify_regime(self.delta_pct_1m);
    }

    /// Calculate buy and sell volumes from a time window
    fn calculate_buy_sell_volume(&self, window: &TimeWindow<ParsedAggTrade>) -> (f64, f64) {
        let mut buy_vol = 0.0;
        let mut sell_vol = 0.0;

        for (_, trade) in window.iter() {
            if trade.is_buyer_maker {
                // Buyer is maker → taker sold → sell volume
                sell_vol += trade.quantity;
            } else {
                // Buyer is taker → taker bought → buy volume
                buy_vol += trade.quantity;
            }
        }

        (buy_vol, sell_vol)
    }

    /// Classify delta percentage into regime
    fn classify_regime(&self, delta_pct: f64) -> DeltaRegime {
        if delta_pct > 50.0 {
            DeltaRegime::StrongBullish
        } else if delta_pct > 30.0 {
            DeltaRegime::Bullish
        } else if delta_pct > 10.0 {
            DeltaRegime::WeakBullish
        } else if delta_pct >= -10.0 {
            DeltaRegime::Neutral
        } else if delta_pct >= -30.0 {
            DeltaRegime::WeakBearish
        } else if delta_pct >= -50.0 {
            DeltaRegime::Bearish
        } else {
            DeltaRegime::StrongBearish
        }
    }

    /// Event 1: Delta threshold exceeded (>30% imbalance)
    fn check_delta_threshold(&mut self, timestamp: i64) {
        if self.delta_pct_1m.abs() > self.thresholds.delta_threshold_pct {
            if self.should_fire(DELTA_THRESHOLD, timestamp) {
                self.publish_event(
                    DELTA_THRESHOLD,
                    timestamp,
                    json!({
                        "delta_pct_1m": self.delta_pct_1m,
                        "delta_pct_5m": self.delta_pct_5m,
                        "delta_pct_15m": self.delta_pct_15m,
                        "threshold": self.thresholds.delta_threshold_pct,
                    }),
                );
            }
        }
    }

    /// Event 2: Delta slope (velocity change)
    fn check_delta_slope(&mut self, timestamp: i64) {
        if self.delta_velocity_1m.abs() > self.thresholds.delta_slope_threshold {
            if self.should_fire(DELTA_SLOPE, timestamp) {
                self.publish_event(
                    DELTA_SLOPE,
                    timestamp,
                    json!({
                        "velocity": self.delta_velocity_1m,
                        "delta_pct": self.delta_pct_1m,
                        "threshold": self.thresholds.delta_slope_threshold,
                    }),
                );
            }
        }
    }

    /// Event 3: Delta acceleration
    fn check_delta_acceleration(&mut self, timestamp: i64) {
        if self.delta_acceleration_1m.abs() > self.thresholds.delta_acceleration_threshold {
            if self.should_fire(DELTA_ACCELERATION, timestamp) {
                self.publish_event(
                    DELTA_ACCELERATION,
                    timestamp,
                    json!({
                        "acceleration": self.delta_acceleration_1m,
                        "velocity": self.delta_velocity_1m,
                        "delta_pct": self.delta_pct_1m,
                    }),
                );
            }
        }
    }

    /// Event 4: Delta regime change
    fn check_delta_regime_change(&mut self, timestamp: i64) {
        if self.current_regime != self.prev_regime {
            if self.should_fire(DELTA_REGIME_CHANGE, timestamp) {
                self.publish_event(
                    DELTA_REGIME_CHANGE,
                    timestamp,
                    json!({
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                        "delta_pct": self.delta_pct_1m,
                    }),
                );
            }
        }
    }

    /// Event 5: Delta reversal (strong regime flip)
    fn check_delta_reversal(&mut self, timestamp: i64) {
        let is_bullish_reversal = matches!(self.prev_regime, DeltaRegime::Bearish | DeltaRegime::StrongBearish)
            && matches!(self.current_regime, DeltaRegime::Bullish | DeltaRegime::StrongBullish);

        let is_bearish_reversal = matches!(self.prev_regime, DeltaRegime::Bullish | DeltaRegime::StrongBullish)
            && matches!(self.current_regime, DeltaRegime::Bearish | DeltaRegime::StrongBearish);

        if is_bullish_reversal || is_bearish_reversal {
            if self.should_fire(DELTA_REVERSAL, timestamp) {
                let direction = if is_bullish_reversal { Direction::Long } else { Direction::Short };
                self.publish_event(
                    DELTA_REVERSAL,
                    timestamp,
                    json!({
                        "reversal_type": if is_bullish_reversal { "bullish" } else { "bearish" },
                        "direction": format!("{:?}", direction),
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                        "delta_pct": self.delta_pct_1m,
                    }),
                );
            }
        }
    }

    /// Event 6: Momentum building
    fn check_delta_momentum_building(&mut self, timestamp: i64) {
        if self.delta_pct_1m.abs() > self.thresholds.delta_momentum_building
            && self.delta_velocity_1m.signum() == self.delta_pct_1m.signum()
            && self.delta_acceleration_1m.signum() == self.delta_pct_1m.signum()
        {
            if self.should_fire(DELTA_MOMENTUM_BUILDING, timestamp) {
                self.publish_event(
                    DELTA_MOMENTUM_BUILDING,
                    timestamp,
                    json!({
                        "delta_pct": self.delta_pct_1m,
                        "velocity": self.delta_velocity_1m,
                        "acceleration": self.delta_acceleration_1m,
                        "regime": format!("{:?}", self.current_regime),
                    }),
                );
            }
        }
    }

    /// Event 7: Delta exhaustion (extreme values with slowing velocity)
    fn check_delta_exhaustion(&mut self, timestamp: i64) {
        let is_exhausted = self.delta_pct_1m.abs() > self.thresholds.delta_exhaustion
            && self.delta_velocity_1m.signum() != self.delta_pct_1m.signum();

        if is_exhausted {
            if self.should_fire(DELTA_EXHAUSTION, timestamp) {
                self.publish_event(
                    DELTA_EXHAUSTION,
                    timestamp,
                    json!({
                        "delta_pct": self.delta_pct_1m,
                        "velocity": self.delta_velocity_1m,
                        "threshold": self.thresholds.delta_exhaustion,
                    }),
                );
            }
        }
    }

    /// Event 8: Delta divergence (1m vs 15m disagreement)
    fn check_delta_divergence(&mut self, timestamp: i64) {
        let divergence = (self.delta_pct_1m - self.delta_pct_15m).abs();
        if divergence > 40.0 && self.delta_pct_1m.signum() != self.delta_pct_15m.signum() {
            if self.should_fire(DELTA_DIVERGENCE, timestamp) {
                self.publish_event(
                    DELTA_DIVERGENCE,
                    timestamp,
                    json!({
                        "delta_pct_1m": self.delta_pct_1m,
                        "delta_pct_15m": self.delta_pct_15m,
                        "divergence": divergence,
                    }),
                );
            }
        }
    }

    /// Event 9: Zero cross (delta changes sign)
    fn check_delta_zero_cross(&mut self, timestamp: i64) {
        let prev_sign = self.prev_delta_1m.signum();
        let curr_sign = self.delta_1m.signum();

        if prev_sign != 0.0 && curr_sign != 0.0 && prev_sign != curr_sign {
            self.last_zero_cross_time = timestamp;

            if curr_sign > 0.0 {
                self.last_negative_to_positive = timestamp;
            } else {
                self.last_positive_to_negative = timestamp;
            }

            if self.should_fire(DELTA_ZERO_CROSS, timestamp) {
                let direction = if curr_sign > 0.0 { Direction::Long } else { Direction::Short };
                self.publish_event(
                    DELTA_ZERO_CROSS,
                    timestamp,
                    json!({
                        "direction": format!("{:?}", direction),
                        "delta_pct": self.delta_pct_1m,
                    }),
                );
            }
        }
    }

    /// Event 10: Delta extreme (>70% imbalance)
    fn check_delta_extreme(&mut self, timestamp: i64) {
        if self.delta_pct_1m.abs() > self.thresholds.delta_extreme_threshold {
            if self.should_fire(DELTA_EXTREME, timestamp) {
                self.publish_event(
                    DELTA_EXTREME,
                    timestamp,
                    json!({
                        "delta_pct": self.delta_pct_1m,
                        "threshold": self.thresholds.delta_extreme_threshold,
                    }),
                );
            }
        }
    }

    /// Event 11: Delta neutral (within ±10%)
    fn check_delta_neutral(&mut self, timestamp: i64) {
        if self.delta_pct_1m.abs() < self.thresholds.delta_neutral_range {
            if self.should_fire(DELTA_NEUTRAL, timestamp) {
                self.publish_event(
                    DELTA_NEUTRAL,
                    timestamp,
                    json!({
                        "delta_pct": self.delta_pct_1m,
                        "range": self.thresholds.delta_neutral_range,
                    }),
                );
            }
        }
    }

    /// Event 12: Imbalance flip (strong → opposite strong)
    fn check_delta_imbalance_flip(&mut self, timestamp: i64) {
        let is_flip = (matches!(self.prev_regime, DeltaRegime::StrongBullish)
            && matches!(self.current_regime, DeltaRegime::StrongBearish))
            || (matches!(self.prev_regime, DeltaRegime::StrongBearish)
                && matches!(self.current_regime, DeltaRegime::StrongBullish));

        if is_flip {
            if self.should_fire(DELTA_IMBALANCE_FLIP, timestamp) {
                self.publish_event(
                    DELTA_IMBALANCE_FLIP,
                    timestamp,
                    json!({
                        "prev_regime": format!("{:?}", self.prev_regime),
                        "new_regime": format!("{:?}", self.current_regime),
                        "delta_pct": self.delta_pct_1m,
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

        // Add common fields
        if let Some(obj) = data.as_object_mut() {
            obj.insert("symbol".to_string(), json!(self.symbol));
        }

        publish_event(
            event_type,
            timestamp,
            serde_json::from_value(data).unwrap_or_default(),
            "trade_aggregator",
            EventPriority::Medium,
        );

        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(
            symbol = %self.symbol,
            event = event_type,
            "Delta event published"
        );
    }

    /// Get number of trades processed
    pub fn trades_processed(&self) -> u64 {
        self.trades_processed
    }

    /// Get number of events fired
    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    // Getters for testing
    #[allow(dead_code)]
    pub fn get_delta_1m(&self) -> f64 {
        self.delta_1m
    }

    #[allow(dead_code)]
    pub fn get_delta_pct_1m(&self) -> f64 {
        self.delta_pct_1m
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_trade(price: f64, qty: f64, is_buyer_maker: bool) -> ParsedAggTrade {
        ParsedAggTrade {
            symbol: "BTCUSDT".to_string(),
            agg_trade_id: 0,
            price,
            quantity: qty,
            first_trade_id: 0,
            last_trade_id: 0,
            timestamp: 0,
            is_buyer_maker,
            event_time: 0,
        }
    }

    #[test]
    fn test_delta_calculation_basic() {
        let thresholds = AggregatorThresholds::default();
        let mut tracker = DeltaTracker::new("BTCUSDT".to_string(), thresholds);

        // Add buy trades (is_buyer_maker=false → taker bought)
        tracker.add_trade(&make_trade(95000.0, 1.0, false), 1000);
        tracker.add_trade(&make_trade(95001.0, 2.0, false), 2000);

        // Add sell trade (is_buyer_maker=true → taker sold)
        tracker.add_trade(&make_trade(94999.0, 0.5, true), 3000);

        tracker.recalculate_delta();

        // Buy: 3.0, Sell: 0.5, Delta: 2.5
        assert!((tracker.get_delta_1m() - 2.5).abs() < 0.001);

        // Delta %: (2.5 / 3.5) * 100 = 71.43%
        let expected_pct = (2.5 / 3.5) * 100.0;
        assert!((tracker.get_delta_pct_1m() - expected_pct).abs() < 0.1);
    }

    #[test]
    fn test_regime_classification() {
        let thresholds = AggregatorThresholds::default();
        let tracker = DeltaTracker::new("BTCUSDT".to_string(), thresholds);

        assert_eq!(tracker.classify_regime(60.0), DeltaRegime::StrongBullish);
        assert_eq!(tracker.classify_regime(40.0), DeltaRegime::Bullish);
        assert_eq!(tracker.classify_regime(20.0), DeltaRegime::WeakBullish);
        assert_eq!(tracker.classify_regime(0.0), DeltaRegime::Neutral);
        assert_eq!(tracker.classify_regime(-20.0), DeltaRegime::WeakBearish);
        assert_eq!(tracker.classify_regime(-40.0), DeltaRegime::Bearish);
        assert_eq!(tracker.classify_regime(-60.0), DeltaRegime::StrongBearish);
    }
}
