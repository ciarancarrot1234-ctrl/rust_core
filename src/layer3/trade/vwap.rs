// VWAP Tracker - Monitors Volume-Weighted Average Price deviations
// Generates 11 VWAP-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer2::parser::ParsedAggTrade;
use crate::layer3::common::event_types::*;
use crate::layer3::common::time_windows::TimeWindow;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// VWAP Calculator for a single timeframe
#[derive(Debug, Clone)]
struct VwapCalculator {
    cumulative_pq: f64,                    // Σ(price × quantity)
    cumulative_q: f64,                     // Σ(quantity)
    trades: TimeWindow<ParsedAggTrade>,
}

impl VwapCalculator {
    fn new(duration_ms: i64, max_capacity: usize) -> Self {
        Self {
            cumulative_pq: 0.0,
            cumulative_q: 0.0,
            trades: TimeWindow::new(duration_ms, max_capacity),
        }
    }

    fn add_trade(&mut self, trade: &ParsedAggTrade, current_time: i64) {
        self.trades.add(current_time, trade.clone());
        self.trades.prune(current_time);

        // Recalculate from scratch (ensures accuracy after pruning)
        self.cumulative_pq = 0.0;
        self.cumulative_q = 0.0;

        for (_, t) in self.trades.iter() {
            self.cumulative_pq += t.price * t.quantity;
            self.cumulative_q += t.quantity;
        }
    }

    fn get_vwap(&self) -> Option<f64> {
        if self.cumulative_q > 0.0 {
            Some(self.cumulative_pq / self.cumulative_q)
        } else {
            None
        }
    }

    fn get_std_dev(&self, vwap: f64) -> f64 {
        if self.trades.is_empty() {
            return 0.0;
        }

        let mut sum_squared_diff = 0.0;
        let mut total_qty = 0.0;

        for (_, trade) in self.trades.iter() {
            let diff = trade.price - vwap;
            sum_squared_diff += diff * diff * trade.quantity;
            total_qty += trade.quantity;
        }

        if total_qty > 0.0 {
            (sum_squared_diff / total_qty).sqrt()
        } else {
            0.0
        }
    }
}

/// Position relative to VWAP
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VwapPosition {
    AboveUpper,   // Above upper band
    Above,        // Above VWAP but below upper band
    At,           // At VWAP (within tolerance)
    Below,        // Below VWAP but above lower band
    BelowLower,   // Below lower band
}

/// VwapTracker monitors price deviations from VWAP across multiple timeframes
pub struct VwapTracker {
    symbol: String,

    // VWAP calculators for 3 timeframes
    vwap_1m: VwapCalculator,
    vwap_5m: VwapCalculator,
    vwap_15m: VwapCalculator,

    // Current prices
    last_price: f64,

    // VWAP values
    current_vwap_1m: Option<f64>,
    current_vwap_5m: Option<f64>,
    current_vwap_15m: Option<f64>,

    // Standard deviations for bands
    std_dev_1m: f64,
    std_dev_5m: f64,
    std_dev_15m: f64,

    // Position tracking
    current_position_1m: VwapPosition,
    prev_position_1m: VwapPosition,

    // Cross detection
    last_cross_time: i64,
    last_cross_direction: Option<String>,

    // Retest tracking
    last_retest_time: i64,

    // Alignment tracking
    all_above: bool,
    all_below: bool,
    prev_all_above: bool,
    prev_all_below: bool,

    // Anchor shift tracking
    prev_vwap_1m: Option<f64>,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    trades_processed: u64,
    events_fired: u64,
}

impl VwapTracker {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            vwap_1m: VwapCalculator::new(60_000, 10_000),
            vwap_5m: VwapCalculator::new(300_000, 50_000),
            vwap_15m: VwapCalculator::new(900_000, 150_000),
            last_price: 0.0,
            current_vwap_1m: None,
            current_vwap_5m: None,
            current_vwap_15m: None,
            std_dev_1m: 0.0,
            std_dev_5m: 0.0,
            std_dev_15m: 0.0,
            current_position_1m: VwapPosition::At,
            prev_position_1m: VwapPosition::At,
            last_cross_time: 0,
            last_cross_direction: None,
            last_retest_time: 0,
            all_above: false,
            all_below: false,
            prev_all_above: false,
            prev_all_below: false,
            prev_vwap_1m: None,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 1000,
            thresholds,
            trades_processed: 0,
            events_fired: 0,
        }
    }

    pub fn add_trade(&mut self, trade: &ParsedAggTrade, current_time: i64) {
        self.trades_processed += 1;
        self.last_price = trade.price;
        self.vwap_1m.add_trade(trade, current_time);
        self.vwap_5m.add_trade(trade, current_time);
        self.vwap_15m.add_trade(trade, current_time);
    }

    pub fn check_events(&mut self, current_time: i64) {
        self.recalculate_vwap();

        if self.current_vwap_1m.is_none() {
            return;
        }

        self.check_vwap_deviation(current_time);
        self.check_vwap_extreme_deviation(current_time);
        self.check_vwap_cross(current_time);
        self.check_vwap_retest(current_time);
        self.check_vwap_rejection(current_time);
        self.check_vwap_alignment(current_time);
        self.check_vwap_band_break(current_time);
        self.check_vwap_convergence(current_time);
        self.check_vwap_divergence(current_time);
        self.check_vwap_anchor_shift(current_time);
        self.check_vwap_reclaim(current_time);
    }

    fn recalculate_vwap(&mut self) {
        self.prev_vwap_1m = self.current_vwap_1m;
        self.prev_position_1m = self.current_position_1m;

        self.current_vwap_1m = self.vwap_1m.get_vwap();
        self.current_vwap_5m = self.vwap_5m.get_vwap();
        self.current_vwap_15m = self.vwap_15m.get_vwap();

        if let Some(vwap) = self.current_vwap_1m {
            self.std_dev_1m = self.vwap_1m.get_std_dev(vwap);
        }
        if let Some(vwap) = self.current_vwap_5m {
            self.std_dev_5m = self.vwap_5m.get_std_dev(vwap);
        }
        if let Some(vwap) = self.current_vwap_15m {
            self.std_dev_15m = self.vwap_15m.get_std_dev(vwap);
        }

        if let Some(vwap) = self.current_vwap_1m {
            self.current_position_1m = self.classify_position(self.last_price, vwap, self.std_dev_1m);
        }

        self.prev_all_above = self.all_above;
        self.prev_all_below = self.all_below;
        self.all_above = self.is_above_all_vwaps();
        self.all_below = self.is_below_all_vwaps();
    }

    fn classify_position(&self, price: f64, vwap: f64, std_dev: f64) -> VwapPosition {
        let upper_band = vwap + (self.thresholds.vwap_band_std_dev * std_dev);
        let lower_band = vwap - (self.thresholds.vwap_band_std_dev * std_dev);
        let tolerance = vwap * (self.thresholds.vwap_retest_tolerance / 100.0);

        if price > upper_band {
            VwapPosition::AboveUpper
        } else if price > vwap + tolerance {
            VwapPosition::Above
        } else if price >= vwap - tolerance {
            VwapPosition::At
        } else if price > lower_band {
            VwapPosition::Below
        } else {
            VwapPosition::BelowLower
        }
    }

    fn is_above_all_vwaps(&self) -> bool {
        if let (Some(vwap_1m), Some(vwap_5m), Some(vwap_15m)) =
            (self.current_vwap_1m, self.current_vwap_5m, self.current_vwap_15m)
        {
            self.last_price > vwap_1m && self.last_price > vwap_5m && self.last_price > vwap_15m
        } else {
            false
        }
    }

    fn is_below_all_vwaps(&self) -> bool {
        if let (Some(vwap_1m), Some(vwap_5m), Some(vwap_15m)) =
            (self.current_vwap_1m, self.current_vwap_5m, self.current_vwap_15m)
        {
            self.last_price < vwap_1m && self.last_price < vwap_5m && self.last_price < vwap_15m
        } else {
            false
        }
    }

    fn check_vwap_deviation(&mut self, timestamp: i64) {
        if let Some(vwap) = self.current_vwap_1m {
            let deviation_pct = ((self.last_price - vwap) / vwap).abs() * 100.0;

            if deviation_pct > self.thresholds.vwap_deviation_pct {
                if self.should_fire(VWAP_DEVIATION, timestamp) {
                    self.publish_event(
                        VWAP_DEVIATION,
                        timestamp,
                        json!({
                            "price": self.last_price,
                            "vwap_1m": vwap,
                            "deviation_pct": deviation_pct,
                            "threshold": self.thresholds.vwap_deviation_pct,
                        }),
                    );
                }
            }
        }
    }

    fn check_vwap_extreme_deviation(&mut self, timestamp: i64) {
        if let Some(vwap) = self.current_vwap_1m {
            let deviation_pct = ((self.last_price - vwap) / vwap).abs() * 100.0;

            if deviation_pct > self.thresholds.vwap_extreme_distance_pct {
                if self.should_fire(VWAP_EXTREME_DEVIATION, timestamp) {
                    self.publish_event(
                        VWAP_EXTREME_DEVIATION,
                        timestamp,
                        json!({
                            "price": self.last_price,
                            "vwap_1m": vwap,
                            "deviation_pct": deviation_pct,
                            "threshold": self.thresholds.vwap_extreme_distance_pct,
                        }),
                    );
                }
            }
        }
    }

    fn check_vwap_cross(&mut self, timestamp: i64) {
        let crossed_above = matches!(self.prev_position_1m, VwapPosition::Below | VwapPosition::BelowLower)
            && matches!(self.current_position_1m, VwapPosition::Above | VwapPosition::AboveUpper);

        let crossed_below = matches!(self.prev_position_1m, VwapPosition::Above | VwapPosition::AboveUpper)
            && matches!(self.current_position_1m, VwapPosition::Below | VwapPosition::BelowLower);

        if crossed_above || crossed_below {
            self.last_cross_time = timestamp;
            self.last_cross_direction = Some(if crossed_above { "above".to_string() } else { "below".to_string() });

            if self.should_fire(VWAP_CROSS, timestamp) {
                self.publish_event(
                    VWAP_CROSS,
                    timestamp,
                    json!({
                        "direction": if crossed_above { "above" } else { "below" },
                        "price": self.last_price,
                        "vwap": self.current_vwap_1m,
                    }),
                );
            }
        }
    }

    fn check_vwap_retest(&mut self, timestamp: i64) {
        if self.current_position_1m == VwapPosition::At {
            let time_since_cross = timestamp - self.last_cross_time;
            if time_since_cross > 5000 && time_since_cross < 60000 {
                if self.should_fire(VWAP_RETEST, timestamp) {
                    self.last_retest_time = timestamp;
                    self.publish_event(
                        VWAP_RETEST,
                        timestamp,
                        json!({
                            "price": self.last_price,
                            "vwap": self.current_vwap_1m,
                            "time_since_cross_ms": time_since_cross,
                        }),
                    );
                }
            }
        }
    }

    fn check_vwap_rejection(&mut self, timestamp: i64) {
        let approached_from_above = matches!(self.prev_position_1m, VwapPosition::At)
            && matches!(self.current_position_1m, VwapPosition::Above | VwapPosition::AboveUpper);

        let approached_from_below = matches!(self.prev_position_1m, VwapPosition::At)
            && matches!(self.current_position_1m, VwapPosition::Below | VwapPosition::BelowLower);

        if approached_from_above || approached_from_below {
            let time_since_retest = timestamp - self.last_retest_time;
            if time_since_retest < 10000 {
                if self.should_fire(VWAP_REJECTION, timestamp) {
                    self.publish_event(
                        VWAP_REJECTION,
                        timestamp,
                        json!({
                            "direction": if approached_from_above { "rejected_down" } else { "rejected_up" },
                            "price": self.last_price,
                            "vwap": self.current_vwap_1m,
                        }),
                    );
                }
            }
        }
    }

    fn check_vwap_alignment(&mut self, timestamp: i64) {
        let aligned_above = self.all_above && !self.prev_all_above;
        let aligned_below = self.all_below && !self.prev_all_below;

        if aligned_above || aligned_below {
            if self.should_fire(VWAP_ALIGNMENT, timestamp) {
                self.publish_event(
                    VWAP_ALIGNMENT,
                    timestamp,
                    json!({
                        "alignment": if aligned_above { "above" } else { "below" },
                        "price": self.last_price,
                        "vwap_1m": self.current_vwap_1m,
                        "vwap_5m": self.current_vwap_5m,
                        "vwap_15m": self.current_vwap_15m,
                    }),
                );
            }
        }
    }

    fn check_vwap_band_break(&mut self, timestamp: i64) {
        let broke_above = matches!(self.current_position_1m, VwapPosition::AboveUpper)
            && !matches!(self.prev_position_1m, VwapPosition::AboveUpper);

        let broke_below = matches!(self.current_position_1m, VwapPosition::BelowLower)
            && !matches!(self.prev_position_1m, VwapPosition::BelowLower);

        if broke_above || broke_below {
            if self.should_fire(VWAP_BAND_BREAK, timestamp) {
                self.publish_event(
                    VWAP_BAND_BREAK,
                    timestamp,
                    json!({
                        "direction": if broke_above { "above" } else { "below" },
                        "price": self.last_price,
                        "vwap": self.current_vwap_1m,
                        "std_dev": self.std_dev_1m,
                        "band_multiplier": self.thresholds.vwap_band_std_dev,
                    }),
                );
            }
        }
    }

    fn check_vwap_convergence(&mut self, timestamp: i64) {
        if let (Some(vwap), Some(prev_vwap)) = (self.current_vwap_1m, self.prev_vwap_1m) {
            let prev_distance = (self.last_price - prev_vwap).abs();
            let curr_distance = (self.last_price - vwap).abs();

            if curr_distance < prev_distance {
                let convergence_pct = ((prev_distance - curr_distance) / prev_distance) * 100.0;
                if convergence_pct > self.thresholds.vwap_convergence_threshold {
                    if self.should_fire(VWAP_CONVERGENCE, timestamp) {
                        self.publish_event(
                            VWAP_CONVERGENCE,
                            timestamp,
                            json!({
                                "price": self.last_price,
                                "vwap": vwap,
                                "convergence_pct": convergence_pct,
                            }),
                        );
                    }
                }
            }
        }
    }

    fn check_vwap_divergence(&mut self, timestamp: i64) {
        if let (Some(vwap), Some(prev_vwap)) = (self.current_vwap_1m, self.prev_vwap_1m) {
            let prev_distance = (self.last_price - prev_vwap).abs();
            let curr_distance = (self.last_price - vwap).abs();

            if curr_distance > prev_distance {
                let divergence_pct = ((curr_distance - prev_distance) / prev_distance) * 100.0;
                if divergence_pct > self.thresholds.vwap_convergence_threshold {
                    if self.should_fire(VWAP_DIVERGENCE, timestamp) {
                        self.publish_event(
                            VWAP_DIVERGENCE,
                            timestamp,
                            json!({
                                "price": self.last_price,
                                "vwap": vwap,
                                "divergence_pct": divergence_pct,
                            }),
                        );
                    }
                }
            }
        }
    }

    fn check_vwap_anchor_shift(&mut self, timestamp: i64) {
        if let (Some(vwap), Some(prev_vwap)) = (self.current_vwap_1m, self.prev_vwap_1m) {
            let shift_pct = ((vwap - prev_vwap) / prev_vwap).abs() * 100.0;

            if shift_pct > 0.1 {
                if self.should_fire(VWAP_ANCHOR_SHIFT, timestamp) {
                    self.publish_event(
                        VWAP_ANCHOR_SHIFT,
                        timestamp,
                        json!({
                            "prev_vwap": prev_vwap,
                            "new_vwap": vwap,
                            "shift_pct": shift_pct,
                        }),
                    );
                }
            }
        }
    }

    fn check_vwap_reclaim(&mut self, timestamp: i64) {
        let reclaimed_above = matches!(self.prev_position_1m, VwapPosition::Below | VwapPosition::BelowLower)
            && matches!(self.current_position_1m, VwapPosition::Above | VwapPosition::AboveUpper)
            && self.last_cross_direction.as_deref() == Some("below");

        let reclaimed_below = matches!(self.prev_position_1m, VwapPosition::Above | VwapPosition::AboveUpper)
            && matches!(self.current_position_1m, VwapPosition::Below | VwapPosition::BelowLower)
            && self.last_cross_direction.as_deref() == Some("above");

        if reclaimed_above || reclaimed_below {
            if self.should_fire(VWAP_RECLAIM, timestamp) {
                self.publish_event(
                    VWAP_RECLAIM,
                    timestamp,
                    json!({
                        "direction": if reclaimed_above { "reclaimed_above" } else { "reclaimed_below" },
                        "price": self.last_price,
                        "vwap": self.current_vwap_1m,
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
            "trade_aggregator",
            EventPriority::Medium,
        );

        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(symbol = %self.symbol, event = event_type, "VWAP event published");
    }

    pub fn trades_processed(&self) -> u64 { self.trades_processed }
    pub fn events_fired(&self) -> u64 { self.events_fired }

    #[allow(dead_code)]
    pub fn get_vwap_1m(&self) -> Option<f64> { self.current_vwap_1m }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_trade(price: f64, qty: f64) -> ParsedAggTrade {
        ParsedAggTrade {
            symbol: "BTCUSDT".to_string(), agg_trade_id: 0, price, quantity: qty,
            first_trade_id: 0, last_trade_id: 0, timestamp: 0, is_buyer_maker: false, event_time: 0,
        }
    }

    #[test]
    fn test_vwap_calculation() {
        let thresholds = AggregatorThresholds::default();
        let mut tracker = VwapTracker::new("BTCUSDT".to_string(), thresholds);

        tracker.add_trade(&make_trade(100.0, 1.0), 1000);
        tracker.add_trade(&make_trade(200.0, 2.0), 2000);
        tracker.recalculate_vwap();

        let vwap = tracker.get_vwap_1m().unwrap();
        let expected = 166.666667;
        assert!((vwap - expected).abs() < 0.01);
    }
}
