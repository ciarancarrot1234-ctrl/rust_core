// Volume Tracker - Monitors volume patterns and anomalies
// Generates 12 volume-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::core::types::Direction;
use crate::layer2::parser::ParsedAggTrade;
use crate::layer3::common::event_types::*;
use crate::layer3::common::time_windows::TimeWindow;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Volume regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeRegime {
    Extreme,   // >5x average
    High,      // 2-5x average
    Normal,    // 0.5-2x average
    Low,       // <0.5x average
}

/// VolumeTracker monitors volume patterns across multiple timeframes
pub struct VolumeTracker {
    symbol: String,

    // Rolling windows for 3 timeframes
    trades_1m: TimeWindow<ParsedAggTrade>,
    trades_5m: TimeWindow<ParsedAggTrade>,
    trades_15m: TimeWindow<ParsedAggTrade>,

    // Volume metrics
    volume_1m: f64,
    volume_5m: f64,
    volume_15m: f64,

    // Buy/sell breakdown
    buy_volume_1m: f64,
    sell_volume_1m: f64,
    buy_sell_ratio_1m: f64,

    // Moving average
    avg_volume_1m: f64,
    volume_history: Vec<f64>,
    max_history_periods: usize,

    // Velocity and acceleration
    prev_volume_1m: f64,
    volume_velocity_1m: f64,
    prev_velocity_1m: f64,
    volume_acceleration_1m: f64,

    // Regime tracking
    current_regime: VolumeRegime,
    prev_regime: VolumeRegime,

    // Climax detection
    max_volume_1m: f64,

    // Price tracking
    last_price: f64,
    prev_price: f64,
    price_rising: bool,
    volume_rising: bool,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    trades_processed: u64,
    events_fired: u64,
}

impl VolumeTracker {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            trades_1m: TimeWindow::new(60_000, 10_000),
            trades_5m: TimeWindow::new(300_000, 50_000),
            trades_15m: TimeWindow::new(900_000, 150_000),
            volume_1m: 0.0, volume_5m: 0.0, volume_15m: 0.0,
            buy_volume_1m: 0.0, sell_volume_1m: 0.0, buy_sell_ratio_1m: 0.5,
            avg_volume_1m: 0.0, volume_history: Vec::new(), max_history_periods: 10,
            prev_volume_1m: 0.0, volume_velocity_1m: 0.0, prev_velocity_1m: 0.0, volume_acceleration_1m: 0.0,
            current_regime: VolumeRegime::Normal, prev_regime: VolumeRegime::Normal,
            max_volume_1m: 0.0,
            last_price: 0.0, prev_price: 0.0, price_rising: false, volume_rising: false,
            last_event_times: HashMap::new(), min_event_interval_ms: 1000,
            thresholds,
            trades_processed: 0, events_fired: 0,
        }
    }

    pub fn add_trade(&mut self, trade: &ParsedAggTrade, current_time: i64) {
        self.trades_processed += 1;
        self.prev_price = self.last_price;
        self.last_price = trade.price;

        self.trades_1m.add(current_time, trade.clone());
        self.trades_5m.add(current_time, trade.clone());
        self.trades_15m.add(current_time, trade.clone());

        self.trades_1m.prune(current_time);
        self.trades_5m.prune(current_time);
        self.trades_15m.prune(current_time);
    }

    pub fn check_events(&mut self, current_time: i64) {
        self.recalculate_volume();

        self.check_volume_spike(current_time);
        self.check_volume_dry_up(current_time);
        self.check_volume_regime_change(current_time);
        self.check_volume_ratio_extreme(current_time);
        self.check_volume_acceleration(current_time);
        self.check_volume_exhaustion(current_time);
        self.check_volume_climax(current_time);
        self.check_volume_distribution(current_time);
        self.check_volume_accumulation(current_time);
        self.check_volume_churn(current_time);
        self.check_volume_divergence(current_time);
        self.check_volume_confirmation(current_time);
    }

    fn recalculate_volume(&mut self) {
        self.prev_volume_1m = self.volume_1m;
        self.prev_regime = self.current_regime;

        self.volume_1m = self.calculate_total_volume(&self.trades_1m);
        self.volume_5m = self.calculate_total_volume(&self.trades_5m);
        self.volume_15m = self.calculate_total_volume(&self.trades_15m);

        let (buy_vol, sell_vol) = self.calculate_buy_sell_volume(&self.trades_1m);
        self.buy_volume_1m = buy_vol;
        self.sell_volume_1m = sell_vol;
        let total = buy_vol + sell_vol;
        self.buy_sell_ratio_1m = if total > 0.0 { buy_vol / total } else { 0.5 };

        self.volume_history.push(self.volume_1m);
        if self.volume_history.len() > self.max_history_periods {
            self.volume_history.remove(0);
        }

        if !self.volume_history.is_empty() {
            self.avg_volume_1m = self.volume_history.iter().sum::<f64>() / self.volume_history.len() as f64;
        }

        self.prev_velocity_1m = self.volume_velocity_1m;
        self.volume_velocity_1m = self.volume_1m - self.prev_volume_1m;
        self.volume_acceleration_1m = self.volume_velocity_1m - self.prev_velocity_1m;

        self.current_regime = self.classify_regime(self.volume_1m, self.avg_volume_1m);

        if self.volume_1m > self.max_volume_1m { self.max_volume_1m = self.volume_1m; }

        self.price_rising = self.last_price > self.prev_price;
        self.volume_rising = self.volume_1m > self.prev_volume_1m;
    }

    fn calculate_total_volume(&self, window: &TimeWindow<ParsedAggTrade>) -> f64 {
        window.iter().map(|(_, trade)| trade.quantity).sum()
    }

    fn calculate_buy_sell_volume(&self, window: &TimeWindow<ParsedAggTrade>) -> (f64, f64) {
        let mut buy_vol = 0.0;
        let mut sell_vol = 0.0;

        for (_, trade) in window.iter() {
            if trade.is_buyer_maker {
                sell_vol += trade.quantity;
            } else {
                buy_vol += trade.quantity;
            }
        }
        (buy_vol, sell_vol)
    }

    fn classify_regime(&self, volume: f64, avg_volume: f64) -> VolumeRegime {
        if avg_volume == 0.0 { return VolumeRegime::Normal; }

        let ratio = volume / avg_volume;
        if ratio >= self.thresholds.volume_climax_threshold {
            VolumeRegime::Extreme
        } else if ratio >= self.thresholds.volume_spike_multiple {
            VolumeRegime::High
        } else if ratio <= self.thresholds.volume_dry_up_threshold {
            VolumeRegime::Low
        } else {
            VolumeRegime::Normal
        }
    }

    fn check_volume_spike(&mut self, timestamp: i64) {
        if self.avg_volume_1m > 0.0 {
            let ratio = self.volume_1m / self.avg_volume_1m;
            if ratio >= self.thresholds.volume_spike_multiple {
                if self.should_fire(VOLUME_SPIKE, timestamp) {
                    self.publish_event(VOLUME_SPIKE, timestamp, json!({
                        "volume_1m": self.volume_1m, "avg_volume": self.avg_volume_1m, "ratio": ratio,
                        "threshold": self.thresholds.volume_spike_multiple,
                    }));
                }
            }
        }
    }

    fn check_volume_dry_up(&mut self, timestamp: i64) {
        if self.avg_volume_1m > 0.0 {
            let ratio = self.volume_1m / self.avg_volume_1m;
            if ratio <= self.thresholds.volume_dry_up_threshold {
                if self.should_fire(VOLUME_DRY_UP, timestamp) {
                    self.publish_event(VOLUME_DRY_UP, timestamp, json!({
                        "volume_1m": self.volume_1m, "avg_volume": self.avg_volume_1m, "ratio": ratio,
                        "threshold": self.thresholds.volume_dry_up_threshold,
                    }));
                }
            }
        }
    }

    fn check_volume_regime_change(&mut self, timestamp: i64) {
        if self.current_regime != self.prev_regime {
            if self.should_fire(VOLUME_REGIME_CHANGE, timestamp) {
                self.publish_event(VOLUME_REGIME_CHANGE, timestamp, json!({
                    "prev_regime": format!("{:?}", self.prev_regime),
                    "new_regime": format!("{:?}", self.current_regime),
                    "volume_1m": self.volume_1m, "avg_volume": self.avg_volume_1m,
                }));
            }
        }
    }

    fn check_volume_ratio_extreme(&mut self, timestamp: i64) {
        let ratio_pct = self.buy_sell_ratio_1m * 100.0;
        if ratio_pct >= self.thresholds.volume_ratio_extreme || ratio_pct <= (100.0 - self.thresholds.volume_ratio_extreme) {
            if self.should_fire(VOLUME_RATIO_EXTREME, timestamp) {
                self.publish_event(VOLUME_RATIO_EXTREME, timestamp, json!({
                    "buy_volume": self.buy_volume_1m, "sell_volume": self.sell_volume_1m,
                    "buy_ratio_pct": ratio_pct, "threshold": self.thresholds.volume_ratio_extreme,
                }));
            }
        }
    }

    fn check_volume_acceleration(&mut self, timestamp: i64) {
        if self.volume_acceleration_1m.abs() > self.thresholds.volume_acceleration_threshold {
            if self.should_fire(VOLUME_ACCELERATION, timestamp) {
                self.publish_event(VOLUME_ACCELERATION, timestamp, json!({
                    "acceleration": self.volume_acceleration_1m,
                    "velocity": self.volume_velocity_1m, "volume_1m": self.volume_1m,
                }));
            }
        }
    }

    fn check_volume_exhaustion(&mut self, timestamp: i64) {
        if self.avg_volume_1m > 0.0 {
            let ratio = self.volume_1m / self.avg_volume_1m;
            if ratio > self.thresholds.volume_spike_multiple && self.volume_velocity_1m < 0.0 {
                if self.should_fire(VOLUME_EXHAUSTION, timestamp) {
                    self.publish_event(VOLUME_EXHAUSTION, timestamp, json!({
                        "volume_1m": self.volume_1m, "velocity": self.volume_velocity_1m, "ratio": ratio,
                    }));
                }
            }
        }
    }

    fn check_volume_climax(&mut self, timestamp: i64) {
        if self.avg_volume_1m > 0.0 {
            let ratio = self.volume_1m / self.avg_volume_1m;
            if ratio >= self.thresholds.volume_climax_threshold {
                if self.should_fire(VOLUME_CLIMAX, timestamp) {
                    self.publish_event(VOLUME_CLIMAX, timestamp, json!({
                        "volume_1m": self.volume_1m, "avg_volume": self.avg_volume_1m,
                        "ratio": ratio, "max_volume": self.max_volume_1m,
                    }));
                }
            }
        }
    }

    fn check_volume_distribution(&mut self, timestamp: i64) {
        if self.avg_volume_1m > 0.0 {
            let ratio = self.volume_1m / self.avg_volume_1m;
            if ratio >= self.thresholds.volume_spike_multiple && !self.price_rising && self.prev_price > 0.0 {
                if self.should_fire(VOLUME_DISTRIBUTION, timestamp) {
                    self.publish_event(VOLUME_DISTRIBUTION, timestamp, json!({
                        "volume_1m": self.volume_1m, "avg_volume": self.avg_volume_1m, "ratio": ratio,
                        "price": self.last_price, "prev_price": self.prev_price,
                    }));
                }
            }
        }
    }

    fn check_volume_accumulation(&mut self, timestamp: i64) {
        if self.avg_volume_1m > 0.0 {
            let ratio = self.volume_1m / self.avg_volume_1m;
            if ratio >= self.thresholds.volume_spike_multiple && self.price_rising && self.prev_price > 0.0 {
                if self.should_fire(VOLUME_ACCUMULATION, timestamp) {
                    self.publish_event(VOLUME_ACCUMULATION, timestamp, json!({
                        "volume_1m": self.volume_1m, "avg_volume": self.avg_volume_1m, "ratio": ratio,
                        "price": self.last_price, "prev_price": self.prev_price,
                    }));
                }
            }
        }
    }

    fn check_volume_churn(&mut self, timestamp: i64) {
        if self.avg_volume_1m > 0.0 && self.prev_price > 0.0 {
            let ratio = self.volume_1m / self.avg_volume_1m;
            let price_change_pct = ((self.last_price - self.prev_price) / self.prev_price).abs() * 100.0;

            if ratio >= self.thresholds.volume_spike_multiple && price_change_pct < 0.1 {
                if self.should_fire(VOLUME_CHURN, timestamp) {
                    self.publish_event(VOLUME_CHURN, timestamp, json!({
                        "volume_1m": self.volume_1m, "avg_volume": self.avg_volume_1m,
                        "ratio": ratio, "price_change_pct": price_change_pct,
                    }));
                }
            }
        }
    }

    fn check_volume_divergence(&mut self, timestamp: i64) {
        if self.prev_price > 0.0 && self.prev_volume_1m > 0.0 {
            let price_direction_up = self.last_price > self.prev_price;
            let volume_direction_up = self.volume_1m > self.prev_volume_1m;

            if price_direction_up != volume_direction_up {
                if self.should_fire(VOLUME_DIVERGENCE, timestamp) {
                    let price_dir = if price_direction_up { Direction::Long } else { Direction::Short };
                    let volume_dir = if volume_direction_up { Direction::Long } else { Direction::Short };
                    self.publish_event(VOLUME_DIVERGENCE, timestamp, json!({
                        "price_direction": format!("{:?}", price_dir),
                        "volume_direction": format!("{:?}", volume_dir),
                        "volume_1m": self.volume_1m, "prev_volume": self.prev_volume_1m,
                    }));
                }
            }
        }
    }

    fn check_volume_confirmation(&mut self, timestamp: i64) {
        if self.prev_price > 0.0 && self.prev_volume_1m > 0.0 && self.avg_volume_1m > 0.0 {
            let price_direction_up = self.last_price > self.prev_price;
            let volume_direction_up = self.volume_1m > self.prev_volume_1m;
            let volume_significant = self.volume_1m >= self.avg_volume_1m;

            if price_direction_up == volume_direction_up && volume_significant {
                if self.should_fire(VOLUME_CONFIRMATION, timestamp) {
                    let direction = if price_direction_up { Direction::Long } else { Direction::Short };
                    self.publish_event(VOLUME_CONFIRMATION, timestamp, json!({
                        "direction": format!("{:?}", direction),
                        "volume_1m": self.volume_1m, "avg_volume": self.avg_volume_1m,
                    }));
                }
            }
        }
    }

    fn should_fire(&self, event_type: &str, current_time: i64) -> bool {
        if let Some(&last_time) = self.last_event_times.get(event_type) {
            current_time - last_time >= self.min_event_interval_ms
        } else { true }
    }

    fn publish_event(&mut self, event_type: &str, timestamp: i64, mut data: serde_json::Value) {
        self.events_fired += 1;
        if let Some(obj) = data.as_object_mut() {
            obj.insert("symbol".to_string(), json!(self.symbol));
        }
        publish_event(event_type, timestamp, serde_json::from_value(data).unwrap_or_default(),
            "trade_aggregator", EventPriority::Medium);
        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(symbol = %self.symbol, event = event_type, "Volume event published");
    }

    pub fn trades_processed(&self) -> u64 { self.trades_processed }
    pub fn events_fired(&self) -> u64 { self.events_fired }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_trade(price: f64, qty: f64, is_buyer_maker: bool) -> ParsedAggTrade {
        ParsedAggTrade {
            symbol: "BTCUSDT".to_string(), agg_trade_id: 0, price, quantity: qty,
            first_trade_id: 0, last_trade_id: 0, timestamp: 0, is_buyer_maker, event_time: 0,
        }
    }

    #[test]
    fn test_volume_calculation() {
        let thresholds = AggregatorThresholds::default();
        let mut tracker = VolumeTracker::new("BTCUSDT".to_string(), thresholds);

        tracker.add_trade(&make_trade(95000.0, 1.0, false), 1000);
        tracker.add_trade(&make_trade(95001.0, 2.0, false), 2000);
        tracker.add_trade(&make_trade(94999.0, 0.5, true), 3000);
        tracker.recalculate_volume();

        assert!((tracker.volume_1m - 3.5).abs() < 0.001);
    }

    #[test]
    fn test_buy_sell_ratio() {
        let thresholds = AggregatorThresholds::default();
        let mut tracker = VolumeTracker::new("BTCUSDT".to_string(), thresholds);

        for i in 0..3 { tracker.add_trade(&make_trade(95000.0, 1.0, false), i * 1000); }
        tracker.add_trade(&make_trade(95000.0, 1.0, true), 3000);
        tracker.recalculate_volume();

        assert!((tracker.buy_sell_ratio_1m - 0.75).abs() < 0.01);
    }
}
