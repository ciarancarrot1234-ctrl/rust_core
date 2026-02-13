// Streak Tracker - Monitors consecutive trade sequences
// Generates 8 streak-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::core::types::OrderSide;
use crate::layer2::parser::ParsedAggTrade;
use crate::layer3::common::event_types::*;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Active streak information
#[derive(Debug, Clone)]
struct Streak {
    side: OrderSide,
    trade_count: u32,
    start_time: i64,
    last_trade_time: i64,
    start_price: f64,
    last_price: f64,
    total_volume: f64,
    intensity: f64,
}

impl Streak {
    fn duration_ms(&self) -> i64 { self.last_trade_time - self.start_time }

    fn price_movement_pct(&self) -> f64 {
        if self.start_price > 0.0 {
            ((self.last_price - self.start_price) / self.start_price).abs() * 100.0
        } else { 0.0 }
    }

    fn calculate_intensity(&mut self) {
        let duration_sec = self.duration_ms() as f64 / 1000.0;
        if duration_sec > 0.0 { self.intensity = self.trade_count as f64 / duration_sec; }
    }
}

/// StreakTracker monitors consecutive trade sequences
pub struct StreakTracker {
    symbol: String,
    current_streak: Option<Streak>,
    prev_streak: Option<Streak>,

    longest_buy_streak: u32,
    longest_sell_streak: u32,
    longest_streak_overall: u32,

    alternating_count: u32,
    last_trade_side: Option<OrderSide>,
    last_trade_time: i64,

    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,
    thresholds: AggregatorThresholds,
    trades_processed: u64,
    events_fired: u64,
}

impl StreakTracker {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            current_streak: None, prev_streak: None,
            longest_buy_streak: 0, longest_sell_streak: 0, longest_streak_overall: 0,
            alternating_count: 0, last_trade_side: None, last_trade_time: 0,
            last_event_times: HashMap::new(), min_event_interval_ms: 1000,
            thresholds,
            trades_processed: 0, events_fired: 0,
        }
    }

    pub fn add_trade(&mut self, trade: &ParsedAggTrade, current_time: i64) {
        self.trades_processed += 1;

        let side = if trade.is_buyer_maker { OrderSide::Sell } else { OrderSide::Buy };
        let time_since_last = current_time - self.last_trade_time;

        // Alternating detection
        if let Some(ref last_side) = self.last_trade_side {
            if last_side != &side && time_since_last < 1000 { self.alternating_count += 1; }
            else { self.alternating_count = 0; }
        }

        self.last_trade_side = Some(side);
        self.last_trade_time = current_time;

        // Check streak continuation
        if let Some(ref mut streak) = self.current_streak {
            let gap = current_time - streak.last_trade_time;

            if streak.side == side && gap <= self.thresholds.streak_max_gap_ms {
                streak.trade_count += 1;
                streak.last_trade_time = current_time;
                streak.last_price = trade.price;
                streak.total_volume += trade.quantity;
                streak.calculate_intensity();
            } else {
                self.end_current_streak();
                self.start_new_streak(side, trade, current_time);
            }
        } else {
            self.start_new_streak(side, trade, current_time);
        }
    }

    pub fn check_events(&mut self, current_time: i64) {
        self.check_streak_started(current_time);
        self.check_streak_extended(current_time);
        self.check_streak_record(current_time);
        self.check_streak_intensity_high(current_time);
        self.check_streak_intensity_low(current_time);
        self.check_alternating(current_time);
        self.check_streak_momentum(current_time);
    }

    fn start_new_streak(&mut self, side: OrderSide, trade: &ParsedAggTrade, current_time: i64) {
        self.current_streak = Some(Streak {
            side,
            trade_count: 1,
            start_time: current_time,
            last_trade_time: current_time,
            start_price: trade.price,
            last_price: trade.price,
            total_volume: trade.quantity,
            intensity: 0.0,
        });
    }

    fn end_current_streak(&mut self) {
        if let Some(streak) = self.current_streak.take() {
            if streak.side == OrderSide::Buy && streak.trade_count > self.longest_buy_streak {
                self.longest_buy_streak = streak.trade_count;
            }
            if streak.side == OrderSide::Sell && streak.trade_count > self.longest_sell_streak {
                self.longest_sell_streak = streak.trade_count;
            }
            if streak.trade_count > self.longest_streak_overall {
                self.longest_streak_overall = streak.trade_count;
            }
            self.prev_streak = Some(streak);
        }
    }

    fn check_streak_started(&mut self, timestamp: i64) {
        if let Some(ref streak) = self.current_streak {
            if streak.trade_count == self.thresholds.streak_min_trades {
                if self.should_fire(STREAK_STARTED, timestamp) {
                    self.publish_event(STREAK_STARTED, timestamp, json!({
                        "side": streak.side.to_string(),
                        "trade_count": streak.trade_count,
                        "total_volume": streak.total_volume,
                        "start_price": streak.start_price,
                    }));
                }
            }
        }
    }

    fn check_streak_extended(&mut self, timestamp: i64) {
        if let Some(ref streak) = self.current_streak {
            if streak.trade_count > self.thresholds.streak_min_trades
               && (streak.trade_count - self.thresholds.streak_min_trades) % 5 == 0 {
                if self.should_fire(STREAK_EXTENDED, timestamp) {
                    self.publish_event(STREAK_EXTENDED, timestamp, json!({
                        "side": streak.side.to_string(),
                        "trade_count": streak.trade_count,
                        "duration_ms": streak.duration_ms(),
                        "intensity": streak.intensity,
                    }));
                }
            }
        }
    }

    pub fn publish_streak_ended(&mut self, timestamp: i64) {
        if let Some(ref streak) = self.prev_streak {
            if streak.trade_count >= self.thresholds.streak_min_trades {
                if self.should_fire(STREAK_ENDED, timestamp) {
                    self.publish_event(STREAK_ENDED, timestamp, json!({
                        "side": streak.side.to_string(),
                        "trade_count": streak.trade_count,
                        "duration_ms": streak.duration_ms(),
                        "total_volume": streak.total_volume,
                        "price_movement_pct": streak.price_movement_pct(),
                    }));
                }
            }
        }
    }

    fn check_streak_record(&mut self, timestamp: i64) {
        if let Some(ref streak) = self.current_streak {
            let is_buy_record = streak.side == OrderSide::Buy && streak.trade_count > self.longest_buy_streak;
            let is_sell_record = streak.side == OrderSide::Sell && streak.trade_count > self.longest_sell_streak;
            let is_overall_record = streak.trade_count > self.longest_streak_overall;

            if (is_buy_record || is_sell_record) && streak.trade_count >= self.thresholds.streak_record_min_length {
                if self.should_fire(STREAK_RECORD, timestamp) {
                    self.publish_event(STREAK_RECORD, timestamp, json!({
                        "side": streak.side.to_string(),
                        "trade_count": streak.trade_count,
                        "prev_record": if streak.side == OrderSide::Buy {
                            self.longest_buy_streak
                        } else { self.longest_sell_streak },
                        "is_overall_record": is_overall_record,
                    }));
                }
            }
        }
    }

    fn check_streak_intensity_high(&mut self, timestamp: i64) {
        if let Some(ref streak) = self.current_streak {
            if streak.trade_count >= self.thresholds.streak_min_trades
               && streak.intensity >= self.thresholds.streak_intensity_threshold {
                if self.should_fire(STREAK_INTENSITY_HIGH, timestamp) {
                    self.publish_event(STREAK_INTENSITY_HIGH, timestamp, json!({
                        "side": streak.side.to_string(),
                        "intensity": streak.intensity,
                        "trade_count": streak.trade_count,
                        "threshold": self.thresholds.streak_intensity_threshold,
                    }));
                }
            }
        }
    }

    fn check_streak_intensity_low(&mut self, timestamp: i64) {
        if let Some(ref streak) = self.current_streak {
            if streak.trade_count >= self.thresholds.streak_min_trades
               && streak.duration_ms() > 10000 && streak.intensity < 0.5 {
                if self.should_fire(STREAK_INTENSITY_LOW, timestamp) {
                    self.publish_event(STREAK_INTENSITY_LOW, timestamp, json!({
                        "side": streak.side.to_string(),
                        "intensity": streak.intensity,
                        "trade_count": streak.trade_count,
                        "duration_ms": streak.duration_ms(),
                    }));
                }
            }
        }
    }

    fn check_alternating(&mut self, timestamp: i64) {
        if self.alternating_count >= 5 {
            if self.should_fire(STREAK_ALTERNATING, timestamp) {
                self.publish_event(STREAK_ALTERNATING, timestamp, json!({
                    "alternation_count": self.alternating_count,
                    "pattern": "rapid_buy_sell_switching",
                }));
            }
        }
    }

    fn check_streak_momentum(&mut self, timestamp: i64) {
        if let Some(ref streak) = self.current_streak {
            if streak.trade_count >= self.thresholds.streak_min_trades {
                let price_movement = streak.price_movement_pct();
                let price_up = streak.last_price > streak.start_price;
                let is_aligned = (streak.side == OrderSide::Buy && price_up)
                              || (streak.side == OrderSide::Sell && !price_up);

                if is_aligned && price_movement >= 0.1 {
                    if self.should_fire(STREAK_MOMENTUM, timestamp) {
                        self.publish_event(STREAK_MOMENTUM, timestamp, json!({
                            "side": streak.side.to_string(),
                            "trade_count": streak.trade_count,
                            "price_movement_pct": price_movement,
                            "start_price": streak.start_price,
                            "last_price": streak.last_price,
                            "intensity": streak.intensity,
                        }));
                    }
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
        debug!(symbol = %self.symbol, event = event_type, "Streak event published");
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
    fn test_streak_detection() {
        let thresholds = AggregatorThresholds::default();
        let mut tracker = StreakTracker::new("BTCUSDT".to_string(), thresholds);

        for i in 0..8 { tracker.add_trade(&make_trade(95000.0, 1.0, false), i * 500); }

        assert!(tracker.current_streak.is_some());
        let streak = tracker.current_streak.as_ref().unwrap();
        assert_eq!(streak.side, OrderSide::Buy);
        assert_eq!(streak.trade_count, 8);
    }

    #[test]
    fn test_streak_break() {
        let thresholds = AggregatorThresholds::default();
        let mut tracker = StreakTracker::new("BTCUSDT".to_string(), thresholds);

        for i in 0..5 { tracker.add_trade(&make_trade(95000.0, 1.0, false), i * 500); }
        tracker.add_trade(&make_trade(95000.0, 1.0, true), 2500);

        assert!(tracker.prev_streak.is_some());
        let prev = tracker.prev_streak.as_ref().unwrap();
        assert_eq!(prev.side, OrderSide::Buy);
        assert_eq!(prev.trade_count, 5);

        assert!(tracker.current_streak.is_some());
        let current = tracker.current_streak.as_ref().unwrap();
        assert_eq!(current.side, OrderSide::Sell);
    }
}
