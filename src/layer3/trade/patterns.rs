// Pattern Detector - Identifies trade patterns and institutional behavior
// Generates 10 pattern-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer2::parser::ParsedAggTrade;
use crate::layer3::common::event_types::*;
use crate::layer3::common::time_windows::TimeWindow;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Price level tracking for iceberg detection
#[derive(Debug, Clone)]
struct PriceLevelActivity {
    price: f64,
    trade_count: u32,
    total_volume: f64,
    last_trade_time: i64,
}

/// PatternDetector identifies trade patterns and institutional behavior
pub struct PatternDetector {
    symbol: String,

    // Trade history
    recent_trades: TimeWindow<ParsedAggTrade>,
    cluster_window: TimeWindow<ParsedAggTrade>,
    absorption_window: TimeWindow<ParsedAggTrade>,

    // Trade rate tracking
    trade_times: Vec<i64>,
    max_trade_times: usize,
    avg_trade_rate: f64,

    // Large trade tracking
    last_large_trade_time: i64,
    last_whale_trade_time: i64,

    // Aggression scoring
    taker_buy_volume: f64,
    taker_sell_volume: f64,
    aggression_score: f64,

    // Sweep detection
    price_levels_hit: Vec<f64>,
    sweep_window_start: i64,

    // Iceberg detection
    price_level_map: HashMap<i64, PriceLevelActivity>,

    // Spoofing detection
    volume_bursts: Vec<(i64, f64)>,

    last_price: f64,
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,
    trades_processed: u64,
    events_fired: u64,
}

impl PatternDetector {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            recent_trades: TimeWindow::new(60_000, 10_000),
            cluster_window: TimeWindow::new(thresholds.cluster_time_window_ms, 1000),
            absorption_window: TimeWindow::new(thresholds.absorption_detection_window_ms, 1000),
            trade_times: Vec::new(), max_trade_times: 100, avg_trade_rate: 0.0,
            last_large_trade_time: 0, last_whale_trade_time: 0,
            taker_buy_volume: 0.0, taker_sell_volume: 0.0, aggression_score: 0.5,
            price_levels_hit: Vec::new(), sweep_window_start: 0,
            price_level_map: HashMap::new(), volume_bursts: Vec::new(),
            last_price: 0.0, last_event_times: HashMap::new(), min_event_interval_ms: 1000,
            thresholds,
            trades_processed: 0, events_fired: 0,
        }
    }

    pub fn add_trade(&mut self, trade: &ParsedAggTrade, current_time: i64) {
        self.trades_processed += 1;
        self.last_price = trade.price;

        self.recent_trades.add(current_time, trade.clone());
        self.cluster_window.add(current_time, trade.clone());
        self.absorption_window.add(current_time, trade.clone());

        self.recent_trades.prune(current_time);
        self.cluster_window.prune(current_time);
        self.absorption_window.prune(current_time);

        self.trade_times.push(current_time);
        if self.trade_times.len() > self.max_trade_times { self.trade_times.remove(0); }

        let notional = trade.notional_usd();
        if trade.is_buyer_maker { self.taker_sell_volume += notional; }
        else { self.taker_buy_volume += notional; }
        let total = self.taker_buy_volume + self.taker_sell_volume;
        if total > 0.0 { self.aggression_score = self.taker_buy_volume / total; }

        self.update_price_level_activity(trade, current_time);
        self.update_sweep_tracking(trade, current_time);
        self.update_volume_bursts(trade, current_time);
    }

    pub fn check_events(&mut self, current_time: i64) {
        self.recalculate_metrics();

        self.check_large_trade(current_time);
        self.check_whale_trade(current_time);
        self.check_cluster(current_time);
        self.check_rate_spike(current_time);
        self.check_institutional(current_time);
        self.check_aggression(current_time);
        self.check_absorption(current_time);
        self.check_sweep(current_time);
        self.check_iceberg(current_time);
        self.check_spoofing(current_time);
    }

    fn recalculate_metrics(&mut self) {
        if self.trade_times.len() >= 2 {
            let time_span = (self.trade_times.last().unwrap() - self.trade_times.first().unwrap()) as f64 / 1000.0;
            if time_span > 0.0 { self.avg_trade_rate = self.trade_times.len() as f64 / time_span; }
        }

        let cutoff_time = self.trade_times.last().copied().unwrap_or(0) - 60_000;
        self.price_level_map.retain(|_, level| level.last_trade_time >= cutoff_time);
        self.volume_bursts.retain(|(ts, _)| *ts >= cutoff_time);
    }

    fn update_price_level_activity(&mut self, trade: &ParsedAggTrade, current_time: i64) {
        let price_key = (trade.price * 10.0) as i64;

        self.price_level_map.entry(price_key)
            .and_modify(|level| {
                level.trade_count += 1;
                level.total_volume += trade.quantity;
                level.last_trade_time = current_time;
            })
            .or_insert(PriceLevelActivity {
                price: trade.price, trade_count: 1, total_volume: trade.quantity, last_trade_time: current_time,
            });
    }

    fn update_sweep_tracking(&mut self, trade: &ParsedAggTrade, current_time: i64) {
        if current_time - self.sweep_window_start > 5000 {
            self.price_levels_hit.clear();
            self.sweep_window_start = current_time;
        }
        if !self.price_levels_hit.iter().any(|&p| (p - trade.price).abs() < 1.0) {
            self.price_levels_hit.push(trade.price);
        }
    }

    fn update_volume_bursts(&mut self, trade: &ParsedAggTrade, current_time: i64) {
        let notional = trade.notional_usd();
        if let Some((last_ts, last_vol)) = self.volume_bursts.last_mut() {
            if current_time - *last_ts < 1000 { *last_vol += notional; return; }
        }
        self.volume_bursts.push((current_time, notional));
    }

    fn check_large_trade(&mut self, timestamp: i64) {
        if let Some((_, trade)) = self.recent_trades.iter().last() {
            let notional = trade.notional_usd();
            if notional >= self.thresholds.large_trade_usd {
                let time_since_last = timestamp - self.last_large_trade_time;
                if time_since_last >= self.min_event_interval_ms {
                    self.last_large_trade_time = timestamp;
                    self.publish_event(PATTERN_LARGE_TRADE, timestamp, json!({
                        "price": trade.price, "quantity": trade.quantity,
                        "notional_usd": notional, "side": trade.side(),
                        "threshold": self.thresholds.large_trade_usd,
                    }));
                }
            }
        }
    }

    fn check_whale_trade(&mut self, timestamp: i64) {
        if let Some((_, trade)) = self.recent_trades.iter().last() {
            let notional = trade.notional_usd();
            if notional >= self.thresholds.whale_trade_usd {
                let time_since_last = timestamp - self.last_whale_trade_time;
                if time_since_last >= self.min_event_interval_ms {
                    self.last_whale_trade_time = timestamp;
                    self.publish_event(PATTERN_WHALE_TRADE, timestamp, json!({
                        "price": trade.price, "quantity": trade.quantity,
                        "notional_usd": notional, "side": trade.side(),
                        "threshold": self.thresholds.whale_trade_usd,
                    }));
                }
            }
        }
    }

    fn check_cluster(&mut self, timestamp: i64) {
        let trade_count = self.cluster_window.len();
        if trade_count >= self.thresholds.cluster_min_trades as usize {
            if self.should_fire(PATTERN_CLUSTER, timestamp) {
                let total_volume: f64 = self.cluster_window.iter().map(|(_, t)| t.quantity).sum();
                let total_notional: f64 = self.cluster_window.iter().map(|(_, t)| t.notional_usd()).sum();
                self.publish_event(PATTERN_CLUSTER, timestamp, json!({
                    "trade_count": trade_count, "total_volume": total_volume,
                    "total_notional_usd": total_notional,
                    "window_ms": self.thresholds.cluster_time_window_ms,
                    "threshold": self.thresholds.cluster_min_trades,
                }));
            }
        }
    }

    fn check_rate_spike(&mut self, timestamp: i64) {
        if self.avg_trade_rate > 0.0 && self.trade_times.len() >= 10 {
            let recent_count = 10.min(self.trade_times.len());
            let recent_span = (self.trade_times[self.trade_times.len() - 1]
                             - self.trade_times[self.trade_times.len() - recent_count]) as f64 / 1000.0;
            if recent_span > 0.0 {
                let recent_rate = recent_count as f64 / recent_span;
                let rate_ratio = recent_rate / self.avg_trade_rate;
                if rate_ratio >= self.thresholds.rate_spike_threshold {
                    if self.should_fire(PATTERN_RATE_SPIKE, timestamp) {
                        self.publish_event(PATTERN_RATE_SPIKE, timestamp, json!({
                            "recent_rate": recent_rate, "avg_rate": self.avg_trade_rate,
                            "ratio": rate_ratio, "threshold": self.thresholds.rate_spike_threshold,
                        }));
                    }
                }
            }
        }
    }

    fn check_institutional(&mut self, timestamp: i64) {
        let recent_large_trades: Vec<&ParsedAggTrade> = self.recent_trades.iter()
            .map(|(_, t)| t)
            .filter(|t| t.notional_usd() >= self.thresholds.institutional_trade_min_usd)
            .collect();

        if recent_large_trades.len() >= 3 {
            let buy_count = recent_large_trades.iter().filter(|t| !t.is_buyer_maker).count();
            let sell_count = recent_large_trades.len() - buy_count;
            let directional_ratio = buy_count.max(sell_count) as f64 / recent_large_trades.len() as f64;

            if directional_ratio >= 0.7 {
                if self.should_fire(PATTERN_INSTITUTIONAL, timestamp) {
                    let total_notional: f64 = recent_large_trades.iter().map(|t| t.notional_usd()).sum();
                    self.publish_event(PATTERN_INSTITUTIONAL, timestamp, json!({
                        "large_trade_count": recent_large_trades.len(),
                        "total_notional_usd": total_notional,
                        "directional_ratio": directional_ratio,
                        "direction": if buy_count > sell_count { "BUY" } else { "SELL" },
                    }));
                }
            }
        }
    }

    fn check_aggression(&mut self, timestamp: i64) {
        if self.aggression_score >= self.thresholds.aggression_score_threshold
           || self.aggression_score <= (1.0 - self.thresholds.aggression_score_threshold) {
            if self.should_fire(PATTERN_AGGRESSION, timestamp) {
                self.publish_event(PATTERN_AGGRESSION, timestamp, json!({
                    "aggression_score": self.aggression_score,
                    "taker_buy_volume": self.taker_buy_volume,
                    "taker_sell_volume": self.taker_sell_volume,
                    "direction": if self.aggression_score > 0.5 { "BUY" } else { "SELL" },
                    "threshold": self.thresholds.aggression_score_threshold,
                }));
            }
        }
    }

    fn check_absorption(&mut self, timestamp: i64) {
        if self.absorption_window.len() >= 5 {
            let first_price = self.absorption_window.iter().next().map(|(_, t)| t.price).unwrap_or(0.0);
            let last_price = self.absorption_window.iter().last().map(|(_, t)| t.price).unwrap_or(0.0);
            let total_notional: f64 = self.absorption_window.iter().map(|(_, t)| t.notional_usd()).sum();

            if first_price > 0.0 {
                let price_change_pct = ((last_price - first_price) / first_price).abs() * 100.0;
                if total_notional >= self.thresholds.large_trade_usd && price_change_pct < 0.1 {
                    if self.should_fire(PATTERN_ABSORPTION, timestamp) {
                        self.publish_event(PATTERN_ABSORPTION, timestamp, json!({
                            "total_notional_usd": total_notional,
                            "price_change_pct": price_change_pct,
                            "trade_count": self.absorption_window.len(),
                        }));
                    }
                }
            }
        }
    }

    fn check_sweep(&mut self, timestamp: i64) {
        if self.price_levels_hit.len() >= 5 {
            let time_span = timestamp - self.sweep_window_start;
            if time_span <= 5000 {
                if self.should_fire(PATTERN_SWEEP, timestamp) {
                    let price_range = self.price_levels_hit.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
                                    - self.price_levels_hit.iter().cloned().fold(f64::INFINITY, f64::min);
                    self.publish_event(PATTERN_SWEEP, timestamp, json!({
                        "levels_hit": self.price_levels_hit.len(),
                        "price_range": price_range, "time_span_ms": time_span,
                    }));
                }
            }
        }
    }

    fn check_iceberg(&mut self, timestamp: i64) {
        let iceberg_levels: Vec<_> = self.price_level_map.iter()
            .filter(|(_, level)| level.trade_count >= self.thresholds.iceberg_detection_min_trades)
            .map(|(_, level)| (level.price, level.trade_count, level.total_volume))
            .collect();

        for (price, trade_count, total_volume) in iceberg_levels {
            if self.should_fire(PATTERN_ICEBERG, timestamp) {
                self.publish_event(PATTERN_ICEBERG, timestamp, json!({
                    "price": price, "trade_count": trade_count,
                    "total_volume": total_volume,
                    "threshold": self.thresholds.iceberg_detection_min_trades,
                }));
            }
        }
    }

    fn check_spoofing(&mut self, timestamp: i64) {
        if self.volume_bursts.len() >= 3 {
            let large_bursts = self.volume_bursts.iter().filter(|(_, vol)| *vol >= self.thresholds.large_trade_usd).count();

            if large_bursts >= 2 && self.last_price > 0.0 {
                let first_burst_time = self.volume_bursts.first().map(|(ts, _)| *ts).unwrap_or(0);
                let price_then = self.recent_trades.iter()
                    .find(|(ts, _)| *ts >= first_burst_time).map(|(_, t)| t.price).unwrap_or(self.last_price);

                if price_then > 0.0 {
                    let price_change_pct = ((self.last_price - price_then) / price_then).abs() * 100.0;
                    if price_change_pct < 0.2 {
                        if self.should_fire(PATTERN_SPOOFING, timestamp) {
                            self.publish_event(PATTERN_SPOOFING, timestamp, json!({
                                "burst_count": large_bursts,
                                "price_change_pct": price_change_pct,
                                "pattern": "rapid_volume_no_follow_through",
                            }));
                        }
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
            "trade_aggregator", EventPriority::High);
        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(symbol = %self.symbol, event = event_type, "Pattern event published");
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
    fn test_large_trade() {
        let thresholds = AggregatorThresholds::default();
        let mut detector = PatternDetector::new("BTCUSDT".to_string(), thresholds);
        detector.add_trade(&make_trade(95000.0, 0.15, false), 1000);
        detector.check_events(1000);
    }

    #[test]
    fn test_aggression_score() {
        let thresholds = AggregatorThresholds::default();
        let mut detector = PatternDetector::new("BTCUSDT".to_string(), thresholds);

        for i in 0..4 { detector.add_trade(&make_trade(95000.0, 1.0, false), i * 1000); }
        detector.add_trade(&make_trade(95000.0, 1.0, true), 4000);

        assert!(detector.aggression_score > 0.75);
    }
}
