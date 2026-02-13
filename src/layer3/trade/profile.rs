// Volume Profile - Tracks volume distribution across price levels
// Generates 8 profile-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer2::parser::ParsedAggTrade;
use crate::layer3::common::event_types::*;
use crate::layer3::common::time_windows::TimeWindow;
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use tracing::debug;

/// Volume bucket for a price level
#[derive(Debug, Clone)]
struct VolumeBucket {
    price: f64,
    total_volume: f64,
    buy_volume: f64,
    sell_volume: f64,
    trade_count: u32,
}

/// VolumeProfile tracks volume distribution across price levels
pub struct VolumeProfile {
    symbol: String,
    price_buckets: BTreeMap<i64, VolumeBucket>,
    tick_size: f64,
    last_recalc_time: i64,
    recalc_interval_ms: i64,
    trades: TimeWindow<ParsedAggTrade>,

    current_poc: Option<f64>,
    prev_poc: Option<f64>,
    value_area_high: Option<f64>,
    value_area_low: Option<f64>,
    total_volume: f64,

    last_price: f64,
    prev_price: f64,
    avg_bucket_volume: f64,
    high_volume_nodes: Vec<f64>,
    low_volume_nodes: Vec<f64>,
    single_print_levels: Vec<f64>,
    profile_is_balanced: bool,

    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,
    thresholds: AggregatorThresholds,
    trades_processed: u64,
    events_fired: u64,
}

impl VolumeProfile {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol, price_buckets: BTreeMap::new(),
            tick_size: thresholds.profile_tick_size,
            last_recalc_time: 0,
            recalc_interval_ms: thresholds.profile_recalc_interval_ms,
            trades: TimeWindow::new(thresholds.profile_duration_ms, 100_000),
            current_poc: None, prev_poc: None,
            value_area_high: None, value_area_low: None,
            total_volume: 0.0,
            last_price: 0.0, prev_price: 0.0,
            avg_bucket_volume: 0.0,
            high_volume_nodes: Vec::new(),
            low_volume_nodes: Vec::new(),
            single_print_levels: Vec::new(),
            profile_is_balanced: false,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 5000,
            thresholds,
            trades_processed: 0, events_fired: 0,
        }
    }

    pub fn add_trade(&mut self, trade: &ParsedAggTrade, current_time: i64) {
        self.trades_processed += 1;
        self.prev_price = self.last_price;
        self.last_price = trade.price;

        self.trades.add(current_time, trade.clone());
        self.trades.prune(current_time);

        let price_key = self.price_to_bucket_key(trade.price);
        self.price_buckets.entry(price_key)
            .and_modify(|bucket| {
                bucket.total_volume += trade.quantity;
                if trade.is_buyer_maker { bucket.sell_volume += trade.quantity; }
                else { bucket.buy_volume += trade.quantity; }
                bucket.trade_count += 1;
            })
            .or_insert(VolumeBucket {
                price: trade.price, total_volume: trade.quantity,
                buy_volume: if trade.is_buyer_maker { 0.0 } else { trade.quantity },
                sell_volume: if trade.is_buyer_maker { trade.quantity } else { 0.0 },
                trade_count: 1,
            });
    }

    pub fn check_events(&mut self, current_time: i64) {
        if current_time - self.last_recalc_time >= self.recalc_interval_ms {
            self.recalculate_profile();
            self.last_recalc_time = current_time;
        }

        if self.current_poc.is_none() { return; }

        self.check_poc_shift(current_time);
        self.check_poc_test(current_time);
        self.check_value_area_break(current_time);
        self.check_value_area_retest(current_time);
        self.check_high_volume_node(current_time);
        self.check_low_volume_node(current_time);
        self.check_single_prints(current_time);
        self.check_balance(current_time);
    }

    fn price_to_bucket_key(&self, price: f64) -> i64 { (price / self.tick_size).round() as i64 }
    fn bucket_key_to_price(&self, key: i64) -> f64 { key as f64 * self.tick_size }

    fn recalculate_profile(&mut self) {
        self.price_buckets.clear();

        for (_, trade) in self.trades.iter() {
            let price_key = self.price_to_bucket_key(trade.price);
            self.price_buckets.entry(price_key)
                .and_modify(|bucket| {
                    bucket.total_volume += trade.quantity;
                    if trade.is_buyer_maker { bucket.sell_volume += trade.quantity; }
                    else { bucket.buy_volume += trade.quantity; }
                    bucket.trade_count += 1;
                })
                .or_insert(VolumeBucket {
                    price: trade.price, total_volume: trade.quantity,
                    buy_volume: if trade.is_buyer_maker { 0.0 } else { trade.quantity },
                    sell_volume: if trade.is_buyer_maker { trade.quantity } else { 0.0 },
                    trade_count: 1,
                });
        }

        self.prev_poc = self.current_poc;
        self.current_poc = self.calculate_poc();
        self.calculate_value_area();
        self.total_volume = self.price_buckets.values().map(|b| b.total_volume).sum();

        if !self.price_buckets.is_empty() {
            self.avg_bucket_volume = self.total_volume / self.price_buckets.len() as f64;
        }

        self.identify_nodes();
        self.check_profile_balance();
    }

    fn calculate_poc(&self) -> Option<f64> {
        self.price_buckets.values()
            .max_by(|a, b| a.total_volume.partial_cmp(&b.total_volume).unwrap())
            .map(|bucket| bucket.price)
    }

    fn calculate_value_area(&mut self) {
        if self.price_buckets.is_empty() || self.current_poc.is_none() {
            self.value_area_high = None;
            self.value_area_low = None;
            return;
        }

        let target_volume = self.total_volume * self.thresholds.value_area_percentage;
        let poc = self.current_poc.unwrap();
        let poc_key = self.price_to_bucket_key(poc);

        let mut accumulated_volume = self.price_buckets.get(&poc_key)
            .map(|b| b.total_volume).unwrap_or(0.0);

        let mut upper_key = poc_key;
        let mut lower_key = poc_key;

        while accumulated_volume < target_volume {
            let upper_next_key = upper_key + 1;
            let upper_volume = self.price_buckets.get(&upper_next_key).map(|b| b.total_volume).unwrap_or(0.0);

            let lower_next_key = lower_key - 1;
            let lower_volume = self.price_buckets.get(&lower_next_key).map(|b| b.total_volume).unwrap_or(0.0);

            if upper_volume >= lower_volume && upper_volume > 0.0 {
                upper_key = upper_next_key;
                accumulated_volume += upper_volume;
            } else if lower_volume > 0.0 {
                lower_key = lower_next_key;
                accumulated_volume += lower_volume;
            } else { break; }
        }

        self.value_area_high = Some(self.bucket_key_to_price(upper_key));
        self.value_area_low = Some(self.bucket_key_to_price(lower_key));
    }

    fn identify_nodes(&mut self) {
        self.high_volume_nodes.clear();
        self.low_volume_nodes.clear();
        self.single_print_levels.clear();

        for bucket in self.price_buckets.values() {
            if bucket.total_volume >= self.avg_bucket_volume * self.thresholds.high_volume_node_threshold {
                self.high_volume_nodes.push(bucket.price);
            }
            if bucket.total_volume <= self.avg_bucket_volume * self.thresholds.low_volume_node_threshold {
                self.low_volume_nodes.push(bucket.price);
            }
            if bucket.trade_count == 1 {
                self.single_print_levels.push(bucket.price);
            }
        }
    }

    fn check_profile_balance(&mut self) {
        if self.current_poc.is_none() || self.price_buckets.len() < 3 {
            self.profile_is_balanced = false;
            return;
        }

        let poc = self.current_poc.unwrap();
        let mut volume_above = 0.0;
        let mut volume_below = 0.0;

        for bucket in self.price_buckets.values() {
            if bucket.price > poc { volume_above += bucket.total_volume; }
            else if bucket.price < poc { volume_below += bucket.total_volume; }
        }

        if volume_above > 0.0 && volume_below > 0.0 {
            let ratio = volume_above / volume_below;
            self.profile_is_balanced = ratio >= 0.8 && ratio <= 1.2;
        } else {
            self.profile_is_balanced = false;
        }
    }

    fn check_poc_shift(&mut self, timestamp: i64) {
        if let (Some(current), Some(prev)) = (self.current_poc, self.prev_poc) {
            let shift_ticks = ((current - prev) / self.tick_size).abs();
            if shift_ticks >= self.thresholds.poc_shift_threshold {
                if self.should_fire(PROFILE_POC_SHIFT, timestamp) {
                    self.publish_event(PROFILE_POC_SHIFT, timestamp, json!({
                        "prev_poc": prev, "new_poc": current,
                        "shift_ticks": shift_ticks, "threshold": self.thresholds.poc_shift_threshold,
                    }));
                }
            }
        }
    }

    fn check_poc_test(&mut self, timestamp: i64) {
        if let Some(poc) = self.current_poc {
            let distance = (self.last_price - poc).abs();
            let tolerance = poc * 0.001;
            if distance <= tolerance {
                if self.should_fire(PROFILE_POC_TEST, timestamp) {
                    self.publish_event(PROFILE_POC_TEST, timestamp, json!({
                        "poc": poc, "price": self.last_price, "distance": distance,
                    }));
                }
            }
        }
    }

    fn check_value_area_break(&mut self, timestamp: i64) {
        if let (Some(va_high), Some(va_low)) = (self.value_area_high, self.value_area_low) {
            let broke_above = self.last_price > va_high && self.prev_price <= va_high && self.prev_price > 0.0;
            let broke_below = self.last_price < va_low && self.prev_price >= va_low && self.prev_price > 0.0;

            if broke_above || broke_below {
                if self.should_fire(PROFILE_VALUE_AREA_BREAK, timestamp) {
                    self.publish_event(PROFILE_VALUE_AREA_BREAK, timestamp, json!({
                        "direction": if broke_above { "above" } else { "below" },
                        "price": self.last_price,
                        "value_area_high": va_high, "value_area_low": va_low,
                    }));
                }
            }
        }
    }

    fn check_value_area_retest(&mut self, timestamp: i64) {
        if let (Some(va_high), Some(va_low)) = (self.value_area_high, self.value_area_low) {
            let at_upper_boundary = (self.last_price - va_high).abs() < self.tick_size;
            let at_lower_boundary = (self.last_price - va_low).abs() < self.tick_size;

            if at_upper_boundary || at_lower_boundary {
                if self.should_fire(PROFILE_VALUE_AREA_RETEST, timestamp) {
                    self.publish_event(PROFILE_VALUE_AREA_RETEST, timestamp, json!({
                        "boundary": if at_upper_boundary { "upper" } else { "lower" },
                        "price": self.last_price,
                        "value_area_high": va_high, "value_area_low": va_low,
                    }));
                }
            }
        }
    }

    fn check_high_volume_node(&mut self, timestamp: i64) {
        let nodes = self.high_volume_nodes.clone();
        for hvn_price in nodes {
            if (self.last_price - hvn_price).abs() < self.tick_size {
                if self.should_fire(PROFILE_HIGH_VOLUME_NODE, timestamp) {
                    self.publish_event(PROFILE_HIGH_VOLUME_NODE, timestamp, json!({
                        "price": self.last_price, "hvn_price": hvn_price,
                        "threshold": self.thresholds.high_volume_node_threshold,
                    }));
                }
            }
        }
    }

    fn check_low_volume_node(&mut self, timestamp: i64) {
        let nodes = self.low_volume_nodes.clone();
        for lvn_price in nodes {
            if (self.last_price - lvn_price).abs() < self.tick_size {
                if self.should_fire(PROFILE_LOW_VOLUME_NODE, timestamp) {
                    self.publish_event(PROFILE_LOW_VOLUME_NODE, timestamp, json!({
                        "price": self.last_price, "lvn_price": lvn_price,
                        "threshold": self.thresholds.low_volume_node_threshold,
                    }));
                }
            }
        }
    }

    fn check_single_prints(&mut self, timestamp: i64) {
        if !self.single_print_levels.is_empty() {
            let levels = self.single_print_levels.clone();
            for sp_price in levels {
                if (self.last_price - sp_price).abs() < self.tick_size {
                    if self.should_fire(PROFILE_SINGLE_PRINTS, timestamp) {
                        self.publish_event(PROFILE_SINGLE_PRINTS, timestamp, json!({
                            "price": self.last_price, "single_print_price": sp_price,
                            "single_print_count": self.single_print_levels.len(),
                        }));
                    }
                }
            }
        }
    }

    fn check_balance(&mut self, timestamp: i64) {
        if self.profile_is_balanced {
            if self.should_fire(PROFILE_BALANCE, timestamp) {
                self.publish_event(PROFILE_BALANCE, timestamp, json!({
                    "poc": self.current_poc,
                    "value_area_high": self.value_area_high,
                    "value_area_low": self.value_area_low,
                    "total_buckets": self.price_buckets.len(),
                }));
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
        debug!(symbol = %self.symbol, event = event_type, "Profile event published");
    }

    pub fn trades_processed(&self) -> u64 { self.trades_processed }
    pub fn events_fired(&self) -> u64 { self.events_fired }
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
    fn test_poc_calculation() {
        let thresholds = AggregatorThresholds::default();
        let mut profile = VolumeProfile::new("BTCUSDT".to_string(), thresholds);

        for _ in 0..5 { profile.add_trade(&make_trade(95000.0, 1.0), 1000); }
        for _ in 0..10 { profile.add_trade(&make_trade(95010.0, 1.0), 2000); }
        for _ in 0..3 { profile.add_trade(&make_trade(95020.0, 1.0), 3000); }

        profile.recalculate_profile();

        let poc = profile.current_poc.unwrap();
        assert!((poc - 95010.0).abs() < 1.0);
    }
}
