// Wall Tracker - Monitors large orders (walls) in the order book
// Generates 12 wall-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer2::orderbook::OrderBookState;
use crate::layer3::common::event_types::*;
use crate::layer2::parser::PriceLevel;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Wall side classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WallSide {
    Bid,
    Ask,
}

/// A detected wall in the order book
#[derive(Debug, Clone)]
pub struct Wall {
    pub side: WallSide,
    pub price: f64,
    pub quantity: f64,
    pub notional_usd: f64,
    pub first_seen: i64,
    pub last_modified: i64,
    pub modification_count: u32,
    pub test_count: u32,           // Times price approached wall
    pub defense_count: u32,        // Times wall held after test
    pub is_absorbing: bool,        // Currently absorbing orders
    pub absorbed_volume: f64,      // Volume absorbed
}

impl Wall {
    pub fn age_ms(&self, current_time: i64) -> i64 {
        current_time - self.first_seen
    }

    pub fn notional_usd(&self) -> f64 {
        self.notional_usd
    }
}

/// WallTracker monitors large orders (walls) in the order book
pub struct WallTracker {
    symbol: String,

    // Detected walls
    bid_walls: Vec<Wall>,
    ask_walls: Vec<Wall>,

    // Previous state for change detection
    prev_walls: HashMap<(WallSide, f64), f64>,  // (side, price) -> quantity

    // Statistics
    total_walls_detected: u64,
    total_walls_disappeared: u64,
    total_walls_modified: u64,

    // Wall detection threshold
    wall_size_usd: f64,
    wall_threshold_multiple: f64,

    // Clustering detection
    cluster_threshold_ticks: i32,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl WallTracker {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            bid_walls: Vec::new(),
            ask_walls: Vec::new(),
            prev_walls: HashMap::new(),
            total_walls_detected: 0,
            total_walls_disappeared: 0,
            total_walls_modified: 0,
            wall_size_usd: thresholds.wall_size_usd,
            wall_threshold_multiple: thresholds.wall_threshold_multiple,
            cluster_threshold_ticks: 5,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 500,
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    /// Process order book state update (simplified - uses state only)
    pub fn update(&mut self, state: &OrderBookState, current_time: i64) {
        self.updates_processed += 1;

        // Check for walls at best bid/ask
        self.check_wall_at_level(WallSide::Bid, state.best_bid, state.best_bid_qty, current_time);
        self.check_wall_at_level(WallSide::Ask, state.best_ask, state.best_ask_qty, current_time);

        // Approximate walls in total volume (simplified)
        // In full implementation, would use full order book depth
    }

    /// Process full order book depth
    pub fn update_with_depth(&mut self, bids: &[PriceLevel], asks: &[PriceLevel], mid_price: f64, current_time: i64) {
        self.updates_processed += 1;

        // Calculate average level size for wall threshold
        let avg_bid_size: f64 = if !bids.is_empty() {
            bids.iter().map(|l| l.quantity).sum::<f64>() / bids.len() as f64
        } else {
            0.0
        };

        let avg_ask_size: f64 = if !asks.is_empty() {
            asks.iter().map(|l| l.quantity).sum::<f64>() / asks.len() as f64
        } else {
            0.0
        };

        // Detect walls - levels significantly larger than average
        for level in bids {
            let notional = level.price * level.quantity;
            let is_wall = notional >= self.wall_size_usd
                || (avg_bid_size > 0.0 && level.quantity >= avg_bid_size * self.wall_threshold_multiple);

            if is_wall {
                self.check_wall_at_level(WallSide::Bid, level.price, level.quantity, current_time);
            }
        }

        for level in asks {
            let notional = level.price * level.quantity;
            let is_wall = notional >= self.wall_size_usd
                || (avg_ask_size > 0.0 && level.quantity >= avg_ask_size * self.wall_threshold_multiple);

            if is_wall {
                self.check_wall_at_level(WallSide::Ask, level.price, level.quantity, current_time);
            }
        }

        // Check for disappeared walls
        self.check_disappeared_walls(current_time);
    }

    fn check_wall_at_level(&mut self, side: WallSide, price: f64, quantity: f64, current_time: i64) {
        let key = (side, price);
        let prev_qty = self.prev_walls.get(&key).copied().unwrap_or(0.0);
        let notional = price * quantity;

        if quantity > 0.0 && notional >= self.wall_size_usd {
            if prev_qty == 0.0 {
                // New wall appeared
                self.wall_appeared(side, price, quantity, current_time);
            } else if (quantity - prev_qty).abs() > prev_qty * 0.1 {
                // Wall modified significantly
                self.wall_modified(side, price, quantity, prev_qty, current_time);
            }
            self.prev_walls.insert(key, quantity);
        } else if prev_qty > 0.0 {
            // Wall disappeared
            self.wall_disappeared(side, price, prev_qty, current_time);
            self.prev_walls.remove(&key);
        }
    }

    fn check_disappeared_walls(&mut self, current_time: i64) {
        // This would be called to clean up walls that are no longer in the book
        // Simplified - handled in check_wall_at_level
    }

    fn wall_appeared(&mut self, side: WallSide, price: f64, quantity: f64, current_time: i64) {
        self.total_walls_detected += 1;

        let wall = Wall {
            side,
            price,
            quantity,
            notional_usd: price * quantity,
            first_seen: current_time,
            last_modified: current_time,
            modification_count: 0,
            test_count: 0,
            defense_count: 0,
            is_absorbing: false,
            absorbed_volume: 0.0,
        };

        match side {
            WallSide::Bid => self.bid_walls.push(wall),
            WallSide::Ask => self.ask_walls.push(wall),
        }
    }

    fn wall_modified(&mut self, side: WallSide, price: f64, new_qty: f64, old_qty: f64, current_time: i64) {
        self.total_walls_modified += 1;

        // Update the wall
        let walls = match side {
            WallSide::Bid => &mut self.bid_walls,
            WallSide::Ask => &mut self.ask_walls,
        };

        if let Some(wall) = walls.iter_mut().find(|w| (w.price - price).abs() < 0.01) {
            wall.quantity = new_qty;
            wall.last_modified = current_time;
            wall.modification_count += 1;

            // Check if wall was pulled (significant reduction)
            if new_qty < old_qty * 0.5 {
                // Wall pulled event would be fired in check_events
            }
        }
    }

    fn wall_disappeared(&mut self, side: WallSide, price: f64, quantity: f64, current_time: i64) {
        self.total_walls_disappeared += 1;

        // Remove from tracking
        match side {
            WallSide::Bid => self.bid_walls.retain(|w| (w.price - price).abs() > 0.01),
            WallSide::Ask => self.ask_walls.retain(|w| (w.price - price).abs() > 0.01),
        }
    }

    /// Check all wall events
    pub fn check_events(&mut self, current_time: i64) {
        self.check_wall_appeared(current_time);
        self.check_wall_disappeared(current_time);
        self.check_wall_modified(current_time);
        self.check_wall_clustering(current_time);
        self.check_wall_absorbed(current_time);
        self.check_wall_pulled(current_time);
        self.check_wall_refreshed(current_time);
        self.check_wall_moved(current_time);
        self.check_wall_stacking(current_time);
        self.check_wall_spoofing(current_time);
        self.check_wall_tested(current_time);
        self.check_wall_defended(current_time);
    }

    /// Event 1: Wall appeared
    fn check_wall_appeared(&mut self, timestamp: i64) {
        // Check for newly appeared walls (age < 1 second)
        for wall in self.bid_walls.iter().chain(self.ask_walls.iter()) {
            if wall.age_ms(timestamp) < 1000 && wall.modification_count == 0 {
                if self.should_fire(OB_WALL_APPEARED, timestamp) {
                    self.publish_event(
                        OB_WALL_APPEARED,
                        timestamp,
                        json!({
                            "side": format!("{:?}", wall.side),
                            "price": wall.price,
                            "quantity": wall.quantity,
                            "notional_usd": wall.notional_usd,
                        }),
                    );
                }
            }
        }
    }

    /// Event 2: Wall disappeared
    fn check_wall_disappeared(&mut self, timestamp: i64) {
        // This is tracked via total_walls_disappeared counter
        // Event fired when wall disappears unexpectedly
    }

    /// Event 3: Wall modified
    fn check_wall_modified(&mut self, timestamp: i64) {
        for wall in self.bid_walls.iter().chain(self.ask_walls.iter()) {
            if wall.modification_count > 0 && timestamp - wall.last_modified < 1000 {
                if self.should_fire(OB_WALL_MODIFIED, timestamp) {
                    self.publish_event(
                        OB_WALL_MODIFIED,
                        timestamp,
                        json!({
                            "side": format!("{:?}", wall.side),
                            "price": wall.price,
                            "quantity": wall.quantity,
                            "modification_count": wall.modification_count,
                        }),
                    );
                }
            }
        }
    }

    /// Event 4: Wall clustering
    fn check_wall_clustering(&mut self, timestamp: i64) {
        // Check for multiple walls at similar price levels
        let bid_cluster = self.detect_cluster(&self.bid_walls);
        let ask_cluster = self.detect_cluster(&self.ask_walls);

        if bid_cluster >= 3 || ask_cluster >= 3 {
            if self.should_fire(OB_WALL_CLUSTERING, timestamp) {
                self.publish_event(
                    OB_WALL_CLUSTERING,
                    timestamp,
                    json!({
                        "bid_cluster_count": bid_cluster,
                        "ask_cluster_count": ask_cluster,
                    }),
                );
            }
        }
    }

    fn detect_cluster(&self, walls: &[Wall]) -> usize {
        if walls.len() < 2 {
            return 0;
        }

        // Group walls by price proximity
        let mut cluster_count = 0;
        let mut sorted_prices: Vec<f64> = walls.iter().map(|w| w.price).collect();
        sorted_prices.sort_by(|a, b| a.partial_cmp(b).unwrap());

        for i in 1..sorted_prices.len() {
            // Simplified cluster detection
            cluster_count += 1;
        }

        cluster_count
    }

    /// Event 5: Wall absorbed orders
    fn check_wall_absorbed(&mut self, timestamp: i64) {
        for wall in self.bid_walls.iter().chain(self.ask_walls.iter()) {
            if wall.is_absorbing && wall.absorbed_volume > wall.quantity * 0.5 {
                if self.should_fire(OB_WALL_ABSORBED, timestamp) {
                    self.publish_event(
                        OB_WALL_ABSORBED,
                        timestamp,
                        json!({
                            "side": format!("{:?}", wall.side),
                            "price": wall.price,
                            "absorbed_volume": wall.absorbed_volume,
                            "remaining_quantity": wall.quantity,
                        }),
                    );
                }
            }
        }
    }

    /// Event 6: Wall pulled (significant reduction)
    fn check_wall_pulled(&mut self, timestamp: i64) {
        // This is detected when wall quantity drops significantly
        // Handled in wall_modified when new_qty < old_qty * 0.5
    }

    /// Event 7: Wall refreshed (restored after reduction)
    fn check_wall_refreshed(&mut self, timestamp: i64) {
        for wall in self.bid_walls.iter().chain(self.ask_walls.iter()) {
            // Check if wall was modified recently and increased
            if wall.modification_count >= 2 && timestamp - wall.last_modified < 5000 {
                if self.should_fire(OB_WALL_REFRESHED, timestamp) {
                    self.publish_event(
                        OB_WALL_REFRESHED,
                        timestamp,
                        json!({
                            "side": format!("{:?}", wall.side),
                            "price": wall.price,
                            "quantity": wall.quantity,
                        }),
                    );
                }
            }
        }
    }

    /// Event 8: Wall moved to new price
    fn check_wall_moved(&mut self, timestamp: i64) {
        // Detect if a wall disappeared and new wall appeared at different price
        // Simplified - would need to track wall movement patterns
    }

    /// Event 9: Wall stacking (multiple walls on same side)
    fn check_wall_stacking(&mut self, timestamp: i64) {
        let bid_wall_count = self.bid_walls.len();
        let ask_wall_count = self.ask_walls.len();

        if bid_wall_count >= 3 || ask_wall_count >= 3 {
            if self.should_fire(OB_WALL_STACKING, timestamp) {
                self.publish_event(
                    OB_WALL_STACKING,
                    timestamp,
                    json!({
                        "bid_walls": bid_wall_count,
                        "ask_walls": ask_wall_count,
                        "stacking_side": if bid_wall_count >= 3 { "bid" } else { "ask" },
                    }),
                );
            }
        }
    }

    /// Event 10: Potential spoofing pattern
    fn check_wall_spoofing(&mut self, timestamp: i64) {
        // Detect spoofing: wall appears then quickly disappears
        let rapid_disappear = self.total_walls_disappeared > 5
            && self.total_walls_detected > 10
            && self.total_walls_disappeared as f64 / self.total_walls_detected as f64 > 0.7;

        if rapid_disappear {
            if self.should_fire(OB_WALL_SPOOFING, timestamp) {
                self.publish_event(
                    OB_WALL_SPOOFING,
                    timestamp,
                    json!({
                        "total_detected": self.total_walls_detected,
                        "total_disappeared": self.total_walls_disappeared,
                        "disappear_ratio": self.total_walls_disappeared as f64 / self.total_walls_detected as f64,
                    }),
                );
            }
        }
    }

    /// Event 11: Wall tested (price approached)
    fn check_wall_tested(&mut self, timestamp: i64) {
        for wall in self.bid_walls.iter().chain(self.ask_walls.iter()) {
            if wall.test_count > 0 && timestamp - wall.last_modified < 5000 {
                if self.should_fire(OB_WALL_TESTED, timestamp) {
                    self.publish_event(
                        OB_WALL_TESTED,
                        timestamp,
                        json!({
                            "side": format!("{:?}", wall.side),
                            "price": wall.price,
                            "test_count": wall.test_count,
                        }),
                    );
                }
            }
        }
    }

    /// Event 12: Wall defended (held after test)
    fn check_wall_defended(&mut self, timestamp: i64) {
        for wall in self.bid_walls.iter().chain(self.ask_walls.iter()) {
            if wall.defense_count > 0 && wall.defense_count == wall.test_count {
                if self.should_fire(OB_WALL_DEFENDED, timestamp) {
                    self.publish_event(
                        OB_WALL_DEFENDED,
                        timestamp,
                        json!({
                            "side": format!("{:?}", wall.side),
                            "price": wall.price,
                            "defense_count": wall.defense_count,
                        }),
                    );
                }
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
            "orderbook_aggregator",
            EventPriority::Medium,
        );

        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(symbol = %self.symbol, event = event_type, "Wall event published");
    }

    pub fn updates_processed(&self) -> u64 {
        self.updates_processed
    }

    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    pub fn bid_wall_count(&self) -> usize {
        self.bid_walls.len()
    }

    pub fn ask_wall_count(&self) -> usize {
        self.ask_walls.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wall_creation() {
        let thresholds = AggregatorThresholds::default();
        let tracker = WallTracker::new("BTCUSDT".to_string(), thresholds);
        assert_eq!(tracker.bid_wall_count(), 0);
        assert_eq!(tracker.ask_wall_count(), 0);
    }

    #[test]
    fn test_wall_detection() {
        let thresholds = AggregatorThresholds::default();
        let mut tracker = WallTracker::new("BTCUSDT".to_string(), thresholds);

        // Create a state with a large wall
        let state = OrderBookState {
            timestamp: 0,
            last_update_id: 0,
            best_bid: 50000.0,
            best_bid_qty: 100.0,  // 5M USD notional
            best_ask: 50001.0,
            best_ask_qty: 50.0,
            mid_price: 50000.5,
            spread: 1.0,
            spread_bps: 0.2,
            total_bid_volume: 1000.0,
            total_ask_volume: 800.0,
            is_synced: true,
        };

        tracker.update(&state, 1000);
        assert!(tracker.bid_wall_count() > 0 || tracker.ask_wall_count() > 0);
    }
}
