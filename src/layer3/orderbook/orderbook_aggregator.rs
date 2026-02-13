// OrderBook Aggregator - Main orchestrator that subscribes to EventBus
// Processes OrderBookState events and generates 95 condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::get_event_bus;
use crate::layer2::orderbook::OrderBookState;
use parking_lot::RwLock;
use std::sync::Arc;
use tracing::{debug, warn};

use super::spread_tracker::SpreadTracker;
use super::imbalance_tracker::ImbalanceTracker;
use super::wall_tracker::WallTracker;
use super::liquidity_tracker::LiquidityTracker;
use super::microprice_tracker::MicropriceTracker;
use super::orderflow_tracker::OrderFlowTracker;

/// OrderBookAggregator orchestrates all 6 specialized trackers
pub struct OrderBookAggregator {
    symbol: String,

    spread_tracker: Arc<RwLock<SpreadTracker>>,
    imbalance_tracker: Arc<RwLock<ImbalanceTracker>>,
    wall_tracker: Arc<RwLock<WallTracker>>,
    liquidity_tracker: Arc<RwLock<LiquidityTracker>>,
    microprice_tracker: Arc<RwLock<MicropriceTracker>>,
    orderflow_tracker: Arc<RwLock<OrderFlowTracker>>,

    thresholds: AggregatorThresholds,
}

impl OrderBookAggregator {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        debug!(symbol = %symbol, "Initializing OrderBookAggregator");

        let aggregator = Self {
            symbol: symbol.clone(),
            spread_tracker: Arc::new(RwLock::new(SpreadTracker::new(symbol.clone(), thresholds.clone()))),
            imbalance_tracker: Arc::new(RwLock::new(ImbalanceTracker::new(symbol.clone(), thresholds.clone()))),
            wall_tracker: Arc::new(RwLock::new(WallTracker::new(symbol.clone(), thresholds.clone()))),
            liquidity_tracker: Arc::new(RwLock::new(LiquidityTracker::new(symbol.clone(), thresholds.clone()))),
            microprice_tracker: Arc::new(RwLock::new(MicropriceTracker::new(symbol.clone(), thresholds.clone()))),
            orderflow_tracker: Arc::new(RwLock::new(OrderFlowTracker::new(symbol, thresholds.clone()))),
            thresholds,
        };

        aggregator.start_listening();
        debug!("OrderBookAggregator initialized");
        aggregator
    }

    fn start_listening(&self) {
        let spread = self.spread_tracker.clone();
        let imbalance = self.imbalance_tracker.clone();
        let wall = self.wall_tracker.clone();
        let liquidity = self.liquidity_tracker.clone();
        let microprice = self.microprice_tracker.clone();
        let orderflow = self.orderflow_tracker.clone();
        let symbol = self.symbol.clone();

        debug!(symbol = %symbol, "Starting background thread for orderbook events");

        let event_bus = get_event_bus();
        let mut rx = event_bus.subscribe_channel();

        std::thread::spawn(move || {
            debug!(symbol = %symbol, "OrderBookAggregator event listener started");

            loop {
                match rx.blocking_recv() {
                    Ok(event) => {
                        // Process orderbook_state events
                        if event.event_type == "orderbook_state" || event.event_type == "depth_update" {
                            if let Some(data_value) = event.data.get("data") {
                                if let Ok(state) = serde_json::from_value::<OrderBookState>(data_value.clone()) {
                                    if state.is_synced {
                                        let timestamp = event.timestamp;

                                        // Update all trackers
                                        spread.write().update(&state, timestamp);
                                        imbalance.write().update(&state, timestamp);
                                        wall.write().update(&state, timestamp);
                                        liquidity.write().update(&state, timestamp);
                                        microprice.write().update(&state, timestamp);
                                        orderflow.write().update(&state, timestamp);

                                        // Check events
                                        spread.write().check_events(timestamp);
                                        imbalance.write().check_events(timestamp);
                                        wall.write().check_events(timestamp);
                                        liquidity.write().check_events(timestamp);
                                        microprice.write().check_events(timestamp);
                                        orderflow.write().check_events(timestamp);
                                    }
                                } else {
                                    warn!("Failed to deserialize OrderBookState");
                                }
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped = skipped, "OrderBookAggregator lagged");
                    }
                    Err(_) => {
                        debug!("Event bus channel closed");
                        break;
                    }
                }
            }
        });
    }

    /// Process an order book state directly (for testing or synchronous use)
    pub fn process_state(&self, state: &OrderBookState, timestamp: i64) {
        if !state.is_synced {
            return;
        }

        // Update all trackers
        self.spread_tracker.write().update(state, timestamp);
        self.imbalance_tracker.write().update(state, timestamp);
        self.wall_tracker.write().update(state, timestamp);
        self.liquidity_tracker.write().update(state, timestamp);
        self.microprice_tracker.write().update(state, timestamp);
        self.orderflow_tracker.write().update(state, timestamp);

        // Check events
        self.spread_tracker.write().check_events(timestamp);
        self.imbalance_tracker.write().check_events(timestamp);
        self.wall_tracker.write().check_events(timestamp);
        self.liquidity_tracker.write().check_events(timestamp);
        self.microprice_tracker.write().check_events(timestamp);
        self.orderflow_tracker.write().check_events(timestamp);
    }

    /// Get aggregator statistics
    pub fn get_stats(&self) -> OrderBookAggregatorStats {
        let spread_updates = self.spread_tracker.read().updates_processed();
        let spread_events = self.spread_tracker.read().events_fired();

        let imbalance_updates = self.imbalance_tracker.read().updates_processed();
        let imbalance_events = self.imbalance_tracker.read().events_fired();

        let wall_updates = self.wall_tracker.read().updates_processed();
        let wall_events = self.wall_tracker.read().events_fired();

        let liquidity_updates = self.liquidity_tracker.read().updates_processed();
        let liquidity_events = self.liquidity_tracker.read().events_fired();

        let microprice_updates = self.microprice_tracker.read().updates_processed();
        let microprice_events = self.microprice_tracker.read().events_fired();

        let orderflow_updates = self.orderflow_tracker.read().updates_processed();
        let orderflow_events = self.orderflow_tracker.read().events_fired();

        OrderBookAggregatorStats {
            symbol: self.symbol.clone(),
            total_updates_processed: spread_updates,
            total_events_fired: spread_events + imbalance_events + wall_events
                + liquidity_events + microprice_events + orderflow_events,
            spread_updates, spread_events,
            imbalance_updates, imbalance_events,
            wall_updates, wall_events,
            liquidity_updates, liquidity_events,
            microprice_updates, microprice_events,
            orderflow_updates, orderflow_events,
        }
    }

    /// Get current spread in bps
    pub fn current_spread_bps(&self) -> f64 {
        self.spread_tracker.read().current_spread_bps()
    }

    /// Get current imbalance
    pub fn current_imbalance(&self) -> f64 {
        self.imbalance_tracker.read().current_imbalance()
    }

    /// Get current microprice distance from mid
    pub fn microprice_distance_bps(&self) -> f64 {
        self.microprice_tracker.read().distance_bps()
    }

    /// Get total depth
    pub fn total_depth(&self) -> f64 {
        self.liquidity_tracker.read().total_depth()
    }

    pub fn thresholds(&self) -> &AggregatorThresholds {
        &self.thresholds
    }
}

#[derive(Debug, Clone)]
pub struct OrderBookAggregatorStats {
    pub symbol: String,
    pub total_updates_processed: u64,
    pub total_events_fired: u64,
    pub spread_updates: u64, pub spread_events: u64,
    pub imbalance_updates: u64, pub imbalance_events: u64,
    pub wall_updates: u64, pub wall_events: u64,
    pub liquidity_updates: u64, pub liquidity_events: u64,
    pub microprice_updates: u64, pub microprice_events: u64,
    pub orderflow_updates: u64, pub orderflow_events: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orderbook_aggregator_creation() {
        let thresholds = AggregatorThresholds::default();
        let aggregator = OrderBookAggregator::new("BTCUSDT".to_string(), thresholds);
        let stats = aggregator.get_stats();
        assert_eq!(stats.symbol, "BTCUSDT");
    }

    #[test]
    fn test_process_state() {
        let thresholds = AggregatorThresholds::default();
        let aggregator = OrderBookAggregator::new("BTCUSDT".to_string(), thresholds);

        let state = OrderBookState {
            timestamp: 1000,
            last_update_id: 12345,
            best_bid: 50000.0,
            best_bid_qty: 10.0,
            best_ask: 50001.0,
            best_ask_qty: 8.0,
            mid_price: 50000.5,
            spread: 1.0,
            spread_bps: 0.2,
            total_bid_volume: 1000.0,
            total_ask_volume: 800.0,
            is_synced: true,
        };

        aggregator.process_state(&state, 1000);

        let stats = aggregator.get_stats();
        assert!(stats.total_updates_processed > 0);
    }
}
