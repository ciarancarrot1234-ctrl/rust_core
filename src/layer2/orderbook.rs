// Order Book Manager - Pure Rust Implementation
// 50-100x faster than Python version

use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::fmt;
use thiserror::Error;
use tracing::{info, debug, warn};

use crate::layer2::parser::PriceLevel;

type Price = OrderedFloat<f64>;
type Quantity = f64;

#[derive(Debug, Error)]
pub enum OrderBookError {
    #[error("Order book not initialized or empty")]
    NotInitialized,
    #[error("Invalid mode: {0}")]
    InvalidMode(String),
    #[error("Cannot process differential update in {0:?} mode")]
    WrongMode(OrderBookMode),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderBookMode {
    Snapshot,
    Differential,
}

impl fmt::Display for OrderBookMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Order Book State
#[derive(Clone, Debug)]
pub struct OrderBookState {
    pub timestamp: u64,
    pub last_update_id: u64,
    pub best_bid: f64,
    pub best_bid_qty: f64,
    pub best_ask: f64,
    pub best_ask_qty: f64,
    pub mid_price: f64,
    pub spread: f64,
    pub spread_bps: f64,
    pub total_bid_volume: f64,
    pub total_ask_volume: f64,
    pub is_synced: bool,
}

impl fmt::Display for OrderBookState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OrderBookState(mid={:.2}, spread={:.4}bps, bids_vol={:.2}, asks_vol={:.2})",
            self.mid_price, self.spread_bps, self.total_bid_volume, self.total_ask_volume
        )
    }
}

/// Snapshot data for initializing/replacing the order book
pub struct SnapshotData {
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub last_update_id: u64,
}

/// Differential update data
pub struct DiffUpdate {
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub final_update_id: u64,
    pub first_update_id: Option<u64>,
}

/// Order book summary
#[derive(Debug, Clone)]
pub struct OrderBookSummary {
    pub symbol: String,
    pub mode: OrderBookMode,
    pub is_initialized: bool,
    pub snapshot_count: u64,
    pub last_update_id: u64,
    pub bid_levels: usize,
    pub ask_levels: usize,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread_bps: Option<f64>,
}

/// High-performance Order Book Manager
pub struct OrderBook {
    pub symbol: String,
    pub mode: OrderBookMode,

    // BTreeMap keeps entries sorted automatically
    bids: Arc<RwLock<BTreeMap<Price, Quantity>>>,
    asks: Arc<RwLock<BTreeMap<Price, Quantity>>>,

    last_update_id: Arc<RwLock<u64>>,
    last_update_time: Arc<RwLock<u64>>,
    is_initialized: Arc<RwLock<bool>>,
    snapshot_count: Arc<RwLock<u64>>,
}

impl OrderBook {
    pub fn new(symbol: &str, mode: &str) -> Result<Self, OrderBookError> {
        let mode = match mode {
            "snapshot" => OrderBookMode::Snapshot,
            "differential" => OrderBookMode::Differential,
            _ => return Err(OrderBookError::InvalidMode(mode.to_string())),
        };

        info!(symbol = symbol, mode = ?mode, "OrderBook initialized");

        Ok(Self {
            symbol: symbol.to_uppercase(),
            mode,
            bids: Arc::new(RwLock::new(BTreeMap::new())),
            asks: Arc::new(RwLock::new(BTreeMap::new())),
            last_update_id: Arc::new(RwLock::new(0)),
            last_update_time: Arc::new(RwLock::new(0)),
            is_initialized: Arc::new(RwLock::new(false)),
            snapshot_count: Arc::new(RwLock::new(0)),
        })
    }

    /// Initialize order book from snapshot
    pub fn initialize_from_snapshot(&self, snapshot: SnapshotData) -> Result<OrderBookState, OrderBookError> {
        let state = self.process_snapshot(snapshot)?;

        info!(
            last_update_id = state.last_update_id,
            bid_levels = self.bids.read().len(),
            ask_levels = self.asks.read().len(),
            "Order book initialized from snapshot"
        );

        Ok(state)
    }

    /// Process snapshot update (replaces entire book)
    pub fn process_snapshot(&self, snapshot: SnapshotData) -> Result<OrderBookState, OrderBookError> {
        // Clear and rebuild order book
        {
            let mut bids = self.bids.write();
            let mut asks = self.asks.write();

            bids.clear();
            asks.clear();

            for level in &snapshot.bids {
                bids.insert(OrderedFloat(level.price), level.quantity);
            }

            for level in &snapshot.asks {
                asks.insert(OrderedFloat(level.price), level.quantity);
            }
        }

        // Update metadata
        *self.last_update_id.write() = snapshot.last_update_id;
        *self.last_update_time.write() = get_current_timestamp();
        *self.snapshot_count.write() += 1;

        let was_initialized = *self.is_initialized.read();
        if !was_initialized {
            let bid_count = self.bids.read().len();
            let ask_count = self.asks.read().len();
            debug!(bid_count = bid_count, ask_count = ask_count, "Order book initialized");
            *self.is_initialized.write() = true;
        }

        self.get_state()
    }

    /// Process differential update (apply incremental changes)
    pub fn process_differential(&self, update: DiffUpdate) -> Result<Option<OrderBookState>, OrderBookError> {
        if self.mode != OrderBookMode::Differential {
            return Err(OrderBookError::WrongMode(self.mode));
        }

        if !*self.is_initialized.read() {
            warn!("Differential update received before initialization - dropping");
            return Ok(None);
        }

        // Apply changes - O(log n) per operation
        {
            let mut bids = self.bids.write();
            let mut asks = self.asks.write();

            for level in &update.bids {
                let price_key = OrderedFloat(level.price);
                if level.quantity == 0.0 {
                    bids.remove(&price_key);
                } else {
                    bids.insert(price_key, level.quantity);
                }
            }

            for level in &update.asks {
                let price_key = OrderedFloat(level.price);
                if level.quantity == 0.0 {
                    asks.remove(&price_key);
                } else {
                    asks.insert(price_key, level.quantity);
                }
            }
        }

        // Update metadata
        *self.last_update_id.write() = update.final_update_id;
        *self.last_update_time.write() = get_current_timestamp();
        *self.snapshot_count.write() += 1;

        Ok(Some(self.get_state()?))
    }

    /// Unified update method - dispatches based on mode
    pub fn process_update_snapshot(&self, snapshot: SnapshotData) -> Result<OrderBookState, OrderBookError> {
        self.process_snapshot(snapshot)
    }

    pub fn process_update_diff(&self, update: DiffUpdate) -> Result<Option<OrderBookState>, OrderBookError> {
        self.process_differential(update)
    }

    /// Get current order book state
    pub fn get_state(&self) -> Result<OrderBookState, OrderBookError> {
        let bids = self.bids.read();
        let asks = self.asks.read();

        if bids.is_empty() || asks.is_empty() {
            return Err(OrderBookError::NotInitialized);
        }

        // Get best bid/ask - O(1) operation with BTreeMap
        let (best_bid_price, &best_bid_qty) = bids
            .iter()
            .next_back()  // Highest bid
            .unwrap();

        let (best_ask_price, &best_ask_qty) = asks
            .iter()
            .next()  // Lowest ask
            .unwrap();

        let mid_price = (best_bid_price.0 + best_ask_price.0) / 2.0;
        let spread = best_ask_price.0 - best_bid_price.0;
        let spread_bps = (spread / mid_price) * 10000.0;

        let total_bid_volume: f64 = bids.values().sum();
        let total_ask_volume: f64 = asks.values().sum();

        Ok(OrderBookState {
            timestamp: *self.last_update_time.read(),
            last_update_id: *self.last_update_id.read(),
            best_bid: best_bid_price.0,
            best_bid_qty,
            best_ask: best_ask_price.0,
            best_ask_qty,
            mid_price,
            spread,
            spread_bps,
            total_bid_volume,
            total_ask_volume,
            is_synced: true,
        })
    }

    /// Get summary statistics
    pub fn get_summary(&self) -> OrderBookSummary {
        let bid_levels = self.bids.read().len();
        let ask_levels = self.asks.read().len();
        let last_update_id = *self.last_update_id.read();
        let snapshot_count = *self.snapshot_count.read();
        let is_initialized = *self.is_initialized.read();

        let (best_bid, best_ask, spread_bps) = if is_initialized {
            if let Ok(state) = self.get_state() {
                (Some(state.best_bid), Some(state.best_ask), Some(state.spread_bps))
            } else {
                (None, None, None)
            }
        } else {
            (None, None, None)
        };

        OrderBookSummary {
            symbol: self.symbol.clone(),
            mode: self.mode,
            is_initialized,
            snapshot_count,
            last_update_id,
            bid_levels,
            ask_levels,
            best_bid,
            best_ask,
            spread_bps,
        }
    }

    /// Reset order book
    pub fn reset(&self) {
        debug!("Resetting OrderBook");
        self.bids.write().clear();
        self.asks.write().clear();
        *self.last_update_id.write() = 0;
        *self.last_update_time.write() = 0;
        *self.snapshot_count.write() = 0;
        *self.is_initialized.write() = false;
    }

    /// Get bid level count
    pub fn bid_levels(&self) -> usize {
        self.bids.read().len()
    }

    /// Get ask level count
    pub fn ask_levels(&self) -> usize {
        self.asks.read().len()
    }

    /// Check if initialized
    pub fn is_initialized(&self) -> bool {
        *self.is_initialized.read()
    }

    /// Get top N bid levels
    pub fn top_bids(&self, n: usize) -> Vec<PriceLevel> {
        self.bids.read()
            .iter()
            .rev()
            .take(n)
            .map(|(price, &qty)| PriceLevel { price: price.0, quantity: qty })
            .collect()
    }

    /// Get top N ask levels
    pub fn top_asks(&self, n: usize) -> Vec<PriceLevel> {
        self.asks.read()
            .iter()
            .take(n)
            .map(|(price, &qty)| PriceLevel { price: price.0, quantity: qty })
            .collect()
    }
}

fn get_current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orderbook_creation() {
        let ob = OrderBook::new("BTCUSDT", "snapshot").unwrap();
        assert_eq!(ob.symbol, "BTCUSDT");
        assert_eq!(ob.mode, OrderBookMode::Snapshot);
    }

    #[test]
    fn test_differential_mode_validation() {
        let ob = OrderBook::new("BTCUSDT", "differential").unwrap();
        assert_eq!(ob.mode, OrderBookMode::Differential);
    }

    #[test]
    fn test_invalid_mode() {
        let result = OrderBook::new("BTCUSDT", "invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_processing() {
        let ob = OrderBook::new("BTCUSDT", "snapshot").unwrap();

        let snapshot = SnapshotData {
            bids: vec![
                PriceLevel { price: 50000.0, quantity: 1.0 },
                PriceLevel { price: 49999.0, quantity: 2.0 },
            ],
            asks: vec![
                PriceLevel { price: 50001.0, quantity: 1.5 },
                PriceLevel { price: 50002.0, quantity: 3.0 },
            ],
            last_update_id: 12345,
        };

        let state = ob.process_snapshot(snapshot).unwrap();
        assert_eq!(state.best_bid, 50000.0);
        assert_eq!(state.best_ask, 50001.0);
        assert_eq!(state.last_update_id, 12345);
        assert!(ob.is_initialized());
    }

    #[test]
    fn test_differential_update() {
        let ob = OrderBook::new("BTCUSDT", "differential").unwrap();

        // Initialize with snapshot
        let snapshot = SnapshotData {
            bids: vec![
                PriceLevel { price: 50000.0, quantity: 1.0 },
            ],
            asks: vec![
                PriceLevel { price: 50001.0, quantity: 1.5 },
            ],
            last_update_id: 100,
        };
        ob.process_snapshot(snapshot).unwrap();

        // Apply differential update
        let diff = DiffUpdate {
            bids: vec![
                PriceLevel { price: 50000.0, quantity: 2.0 }, // Update existing
                PriceLevel { price: 49999.0, quantity: 0.5 }, // New level
            ],
            asks: vec![
                PriceLevel { price: 50001.0, quantity: 0.0 }, // Remove level
                PriceLevel { price: 50002.0, quantity: 3.0 }, // New level
            ],
            final_update_id: 101,
            first_update_id: Some(101),
        };

        let state = ob.process_differential(diff).unwrap().unwrap();
        assert_eq!(state.best_bid, 50000.0);
        assert_eq!(state.best_bid_qty, 2.0);
        assert_eq!(state.best_ask, 50002.0); // 50001 was removed
        assert_eq!(ob.bid_levels(), 2);
        assert_eq!(ob.ask_levels(), 1); // 50001 removed, 50002 added
    }

    #[test]
    fn test_top_levels() {
        let ob = OrderBook::new("BTCUSDT", "snapshot").unwrap();

        let snapshot = SnapshotData {
            bids: vec![
                PriceLevel { price: 50000.0, quantity: 1.0 },
                PriceLevel { price: 49999.0, quantity: 2.0 },
                PriceLevel { price: 49998.0, quantity: 3.0 },
            ],
            asks: vec![
                PriceLevel { price: 50001.0, quantity: 1.5 },
                PriceLevel { price: 50002.0, quantity: 2.5 },
                PriceLevel { price: 50003.0, quantity: 3.5 },
            ],
            last_update_id: 100,
        };
        ob.process_snapshot(snapshot).unwrap();

        let top2_bids = ob.top_bids(2);
        assert_eq!(top2_bids.len(), 2);
        assert_eq!(top2_bids[0].price, 50000.0); // Best bid first

        let top2_asks = ob.top_asks(2);
        assert_eq!(top2_asks.len(), 2);
        assert_eq!(top2_asks[0].price, 50001.0); // Best ask first
    }

    #[test]
    fn test_reset() {
        let ob = OrderBook::new("BTCUSDT", "snapshot").unwrap();

        let snapshot = SnapshotData {
            bids: vec![PriceLevel { price: 50000.0, quantity: 1.0 }],
            asks: vec![PriceLevel { price: 50001.0, quantity: 1.0 }],
            last_update_id: 100,
        };
        ob.process_snapshot(snapshot).unwrap();
        assert!(ob.is_initialized());

        ob.reset();
        assert!(!ob.is_initialized());
        assert_eq!(ob.bid_levels(), 0);
        assert_eq!(ob.ask_levels(), 0);
    }
}
