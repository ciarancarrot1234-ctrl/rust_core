// Layer 3 - Aggregators
// Analyzes processed data and generates condition events

// Common utilities
pub mod common;

// Trade Aggregator - 85 events
pub mod trade;

// OrderBook Aggregator - 95 events
pub mod orderbook;

// Kline Aggregator - 105 events
pub mod kline;

// MarkPrice Aggregator - 58 events
pub mod markprice;

// Liquidation Aggregator - 52 events
pub mod liquidation;

// Binance Data Aggregator - 75 events
pub mod binance_data;

// Trade re-exports
pub use trade::{TradeAggregator, TradeAggregatorStats};

// OrderBook re-exports
pub use orderbook::{OrderBookAggregator, OrderBookAggregatorStats};

// Kline re-exports
pub use kline::{KlineAggregator, KlineAggregatorStats};

// MarkPrice re-exports
pub use markprice::{MarkPriceAggregator, MarkPriceAggregatorStats};

// Liquidation re-exports
pub use liquidation::{LiquidationAggregator, LiquidationAggregatorStats};

// Binance Data re-exports
pub use binance_data::{BinanceDataAggregator, BinanceDataAggregatorStats};
