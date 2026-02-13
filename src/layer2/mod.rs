// Layer 2 - Data Processing & Synchronization
// Pure Rust - processes market data streams and maintains synchronized state

pub mod orderbook;
pub mod parser;
pub mod synchronizer;
pub mod pipeline;
pub mod market_data_store;
pub mod metrics;

// Re-export commonly used items
pub use orderbook::{
    OrderBook, OrderBookState, OrderBookMode, OrderBookSummary,
    OrderBookError, SnapshotData, DiffUpdate,
};
pub use parser::{
    MessageParser, ParsedMessage, ParseError, ParserStats, PriceLevel,
    ParsedAggTrade, ParsedDepthUpdate, ParsedDepthSnapshot,
    ParsedKline, ParsedMarkPrice, ParsedLiquidation, ParsedBookTicker,
    parse_depth_update_raw,
};
pub use synchronizer::{
    DataSynchronizer, SynchronizedMessage, QualityMetadata,
    SyncState, SynchronizerStats,
};
pub use pipeline::{RustPipeline, PipelineStats};
pub use market_data_store::{MarketDataStore, MarketDataStoreStats};
pub use metrics::UnifiedMetrics;
