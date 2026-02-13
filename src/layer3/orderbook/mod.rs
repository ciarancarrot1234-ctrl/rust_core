// OrderBook Aggregator - Analyzes order book state to generate 95 condition events
// Consumes OrderBookState events from EventBus and publishes condition events

// Module structure:
// - orderbook_aggregator.rs: Main OrderBookAggregator orchestrator (EventBus subscriber)
// - spread_tracker.rs: SpreadTracker (8 events)
// - imbalance_tracker.rs: ImbalanceTracker (12 events)
// - wall_tracker.rs: WallTracker (12 events)
// - liquidity_tracker.rs: LiquidityTracker (10 events)
// - microprice_tracker.rs: MicropriceTracker (8 events)
// - orderflow_tracker.rs: OrderFlowTracker (8 events)
// Binance-specific events: 37 additional events

// Tracker modules
pub mod spread_tracker;
pub mod imbalance_tracker;
pub mod wall_tracker;
pub mod liquidity_tracker;
pub mod microprice_tracker;
pub mod orderflow_tracker;

// Main aggregator module
pub mod orderbook_aggregator;

// Re-exports
pub use orderbook_aggregator::{OrderBookAggregator, OrderBookAggregatorStats};
pub use spread_tracker::SpreadTracker;
pub use imbalance_tracker::ImbalanceTracker;
pub use wall_tracker::WallTracker;
pub use liquidity_tracker::LiquidityTracker;
pub use microprice_tracker::MicropriceTracker;
pub use orderflow_tracker::OrderFlowTracker;
