// Trade Aggregator - Analyzes aggregate trade data to generate 85 condition events
// Consumes ParsedAggTrade events from EventBus and publishes condition events

// Module structure:
// - trade_aggregator.rs: Main TradeAggregator orchestrator (EventBus subscriber)
// - delta.rs: DeltaTracker (12 events)
// - vwap.rs: VwapTracker (11 events)
// - volume.rs: VolumeTracker (12 events)
// - patterns.rs: PatternDetector (10 events)
// - profile.rs: VolumeProfile (8 events)
// - streaks.rs: StreakTracker (8 events)

// Tracker modules
pub mod delta;
pub mod vwap;
pub mod volume;
pub mod patterns;
pub mod profile;
pub mod streaks;

// Main aggregator module
pub mod trade_aggregator;

// Re-exports
pub use trade_aggregator::{TradeAggregator, TradeAggregatorStats};
pub use delta::DeltaTracker;
pub use vwap::VwapTracker;
pub use volume::VolumeTracker;
pub use patterns::PatternDetector;
pub use profile::VolumeProfile;
pub use streaks::StreakTracker;
