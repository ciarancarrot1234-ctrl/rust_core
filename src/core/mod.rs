// Core Module - Foundational types, config, logging, events
// Pure Rust - no Python dependencies

pub mod types;
pub mod config;
pub mod logger;
pub mod events;

// Re-export commonly used items for convenience
pub use types::*;
pub use config::{
    BinanceConfig, TradingConfig, AggregatorThresholds, StrategyConfig, MonitoringConfig,
    ConfigManager, ConfigSummary, ConfigError, get_config,
};
pub use logger::setup_logging;
pub use events::{Event, EventPriority, EventBus, EventBusStatsSnapshot, get_event_bus};
