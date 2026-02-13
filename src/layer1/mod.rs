// Layer 1 - Data Connectors for Binance Futures
// Pure Rust - no Python dependencies

pub mod websocket;
pub mod rest_client;
pub mod user_data_stream;
pub mod time_synchronizer;

// Re-export commonly used items for convenience
pub use websocket::{WebSocketClient, WebSocketMessage, WebSocketStats};
pub use rest_client::{BinanceRestClient, RestClientError, RestClientStats, RateLimiter};
pub use user_data_stream::{
    UserDataStreamManager, UserDataStreamError, UserDataStreamStats,
    UserDataEvent, OrderUpdate, AccountUpdate, BalanceUpdate, PositionUpdate,
};
pub use time_synchronizer::{TimeSynchronizer, TimeSyncError, TimeSyncStats};
