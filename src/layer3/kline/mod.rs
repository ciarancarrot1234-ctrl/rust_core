// Kline Aggregator - Analyzes kline/candlestick data to generate 105 condition events
// Consumes Kline events from EventBus and publishes condition events

pub mod kline_aggregator;
pub mod ema_tracker;
pub mod rsi_tracker;
pub mod macd_tracker;
pub mod candle_tracker;
pub mod bb_tracker;
pub mod trend_tracker;

// Re-exports
pub use kline_aggregator::{KlineAggregator, KlineAggregatorStats};
pub use ema_tracker::{EMATracker, EMAAlignment};
pub use rsi_tracker::{RSITracker, RSIRegime};
pub use macd_tracker::{MACDTracker, HistogramRegime};
pub use candle_tracker::{CandleTracker, CandlePattern};
pub use bb_tracker::{BBTracker, BBPosition, VolatilityRegime};
pub use trend_tracker::{TrendTracker, TrendDirection};
