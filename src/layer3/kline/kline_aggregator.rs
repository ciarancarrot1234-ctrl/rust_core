// Kline Aggregator - Main orchestrator that subscribes to EventBus
// Processes Kline events and generates 105 condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::get_event_bus;
use parking_lot::RwLock;
use std::sync::Arc;
use tracing::{debug, warn};

use super::ema_tracker::EMATracker;
use super::rsi_tracker::RSITracker;
use super::macd_tracker::MACDTracker;
use super::candle_tracker::{CandleTracker, Candle};
use super::bb_tracker::BBTracker;
use super::trend_tracker::TrendTracker;

/// Kline data from parser
#[derive(Debug, Clone, serde::Deserialize)]
pub struct KlineData {
    pub symbol: String,
    pub interval: String,
    pub open_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub close_time: i64,
    // Technical indicators (calculated or from parser)
    pub ema9: Option<f64>,
    pub ema21: Option<f64>,
    pub ema50: Option<f64>,
    pub ema200: Option<f64>,
    pub rsi: Option<f64>,
    pub macd: Option<f64>,
    pub macd_signal: Option<f64>,
    pub bb_middle: Option<f64>,
    pub bb_upper: Option<f64>,
    pub bb_lower: Option<f64>,
    pub atr: Option<f64>,
}

/// KlineAggregator orchestrates all 6 specialized trackers
pub struct KlineAggregator {
    symbol: String,

    ema_tracker: Arc<RwLock<EMATracker>>,
    rsi_tracker: Arc<RwLock<RSITracker>>,
    macd_tracker: Arc<RwLock<MACDTracker>>,
    candle_tracker: Arc<RwLock<CandleTracker>>,
    bb_tracker: Arc<RwLock<BBTracker>>,
    trend_tracker: Arc<RwLock<TrendTracker>>,

    thresholds: AggregatorThresholds,
}

impl KlineAggregator {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        debug!(symbol = %symbol, "Initializing KlineAggregator");

        // Create trackers for each supported interval
        let interval = "1m".to_string();  // Default interval

        let aggregator = Self {
            symbol: symbol.clone(),
            ema_tracker: Arc::new(RwLock::new(EMATracker::new(symbol.clone(), interval.clone(), thresholds.clone()))),
            rsi_tracker: Arc::new(RwLock::new(RSITracker::new(symbol.clone(), interval.clone(), thresholds.clone()))),
            macd_tracker: Arc::new(RwLock::new(MACDTracker::new(symbol.clone(), interval.clone(), thresholds.clone()))),
            candle_tracker: Arc::new(RwLock::new(CandleTracker::new(symbol.clone(), interval.clone(), thresholds.clone()))),
            bb_tracker: Arc::new(RwLock::new(BBTracker::new(symbol.clone(), interval.clone(), thresholds.clone()))),
            trend_tracker: Arc::new(RwLock::new(TrendTracker::new(symbol, interval, thresholds.clone()))),
            thresholds,
        };

        aggregator.start_listening();
        debug!("KlineAggregator initialized");
        aggregator
    }

    fn start_listening(&self) {
        let ema = self.ema_tracker.clone();
        let rsi = self.rsi_tracker.clone();
        let macd = self.macd_tracker.clone();
        let candle = self.candle_tracker.clone();
        let bb = self.bb_tracker.clone();
        let trend = self.trend_tracker.clone();
        let symbol = self.symbol.clone();

        debug!(symbol = %symbol, "Starting background thread for kline events");

        let event_bus = get_event_bus();
        let mut rx = event_bus.subscribe_channel();

        std::thread::spawn(move || {
            debug!(symbol = %symbol, "KlineAggregator event listener started");

            loop {
                match rx.blocking_recv() {
                    Ok(event) => {
                        if event.event_type != "kline" { continue; }

                        if let Some(data_value) = event.data.get("data") {
                            if let Ok(kline) = serde_json::from_value::<KlineData>(data_value.clone()) {
                                if kline.symbol != symbol { continue; }

                                let timestamp = kline.open_time;

                                // Update candle tracker
                                {
                                    let candle_data = Candle {
                                        open: kline.open,
                                        high: kline.high,
                                        low: kline.low,
                                        close: kline.close,
                                        volume: kline.volume,
                                        timestamp: kline.open_time,
                                    };
                                    candle.write().update(candle_data, timestamp);
                                }

                                // Update EMA tracker
                                if let (Some(ema9), Some(ema21), Some(ema50), Some(ema200)) =
                                    (kline.ema9, kline.ema21, kline.ema50, kline.ema200) {
                                    ema.write().update(kline.close, ema9, ema21, ema50, ema200, timestamp);
                                }

                                // Update RSI tracker
                                if let Some(rsi_val) = kline.rsi {
                                    rsi.write().update(rsi_val, kline.close, timestamp);
                                }

                                // Update MACD tracker
                                if let (Some(macd_val), Some(signal)) = (kline.macd, kline.macd_signal) {
                                    macd.write().update(macd_val, signal, timestamp);
                                }

                                // Update BB tracker
                                if let (Some(middle), Some(upper), Some(lower), Some(atr)) =
                                    (kline.bb_middle, kline.bb_upper, kline.bb_lower, kline.atr) {
                                    bb.write().update(kline.close, middle, upper, lower, atr, timestamp);
                                }

                                // Update trend tracker
                                trend.write().update(kline.high, kline.low, kline.close, timestamp);

                                // Check events
                                ema.write().check_events(timestamp);
                                rsi.write().check_events(timestamp);
                                macd.write().check_events(timestamp);
                                candle.write().check_events(timestamp);
                                bb.write().check_events(timestamp);
                                trend.write().check_events(timestamp);
                            } else {
                                warn!("Failed to deserialize kline");
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped = skipped, "KlineAggregator lagged");
                    }
                    Err(_) => {
                        debug!("Event bus channel closed");
                        break;
                    }
                }
            }
        });
    }

    /// Process kline data directly
    pub fn process_kline(&self, kline: &KlineData) {
        let timestamp = kline.open_time;

        // Update candle tracker
        {
            let candle_data = Candle {
                open: kline.open,
                high: kline.high,
                low: kline.low,
                close: kline.close,
                volume: kline.volume,
                timestamp: kline.open_time,
            };
            self.candle_tracker.write().update(candle_data, timestamp);
        }

        // Update other trackers if data is available
        if let (Some(ema9), Some(ema21), Some(ema50), Some(ema200)) =
            (kline.ema9, kline.ema21, kline.ema50, kline.ema200) {
            self.ema_tracker.write().update(kline.close, ema9, ema21, ema50, ema200, timestamp);
        }

        if let Some(rsi_val) = kline.rsi {
            self.rsi_tracker.write().update(rsi_val, kline.close, timestamp);
        }

        if let (Some(macd_val), Some(signal)) = (kline.macd, kline.macd_signal) {
            self.macd_tracker.write().update(macd_val, signal, timestamp);
        }

        if let (Some(middle), Some(upper), Some(lower), Some(atr)) =
            (kline.bb_middle, kline.bb_upper, kline.bb_lower, kline.atr) {
            self.bb_tracker.write().update(kline.close, middle, upper, lower, atr, timestamp);
        }

        self.trend_tracker.write().update(kline.high, kline.low, kline.close, timestamp);

        // Check events
        self.ema_tracker.write().check_events(timestamp);
        self.rsi_tracker.write().check_events(timestamp);
        self.macd_tracker.write().check_events(timestamp);
        self.candle_tracker.write().check_events(timestamp);
        self.bb_tracker.write().check_events(timestamp);
        self.trend_tracker.write().check_events(timestamp);
    }

    pub fn get_stats(&self) -> KlineAggregatorStats {
        KlineAggregatorStats {
            symbol: self.symbol.clone(),
            ema_updates: self.ema_tracker.read().updates_processed(),
            ema_events: self.ema_tracker.read().events_fired(),
            rsi_updates: self.rsi_tracker.read().updates_processed(),
            rsi_events: self.rsi_tracker.read().events_fired(),
            macd_updates: self.macd_tracker.read().updates_processed(),
            macd_events: self.macd_tracker.read().events_fired(),
            candle_updates: self.candle_tracker.read().updates_processed(),
            candle_events: self.candle_tracker.read().events_fired(),
            bb_updates: self.bb_tracker.read().updates_processed(),
            bb_events: self.bb_tracker.read().events_fired(),
            trend_updates: self.trend_tracker.read().updates_processed(),
            trend_events: self.trend_tracker.read().events_fired(),
        }
    }

    pub fn thresholds(&self) -> &AggregatorThresholds {
        &self.thresholds
    }
}

#[derive(Debug, Clone)]
pub struct KlineAggregatorStats {
    pub symbol: String,
    pub ema_updates: u64, pub ema_events: u64,
    pub rsi_updates: u64, pub rsi_events: u64,
    pub macd_updates: u64, pub macd_events: u64,
    pub candle_updates: u64, pub candle_events: u64,
    pub bb_updates: u64, pub bb_events: u64,
    pub trend_updates: u64, pub trend_events: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kline_aggregator_creation() {
        let thresholds = AggregatorThresholds::default();
        let aggregator = KlineAggregator::new("BTCUSDT".to_string(), thresholds);
        let stats = aggregator.get_stats();
        assert_eq!(stats.symbol, "BTCUSDT");
    }
}
