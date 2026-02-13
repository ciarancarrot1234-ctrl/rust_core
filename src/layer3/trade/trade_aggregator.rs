// Trade Aggregator - Main orchestrator that subscribes to EventBus
// Processes ParsedAggTrade events and generates 85 condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::get_event_bus;
use crate::layer2::parser::ParsedAggTrade;
use parking_lot::RwLock;
use std::sync::Arc;
use tracing::{debug, warn};

use super::delta::DeltaTracker;
use super::vwap::VwapTracker;
use super::volume::VolumeTracker;
use super::patterns::PatternDetector;
use super::profile::VolumeProfile;
use super::streaks::StreakTracker;

/// TradeAggregator orchestrates all 6 specialized trackers
pub struct TradeAggregator {
    symbol: String,

    delta_tracker: Arc<RwLock<DeltaTracker>>,
    vwap_tracker: Arc<RwLock<VwapTracker>>,
    volume_tracker: Arc<RwLock<VolumeTracker>>,
    pattern_detector: Arc<RwLock<PatternDetector>>,
    volume_profile: Arc<RwLock<VolumeProfile>>,
    streak_tracker: Arc<RwLock<StreakTracker>>,

    thresholds: AggregatorThresholds,
}

impl TradeAggregator {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        debug!(symbol = %symbol, "Initializing TradeAggregator");

        let aggregator = Self {
            symbol: symbol.clone(),
            delta_tracker: Arc::new(RwLock::new(DeltaTracker::new(symbol.clone(), thresholds.clone()))),
            vwap_tracker: Arc::new(RwLock::new(VwapTracker::new(symbol.clone(), thresholds.clone()))),
            volume_tracker: Arc::new(RwLock::new(VolumeTracker::new(symbol.clone(), thresholds.clone()))),
            pattern_detector: Arc::new(RwLock::new(PatternDetector::new(symbol.clone(), thresholds.clone()))),
            volume_profile: Arc::new(RwLock::new(VolumeProfile::new(symbol.clone(), thresholds.clone()))),
            streak_tracker: Arc::new(RwLock::new(StreakTracker::new(symbol, thresholds.clone()))),
            thresholds,
        };

        aggregator.start_listening();
        debug!(symbol = %symbol, "TradeAggregator initialized");
        aggregator
    }

    fn start_listening(&self) {
        let delta = self.delta_tracker.clone();
        let vwap = self.vwap_tracker.clone();
        let volume = self.volume_tracker.clone();
        let pattern = self.pattern_detector.clone();
        let profile = self.volume_profile.clone();
        let streak = self.streak_tracker.clone();
        let symbol = self.symbol.clone();

        debug!(symbol = %symbol, "Starting background thread for agg_trade events");

        let event_bus = get_event_bus();
        let mut rx = event_bus.subscribe_channel();

        std::thread::spawn(move || {
            debug!(symbol = %symbol, "TradeAggregator event listener started");

            loop {
                match rx.blocking_recv() {
                    Ok(event) => {
                        if event.event_type != "agg_trade" { continue; }

                        if let Some(data_value) = event.data.get("data") {
                            if let Ok(trade) = serde_json::from_value::<ParsedAggTrade>(data_value.clone()) {
                                if trade.symbol != symbol { continue; }

                                let timestamp = trade.event_time as i64;

                                delta.write().add_trade(&trade, timestamp);
                                vwap.write().add_trade(&trade, timestamp);
                                volume.write().add_trade(&trade, timestamp);
                                pattern.write().add_trade(&trade, timestamp);
                                profile.write().add_trade(&trade, timestamp);
                                streak.write().add_trade(&trade, timestamp);

                                delta.write().check_events(timestamp);
                                vwap.write().check_events(timestamp);
                                volume.write().check_events(timestamp);
                                pattern.write().check_events(timestamp);
                                profile.write().check_events(timestamp);
                                streak.write().check_events(timestamp);
                            } else {
                                warn!("Failed to deserialize agg_trade");
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped = skipped, "TradeAggregator lagged");
                    }
                    Err(_) => {
                        debug!("Event bus channel closed");
                        break;
                    }
                }
            }
        });
    }

    pub fn get_stats(&self) -> TradeAggregatorStats {
        let delta_trades = self.delta_tracker.read().trades_processed();
        let delta_events = self.delta_tracker.read().events_fired();

        let vwap_trades = self.vwap_tracker.read().trades_processed();
        let vwap_events = self.vwap_tracker.read().events_fired();

        let volume_trades = self.volume_tracker.read().trades_processed();
        let volume_events = self.volume_tracker.read().events_fired();

        let pattern_trades = self.pattern_detector.read().trades_processed();
        let pattern_events = self.pattern_detector.read().events_fired();

        let profile_trades = self.volume_profile.read().trades_processed();
        let profile_events = self.volume_profile.read().events_fired();

        let streak_trades = self.streak_tracker.read().trades_processed();
        let streak_events = self.streak_tracker.read().events_fired();

        TradeAggregatorStats {
            symbol: self.symbol.clone(),
            total_trades_processed: delta_trades,
            total_events_fired: delta_events + vwap_events + volume_events
                + pattern_events + profile_events + streak_events,
            delta_trades, delta_events,
            vwap_trades, vwap_events,
            volume_trades, volume_events,
            pattern_trades, pattern_events,
            profile_trades, profile_events,
            streak_trades, streak_events,
        }
    }

    pub fn thresholds(&self) -> &AggregatorThresholds { &self.thresholds }
}

#[derive(Debug, Clone)]
pub struct TradeAggregatorStats {
    pub symbol: String,
    pub total_trades_processed: u64,
    pub total_events_fired: u64,
    pub delta_trades: u64, pub delta_events: u64,
    pub vwap_trades: u64, pub vwap_events: u64,
    pub volume_trades: u64, pub volume_events: u64,
    pub pattern_trades: u64, pub pattern_events: u64,
    pub profile_trades: u64, pub profile_events: u64,
    pub streak_trades: u64, pub streak_events: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_aggregator_creation() {
        let thresholds = AggregatorThresholds::default();
        let aggregator = TradeAggregator::new("BTCUSDT".to_string(), thresholds);
        let stats = aggregator.get_stats();
        assert_eq!(stats.symbol, "BTCUSDT");
    }
}
