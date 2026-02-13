// Binance Data Aggregator - Main orchestrator for Binance data tracking
// Subscribes to EventBus for Binance data events and generates 75 condition events
// Input sources: REST API polling at various intervals

use crate::core::config::AggregatorThresholds;
use crate::core::events::get_event_bus;
use parking_lot::RwLock;
use std::sync::Arc;
use tracing::{debug, warn};

use super::open_interest_tracker::{OpenInterestTracker, OIDataPoint};
use super::long_short_tracker::{LongShortTracker, LSDataPoint};
use super::taker_volume_tracker::{TakerVolumeTracker, TakerVolumeDataPoint};
use super::top_trader_tracker::{TopTraderTracker, TopTraderDataPoint};
use super::insurance_fund_tracker::{InsuranceFundTracker, InsuranceFundDataPoint};

/// BinanceDataAggregator orchestrates all 5 specialized Binance data trackers
pub struct BinanceDataAggregator {
    symbol: String,

    open_interest_tracker: Arc<RwLock<OpenInterestTracker>>,
    long_short_tracker: Arc<RwLock<LongShortTracker>>,
    taker_volume_tracker: Arc<RwLock<TakerVolumeTracker>>,
    top_trader_tracker: Arc<RwLock<TopTraderTracker>>,
    insurance_fund_tracker: Arc<RwLock<InsuranceFundTracker>>,

    thresholds: AggregatorThresholds,
}

impl BinanceDataAggregator {
    /// Create a new BinanceDataAggregator
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        debug!(symbol = %symbol, "Initializing BinanceDataAggregator");

        let aggregator = Self {
            symbol: symbol.clone(),
            open_interest_tracker: Arc::new(RwLock::new(
                OpenInterestTracker::new(symbol.clone(), thresholds.clone())
            )),
            long_short_tracker: Arc::new(RwLock::new(
                LongShortTracker::new(symbol.clone(), thresholds.clone())
            )),
            taker_volume_tracker: Arc::new(RwLock::new(
                TakerVolumeTracker::new(symbol.clone(), thresholds.clone())
            )),
            top_trader_tracker: Arc::new(RwLock::new(
                TopTraderTracker::new(symbol.clone(), thresholds.clone())
            )),
            insurance_fund_tracker: Arc::new(RwLock::new(
                InsuranceFundTracker::new(symbol, thresholds.clone())
            )),
            thresholds,
        };

        aggregator.start_listening();
        debug!("BinanceDataAggregator initialized");
        aggregator
    }

    /// Start listening for Binance data events
    fn start_listening(&self) {
        let oi_tracker = self.open_interest_tracker.clone();
        let ls_tracker = self.long_short_tracker.clone();
        let taker_tracker = self.taker_volume_tracker.clone();
        let top_tracker = self.top_trader_tracker.clone();
        let insurance_tracker = self.insurance_fund_tracker.clone();
        let symbol = self.symbol.clone();

        debug!(symbol = %symbol, "Starting background thread for Binance data events");

        let event_bus = get_event_bus();
        let mut rx = event_bus.subscribe_channel();

        std::thread::spawn(move || {
            debug!(symbol = %symbol, "BinanceDataAggregator event listener started");

            loop {
                match rx.blocking_recv() {
                    Ok(event) => {
                        // Process open interest events
                        if event.event_type == "open_interest" {
                            if let Some(data_value) = event.data.get("data") {
                                if let Ok(oi_data) = serde_json::from_value::<OIDataPoint>(data_value.clone()) {
                                    if oi_data.symbol == symbol {
                                        let timestamp = oi_data.timestamp;
                                        oi_tracker.write().update_oi(&oi_data, timestamp);
                                        oi_tracker.write().check_events(timestamp);
                                    }
                                }
                            }
                        }

                        // Process long/short ratio events
                        else if event.event_type == "long_short_ratio" {
                            if let Some(data_value) = event.data.get("data") {
                                if let Ok(ls_data) = serde_json::from_value::<LSDataPoint>(data_value.clone()) {
                                    if ls_data.symbol == symbol {
                                        let timestamp = ls_data.timestamp;
                                        ls_tracker.write().update_ls(&ls_data, timestamp);
                                        ls_tracker.write().check_events(timestamp);
                                    }
                                }
                            }
                        }

                        // Process taker volume events
                        else if event.event_type == "taker_volume" {
                            if let Some(data_value) = event.data.get("data") {
                                if let Ok(taker_data) = serde_json::from_value::<TakerVolumeDataPoint>(data_value.clone()) {
                                    if taker_data.symbol == symbol {
                                        let timestamp = taker_data.timestamp;
                                        taker_tracker.write().update_taker_volume(&taker_data, timestamp);
                                        taker_tracker.write().check_events(timestamp);
                                    }
                                }
                            }
                        }

                        // Process top trader events
                        else if event.event_type == "top_trader" {
                            if let Some(data_value) = event.data.get("data") {
                                if let Ok(top_data) = serde_json::from_value::<TopTraderDataPoint>(data_value.clone()) {
                                    if top_data.symbol == symbol {
                                        let timestamp = top_data.timestamp;
                                        top_tracker.write().update_top_trader(&top_data, timestamp);
                                        top_tracker.write().check_events(timestamp);
                                    }
                                }
                            }
                        }

                        // Process insurance fund events
                        else if event.event_type == "insurance_fund" {
                            if let Some(data_value) = event.data.get("data") {
                                if let Ok(ins_data) = serde_json::from_value::<InsuranceFundDataPoint>(data_value.clone()) {
                                    if ins_data.symbol == symbol {
                                        let timestamp = ins_data.timestamp;
                                        insurance_tracker.write().update_insurance(&ins_data, timestamp);
                                        insurance_tracker.write().check_events(timestamp);
                                    }
                                }
                            }
                        }

                        // Cross-tracker updates (price, liquidations)
                        else if event.event_type == "price_update" {
                            if let Some(price) = event.data.get("price").and_then(|p| p.as_f64()) {
                                oi_tracker.write().update_price(price);
                            }
                        }
                        else if event.event_type == "liquidation" {
                            if let Some(liquidation_usd) = event.data.get("usd_value").and_then(|v| v.as_f64()) {
                                insurance_tracker.write().update_liquidations(liquidation_usd);
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped = skipped, "BinanceDataAggregator lagged");
                    }
                    Err(_) => {
                        debug!("Event bus channel closed");
                        break;
                    }
                }
            }
        });
    }

    /// Get aggregator statistics
    pub fn get_stats(&self) -> BinanceDataAggregatorStats {
        let oi_updates = self.open_interest_tracker.read().updates_processed();
        let oi_events = self.open_interest_tracker.read().events_fired();

        let ls_updates = self.long_short_tracker.read().updates_processed();
        let ls_events = self.long_short_tracker.read().events_fired();

        let taker_updates = self.taker_volume_tracker.read().updates_processed();
        let taker_events = self.taker_volume_tracker.read().events_fired();

        let top_updates = self.top_trader_tracker.read().updates_processed();
        let top_events = self.top_trader_tracker.read().events_fired();

        let insurance_updates = self.insurance_fund_tracker.read().updates_processed();
        let insurance_events = self.insurance_fund_tracker.read().events_fired();

        BinanceDataAggregatorStats {
            symbol: self.symbol.clone(),
            total_updates_processed: oi_updates + ls_updates + taker_updates + top_updates + insurance_updates,
            total_events_fired: oi_events + ls_events + taker_events + top_events + insurance_events,
            oi_updates,
            oi_events,
            ls_updates,
            ls_events,
            taker_updates,
            taker_events,
            top_updates,
            top_events,
            insurance_updates,
            insurance_events,
        }
    }

    /// Get thresholds reference
    pub fn thresholds(&self) -> &AggregatorThresholds {
        &self.thresholds
    }

    /// Get current open interest
    pub fn current_open_interest(&self) -> f64 {
        self.open_interest_tracker.read().current_oi()
    }

    /// Get current long/short ratio
    pub fn current_long_short_ratio(&self) -> f64 {
        self.long_short_tracker.read().current_ratio()
    }

    /// Get current taker buy/sell ratio
    pub fn current_taker_ratio(&self) -> f64 {
        self.taker_volume_tracker.read().current_ratio()
    }

    /// Get current top trader ratio
    pub fn current_top_trader_ratio(&self) -> f64 {
        self.top_trader_tracker.read().current_ratio()
    }

    /// Get current insurance fund balance
    pub fn current_insurance_balance(&self) -> f64 {
        self.insurance_fund_tracker.read().current_balance()
    }
}

/// Statistics for BinanceDataAggregator
#[derive(Debug, Clone)]
pub struct BinanceDataAggregatorStats {
    pub symbol: String,
    pub total_updates_processed: u64,
    pub total_events_fired: u64,
    pub oi_updates: u64,
    pub oi_events: u64,
    pub ls_updates: u64,
    pub ls_events: u64,
    pub taker_updates: u64,
    pub taker_events: u64,
    pub top_updates: u64,
    pub top_events: u64,
    pub insurance_updates: u64,
    pub insurance_events: u64,
}
