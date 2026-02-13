// MarkPrice Aggregator - Main orchestrator that subscribes to EventBus
// Processes MarkPrice events and generates 58 condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::get_event_bus;
use crate::core::types::MarkPrice;
use parking_lot::RwLock;
use std::sync::Arc;
use tracing::{debug, warn};

use super::premium_tracker::PremiumTracker;
use super::funding_tracker::FundingTracker;
use super::divergence_tracker::DivergenceTracker;
use super::liquidation_zone_tracker::LiquidationZoneTracker;

/// MarkPriceAggregator orchestrates all 4 specialized trackers
pub struct MarkPriceAggregator {
    symbol: String,

    premium_tracker: Arc<RwLock<PremiumTracker>>,
    funding_tracker: Arc<RwLock<FundingTracker>>,
    divergence_tracker: Arc<RwLock<DivergenceTracker>>,
    liquidation_zone_tracker: Arc<RwLock<LiquidationZoneTracker>>,

    thresholds: AggregatorThresholds,
}

impl MarkPriceAggregator {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        debug!(symbol = %symbol, "Initializing MarkPriceAggregator");

        let aggregator = Self {
            symbol: symbol.clone(),
            premium_tracker: Arc::new(RwLock::new(PremiumTracker::new(symbol.clone(), thresholds.clone()))),
            funding_tracker: Arc::new(RwLock::new(FundingTracker::new(symbol.clone(), thresholds.clone()))),
            divergence_tracker: Arc::new(RwLock::new(DivergenceTracker::new(symbol.clone(), thresholds.clone()))),
            liquidation_zone_tracker: Arc::new(RwLock::new(LiquidationZoneTracker::new(symbol, thresholds.clone()))),
            thresholds,
        };

        aggregator.start_listening();
        debug!("MarkPriceAggregator initialized");
        aggregator
    }

    fn start_listening(&self) {
        let premium = self.premium_tracker.clone();
        let funding = self.funding_tracker.clone();
        let divergence = self.divergence_tracker.clone();
        let liquidation_zone = self.liquidation_zone_tracker.clone();
        let symbol = self.symbol.clone();

        debug!(symbol = %symbol, "Starting background thread for mark_price events");

        let event_bus = get_event_bus();
        let mut rx = event_bus.subscribe_channel();

        std::thread::spawn(move || {
            debug!(symbol = %symbol, "MarkPriceAggregator event listener started");

            loop {
                match rx.blocking_recv() {
                    Ok(event) => {
                        // Process mark_price events
                        if event.event_type == "mark_price" {
                            if let Some(data_value) = event.data.get("data") {
                                if let Ok(mark_price) = serde_json::from_value::<MarkPrice>(data_value.clone()) {
                                    let timestamp = event.timestamp;

                                    // Update all trackers
                                    premium.write().update(&mark_price, timestamp);
                                    funding.write().update(&mark_price, timestamp);
                                    divergence.write().update(&mark_price, timestamp);
                                    liquidation_zone.write().update(&mark_price, timestamp);

                                    // Check events
                                    premium.write().check_events(timestamp);
                                    funding.write().check_events(timestamp);
                                    divergence.write().check_events(timestamp);
                                    liquidation_zone.write().check_events(timestamp);
                                } else {
                                    warn!("Failed to deserialize MarkPrice");
                                }
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped = skipped, "MarkPriceAggregator lagged");
                    }
                    Err(_) => {
                        debug!("Event bus channel closed");
                        break;
                    }
                }
            }
        });
    }

    /// Process a mark price update directly (for testing or synchronous use)
    pub fn process_mark_price(&self, data: &MarkPrice, timestamp: i64) {
        // Update all trackers
        self.premium_tracker.write().update(data, timestamp);
        self.funding_tracker.write().update(data, timestamp);
        self.divergence_tracker.write().update(data, timestamp);
        self.liquidation_zone_tracker.write().update(data, timestamp);

        // Check events
        self.premium_tracker.write().check_events(timestamp);
        self.funding_tracker.write().check_events(timestamp);
        self.divergence_tracker.write().check_events(timestamp);
        self.liquidation_zone_tracker.write().check_events(timestamp);
    }

    /// Get aggregator statistics
    pub fn get_stats(&self) -> MarkPriceAggregatorStats {
        let premium_updates = self.premium_tracker.read().updates_processed();
        let premium_events = self.premium_tracker.read().events_fired();

        let funding_updates = self.funding_tracker.read().updates_processed();
        let funding_events = self.funding_tracker.read().events_fired();

        let divergence_updates = self.divergence_tracker.read().updates_processed();
        let divergence_events = self.divergence_tracker.read().events_fired();

        let liquidation_zone_updates = self.liquidation_zone_tracker.read().updates_processed();
        let liquidation_zone_events = self.liquidation_zone_tracker.read().events_fired();

        MarkPriceAggregatorStats {
            symbol: self.symbol.clone(),
            total_updates_processed: premium_updates,
            total_events_fired: premium_events + funding_events + divergence_events + liquidation_zone_events,
            premium_updates, premium_events,
            funding_updates, funding_events,
            divergence_updates, divergence_events,
            liquidation_zone_updates, liquidation_zone_events,
        }
    }

    /// Get current premium in bps
    pub fn current_premium_bps(&self) -> f64 {
        self.premium_tracker.read().current_premium_bps()
    }

    /// Get current funding rate
    pub fn current_funding_rate(&self) -> f64 {
        self.funding_tracker.read().current_funding_rate()
    }

    /// Get annualized funding rate
    pub fn annualized_funding(&self) -> f64 {
        self.funding_tracker.read().annualized_funding()
    }

    /// Get mark-index divergence in bps
    pub fn mark_index_divergence_bps(&self) -> f64 {
        self.divergence_tracker.read().mark_index_divergence_bps()
    }

    /// Get cascade risk score
    pub fn cascade_risk_score(&self) -> f64 {
        self.liquidation_zone_tracker.read().cascade_risk_score()
    }

    /// Get time to next funding
    pub fn time_to_funding_ms(&self) -> i64 {
        self.funding_tracker.read().time_to_funding_ms()
    }

    pub fn thresholds(&self) -> &AggregatorThresholds {
        &self.thresholds
    }
}

#[derive(Debug, Clone)]
pub struct MarkPriceAggregatorStats {
    pub symbol: String,
    pub total_updates_processed: u64,
    pub total_events_fired: u64,
    pub premium_updates: u64, pub premium_events: u64,
    pub funding_updates: u64, pub funding_events: u64,
    pub divergence_updates: u64, pub divergence_events: u64,
    pub liquidation_zone_updates: u64, pub liquidation_zone_events: u64,
}
