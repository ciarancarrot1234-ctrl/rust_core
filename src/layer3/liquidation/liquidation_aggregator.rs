// Liquidation Aggregator - Main orchestrator that subscribes to EventBus
// Processes ParsedLiquidation events and generates 52 condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::get_event_bus;
use crate::layer2::parser::ParsedLiquidation;
use parking_lot::RwLock;
use std::sync::Arc;
use tracing::{debug, warn};

use super::individual_tracker::IndividualTracker;
use super::cluster_tracker::ClusterTracker;
use super::cascade_tracker::CascadeTracker;
use super::directional_tracker::DirectionalTracker;
use super::impact_tracker::ImpactTracker;

/// LiquidationAggregator orchestrates all 5 specialized liquidation trackers
pub struct LiquidationAggregator {
    symbol: String,

    individual_tracker: Arc<RwLock<IndividualTracker>>,
    cluster_tracker: Arc<RwLock<ClusterTracker>>,
    cascade_tracker: Arc<RwLock<CascadeTracker>>,
    directional_tracker: Arc<RwLock<DirectionalTracker>>,
    impact_tracker: Arc<RwLock<ImpactTracker>>,

    thresholds: AggregatorThresholds,
}

impl LiquidationAggregator {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        debug!(symbol = %symbol, "Initializing LiquidationAggregator");

        let aggregator = Self {
            symbol: symbol.clone(),
            individual_tracker: Arc::new(RwLock::new(IndividualTracker::new(symbol.clone(), thresholds.clone()))),
            cluster_tracker: Arc::new(RwLock::new(ClusterTracker::new(symbol.clone(), thresholds.clone()))),
            cascade_tracker: Arc::new(RwLock::new(CascadeTracker::new(symbol.clone(), thresholds.clone()))),
            directional_tracker: Arc::new(RwLock::new(DirectionalTracker::new(symbol.clone(), thresholds.clone()))),
            impact_tracker: Arc::new(RwLock::new(ImpactTracker::new(symbol, thresholds.clone()))),
            thresholds,
        };

        aggregator.start_listening();
        debug!(symbol = %symbol, "LiquidationAggregator initialized");
        aggregator
    }

    fn start_listening(&self) {
        let individual = self.individual_tracker.clone();
        let cluster = self.cluster_tracker.clone();
        let cascade = self.cascade_tracker.clone();
        let directional = self.directional_tracker.clone();
        let impact = self.impact_tracker.clone();
        let symbol = self.symbol.clone();

        debug!(symbol = %symbol, "Starting background thread for liquidation events");

        let event_bus = get_event_bus();
        let mut rx = event_bus.subscribe_channel();

        std::thread::spawn(move || {
            debug!(symbol = %symbol, "LiquidationAggregator event listener started");

            loop {
                match rx.blocking_recv() {
                    Ok(event) => {
                        if event.event_type != "liquidation" { continue; }

                        if let Some(data_value) = event.data.get("data") {
                            if let Ok(liq) = serde_json::from_value::<ParsedLiquidation>(data_value.clone()) {
                                if liq.symbol != symbol { continue; }

                                let timestamp = liq.event_time as i64;

                                individual.write().add_liquidation(&liq, timestamp);
                                cluster.write().add_liquidation(&liq, timestamp);
                                cascade.write().add_liquidation(&liq, timestamp);
                                directional.write().add_liquidation(&liq, timestamp);
                                impact.write().add_liquidation(&liq, timestamp);

                                individual.write().check_events(timestamp);
                                cluster.write().check_events(timestamp);
                                cascade.write().check_events(timestamp);
                                directional.write().check_events(timestamp);
                                impact.write().check_events(timestamp);
                            } else {
                                warn!("Failed to deserialize liquidation event");
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped = skipped, "LiquidationAggregator lagged");
                    }
                    Err(_) => {
                        debug!("Event bus channel closed");
                        break;
                    }
                }
            }
        });
    }

    pub fn get_stats(&self) -> LiquidationAggregatorStats {
        let individual_liqs = self.individual_tracker.read().liquidations_processed();
        let individual_events = self.individual_tracker.read().events_fired();

        let cluster_liqs = self.cluster_tracker.read().liquidations_processed();
        let cluster_events = self.cluster_tracker.read().events_fired();

        let cascade_liqs = self.cascade_tracker.read().liquidations_processed();
        let cascade_events = self.cascade_tracker.read().events_fired();

        let directional_liqs = self.directional_tracker.read().liquidations_processed();
        let directional_events = self.directional_tracker.read().events_fired();

        let impact_liqs = self.impact_tracker.read().liquidations_processed();
        let impact_events = self.impact_tracker.read().events_fired();

        LiquidationAggregatorStats {
            symbol: self.symbol.clone(),
            total_liquidations_processed: individual_liqs,
            total_events_fired: individual_events + cluster_events + cascade_events
                + directional_events + impact_events,
            individual_liqs, individual_events,
            cluster_liqs, cluster_events,
            cascade_liqs, cascade_events,
            directional_liqs, directional_events,
            impact_liqs, impact_events,
        }
    }

    pub fn thresholds(&self) -> &AggregatorThresholds { &self.thresholds }
}

#[derive(Debug, Clone)]
pub struct LiquidationAggregatorStats {
    pub symbol: String,
    pub total_liquidations_processed: u64,
    pub total_events_fired: u64,
    pub individual_liqs: u64, pub individual_events: u64,
    pub cluster_liqs: u64, pub cluster_events: u64,
    pub cascade_liqs: u64, pub cascade_events: u64,
    pub directional_liqs: u64, pub directional_events: u64,
    pub impact_liqs: u64, pub impact_events: u64,
}
