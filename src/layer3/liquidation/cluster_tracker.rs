// Cluster Tracker - Monitors liquidation clusters (multiple liquidations in a short window)
// Generates 10 liquidation cluster-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::layer2::parser::ParsedLiquidation;
use crate::layer3::common::event_types::*;
use crate::layer3::common::time_windows::TimeWindow;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// Cluster state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterState {
    Inactive,
    Starting,
    Growing,
    Peaked,
    Ending,
}

/// ClusterTracker detects groups of liquidations occurring in short time windows
pub struct ClusterTracker {
    symbol: String,

    // Rolling windows for cluster detection
    liquidations_window: TimeWindow<ParsedLiquidation>,

    // Cluster metrics
    cluster_count: u64,              // Number of liquidations in current cluster
    cluster_volume: f64,             // Total volume in current cluster
    cluster_start_time: i64,
    cluster_state: ClusterState,

    // Side tracking
    long_count: u64,
    short_count: u64,

    // Price impact tracking
    cluster_start_price: f64,
    cluster_end_price: f64,
    price_impact_pct: f64,

    // Intensity metrics
    intensity: f64,                  // Liquidations per second in cluster
    peak_intensity: f64,

    // Historical clusters
    total_clusters: u64,
    avg_cluster_size: f64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    liquidations_processed: u64,
    events_fired: u64,
}

impl ClusterTracker {
    /// Create a new ClusterTracker
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        let window_ms = (thresholds.liquidation_cluster_window_sec as i64) * 1000;
        Self {
            symbol,
            liquidations_window: TimeWindow::new(window_ms, 1_000),
            cluster_count: 0,
            cluster_volume: 0.0,
            cluster_start_time: 0,
            cluster_state: ClusterState::Inactive,
            long_count: 0,
            short_count: 0,
            cluster_start_price: 0.0,
            cluster_end_price: 0.0,
            price_impact_pct: 0.0,
            intensity: 0.0,
            peak_intensity: 0.0,
            total_clusters: 0,
            avg_cluster_size: 3.0,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 1000,
            thresholds,
            liquidations_processed: 0,
            events_fired: 0,
        }
    }

    /// Add a liquidation
    pub fn add_liquidation(&mut self, liq: &ParsedLiquidation, current_time: i64) {
        self.liquidations_processed += 1;

        let is_long = liq.side == "BUY";
        let notional = liq.notional_usd();

        // Add to window
        self.liquidations_window.add(current_time, liq.clone());
        self.liquidations_window.prune(current_time);

        // Update cluster metrics
        let window_count = self.liquidations_window.len() as u64;

        // Check for cluster start (3+ liquidations in window)
        if window_count >= 3 && self.cluster_state == ClusterState::Inactive {
            self.cluster_state = ClusterState::Starting;
            self.cluster_start_time = current_time;
            self.cluster_start_price = liq.average_price;
            self.long_count = 0;
            self.short_count = 0;
        }

        // Update side counts
        if is_long {
            self.long_count += 1;
        } else {
            self.short_count += 1;
        }

        self.cluster_end_price = liq.average_price;
        self.cluster_count = window_count;
        self.cluster_volume += notional;

        // Calculate intensity
        if self.cluster_start_time > 0 {
            let elapsed_sec = (current_time - self.cluster_start_time) as f64 / 1000.0;
            if elapsed_sec > 0.0 {
                self.intensity = self.cluster_count as f64 / elapsed_sec;
                if self.intensity > self.peak_intensity {
                    self.peak_intensity = self.intensity;
                }
            }
        }

        // Calculate price impact
        if self.cluster_start_price > 0.0 {
            self.price_impact_pct = ((self.cluster_end_price - self.cluster_start_price)
                / self.cluster_start_price) * 100.0;
        }

        // Update state based on intensity
        if self.cluster_state == ClusterState::Starting && self.intensity > 0.5 {
            self.cluster_state = ClusterState::Growing;
        }
    }

    /// Check all cluster events
    pub fn check_events(&mut self, current_time: i64) {
        // Recalculate window metrics
        self.recalculate_metrics();

        // Event 1: Cluster started
        self.check_liq_cluster_started(current_time);

        // Event 2: Cluster growing
        self.check_liq_cluster_growing(current_time);

        // Event 3: Cluster ended
        self.check_liq_cluster_ended(current_time);

        // Event 4: Cluster side dominant
        self.check_liq_cluster_side_dominant(current_time);

        // Event 5: Cluster bilateral
        self.check_liq_cluster_bilateral(current_time);

        // Event 6: Cluster intensity high
        self.check_liq_cluster_intensity_high(current_time);

        // Event 7: Cluster price impact
        self.check_liq_cluster_price_impact(current_time);

        // Event 8: Cluster absorbed
        self.check_liq_cluster_absorbed(current_time);

        // Event 9: Cluster cascading
        self.check_liq_cluster_cascading(current_time);

        // Event 10: Cluster exhausted
        self.check_liq_cluster_exhausted(current_time);
    }

    /// Recalculate metrics from current window
    fn recalculate_metrics(&mut self) {
        let mut volume = 0.0;
        let mut longs = 0u64;
        let mut shorts = 0u64;

        for (_, liq) in self.liquidations_window.iter() {
            volume += liq.notional_usd();
            if liq.side == "BUY" {
                longs += 1;
            } else {
                shorts += 1;
            }
        }

        self.cluster_volume = volume;
        self.long_count = longs;
        self.short_count = shorts;
    }

    /// Event 1: Cluster started
    fn check_liq_cluster_started(&mut self, timestamp: i64) {
        if self.cluster_state == ClusterState::Starting {
            if self.should_fire(LIQ_CLUSTER_STARTED, timestamp) {
                self.total_clusters += 1;
                self.publish_event(
                    LIQ_CLUSTER_STARTED,
                    timestamp,
                    json!({
                        "cluster_count": self.cluster_count,
                        "start_price": self.cluster_start_price,
                    }),
                );
            }
        }
    }

    /// Event 2: Cluster growing
    fn check_liq_cluster_growing(&mut self, timestamp: i64) {
        if self.cluster_state == ClusterState::Growing && self.cluster_count >= 5 {
            if self.should_fire(LIQ_CLUSTER_GROWING, timestamp) {
                self.publish_event(
                    LIQ_CLUSTER_GROWING,
                    timestamp,
                    json!({
                        "cluster_count": self.cluster_count,
                        "cluster_volume": self.cluster_volume,
                        "intensity": self.intensity,
                        "long_count": self.long_count,
                        "short_count": self.short_count,
                    }),
                );
            }
        }
    }

    /// Event 3: Cluster ended
    fn check_liq_cluster_ended(&mut self, timestamp: i64) {
        let window_count = self.liquidations_window.len() as u64;

        // Check if cluster has ended (window count drops below 3)
        if self.cluster_state != ClusterState::Inactive && window_count < 3 {
            if self.should_fire(LIQ_CLUSTER_ENDED, timestamp) {
                self.publish_event(
                    LIQ_CLUSTER_ENDED,
                    timestamp,
                    json!({
                        "final_count": self.cluster_count,
                        "final_volume": self.cluster_volume,
                        "duration_ms": timestamp - self.cluster_start_time,
                        "peak_intensity": self.peak_intensity,
                        "price_impact_pct": self.price_impact_pct,
                    }),
                );

                // Reset cluster state
                self.cluster_state = ClusterState::Inactive;
                self.cluster_count = 0;
                self.cluster_volume = 0.0;
                self.peak_intensity = 0.0;
                self.intensity = 0.0;
            }
        }
    }

    /// Event 4: Cluster side dominant (> 70% one side)
    fn check_liq_cluster_side_dominant(&mut self, timestamp: i64) {
        if self.cluster_count >= 3 {
            let total = self.long_count + self.short_count;
            if total > 0 {
                let long_pct = (self.long_count as f64 / total as f64) * 100.0;
                let short_pct = (self.short_count as f64 / total as f64) * 100.0;

                if long_pct > 70.0 || short_pct > 70.0 {
                    if self.should_fire(LIQ_CLUSTER_SIDE_DOMINANT, timestamp) {
                        let dominant_side = if long_pct > 70.0 { "LONG" } else { "SHORT" };
                        self.publish_event(
                            LIQ_CLUSTER_SIDE_DOMINANT,
                            timestamp,
                            json!({
                                "dominant_side": dominant_side,
                                "long_pct": long_pct,
                                "short_pct": short_pct,
                                "long_count": self.long_count,
                                "short_count": self.short_count,
                            }),
                        );
                    }
                }
            }
        }
    }

    /// Event 5: Cluster bilateral (balanced liquidations on both sides)
    fn check_liq_cluster_bilateral(&mut self, timestamp: i64) {
        if self.cluster_count >= 5 {
            let total = self.long_count + self.short_count;
            if total > 0 {
                let long_pct = (self.long_count as f64 / total as f64) * 100.0;
                let short_pct = (self.short_count as f64 / total as f64) * 100.0;

                // Bilateral if both sides have 40-60%
                if long_pct >= 40.0 && long_pct <= 60.0 {
                    if self.should_fire(LIQ_CLUSTER_BILATERAL, timestamp) {
                        self.publish_event(
                            LIQ_CLUSTER_BILATERAL,
                            timestamp,
                            json!({
                                "long_pct": long_pct,
                                "short_pct": short_pct,
                                "cluster_count": self.cluster_count,
                                "cluster_volume": self.cluster_volume,
                            }),
                        );
                    }
                }
            }
        }
    }

    /// Event 6: Cluster intensity high (> 1 liquidation per second)
    fn check_liq_cluster_intensity_high(&mut self, timestamp: i64) {
        if self.intensity > 1.0 && self.cluster_count >= 5 {
            if self.should_fire(LIQ_CLUSTER_INTENSITY_HIGH, timestamp) {
                self.publish_event(
                    LIQ_CLUSTER_INTENSITY_HIGH,
                    timestamp,
                    json!({
                        "intensity": self.intensity,
                        "cluster_count": self.cluster_count,
                        "cluster_volume": self.cluster_volume,
                    }),
                );
            }
        }
    }

    /// Event 7: Cluster price impact (> 0.5% price move)
    fn check_liq_cluster_price_impact(&mut self, timestamp: i64) {
        if self.price_impact_pct.abs() > 0.5 && self.cluster_count >= 3 {
            if self.should_fire(LIQ_CLUSTER_PRICE_IMPACT, timestamp) {
                self.publish_event(
                    LIQ_CLUSTER_PRICE_IMPACT,
                    timestamp,
                    json!({
                        "price_impact_pct": self.price_impact_pct,
                        "start_price": self.cluster_start_price,
                        "end_price": self.cluster_end_price,
                        "cluster_count": self.cluster_count,
                        "direction": if self.price_impact_pct > 0.0 { "UP" } else { "DOWN" },
                    }),
                );
            }
        }
    }

    /// Event 8: Cluster absorbed (high volume but minimal price impact)
    fn check_liq_cluster_absorbed(&mut self, timestamp: i64) {
        let has_high_volume = self.cluster_volume > self.thresholds.cascade_threshold_usd;
        let low_impact = self.price_impact_pct.abs() < 0.2;
        let has_cluster = self.cluster_count >= 5;

        if has_high_volume && low_impact && has_cluster {
            if self.should_fire(LIQ_CLUSTER_ABSORBED, timestamp) {
                self.publish_event(
                    LIQ_CLUSTER_ABSORBED,
                    timestamp,
                    json!({
                        "cluster_volume": self.cluster_volume,
                        "price_impact_pct": self.price_impact_pct,
                        "cluster_count": self.cluster_count,
                    }),
                );
            }
        }
    }

    /// Event 9: Cluster cascading (intensity accelerating)
    fn check_liq_cluster_cascading(&mut self, timestamp: i64) {
        // Cascading = high intensity + accelerating + price impact
        let is_cascading = self.intensity > 2.0
            && self.price_impact_pct.abs() > 0.3
            && self.cluster_count >= 5;

        if is_cascading {
            if self.should_fire(LIQ_CLUSTER_CASCADING, timestamp) {
                self.publish_event(
                    LIQ_CLUSTER_CASCADING,
                    timestamp,
                    json!({
                        "intensity": self.intensity,
                        "price_impact_pct": self.price_impact_pct,
                        "cluster_count": self.cluster_count,
                        "cluster_volume": self.cluster_volume,
                    }),
                );
            }
        }
    }

    /// Event 10: Cluster exhausted (intensity declining after peak)
    fn check_liq_cluster_exhausted(&mut self, timestamp: i64) {
        let intensity_declining = self.intensity < self.peak_intensity * 0.5;
        let had_peak = self.peak_intensity > 1.0;
        let still_active = self.cluster_state != ClusterState::Inactive;

        if intensity_declining && had_peak && still_active {
            if self.should_fire(LIQ_CLUSTER_EXHAUSTED, timestamp) {
                self.publish_event(
                    LIQ_CLUSTER_EXHAUSTED,
                    timestamp,
                    json!({
                        "current_intensity": self.intensity,
                        "peak_intensity": self.peak_intensity,
                        "cluster_count": self.cluster_count,
                        "cluster_volume": self.cluster_volume,
                    }),
                );
            }
        }
    }

    /// Check if event should be fired (debouncing)
    fn should_fire(&self, event_type: &str, current_time: i64) -> bool {
        if let Some(&last_time) = self.last_event_times.get(event_type) {
            current_time - last_time >= self.min_event_interval_ms
        } else {
            true
        }
    }

    /// Publish event to EventBus
    fn publish_event(&mut self, event_type: &str, timestamp: i64, mut data: serde_json::Value) {
        self.events_fired += 1;

        if let Some(obj) = data.as_object_mut() {
            obj.insert("symbol".to_string(), json!(self.symbol));
        }

        publish_event(
            event_type,
            timestamp,
            serde_json::from_value(data).unwrap_or_default(),
            "liquidation_aggregator",
            EventPriority::High,
        );

        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(
            symbol = %self.symbol,
            event = event_type,
            "Cluster liquidation event published"
        );
    }

    /// Get liquidations processed
    pub fn liquidations_processed(&self) -> u64 {
        self.liquidations_processed
    }

    /// Get events fired
    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    /// Get current cluster state
    #[allow(dead_code)]
    pub fn get_cluster_state(&self) -> ClusterState {
        self.cluster_state
    }

    /// Get current cluster count
    #[allow(dead_code)]
    pub fn get_cluster_count(&self) -> u64 {
        self.cluster_count
    }
}
