// Liquidation Zone Tracker - Monitors liquidation zones based on mark price
// Generates 6 liquidation-zone-related condition events

use crate::core::config::AggregatorThresholds;
use crate::core::events::{publish_event, EventPriority};
use crate::core::types::MarkPrice;
use crate::layer3::common::event_types::*;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

/// LiquidationZoneTracker monitors proximity to liquidation zones
pub struct LiquidationZoneTracker {
    symbol: String,

    // Current mark price
    mark_price: f64,
    index_price: f64,

    // Estimated liquidation zones (these would typically come from external data)
    long_liquidation_zone: f64,
    short_liquidation_zone: f64,
    
    // Distance to zones (in percentage)
    distance_to_long_liq_pct: f64,
    distance_to_short_liq_pct: f64,

    // Zone status
    near_long_zone: bool,
    near_short_zone: bool,
    in_long_zone: bool,
    in_short_zone: bool,
    prev_in_long_zone: bool,
    prev_in_short_zone: bool,

    // Zone tracking
    time_near_zone_ms: u64,
    cluster_strength: f64,

    // Magnet effect tracking
    magnet_pull_strength: f64,
    magnet_direction: Option<String>,

    // Defense tracking
    defense_detected: bool,
    defense_count: u32,

    // Cascade tracking
    cascade_risk_score: f64,
    cascade_zone_active: bool,

    // Zone history
    zone_visits: u64,
    zone_tests: u64,

    // Event debouncing
    last_event_times: HashMap<String, i64>,
    min_event_interval_ms: i64,

    thresholds: AggregatorThresholds,

    // Statistics
    updates_processed: u64,
    events_fired: u64,
}

impl LiquidationZoneTracker {
    pub fn new(symbol: String, thresholds: AggregatorThresholds) -> Self {
        Self {
            symbol,
            mark_price: 0.0,
            index_price: 0.0,
            long_liquidation_zone: 0.0,
            short_liquidation_zone: 0.0,
            distance_to_long_liq_pct: f64::MAX,
            distance_to_short_liq_pct: f64::MAX,
            near_long_zone: false,
            near_short_zone: false,
            in_long_zone: false,
            in_short_zone: false,
            prev_in_long_zone: false,
            prev_in_short_zone: false,
            time_near_zone_ms: 0,
            cluster_strength: 0.0,
            magnet_pull_strength: 0.0,
            magnet_direction: None,
            defense_detected: false,
            defense_count: 0,
            cascade_risk_score: 0.0,
            cascade_zone_active: false,
            zone_visits: 0,
            zone_tests: 0,
            last_event_times: HashMap::new(),
            min_event_interval_ms: 1000,
            thresholds,
            updates_processed: 0,
            events_fired: 0,
        }
    }

    /// Process mark price update
    pub fn update(&mut self, data: &MarkPrice, current_time: i64) {
        self.updates_processed += 1;

        // Store previous zone status
        self.prev_in_long_zone = self.in_long_zone;
        self.prev_in_short_zone = self.in_short_zone;

        // Update current prices
        self.mark_price = data.mark_price;
        self.index_price = data.index_price;

        // Estimate liquidation zones based on price movement
        // In production, these would come from exchange data or calculations
        self.estimate_liquidation_zones();

        // Calculate distances
        self.calculate_distances();

        // Update zone status
        self.update_zone_status();

        // Update tracking metrics
        self.update_tracking_metrics();
    }

    /// Check all liquidation zone events
    pub fn check_events(&mut self, current_time: i64) {
        self.check_liquidation_near_cluster(current_time);
        self.check_liquidation_breached(current_time);
        self.check_liquidation_exited(current_time);
        self.check_liquidation_magnet(current_time);
        self.check_liquidation_defended(current_time);
        self.check_liquidation_cascade_zone(current_time);
    }

    fn estimate_liquidation_zones(&mut self) {
        // Estimate liquidation zones at +/- 5% from current price
        // In production, these would be based on actual positions and leverage data
        let zone_distance_pct = 0.05;
        self.long_liquidation_zone = self.mark_price * (1.0 - zone_distance_pct);
        self.short_liquidation_zone = self.mark_price * (1.0 + zone_distance_pct);
    }

    fn calculate_distances(&mut self) {
        if self.mark_price > 0.0 {
            self.distance_to_long_liq_pct = (self.mark_price - self.long_liquidation_zone) / self.mark_price * 100.0;
            self.distance_to_short_liq_pct = (self.short_liquidation_zone - self.mark_price) / self.mark_price * 100.0;
        }
    }

    fn update_zone_status(&mut self) {
        let near_threshold = 2.0; // 2% 
        let in_zone_threshold = 0.5; // 0.5%

        // Check proximity to zones
        self.near_long_zone = self.distance_to_long_liq_pct < near_threshold;
        self.near_short_zone = self.distance_to_short_liq_pct < near_threshold;

        // Check if in zone
        self.in_long_zone = self.distance_to_long_liq_pct < in_zone_threshold;
        self.in_short_zone = self.distance_to_short_liq_pct < in_zone_threshold;
    }

    fn update_tracking_metrics(&mut self) {
        // Track time near zones
        if self.near_long_zone || self.near_short_zone {
            self.time_near_zone_ms += 1;
        } else {
            self.time_near_zone_ms = 0;
        }

        // Calculate cluster strength (simulated)
        self.cluster_strength = if self.near_long_zone || self.near_short_zone {
            1.0 - (self.distance_to_long_liq_pct.min(self.distance_to_short_liq_pct) / 2.0)
        } else {
            0.0
        };

        // Calculate magnet effect
        self.calculate_magnet_effect();

        // Update cascade risk
        self.update_cascade_risk();
    }

    fn calculate_magnet_effect(&mut self) {
        if self.near_long_zone && self.mark_price < self.index_price {
            self.magnet_pull_strength = (1.0 - self.distance_to_long_liq_pct / 2.0) * 0.5;
            self.magnet_direction = Some("down".to_string());
        } else if self.near_short_zone && self.mark_price > self.index_price {
            self.magnet_pull_strength = (1.0 - self.distance_to_short_liq_pct / 2.0) * 0.5;
            self.magnet_direction = Some("up".to_string());
        } else {
            self.magnet_pull_strength = 0.0;
            self.magnet_direction = None;
        }
    }

    fn update_cascade_risk(&mut self) {
        // Cascade risk increases when price is near multiple liquidation zones
        // or when there's strong directional movement
        let near_both = self.near_long_zone && self.near_short_zone;
        let near_any = self.near_long_zone || self.near_short_zone;

        self.cascade_risk_score = if near_both {
            0.9
        } else if near_any {
            self.cluster_strength * 0.7
        } else {
            0.0
        };

        self.cascade_zone_active = self.cascade_risk_score > 0.5;
    }

    /// Event 1: Near liquidation cluster detected
    fn check_liquidation_near_cluster(&mut self, timestamp: i64) {
        if self.cluster_strength > 0.7 && (self.near_long_zone || self.near_short_zone) {
            if self.should_fire(MP_LIQUIDATION_NEAR_CLUSTER, timestamp) {
                self.publish_event(
                    MP_LIQUIDATION_NEAR_CLUSTER,
                    timestamp,
                    json!({
                        "mark_price": self.mark_price,
                        "cluster_strength": self.cluster_strength,
                        "zone_type": if self.near_long_zone { "long" } else { "short" },
                        "distance_pct": if self.near_long_zone { 
                            self.distance_to_long_liq_pct 
                        } else { 
                            self.distance_to_short_liq_pct 
                        },
                    }),
                );
                self.zone_visits += 1;
            }
        }
    }

    /// Event 2: Liquidation zone breached
    fn check_liquidation_breached(&mut self, timestamp: i64) {
        let breached_long = self.in_long_zone && !self.prev_in_long_zone;
        let breached_short = self.in_short_zone && !self.prev_in_short_zone;

        if breached_long || breached_short {
            if self.should_fire(MP_LIQUIDATION_BREACHED, timestamp) {
                self.publish_event(
                    MP_LIQUIDATION_BREACHED,
                    timestamp,
                    json!({
                        "mark_price": self.mark_price,
                        "zone_type": if breached_long { "long" } else { "short" },
                        "zone_price": if breached_long { 
                            self.long_liquidation_zone 
                        } else { 
                            self.short_liquidation_zone 
                        },
                    }),
                );
            }
        }
    }

    /// Event 3: Exited liquidation zone
    fn check_liquidation_exited(&mut self, timestamp: i64) {
        let exited_long = !self.in_long_zone && self.prev_in_long_zone;
        let exited_short = !self.in_short_zone && self.prev_in_short_zone;

        if exited_long || exited_short {
            if self.should_fire(MP_LIQUIDATION_EXITED, timestamp) {
                self.publish_event(
                    MP_LIQUIDATION_EXITED,
                    timestamp,
                    json!({
                        "mark_price": self.mark_price,
                        "zone_type": if exited_long { "long" } else { "short" },
                        "zone_price": if exited_long { 
                            self.long_liquidation_zone 
                        } else { 
                            self.short_liquidation_zone 
                        },
                    }),
                );
                self.zone_tests += 1;
            }
        }
    }

    /// Event 4: Magnet effect detected
    fn check_liquidation_magnet(&mut self, timestamp: i64) {
        if self.magnet_pull_strength > 0.3 {
            if let Some(ref direction) = self.magnet_direction {
                if self.should_fire(MP_LIQUIDATION_MAGNET, timestamp) {
                    self.publish_event(
                        MP_LIQUIDATION_MAGNET,
                        timestamp,
                        json!({
                            "mark_price": self.mark_price,
                            "magnet_strength": self.magnet_pull_strength,
                            "direction": direction,
                            "target_zone": if direction == "down" { 
                                "long_liquidations" 
                            } else { 
                                "short_liquidations" 
                            },
                        }),
                    );
                }
            }
        }
    }

    /// Event 5: Zone defense detected
    fn check_liquidation_defended(&mut self, timestamp: i64) {
        // Defense detected when price bounces from zone
        let was_in_zone = self.prev_in_long_zone || self.prev_in_short_zone;
        let now_near = self.near_long_zone || self.near_short_zone;
        let exited_zone = was_in_zone && !self.in_long_zone && !self.in_short_zone && now_near;

        if exited_zone {
            self.defense_count += 1;
            self.defense_detected = true;
        }

        if self.defense_detected && self.defense_count >= 1 {
            if self.should_fire(MP_LIQUIDATION_DEFENDED, timestamp) {
                self.publish_event(
                    MP_LIQUIDATION_DEFENDED,
                    timestamp,
                    json!({
                        "mark_price": self.mark_price,
                        "defense_count": self.defense_count,
                        "zone_type": if self.near_long_zone { "long" } else { "short" },
                    }),
                );
                self.defense_detected = false;
            }
        }
    }

    /// Event 6: Cascade zone activated
    fn check_liquidation_cascade_zone(&mut self, timestamp: i64) {
        if self.cascade_zone_active {
            if self.should_fire(MP_LIQUIDATION_CASCADE_ZONE, timestamp) {
                self.publish_event(
                    MP_LIQUIDATION_CASCADE_ZONE,
                    timestamp,
                    json!({
                        "mark_price": self.mark_price,
                        "cascade_risk_score": self.cascade_risk_score,
                        "near_long_zone": self.near_long_zone,
                        "near_short_zone": self.near_short_zone,
                        "cluster_strength": self.cluster_strength,
                    }),
                );
            }
        }
    }

    fn should_fire(&self, event_type: &str, current_time: i64) -> bool {
        if let Some(&last_time) = self.last_event_times.get(event_type) {
            current_time - last_time >= self.min_event_interval_ms
        } else {
            true
        }
    }

    fn publish_event(&mut self, event_type: &str, timestamp: i64, mut data: serde_json::Value) {
        self.events_fired += 1;

        if let Some(obj) = data.as_object_mut() {
            obj.insert("symbol".to_string(), json!(self.symbol));
        }

        publish_event(
            event_type,
            timestamp,
            serde_json::from_value(data).unwrap_or_default(),
            "markprice_aggregator",
            EventPriority::Medium,
        );

        self.last_event_times.insert(event_type.to_string(), timestamp);
        debug!(symbol = %self.symbol, event = event_type, "Liquidation zone event published");
    }

    pub fn updates_processed(&self) -> u64 {
        self.updates_processed
    }

    pub fn events_fired(&self) -> u64 {
        self.events_fired
    }

    /// Get cascade risk score
    pub fn cascade_risk_score(&self) -> f64 {
        self.cascade_risk_score
    }

    /// Get distance to long liquidation zone
    pub fn distance_to_long_liq_pct(&self) -> f64 {
        self.distance_to_long_liq_pct
    }

    /// Get distance to short liquidation zone
    pub fn distance_to_short_liq_pct(&self) -> f64 {
        self.distance_to_short_liq_pct
    }
}
