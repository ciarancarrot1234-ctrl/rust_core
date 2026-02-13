// Time Synchronizer for Binance - Pure Rust
// NTP-style time synchronization with network latency compensation
// Critical: Binance rejects requests with timestamp >1000ms off

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use parking_lot::RwLock;
use tokio::task::JoinHandle;
use std::fmt;
use tracing::{info, warn, debug};

use crate::layer1::rest_client::BinanceRestClient;

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum TimeSyncError {
    #[error("REST client error: {0}")]
    RestClient(#[from] crate::layer1::rest_client::RestClientError),
    #[error("Sync failed after {0} samples")]
    SyncFailed(u32),
    #[error("Not synced")]
    NotSynced,
}

// ============================================================================
// Statistics
// ============================================================================

#[derive(Debug, Clone)]
pub struct TimeSyncStats {
    pub offset_ms: f64,
    pub last_sync_age_secs: Option<f64>,
    pub last_latency_ms: f64,
    pub sync_count: u64,
    pub sync_failures: u64,
    pub is_synced: bool,
    pub offset_stability_ms: Option<f64>,
}

impl fmt::Display for TimeSyncStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TimeSyncStats(offset={:.1}ms, latency={:.1}ms, syncs={}, failures={}, synced={})",
            self.offset_ms, self.last_latency_ms, self.sync_count,
            self.sync_failures, self.is_synced
        )
    }
}

// ============================================================================
// Offset History Entry
// ============================================================================

#[derive(Debug, Clone)]
struct OffsetSample {
    timestamp: f64,
    offset_ms: f64,
    latency_ms: f64,
}

// ============================================================================
// Time Synchronizer
// ============================================================================

/// Maintains time synchronization with Binance servers
///
/// Features:
/// - Periodic sync with Binance server time (every 5 minutes)
/// - Multiple samples with median filtering to reduce jitter
/// - Network latency compensation (half round-trip)
/// - Drift monitoring and alerting (>1s drift)
/// - Offset stability tracking (std dev of recent offsets)
pub struct TimeSynchronizer {
    rest_client: Arc<BinanceRestClient>,

    // Time offset (milliseconds): server_time = local_time + offset
    offset_ms: Arc<RwLock<f64>>,
    last_sync_time: Arc<RwLock<f64>>,

    // Drift tracking
    offset_history: Arc<RwLock<Vec<OffsetSample>>>,
    max_history: usize,
    drift_threshold_ms: f64,

    // Sync configuration
    sync_interval_secs: u64,

    // Stats
    sync_count: Arc<RwLock<u64>>,
    sync_failures: Arc<RwLock<u64>>,
    last_network_latency_ms: Arc<RwLock<f64>>,

    // Background task handle
    sync_task: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl TimeSynchronizer {
    pub fn new(rest_client: Arc<BinanceRestClient>) -> Self {
        info!("Time synchronizer initialized");

        Self {
            rest_client,
            offset_ms: Arc::new(RwLock::new(0.0)),
            last_sync_time: Arc::new(RwLock::new(0.0)),
            offset_history: Arc::new(RwLock::new(Vec::new())),
            max_history: 100,
            drift_threshold_ms: 1000.0,
            sync_interval_secs: 300, // 5 minutes
            sync_count: Arc::new(RwLock::new(0)),
            sync_failures: Arc::new(RwLock::new(0)),
            last_network_latency_ms: Arc::new(RwLock::new(0.0)),
            sync_task: Arc::new(RwLock::new(None)),
        }
    }

    /// Start periodic time synchronization
    pub async fn start(&self) -> Result<(), TimeSyncError> {
        // Initial sync
        self.sync_time(5).await?;

        // Start periodic sync loop
        let offset_ms = self.offset_ms.clone();
        let last_sync_time = self.last_sync_time.clone();
        let offset_history = self.offset_history.clone();
        let sync_count = self.sync_count.clone();
        let sync_failures = self.sync_failures.clone();
        let last_latency = self.last_network_latency_ms.clone();
        let rest_client = self.rest_client.clone();
        let interval = self.sync_interval_secs;
        let max_history = self.max_history;
        let drift_threshold = self.drift_threshold_ms;

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(interval)).await;

                if let Err(e) = sync_time_inner(
                    &rest_client, 5,
                    &offset_ms, &last_sync_time, &offset_history,
                    max_history, drift_threshold,
                    &sync_count, &sync_failures, &last_latency,
                ).await {
                    warn!(error = %e, "Sync loop error");
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
        });

        *self.sync_task.write() = Some(handle);
        info!("Time synchronization started");
        Ok(())
    }

    /// Stop time synchronization
    pub fn stop(&self) {
        if let Some(handle) = self.sync_task.write().take() {
            handle.abort();
        }
        info!("Time synchronization stopped");
    }

    /// Synchronize time with Binance server
    ///
    /// Takes multiple samples and uses median to reduce network jitter effects
    pub async fn sync_time(&self, samples: u32) -> Result<bool, TimeSyncError> {
        sync_time_inner(
            &self.rest_client, samples,
            &self.offset_ms, &self.last_sync_time, &self.offset_history,
            self.max_history, self.drift_threshold_ms,
            &self.sync_count, &self.sync_failures, &self.last_network_latency_ms,
        ).await
    }

    /// Get current time in milliseconds, adjusted for Binance offset
    pub fn get_current_time_ms(&self) -> u64 {
        let local_ms = now_millis_f64();
        let offset = *self.offset_ms.read();
        (local_ms + offset) as u64
    }

    /// Get current time offset in milliseconds
    pub fn get_offset_ms(&self) -> f64 {
        *self.offset_ms.read()
    }

    /// Check if time is recently synced
    pub fn is_synced(&self, max_age_secs: u64) -> bool {
        let last_sync = *self.last_sync_time.read();
        if last_sync == 0.0 {
            return false;
        }
        let age = now_secs_f64() - last_sync;
        age < max_age_secs as f64
    }

    /// Get synchronization statistics
    pub fn get_stats(&self) -> TimeSyncStats {
        let offset = *self.offset_ms.read();
        let last_sync = *self.last_sync_time.read();
        let last_sync_age = if last_sync > 0.0 {
            Some(now_secs_f64() - last_sync)
        } else {
            None
        };

        let history = self.offset_history.read();
        let (stability, avg_latency) = if history.len() > 1 {
            let recent: Vec<&OffsetSample> = history.iter()
                .rev()
                .take(10)
                .collect();
            let offsets: Vec<f64> = recent.iter().map(|s| s.offset_ms).collect();
            let mean = offsets.iter().sum::<f64>() / offsets.len() as f64;
            let variance = offsets.iter()
                .map(|x| (x - mean).powi(2))
                .sum::<f64>() / offsets.len() as f64;
            let avg_lat = recent.iter().map(|s| s.latency_ms).sum::<f64>() / recent.len() as f64;
            (Some(variance.sqrt()), avg_lat)
        } else {
            (None, *self.last_network_latency_ms.read())
        };

        TimeSyncStats {
            offset_ms: offset,
            last_sync_age_secs: last_sync_age,
            last_latency_ms: avg_latency,
            sync_count: *self.sync_count.read(),
            sync_failures: *self.sync_failures.read(),
            is_synced: self.is_synced(600),
            offset_stability_ms: stability,
        }
    }
}

/// Internal sync implementation (shared between initial sync and background loop)
async fn sync_time_inner(
    rest_client: &BinanceRestClient,
    samples: u32,
    offset_ms: &RwLock<f64>,
    last_sync_time: &RwLock<f64>,
    offset_history: &RwLock<Vec<OffsetSample>>,
    max_history: usize,
    drift_threshold_ms: f64,
    sync_count: &RwLock<u64>,
    sync_failures: &RwLock<u64>,
    last_network_latency_ms: &RwLock<f64>,
) -> Result<bool, TimeSyncError> {
    let mut offsets = Vec::with_capacity(samples as usize);
    let mut latencies = Vec::with_capacity(samples as usize);

    for _ in 0..samples {
        // Measure round-trip time
        let start_local = now_millis_f64();
        let server_time = rest_client.get_server_time().await? as f64;
        let end_local = now_millis_f64();

        // Network latency = half of round-trip
        let latency = (end_local - start_local) / 2.0;
        latencies.push(latency);

        // Calculate offset: assume server time is at midpoint of request
        let mid_local = start_local + latency;
        let offset = server_time - mid_local;
        offsets.push(offset);

        // Small delay between samples
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    if offsets.is_empty() {
        *sync_failures.write() += 1;
        return Err(TimeSyncError::SyncFailed(samples));
    }

    // Use median to reduce outlier effects
    offsets.sort_by(|a, b| a.partial_cmp(b).unwrap());
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let median_offset = median(&offsets);
    let median_latency = median(&latencies);

    let now = now_secs_f64();

    // Check for significant drift before updating
    {
        let history = offset_history.read();
        if let Some(prev) = history.last() {
            let drift = (median_offset - prev.offset_ms).abs();
            if drift > drift_threshold_ms {
                warn!(
                    drift_ms = drift,
                    previous_ms = prev.offset_ms,
                    current_ms = median_offset,
                    "Significant time drift detected"
                );
            }
        }
    }

    // Update state
    *offset_ms.write() = median_offset;
    *last_sync_time.write() = now;
    *last_network_latency_ms.write() = median_latency;

    // Track history
    {
        let mut history = offset_history.write();
        history.push(OffsetSample {
            timestamp: now,
            offset_ms: median_offset,
            latency_ms: median_latency,
        });
        // Age-based pruning: remove samples older than 1 hour
        let one_hour_ago = now - 3600.0;
        history.retain(|s| s.timestamp > one_hour_ago);
        // Cap at max_history to prevent unbounded growth
        while history.len() > max_history {
            history.remove(0);
        }
    }

    *sync_count.write() += 1;

    debug!(
        offset_ms = format!("{:.1}", median_offset),
        latency_ms = format!("{:.1}", median_latency),
        samples = samples,
        "Time synchronized"
    );

    Ok(true)
}

fn median(values: &[f64]) -> f64 {
    let len = values.len();
    if len == 0 {
        return 0.0;
    }
    if len % 2 == 0 {
        (values[len / 2 - 1] + values[len / 2]) / 2.0
    } else {
        values[len / 2]
    }
}

fn now_millis_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64() * 1000.0
}

fn now_secs_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

impl Drop for TimeSynchronizer {
    fn drop(&mut self) {
        if let Some(handle) = self.sync_task.write().take() {
            handle.abort();
        }
        debug!("TimeSynchronizer dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_median_odd() {
        assert_eq!(median(&[1.0, 3.0, 5.0]), 3.0);
    }

    #[test]
    fn test_median_even() {
        assert_eq!(median(&[1.0, 2.0, 3.0, 4.0]), 2.5);
    }

    #[test]
    fn test_median_single() {
        assert_eq!(median(&[42.0]), 42.0);
    }

    #[test]
    fn test_median_empty() {
        assert_eq!(median(&[]), 0.0);
    }

    #[test]
    fn test_now_millis() {
        let ms = now_millis_f64();
        // Should be roughly current time in ms (> 2024 epoch)
        assert!(ms > 1_700_000_000_000.0);
    }

    #[test]
    fn test_synchronizer_creation() {
        let client = Arc::new(
            BinanceRestClient::new(
                "https://testnet.binancefuture.com",
                "", "", 1200, 300, 10,
            ).unwrap()
        );
        let sync = TimeSynchronizer::new(client);
        assert_eq!(sync.get_offset_ms(), 0.0);
        assert!(!sync.is_synced(600));
    }

    #[test]
    fn test_get_current_time_ms() {
        let client = Arc::new(
            BinanceRestClient::new(
                "https://testnet.binancefuture.com",
                "", "", 1200, 300, 10,
            ).unwrap()
        );
        let sync = TimeSynchronizer::new(client);

        // With zero offset, should be close to local time
        let current = sync.get_current_time_ms();
        let local = now_millis_f64() as u64;
        assert!((current as i64 - local as i64).unsigned_abs() < 100);
    }

    #[test]
    fn test_stats_initial() {
        let client = Arc::new(
            BinanceRestClient::new(
                "https://testnet.binancefuture.com",
                "", "", 1200, 300, 10,
            ).unwrap()
        );
        let sync = TimeSynchronizer::new(client);
        let stats = sync.get_stats();

        assert_eq!(stats.offset_ms, 0.0);
        assert_eq!(stats.sync_count, 0);
        assert_eq!(stats.sync_failures, 0);
        assert!(!stats.is_synced);
        assert!(stats.last_sync_age_secs.is_none());
        assert!(stats.offset_stability_ms.is_none());
    }
}
