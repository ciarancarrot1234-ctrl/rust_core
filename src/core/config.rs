// Configuration Management for Liquidity Hunt System
// Pure Rust - no Python dependencies

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use parking_lot::RwLock;
use thiserror::Error;
use tracing::{info, warn};

// ============================================================================
// Error Type
// ============================================================================

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Validation error: {0}")]
    Validation(String),
}

// ============================================================================
// Configuration Structures
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceConfig {
    pub api_key: String,
    pub api_secret: String,
    pub testnet: bool,
    pub base_url: String,
    pub ws_base_url: String,

    // Connection settings
    pub max_reconnect_attempts: u32,
    pub reconnect_delay_seconds: u32,
    pub ping_interval_seconds: u32,
    pub request_timeout_seconds: u32,

    // Rate limiting
    pub requests_per_minute: u32,
    pub orders_per_minute: u32,

    // WebSocket settings
    pub ws_ping_interval_secs: u64,
    pub ws_health_check_interval_secs: u64,
    pub ws_stale_timeout_secs: u64,
    pub ws_worker_threads: usize,
    pub ws_connection_wait_ms: u64,

    // Parser settings
    pub parser_max_latency_ms: f64,
    pub parser_min_price: f64,
    pub parser_max_price: f64,

    // Synchronizer settings
    pub sync_buffer_capacity: usize,
    pub sync_small_gap_threshold: u64,
    pub sync_resync_threshold: u64,
}

impl Default for BinanceConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            api_secret: String::new(),
            testnet: false,
            base_url: "https://fapi.binance.com".to_string(),
            ws_base_url: "wss://fstream.binance.com".to_string(),
            max_reconnect_attempts: 5,
            reconnect_delay_seconds: 5,
            ping_interval_seconds: 180,
            request_timeout_seconds: 10,
            requests_per_minute: 1200,
            orders_per_minute: 300,
            ws_ping_interval_secs: 20,
            ws_health_check_interval_secs: 30,
            ws_stale_timeout_secs: 60,
            ws_worker_threads: 2,
            ws_connection_wait_ms: 500,
            parser_max_latency_ms: 5000.0,
            parser_min_price: 1000.0,
            parser_max_price: 1_000_000.0,
            sync_buffer_capacity: 10000,
            sync_small_gap_threshold: 100,
            sync_resync_threshold: 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingConfig {
    pub symbol: String,
    pub leverage: u32,
    pub position_mode: String, // ONE_WAY or HEDGE

    // Risk limits
    pub max_risk_per_trade_pct: f64,
    pub daily_loss_limit_pct: f64,
    pub max_drawdown_pct: f64,
    pub max_position_size_usd: f64,

    // Execution
    pub order_type: String,
    pub time_in_force: String,
    pub reduce_only: bool,
}

impl Default for TradingConfig {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            leverage: 10,
            position_mode: "ONE_WAY".to_string(),
            max_risk_per_trade_pct: 1.0,
            daily_loss_limit_pct: 3.0,
            max_drawdown_pct: 10.0,
            max_position_size_usd: 100000.0,
            order_type: "LIMIT".to_string(),
            time_in_force: "GTC".to_string(),
            reduce_only: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatorThresholds {
    // Trade Aggregator - Basic
    pub delta_threshold_pct: f64,
    pub large_trade_usd: f64,
    pub whale_trade_usd: f64,
    pub vwap_deviation_pct: f64,

    // Trade Aggregator - Delta Events (12 events)
    pub delta_slope_threshold: f64,           // Delta velocity change threshold (%)
    pub delta_acceleration_threshold: f64,    // Delta acceleration threshold (%)
    pub delta_regime_threshold: f64,          // Regime change threshold (%)
    pub delta_momentum_building: f64,         // Momentum building threshold (%)
    pub delta_exhaustion: f64,                // Exhaustion threshold (%)
    pub delta_extreme_threshold: f64,         // Extreme delta threshold (%)
    pub delta_neutral_range: f64,             // Neutral zone range (±%)

    // Trade Aggregator - VWAP Events (11 events)
    pub vwap_band_std_dev: f64,              // Standard deviations for bands
    pub vwap_extreme_distance_pct: f64,      // Extreme deviation threshold (%)
    pub vwap_retest_tolerance: f64,          // Retest tolerance (%)
    pub vwap_rejection_threshold: f64,       // Rejection detection threshold (%)
    pub vwap_convergence_threshold: f64,     // Convergence detection threshold (%)

    // Trade Aggregator - Volume Events (12 events)
    pub volume_spike_multiple: f64,          // Volume spike multiplier (x avg)
    pub volume_dry_up_threshold: f64,        // Volume dry-up threshold (x avg)
    pub volume_ratio_extreme: f64,           // Buy/sell ratio extreme (%)
    pub volume_acceleration_threshold: f64,  // Volume acceleration threshold
    pub volume_climax_threshold: f64,        // Volume climax threshold (x avg)
    pub volume_divergence_periods: u32,      // Periods for divergence detection

    // Trade Aggregator - Pattern Events (10 events)
    pub cluster_time_window_ms: i64,         // Trade cluster window (ms)
    pub cluster_min_trades: u32,             // Minimum trades for cluster
    pub rate_spike_threshold: f64,           // Trade rate spike threshold (x avg)
    pub institutional_trade_min_usd: f64,    // Institutional trade minimum ($)
    pub aggression_score_threshold: f64,     // Aggression score threshold
    pub absorption_detection_window_ms: i64, // Absorption detection window (ms)
    pub iceberg_detection_min_trades: u32,   // Minimum trades for iceberg detection

    // Trade Aggregator - Volume Profile Events (8 events)
    pub profile_tick_size: f64,              // Price bucket size for profile
    pub profile_duration_ms: i64,            // Profile calculation duration (ms)
    pub profile_recalc_interval_ms: i64,     // Profile recalculation interval (ms)
    pub poc_shift_threshold: f64,            // POC shift threshold (ticks)
    pub value_area_percentage: f64,          // Value area percentage (e.g., 0.70)
    pub high_volume_node_threshold: f64,     // High volume node threshold (x avg)
    pub low_volume_node_threshold: f64,      // Low volume node threshold (x avg)

    // Trade Aggregator - Streak Events (8 events)
    pub streak_min_trades: u32,              // Minimum trades for streak
    pub streak_max_gap_ms: i64,              // Maximum gap between trades (ms)
    pub streak_intensity_threshold: f64,     // Streak intensity threshold (trades/sec)
    pub streak_record_min_length: u32,       // Minimum length for record streak

    // OrderBook Aggregator
    pub spread_threshold_bps: f64,
    pub imbalance_threshold: f64,
    pub wall_size_usd: f64,
    pub wall_threshold_multiple: f64,
    pub depth_levels: u32,

    // Kline Aggregator
    pub kline_intervals: Vec<String>,
    pub ema_periods: Vec<u32>,
    pub rsi_period: u32,
    pub rsi_overbought: f64,
    pub rsi_oversold: f64,
    pub macd_fast: u32,
    pub macd_slow: u32,
    pub macd_signal: u32,
    pub bb_period: u32,
    pub bb_std_dev: f64,
    pub atr_period: u32,

    // MarkPrice Aggregator
    pub premium_threshold_pct: f64,
    pub funding_rate_threshold_pct: f64,

    // Liquidation Aggregator
    pub large_liquidation_usd: f64,
    pub liquidation_cluster_window_sec: u32,
    pub cascade_threshold_usd: f64,

    // Binance Data Aggregator
    pub oi_change_threshold_pct: f64,
    pub long_short_ratio_threshold: f64,
    pub taker_volume_threshold_pct: f64,
}

impl Default for AggregatorThresholds {
    fn default() -> Self {
        Self {
            // Trade Aggregator - Basic
            delta_threshold_pct: 30.0,
            large_trade_usd: 10000.0,
            whale_trade_usd: 100000.0,
            vwap_deviation_pct: 0.15,

            // Trade Aggregator - Delta Events
            delta_slope_threshold: 5.0,
            delta_acceleration_threshold: 2.0,
            delta_regime_threshold: 40.0,
            delta_momentum_building: 50.0,
            delta_exhaustion: 80.0,
            delta_extreme_threshold: 70.0,
            delta_neutral_range: 10.0,

            // Trade Aggregator - VWAP Events
            vwap_band_std_dev: 2.0,
            vwap_extreme_distance_pct: 0.5,
            vwap_retest_tolerance: 0.05,
            vwap_rejection_threshold: 0.2,
            vwap_convergence_threshold: 0.1,

            // Trade Aggregator - Volume Events
            volume_spike_multiple: 3.0,
            volume_dry_up_threshold: 0.3,
            volume_ratio_extreme: 80.0,
            volume_acceleration_threshold: 2.0,
            volume_climax_threshold: 5.0,
            volume_divergence_periods: 10,

            // Trade Aggregator - Pattern Events
            cluster_time_window_ms: 5000,
            cluster_min_trades: 10,
            rate_spike_threshold: 5.0,
            institutional_trade_min_usd: 50000.0,
            aggression_score_threshold: 0.7,
            absorption_detection_window_ms: 3000,
            iceberg_detection_min_trades: 5,

            // Trade Aggregator - Volume Profile Events
            profile_tick_size: 0.1,
            profile_duration_ms: 3600000,        // 1 hour
            profile_recalc_interval_ms: 300000,  // 5 minutes
            poc_shift_threshold: 10.0,
            value_area_percentage: 0.70,
            high_volume_node_threshold: 2.0,
            low_volume_node_threshold: 0.5,

            // Trade Aggregator - Streak Events
            streak_min_trades: 5,
            streak_max_gap_ms: 2000,
            streak_intensity_threshold: 2.0,
            streak_record_min_length: 10,

            // OrderBook Aggregator
            spread_threshold_bps: 5.0,
            imbalance_threshold: 0.6,
            wall_size_usd: 50000.0,
            wall_threshold_multiple: 3.0,
            depth_levels: 20,

            // Kline Aggregator
            kline_intervals: vec!["1m".to_string(), "5m".to_string(), "15m".to_string()],
            ema_periods: vec![9, 21, 50, 200],
            rsi_period: 14,
            rsi_overbought: 70.0,
            rsi_oversold: 30.0,
            macd_fast: 12,
            macd_slow: 26,
            macd_signal: 9,
            bb_period: 20,
            bb_std_dev: 2.0,
            atr_period: 14,

            // MarkPrice Aggregator
            premium_threshold_pct: 0.1,
            funding_rate_threshold_pct: 0.1,

            // Liquidation Aggregator
            large_liquidation_usd: 10000.0,
            liquidation_cluster_window_sec: 60,
            cascade_threshold_usd: 100000.0,

            // Binance Data Aggregator
            oi_change_threshold_pct: 5.0,
            long_short_ratio_threshold: 0.6,
            taker_volume_threshold_pct: 55.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub mode: String, // CONSERVATIVE, BALANCED, AGGRESSIVE, SCALPING, SWING
    pub min_confidence: f64,
    pub max_concurrent_signals: u32,
    pub signal_timeout_seconds: u32,

    // Strategy-specific settings
    pub conservative_min_confidence: f64,
    pub balanced_min_confidence: f64,
    pub aggressive_min_confidence: f64,
    pub scalping_min_confidence: f64,
    pub swing_min_confidence: f64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            mode: "CONSERVATIVE".to_string(),
            min_confidence: 0.70,
            max_concurrent_signals: 3,
            signal_timeout_seconds: 300,
            conservative_min_confidence: 0.75,
            balanced_min_confidence: 0.70,
            aggressive_min_confidence: 0.65,
            scalping_min_confidence: 0.65,
            swing_min_confidence: 0.75,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    pub health_check_interval_seconds: u32,
    pub metrics_collection_interval_seconds: u32,
    pub log_level: String,
    pub log_file: String,
    pub log_rotation_size_mb: u32,
    pub log_retention_days: u32,

    // Alerts
    pub alert_on_connection_loss: bool,
    pub alert_on_order_rejection: bool,
    pub alert_on_daily_loss_limit: bool,
    pub alert_on_drawdown_limit: bool,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            health_check_interval_seconds: 60,
            metrics_collection_interval_seconds: 10,
            log_level: "INFO".to_string(),
            log_file: "logs/liquidity_hunt.log".to_string(),
            log_rotation_size_mb: 100,
            log_retention_days: 30,
            alert_on_connection_loss: true,
            alert_on_order_rejection: true,
            alert_on_daily_loss_limit: true,
            alert_on_drawdown_limit: true,
        }
    }
}

// ============================================================================
// Configuration Summary
// ============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct ConfigSummary {
    pub symbol: String,
    pub testnet: bool,
    pub leverage: u32,
    pub strategy_mode: String,
    pub max_risk_pct: f64,
    pub log_level: String,
}

// ============================================================================
// Configuration Manager
// ============================================================================

pub struct ConfigManager {
    binance: Arc<RwLock<BinanceConfig>>,
    trading: Arc<RwLock<TradingConfig>>,
    thresholds: Arc<RwLock<AggregatorThresholds>>,
    strategy: Arc<RwLock<StrategyConfig>>,
    monitoring: Arc<RwLock<MonitoringConfig>>,
}

impl ConfigManager {
    pub fn new(config_path: Option<&str>) -> Result<Self, ConfigError> {
        let mut manager = Self {
            binance: Arc::new(RwLock::new(BinanceConfig::default())),
            trading: Arc::new(RwLock::new(TradingConfig::default())),
            thresholds: Arc::new(RwLock::new(AggregatorThresholds::default())),
            strategy: Arc::new(RwLock::new(StrategyConfig::default())),
            monitoring: Arc::new(RwLock::new(MonitoringConfig::default())),
        };

        // Load from file if provided
        if let Some(path) = config_path {
            manager.load_from_file(path)?;
        }

        // Load from environment variables
        manager.load_from_env();

        info!("Configuration initialized");
        Ok(manager)
    }

    /// Load configuration from JSON file
    pub fn load_from_file(&mut self, config_path: &str) -> Result<(), ConfigError> {
        let path = Path::new(config_path);
        if !path.exists() {
            warn!(path = config_path, "Config file not found");
            return Ok(());
        }

        let content = fs::read_to_string(path)?;
        let config_data: HashMap<String, serde_json::Value> = serde_json::from_str(&content)?;

        // Update configurations
        if let Some(binance_data) = config_data.get("binance") {
            if let Ok(binance) = serde_json::from_value::<BinanceConfig>(binance_data.clone()) {
                *self.binance.write() = binance;
            }
        }

        if let Some(trading_data) = config_data.get("trading") {
            if let Ok(trading) = serde_json::from_value::<TradingConfig>(trading_data.clone()) {
                *self.trading.write() = trading;
            }
        }

        if let Some(thresholds_data) = config_data.get("thresholds") {
            if let Ok(thresholds) = serde_json::from_value::<AggregatorThresholds>(thresholds_data.clone()) {
                *self.thresholds.write() = thresholds;
            }
        }

        if let Some(strategy_data) = config_data.get("strategy") {
            if let Ok(strategy) = serde_json::from_value::<StrategyConfig>(strategy_data.clone()) {
                *self.strategy.write() = strategy;
            }
        }

        if let Some(monitoring_data) = config_data.get("monitoring") {
            if let Ok(monitoring) = serde_json::from_value::<MonitoringConfig>(monitoring_data.clone()) {
                *self.monitoring.write() = monitoring;
            }
        }

        info!(path = config_path, "Configuration loaded");
        Ok(())
    }

    /// Load sensitive data from environment variables
    pub fn load_from_env(&mut self) {
        if let Ok(api_key) = std::env::var("BINANCE_API_KEY") {
            self.binance.write().api_key = api_key;
        }
        if let Ok(api_secret) = std::env::var("BINANCE_API_SECRET") {
            self.binance.write().api_secret = api_secret;
        }
        if let Ok(testnet) = std::env::var("BINANCE_TESTNET") {
            self.binance.write().testnet = testnet.to_lowercase() == "true";
        }
    }

    /// Save configuration to JSON file (excludes secrets)
    pub fn save_to_file(&self, config_path: &str) -> Result<(), ConfigError> {
        let binance = self.binance.read();
        let trading = self.trading.read();
        let thresholds = self.thresholds.read();
        let strategy = self.strategy.read();
        let monitoring = self.monitoring.read();

        // Create config object without secrets
        let mut config_map = HashMap::new();

        // Binance config without secrets
        let mut binance_map = HashMap::new();
        binance_map.insert("testnet", serde_json::json!(binance.testnet));
        binance_map.insert("base_url", serde_json::json!(binance.base_url.clone()));
        binance_map.insert("ws_base_url", serde_json::json!(binance.ws_base_url.clone()));
        binance_map.insert("max_reconnect_attempts", serde_json::json!(binance.max_reconnect_attempts));
        binance_map.insert("reconnect_delay_seconds", serde_json::json!(binance.reconnect_delay_seconds));
        binance_map.insert("ping_interval_seconds", serde_json::json!(binance.ping_interval_seconds));
        binance_map.insert("request_timeout_seconds", serde_json::json!(binance.request_timeout_seconds));
        binance_map.insert("requests_per_minute", serde_json::json!(binance.requests_per_minute));
        binance_map.insert("orders_per_minute", serde_json::json!(binance.orders_per_minute));
        binance_map.insert("ws_ping_interval_secs", serde_json::json!(binance.ws_ping_interval_secs));
        binance_map.insert("ws_health_check_interval_secs", serde_json::json!(binance.ws_health_check_interval_secs));
        binance_map.insert("ws_stale_timeout_secs", serde_json::json!(binance.ws_stale_timeout_secs));

        config_map.insert("binance", serde_json::json!(binance_map));
        config_map.insert("trading", serde_json::to_value(&*trading)?);
        config_map.insert("thresholds", serde_json::to_value(&*thresholds)?);
        config_map.insert("strategy", serde_json::to_value(&*strategy)?);
        config_map.insert("monitoring", serde_json::to_value(&*monitoring)?);

        // Ensure parent directory exists
        if let Some(parent) = Path::new(config_path).parent() {
            fs::create_dir_all(parent)?;
        }

        let json = serde_json::to_string_pretty(&config_map)?;
        fs::write(config_path, json)?;

        info!(path = config_path, "Configuration saved");
        Ok(())
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<bool, ConfigError> {
        let mut errors = Vec::new();
        let binance = self.binance.read();
        let trading = self.trading.read();

        // Validate API keys if not in testnet mode
        if !binance.testnet {
            if binance.api_key.is_empty() {
                errors.push("Binance API key is required for mainnet".to_string());
            }
            if binance.api_secret.is_empty() {
                errors.push("Binance API secret is required for mainnet".to_string());
            }
        }

        // Validate risk parameters
        if trading.max_risk_per_trade_pct <= 0.0 || trading.max_risk_per_trade_pct > 10.0 {
            errors.push("max_risk_per_trade_pct must be between 0 and 10".to_string());
        }

        if trading.leverage < 1 || trading.leverage > 125 {
            errors.push("leverage must be between 1 and 125".to_string());
        }

        if !errors.is_empty() {
            for error in &errors {
                warn!(error = %error, "Config validation error");
            }
            return Ok(false);
        }

        info!("Configuration validated successfully");
        Ok(true)
    }

    /// Get configuration summary
    pub fn get_summary(&self) -> ConfigSummary {
        let binance = self.binance.read();
        let trading = self.trading.read();
        let strategy = self.strategy.read();
        let monitoring = self.monitoring.read();

        ConfigSummary {
            symbol: trading.symbol.clone(),
            testnet: binance.testnet,
            leverage: trading.leverage,
            strategy_mode: strategy.mode.clone(),
            max_risk_pct: trading.max_risk_per_trade_pct,
            log_level: monitoring.log_level.clone(),
        }
    }

    // Getters for each config section
    pub fn binance(&self) -> BinanceConfig {
        self.binance.read().clone()
    }

    pub fn trading(&self) -> TradingConfig {
        self.trading.read().clone()
    }

    pub fn thresholds(&self) -> AggregatorThresholds {
        self.thresholds.read().clone()
    }

    pub fn strategy(&self) -> StrategyConfig {
        self.strategy.read().clone()
    }

    pub fn monitoring(&self) -> MonitoringConfig {
        self.monitoring.read().clone()
    }
}

// Global config instance (thread-safe singleton)
static GLOBAL_CONFIG: OnceLock<Arc<RwLock<ConfigManager>>> = OnceLock::new();

/// Get global configuration instance (singleton)
pub fn get_config() -> Arc<RwLock<ConfigManager>> {
    Arc::clone(GLOBAL_CONFIG.get_or_init(|| {
        Arc::new(RwLock::new(
            ConfigManager::new(None).expect("Failed to create default config"),
        ))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_configs() {
        let binance = BinanceConfig::default();
        assert!(!binance.testnet);
        assert_eq!(binance.base_url, "https://fapi.binance.com");
        assert_eq!(binance.ws_base_url, "wss://fstream.binance.com");
        assert_eq!(binance.requests_per_minute, 1200);

        let trading = TradingConfig::default();
        assert_eq!(trading.symbol, "BTCUSDT");
        assert_eq!(trading.leverage, 10);
    }

    #[test]
    fn test_config_manager() {
        let manager = ConfigManager::new(None).unwrap();
        // Validation requires API keys on mainnet (testnet=false by default)
        // So validate() should return Ok(false) indicating config is invalid
        let validation_result = manager.validate();
        assert!(validation_result.is_ok(), "validate() should not error");
        assert!(!validation_result.unwrap(), "default config should be invalid (missing API keys)");
    }

    #[test]
    fn test_config_summary() {
        let manager = ConfigManager::new(None).unwrap();
        let summary = manager.get_summary();
        assert_eq!(summary.symbol, "BTCUSDT");
        assert!(!summary.testnet);
    }
}
