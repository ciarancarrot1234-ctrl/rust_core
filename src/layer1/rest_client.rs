// REST Client for Binance Futures API - Pure Rust
// Rate-limited HTTP client with HMAC-SHA256 signing and retry logic

use hmac::{Hmac, Mac};
use sha2::Sha256;
use reqwest::{Client, StatusCode};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use thiserror::Error;
use std::fmt;

type HmacSha256 = Hmac<Sha256>;

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, Error)]
pub enum RestClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error {code}: {message}")]
    Api { code: i64, message: String },
    #[error("Timestamp error: {0}")]
    Timestamp(String),
    #[error("Insufficient balance: {0}")]
    InsufficientBalance(String),
    #[error("Not connected")]
    NotConnected,
    #[error("Max retries exceeded")]
    MaxRetries,
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

// ============================================================================
// Rate Limiter
// ============================================================================

/// Token bucket rate limiter for API requests
/// Note: This struct is used behind Arc<Mutex<RateLimiter>>, so no internal lock needed.
pub struct RateLimiter {
    tokens: f64,
    pub max_tokens: f64,
    rate_per_sec: f64,
    last_update: f64,
}

impl RateLimiter {
    pub fn new(requests_per_minute: u32) -> Self {
        let rpm = requests_per_minute as f64;
        Self {
            tokens: rpm,
            max_tokens: rpm,
            rate_per_sec: rpm / 60.0,
            last_update: now_secs(),
        }
    }

    /// Wait until a token is available (caller must hold the Mutex)
    pub async fn acquire(&mut self) {
        loop {
            self.add_tokens();

            if self.tokens >= 1.0 {
                self.tokens -= 1.0;
                return;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn add_tokens(&mut self) {
        let now = now_secs();
        let elapsed = now - self.last_update;
        let tokens_to_add = elapsed * self.rate_per_sec;
        self.tokens = (self.tokens + tokens_to_add).min(self.max_tokens);
        self.last_update = now;
    }
}

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ============================================================================
// Client Statistics
// ============================================================================

#[derive(Debug, Clone)]
pub struct RestClientStats {
    pub requests_sent: u64,
    pub requests_succeeded: u64,
    pub requests_failed: u64,
    pub retries: u64,
    pub success_rate: f64,
}

impl fmt::Display for RestClientStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RestClientStats(sent={}, ok={}, fail={}, retries={}, rate={:.2}%)",
            self.requests_sent, self.requests_succeeded, self.requests_failed,
            self.retries, self.success_rate * 100.0
        )
    }
}

// ============================================================================
// Binance REST Client
// ============================================================================

/// REST API client for Binance Futures
///
/// Features:
/// - Token bucket rate limiting (request + order limiters)
/// - HMAC-SHA256 request signing
/// - Automatic retries with exponential backoff
/// - All public + private Binance Futures endpoints
pub struct BinanceRestClient {
    base_url: String,
    api_key: String,
    api_secret: String,

    client: Client,

    // Rate limiters
    request_limiter: Arc<Mutex<RateLimiter>>,
    order_limiter: Arc<Mutex<RateLimiter>>,

    // Stats
    requests_sent: Arc<Mutex<u64>>,
    requests_succeeded: Arc<Mutex<u64>>,
    requests_failed: Arc<Mutex<u64>>,
    retries: Arc<Mutex<u64>>,
}

impl BinanceRestClient {
    /// Create a new REST client
    pub fn new(
        base_url: &str,
        api_key: &str,
        api_secret: &str,
        requests_per_minute: u32,
        orders_per_minute: u32,
        timeout_seconds: u64,
    ) -> Result<Self, RestClientError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            .build()?;

        Ok(Self {
            base_url: base_url.to_string(),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            client,
            request_limiter: Arc::new(Mutex::new(RateLimiter::new(requests_per_minute))),
            order_limiter: Arc::new(Mutex::new(RateLimiter::new(orders_per_minute))),
            requests_sent: Arc::new(Mutex::new(0)),
            requests_succeeded: Arc::new(Mutex::new(0)),
            requests_failed: Arc::new(Mutex::new(0)),
            retries: Arc::new(Mutex::new(0)),
        })
    }

    /// Create from config
    pub fn from_config(config: &crate::core::BinanceConfig) -> Result<Self, RestClientError> {
        Self::new(
            &config.base_url,
            &config.api_key,
            &config.api_secret,
            config.requests_per_minute,
            config.orders_per_minute,
            config.request_timeout_seconds as u64,
        )
    }

    /// Generate HMAC-SHA256 signature
    fn sign(&self, query_string: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC key can be any size");
        mac.update(query_string.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Internal request method with rate limiting and retries
    async fn request(
        &self,
        method: &str,
        endpoint: &str,
        signed: bool,
        params: Option<HashMap<String, String>>,
        is_order: bool,
    ) -> Result<Value, RestClientError> {
        // Apply rate limiting
        if is_order {
            self.order_limiter.lock().await.acquire().await;
        } else {
            self.request_limiter.lock().await.acquire().await;
        }

        let url = format!("{}{}", self.base_url, endpoint);

        let mut params = params.unwrap_or_default();
        if signed {
            params.insert("timestamp".to_string(), now_millis().to_string());
            let query_string = build_query_string(&params);
            let signature = self.sign(&query_string);
            params.insert("signature".to_string(), signature);
        }

        let max_retries = 3u32;

        for attempt in 0..=max_retries {
            *self.requests_sent.lock().await += 1;

            let result = match method {
                "GET" => {
                    let mut req = self.client.get(&url);
                    if !self.api_key.is_empty() {
                        req = req.header("X-MBX-APIKEY", &self.api_key);
                    }
                    req.query(&params).send().await
                }
                "POST" => {
                    let mut req = self.client.post(&url);
                    if !self.api_key.is_empty() {
                        req = req.header("X-MBX-APIKEY", &self.api_key);
                    }
                    req.query(&params).send().await
                }
                "DELETE" => {
                    let mut req = self.client.delete(&url);
                    if !self.api_key.is_empty() {
                        req = req.header("X-MBX-APIKEY", &self.api_key);
                    }
                    req.query(&params).send().await
                }
                "PUT" => {
                    let mut req = self.client.put(&url);
                    if !self.api_key.is_empty() {
                        req = req.header("X-MBX-APIKEY", &self.api_key);
                    }
                    req.query(&params).send().await
                }
                _ => return Err(RestClientError::Api {
                    code: -1,
                    message: format!("Unsupported HTTP method: {}", method),
                }),
            };

            match result {
                Ok(response) => {
                    let status = response.status();
                    let data: Value = response.json().await?;

                    if status == StatusCode::OK {
                        *self.requests_succeeded.lock().await += 1;
                        return Ok(data);
                    }

                    // Handle API errors
                    let error_code = data.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
                    let error_msg = data.get("msg").and_then(|m| m.as_str()).unwrap_or("Unknown error").to_string();

                    // Non-retryable errors
                    if error_code == -1021 || error_code == -1022 {
                        *self.requests_failed.lock().await += 1;
                        return Err(RestClientError::Timestamp(error_msg));
                    }
                    if error_code == -2010 {
                        *self.requests_failed.lock().await += 1;
                        return Err(RestClientError::InsufficientBalance(error_msg));
                    }

                    // Retryable error
                    if attempt < max_retries {
                        let delay = 2u64.pow(attempt);
                        tracing::warn!(
                            attempt = attempt + 1,
                            max_retries = max_retries + 1,
                            error_code = error_code,
                            error_msg = %error_msg,
                            delay_secs = delay,
                            "Request failed, retrying"
                        );
                        *self.retries.lock().await += 1;
                        tokio::time::sleep(Duration::from_secs(delay)).await;

                        // Re-sign with fresh timestamp
                        if signed {
                            params.insert("timestamp".to_string(), now_millis().to_string());
                            let query_no_sig: HashMap<String, String> = params.iter()
                                .filter(|(k, _)| k.as_str() != "signature")
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect();
                            let query_string = build_query_string(&query_no_sig);
                            let signature = self.sign(&query_string);
                            params.insert("signature".to_string(), signature);
                        }
                        continue;
                    }

                    *self.requests_failed.lock().await += 1;
                    return Err(RestClientError::Api { code: error_code, message: error_msg });
                }
                Err(e) => {
                    if attempt < max_retries {
                        let delay = 2u64.pow(attempt);
                        tracing::warn!(
                            attempt = attempt + 1,
                            error = %e,
                            delay_secs = delay,
                            "Network error, retrying"
                        );
                        *self.retries.lock().await += 1;
                        tokio::time::sleep(Duration::from_secs(delay)).await;
                        continue;
                    }

                    *self.requests_failed.lock().await += 1;
                    return Err(RestClientError::Http(e));
                }
            }
        }

        *self.requests_failed.lock().await += 1;
        Err(RestClientError::MaxRetries)
    }

    // ========================================================================
    // Market Data Endpoints (Public)
    // ========================================================================

    /// Get exchange trading rules and symbol information
    pub async fn get_exchange_info(&self, symbol: Option<&str>) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        if let Some(s) = symbol {
            params.insert("symbol".to_string(), s.to_string());
        }
        self.request("GET", "/fapi/v1/exchangeInfo", false, Some(params), false).await
    }

    /// Get order book snapshot
    pub async fn get_order_book(&self, symbol: &str, limit: u32) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("limit".to_string(), limit.to_string());
        self.request("GET", "/fapi/v1/depth", false, Some(params), false).await
    }

    /// Get recent trades
    pub async fn get_recent_trades(&self, symbol: &str, limit: u32) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("limit".to_string(), limit.to_string());
        self.request("GET", "/fapi/v1/trades", false, Some(params), false).await
    }

    /// Get candlestick/kline data
    pub async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: u32,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("interval".to_string(), interval.to_string());
        params.insert("limit".to_string(), limit.to_string());
        if let Some(st) = start_time {
            params.insert("startTime".to_string(), st.to_string());
        }
        if let Some(et) = end_time {
            params.insert("endTime".to_string(), et.to_string());
        }
        self.request("GET", "/fapi/v1/klines", false, Some(params), false).await
    }

    /// Get mark price and funding rate
    pub async fn get_mark_price(&self, symbol: Option<&str>) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        if let Some(s) = symbol {
            params.insert("symbol".to_string(), s.to_string());
        }
        self.request("GET", "/fapi/v1/premiumIndex", false, Some(params), false).await
    }

    /// Get historical funding rate
    pub async fn get_funding_rate(&self, symbol: &str, limit: u32) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("limit".to_string(), limit.to_string());
        self.request("GET", "/fapi/v1/fundingRate", false, Some(params), false).await
    }

    /// Get open interest
    pub async fn get_open_interest(&self, symbol: &str) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        self.request("GET", "/fapi/v1/openInterest", false, Some(params), false).await
    }

    /// Get 24hr ticker
    pub async fn get_ticker_24hr(&self, symbol: Option<&str>) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        if let Some(s) = symbol {
            params.insert("symbol".to_string(), s.to_string());
        }
        self.request("GET", "/fapi/v1/ticker/24hr", false, Some(params), false).await
    }

    // ========================================================================
    // Account Endpoints (Private - Signed)
    // ========================================================================

    /// Get account information
    pub async fn get_account_info(&self) -> Result<Value, RestClientError> {
        self.request("GET", "/fapi/v2/account", true, None, false).await
    }

    /// Get position information
    pub async fn get_position_info(&self, symbol: Option<&str>) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        if let Some(s) = symbol {
            params.insert("symbol".to_string(), s.to_string());
        }
        self.request("GET", "/fapi/v2/positionRisk", true, Some(params), false).await
    }

    /// Get account balance
    pub async fn get_balance(&self) -> Result<Value, RestClientError> {
        self.request("GET", "/fapi/v2/balance", true, None, false).await
    }

    // ========================================================================
    // Order Endpoints (Private - Signed)
    // ========================================================================

    /// Place a new order
    pub async fn place_order(
        &self,
        symbol: &str,
        side: &str,
        order_type: &str,
        quantity: f64,
        price: Option<f64>,
        time_in_force: &str,
        reduce_only: bool,
        position_side: &str,
    ) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("side".to_string(), side.to_string());
        params.insert("type".to_string(), order_type.to_string());
        params.insert("quantity".to_string(), quantity.to_string());
        params.insert("timeInForce".to_string(), time_in_force.to_string());
        params.insert("reduceOnly".to_string(), if reduce_only { "true" } else { "false" }.to_string());
        params.insert("positionSide".to_string(), position_side.to_string());

        if let Some(p) = price {
            params.insert("price".to_string(), p.to_string());
        }

        self.request("POST", "/fapi/v1/order", true, Some(params), true).await
    }

    /// Cancel an order
    pub async fn cancel_order(&self, symbol: &str, order_id: i64) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("orderId".to_string(), order_id.to_string());
        self.request("DELETE", "/fapi/v1/order", true, Some(params), true).await
    }

    /// Cancel all open orders for a symbol
    pub async fn cancel_all_orders(&self, symbol: &str) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        self.request("DELETE", "/fapi/v1/allOpenOrders", true, Some(params), true).await
    }

    /// Get order status
    pub async fn get_order(&self, symbol: &str, order_id: i64) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("orderId".to_string(), order_id.to_string());
        self.request("GET", "/fapi/v1/order", true, Some(params), false).await
    }

    /// Get all open orders
    pub async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        if let Some(s) = symbol {
            params.insert("symbol".to_string(), s.to_string());
        }
        self.request("GET", "/fapi/v1/openOrders", true, Some(params), false).await
    }

    // ========================================================================
    // Leverage & Margin
    // ========================================================================

    /// Change leverage for a symbol
    pub async fn change_leverage(&self, symbol: &str, leverage: u32) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("leverage".to_string(), leverage.to_string());
        self.request("POST", "/fapi/v1/leverage", true, Some(params), false).await
    }

    /// Change margin type (ISOLATED or CROSSED)
    pub async fn change_margin_type(&self, symbol: &str, margin_type: &str) -> Result<Value, RestClientError> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("marginType".to_string(), margin_type.to_string());
        self.request("POST", "/fapi/v1/marginType", true, Some(params), false).await
    }

    // ========================================================================
    // Utility
    // ========================================================================

    /// Get Binance server time
    pub async fn get_server_time(&self) -> Result<u64, RestClientError> {
        let data = self.request("GET", "/fapi/v1/time", false, None, false).await?;
        Ok(data["serverTime"].as_u64().unwrap_or(0))
    }

    /// Test connectivity
    pub async fn ping(&self) -> bool {
        self.request("GET", "/fapi/v1/ping", false, None, false).await.is_ok()
    }

    /// Simple request with API key header but no signing (e.g., listenKey endpoints)
    pub async fn request_with_key(
        &self,
        method: &str,
        endpoint: &str,
    ) -> Result<Value, RestClientError> {
        self.request(method, endpoint, false, None, false).await
    }

    /// Get client statistics
    pub async fn get_stats(&self) -> RestClientStats {
        let sent = *self.requests_sent.lock().await;
        let succeeded = *self.requests_succeeded.lock().await;
        let failed = *self.requests_failed.lock().await;
        let retries = *self.retries.lock().await;

        RestClientStats {
            requests_sent: sent,
            requests_succeeded: succeeded,
            requests_failed: failed,
            retries,
            success_rate: if sent > 0 { succeeded as f64 / sent as f64 } else { 0.0 },
        }
    }
}

/// Build URL-encoded query string from params (sorted for consistent signing)
fn build_query_string(params: &HashMap<String, String>) -> String {
    let mut sorted_keys: Vec<&String> = params.keys().collect();
    sorted_keys.sort();

    sorted_keys.iter()
        .map(|k| format!("{}={}", k, params[*k]))
        .collect::<Vec<_>>()
        .join("&")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_creation() {
        let limiter = RateLimiter::new(1200);
        assert_eq!(limiter.max_tokens, 1200.0);
    }

    #[test]
    fn test_hmac_signing() {
        let client = BinanceRestClient::new(
            "https://testnet.binancefuture.com",
            "test_key",
            "test_secret",
            1200,
            300,
            10,
        ).unwrap();

        let sig = client.sign("symbol=BTCUSDT&timestamp=1234567890");
        assert!(!sig.is_empty());
        assert_eq!(sig.len(), 64); // SHA256 hex = 64 chars
    }

    #[test]
    fn test_hmac_deterministic() {
        let client = BinanceRestClient::new(
            "https://testnet.binancefuture.com",
            "key",
            "secret",
            1200,
            300,
            10,
        ).unwrap();

        let sig1 = client.sign("test_data");
        let sig2 = client.sign("test_data");
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_build_query_string() {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), "BTCUSDT".to_string());
        params.insert("limit".to_string(), "100".to_string());

        let qs = build_query_string(&params);
        // Sorted alphabetically
        assert_eq!(qs, "limit=100&symbol=BTCUSDT");
    }

    #[test]
    fn test_client_creation_from_config() {
        let config = crate::core::BinanceConfig::default();
        let client = BinanceRestClient::from_config(&config);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_stats_initial() {
        let client = BinanceRestClient::new(
            "https://testnet.binancefuture.com",
            "",
            "",
            1200,
            300,
            10,
        ).unwrap();

        let stats = client.get_stats().await;
        assert_eq!(stats.requests_sent, 0);
        assert_eq!(stats.requests_succeeded, 0);
        assert_eq!(stats.requests_failed, 0);
        assert_eq!(stats.success_rate, 0.0);
    }
}
