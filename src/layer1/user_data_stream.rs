// User Data Stream Manager for Binance Futures - Pure Rust
// Authenticated WebSocket for order updates, position changes, and account events

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use parking_lot::RwLock;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures::StreamExt;
use serde_json::Value;
use std::fmt;
use tracing::{info, warn, error, debug};

use crate::core::ConnectionStatus;
use crate::layer1::rest_client::{BinanceRestClient, RestClientError};

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum UserDataStreamError {
    #[error("REST client error: {0}")]
    RestClient(#[from] RestClientError),
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("Listen key not created")]
    NoListenKey,
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    #[error("Max reconnection attempts reached")]
    MaxReconnects,
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

// ============================================================================
// User Data Event Types
// ============================================================================

/// Parsed user data events from Binance
#[derive(Debug, Clone)]
pub enum UserDataEvent {
    /// Order execution update
    OrderUpdate(OrderUpdate),
    /// Account balance update
    AccountUpdate(AccountUpdate),
    /// Margin call warning (CRITICAL)
    MarginCall(Value),
    /// Account configuration update
    AccountConfigUpdate(Value),
    /// Unknown event type
    Unknown { event_type: String, data: Value },
}

#[derive(Debug, Clone)]
pub struct OrderUpdate {
    pub symbol: String,
    pub order_id: i64,
    pub client_order_id: String,
    pub side: String,
    pub order_type: String,
    pub order_status: String,
    pub execution_type: String,
    pub price: String,
    pub quantity: String,
    pub filled_quantity: String,
    pub average_price: String,
    pub commission: String,
    pub commission_asset: String,
    pub is_maker: bool,
    pub reduce_only: bool,
    pub position_side: String,
    pub realized_pnl: String,
    pub timestamp: u64,
}

impl fmt::Display for OrderUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OrderUpdate({} {} {} qty={} status={})",
            self.symbol, self.side, self.order_type, self.quantity, self.order_status
        )
    }
}

#[derive(Debug, Clone)]
pub struct BalanceUpdate {
    pub asset: String,
    pub wallet_balance: String,
    pub cross_wallet_balance: String,
}

#[derive(Debug, Clone)]
pub struct PositionUpdate {
    pub symbol: String,
    pub position_side: String,
    pub position_amount: String,
    pub entry_price: String,
    pub unrealized_pnl: String,
    pub margin_type: String,
    pub isolated_wallet: String,
}

impl fmt::Display for PositionUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PositionUpdate({} side={} amt={} entry={})",
            self.symbol, self.position_side, self.position_amount, self.entry_price
        )
    }
}

#[derive(Debug, Clone)]
pub struct AccountUpdate {
    pub reason: String,
    pub balances: Vec<BalanceUpdate>,
    pub positions: Vec<PositionUpdate>,
    pub timestamp: u64,
}

// ============================================================================
// Statistics
// ============================================================================

#[derive(Debug, Clone)]
pub struct UserDataStreamStats {
    pub status: ConnectionStatus,
    pub listen_key_age_secs: Option<f64>,
    pub messages_received: u64,
    pub messages_processed: u64,
    pub order_updates: u64,
    pub position_updates: u64,
    pub account_updates: u64,
    pub errors: u64,
    pub last_message_age_secs: Option<f64>,
}

impl fmt::Display for UserDataStreamStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "UserDataStreamStats(status={}, msgs={}, orders={}, positions={}, errors={})",
            self.status, self.messages_received, self.order_updates,
            self.position_updates, self.errors
        )
    }
}

// ============================================================================
// User Data Stream Manager
// ============================================================================

/// Manages authenticated WebSocket for real-time account updates
///
/// Features:
/// - Listen key management (create, extend every 30min, close)
/// - Automatic reconnection with exponential backoff
/// - Order execution updates (ORDER_TRADE_UPDATE)
/// - Account balance updates (ACCOUNT_UPDATE)
/// - Position updates
/// - Margin call warnings (MARGIN_CALL)
/// - Connection health monitoring
pub struct UserDataStreamManager {
    rest_client: Arc<BinanceRestClient>,
    ws_base_url: String,

    // Listen key
    listen_key: Arc<RwLock<Option<String>>>,
    listen_key_created_at: Arc<RwLock<f64>>,
    listen_key_extend_interval_secs: u64, // 30 minutes

    // Connection state
    status: Arc<RwLock<ConnectionStatus>>,
    max_reconnect_attempts: u32,

    // Event channel: consumers receive parsed events
    event_tx: Arc<tokio::sync::RwLock<Option<tokio::sync::mpsc::UnboundedSender<UserDataEvent>>>>,
    event_rx: Arc<tokio::sync::RwLock<Option<tokio::sync::mpsc::UnboundedReceiver<UserDataEvent>>>>,

    // Task handles
    ws_task: Arc<RwLock<Option<JoinHandle<()>>>>,
    extend_task: Arc<RwLock<Option<JoinHandle<()>>>>,

    // Stats
    messages_received: Arc<RwLock<u64>>,
    messages_processed: Arc<RwLock<u64>>,
    order_updates: Arc<RwLock<u64>>,
    position_updates: Arc<RwLock<u64>>,
    account_updates: Arc<RwLock<u64>>,
    errors: Arc<RwLock<u64>>,
    last_message_time: Arc<RwLock<f64>>,
}

impl UserDataStreamManager {
    pub fn new(rest_client: Arc<BinanceRestClient>, ws_base_url: &str) -> Self {
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();

        info!("User data stream manager initialized");

        Self {
            rest_client,
            ws_base_url: ws_base_url.to_string(),
            listen_key: Arc::new(RwLock::new(None)),
            listen_key_created_at: Arc::new(RwLock::new(0.0)),
            listen_key_extend_interval_secs: 1800, // 30 minutes
            status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            max_reconnect_attempts: 10,
            event_tx: Arc::new(tokio::sync::RwLock::new(Some(event_tx))),
            event_rx: Arc::new(tokio::sync::RwLock::new(Some(event_rx))),
            ws_task: Arc::new(RwLock::new(None)),
            extend_task: Arc::new(RwLock::new(None)),
            messages_received: Arc::new(RwLock::new(0)),
            messages_processed: Arc::new(RwLock::new(0)),
            order_updates: Arc::new(RwLock::new(0)),
            position_updates: Arc::new(RwLock::new(0)),
            account_updates: Arc::new(RwLock::new(0)),
            errors: Arc::new(RwLock::new(0)),
            last_message_time: Arc::new(RwLock::new(0.0)),
        }
    }

    /// Create from config
    pub fn from_config(
        rest_client: Arc<BinanceRestClient>,
        config: &crate::core::BinanceConfig,
    ) -> Self {
        let mut mgr = Self::new(rest_client, &config.ws_base_url);
        mgr.max_reconnect_attempts = config.max_reconnect_attempts;
        mgr
    }

    /// Start user data stream
    pub async fn start(&self) -> Result<bool, UserDataStreamError> {
        // Create listen key
        self.create_listen_key().await?;

        // Start WebSocket connection
        self.spawn_ws_task();

        // Start listen key extension loop
        self.spawn_extend_task();

        info!("User data stream started");
        Ok(true)
    }

    /// Stop user data stream
    pub async fn stop(&self) {
        info!("Stopping user data stream");

        // Abort tasks
        if let Some(handle) = self.ws_task.write().take() {
            handle.abort();
        }
        if let Some(handle) = self.extend_task.write().take() {
            handle.abort();
        }

        // Close listen key
        if let Err(e) = self.close_listen_key().await {
            warn!(error = %e, "Failed to close listen key");
        }

        *self.status.write() = ConnectionStatus::Disconnected;
        info!("User data stream stopped");
    }

    /// Receive next user data event
    pub async fn recv(&self) -> Option<UserDataEvent> {
        let mut rx_lock = self.event_rx.write().await;
        if let Some(rx) = rx_lock.as_mut() {
            rx.recv().await
        } else {
            None
        }
    }

    /// Get connection status
    pub fn get_status(&self) -> ConnectionStatus {
        *self.status.read()
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        *self.status.read() == ConnectionStatus::Connected
    }

    /// Get stream statistics
    pub fn get_stats(&self) -> UserDataStreamStats {
        let listen_key_age = {
            let created = *self.listen_key_created_at.read();
            if created > 0.0 {
                Some(now_secs() - created)
            } else {
                None
            }
        };

        let last_msg_age = {
            let last = *self.last_message_time.read();
            if last > 0.0 {
                Some(now_secs() - last)
            } else {
                None
            }
        };

        UserDataStreamStats {
            status: *self.status.read(),
            listen_key_age_secs: listen_key_age,
            messages_received: *self.messages_received.read(),
            messages_processed: *self.messages_processed.read(),
            order_updates: *self.order_updates.read(),
            position_updates: *self.position_updates.read(),
            account_updates: *self.account_updates.read(),
            errors: *self.errors.read(),
            last_message_age_secs: last_msg_age,
        }
    }

    // ========================================================================
    // Listen Key Management
    // ========================================================================

    /// Create a new listen key via REST API
    async fn create_listen_key(&self) -> Result<(), UserDataStreamError> {
        let data = self.rest_client.request_with_key(
            "POST", "/fapi/v1/listenKey",
        ).await?;

        let key = data.get("listenKey")
            .and_then(|v| v.as_str())
            .ok_or_else(|| UserDataStreamError::ConnectionFailed("No listenKey in response".to_string()))?;

        let key_preview = if key.len() > 10 { &key[..10] } else { key };
        info!(key_preview = key_preview, "Listen key created");

        *self.listen_key.write() = Some(key.to_string());
        *self.listen_key_created_at.write() = now_secs();

        Ok(())
    }

    /// Extend listen key lifetime
    pub async fn extend_listen_key(&self) -> Result<(), UserDataStreamError> {
        extend_listen_key_static(&self.rest_client, &self.listen_key_created_at).await
    }

    /// Close listen key
    async fn close_listen_key(&self) -> Result<(), UserDataStreamError> {
        if self.listen_key.read().is_none() {
            return Ok(());
        }

        self.rest_client.request_with_key(
            "DELETE", "/fapi/v1/listenKey",
        ).await?;

        *self.listen_key.write() = None;
        info!("Listen key closed");
        Ok(())
    }

    // ========================================================================
    // Background Tasks
    // ========================================================================

    fn spawn_ws_task(&self) {
        let listen_key = self.listen_key.clone();
        let ws_base_url = self.ws_base_url.clone();
        let status = self.status.clone();
        let event_tx = self.event_tx.clone();
        let messages_received = self.messages_received.clone();
        let messages_processed = self.messages_processed.clone();
        let order_updates = self.order_updates.clone();
        let position_updates = self.position_updates.clone();
        let account_updates = self.account_updates.clone();
        let errors = self.errors.clone();
        let last_message_time = self.last_message_time.clone();
        let max_reconnects = self.max_reconnect_attempts;
        let rest_client = self.rest_client.clone();
        let listen_key_created = self.listen_key_created_at.clone();

        let handle = tokio::spawn(async move {
            let mut reconnect_attempt = 0u32;

            loop {
                // Clone the listen key value immediately to avoid holding
                // the parking_lot guard across an await point
                let existing_key = { listen_key.read().clone() };
                let key = match existing_key {
                    Some(k) => k,
                    None => {
                        warn!("No listen key available, creating new one");
                        match create_listen_key_static(
                            &rest_client, &listen_key, &listen_key_created,
                        ).await {
                            Ok(_) => {
                                let new_key = { listen_key.read().clone() };
                                match new_key {
                                    Some(k) => k,
                                    None => {
                                        error!("Failed to get listen key after creation");
                                        tokio::time::sleep(Duration::from_secs(5)).await;
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Failed to create listen key");
                                tokio::time::sleep(Duration::from_secs(5)).await;
                                continue;
                            }
                        }
                    }
                };

                let ws_url = format!("{}/ws/{}", ws_base_url, key);

                match run_user_stream(
                    &ws_url, &status, &event_tx,
                    &messages_received, &messages_processed,
                    &order_updates, &position_updates, &account_updates,
                    &errors, &last_message_time,
                ).await {
                    Ok(_) => {
                        info!("User data stream ended gracefully");
                        reconnect_attempt = 0;
                    }
                    Err(e) => {
                        error!(error = %e, "User data stream error");
                        *errors.write() += 1;
                    }
                }

                if reconnect_attempt >= max_reconnects {
                    error!(max_reconnects = max_reconnects, "Max reconnection attempts reached");
                    *status.write() = ConnectionStatus::Failed;
                    break;
                }

                let delay = std::cmp::min(2u64.pow(reconnect_attempt), 60);
                reconnect_attempt += 1;

                info!(
                    delay_secs = delay,
                    attempt = reconnect_attempt,
                    max_attempts = max_reconnects,
                    "Reconnecting user data stream"
                );
                *status.write() = ConnectionStatus::Disconnected;
                tokio::time::sleep(Duration::from_secs(delay)).await;

                // Recreate listen key on reconnect
                if let Err(e) = create_listen_key_static(
                    &rest_client, &listen_key, &listen_key_created,
                ).await {
                    error!(error = %e, "Failed to recreate listen key");
                }
            }
        });

        *self.ws_task.write() = Some(handle);
    }

    fn spawn_extend_task(&self) {
        let rest_client = self.rest_client.clone();
        let listen_key_created = self.listen_key_created_at.clone();
        let interval = self.listen_key_extend_interval_secs;

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(interval)).await;

                if let Err(e) = extend_listen_key_static(&rest_client, &listen_key_created).await {
                    warn!(error = %e, "Failed to extend listen key");
                }
            }
        });

        *self.extend_task.write() = Some(handle);
    }
}

// ============================================================================
// Static helpers (used by background tasks)
// ============================================================================

async fn create_listen_key_static(
    rest_client: &BinanceRestClient,
    listen_key: &RwLock<Option<String>>,
    listen_key_created: &RwLock<f64>,
) -> Result<(), UserDataStreamError> {
    let data = rest_client.request_with_key("POST", "/fapi/v1/listenKey").await?;

    let key = data.get("listenKey")
        .and_then(|v| v.as_str())
        .ok_or_else(|| UserDataStreamError::ConnectionFailed("No listenKey in response".to_string()))?;

    *listen_key.write() = Some(key.to_string());
    *listen_key_created.write() = now_secs();
    Ok(())
}

/// Extend listen key lifetime (used by background task and instance method)
async fn extend_listen_key_static(
    rest_client: &BinanceRestClient,
    listen_key_created: &RwLock<f64>,
) -> Result<(), UserDataStreamError> {
    rest_client.request_with_key("PUT", "/fapi/v1/listenKey").await?;
    *listen_key_created.write() = now_secs();
    debug!("Listen key extended");
    Ok(())
}

/// Run a single user data stream connection
async fn run_user_stream(
    ws_url: &str,
    status: &RwLock<ConnectionStatus>,
    event_tx: &tokio::sync::RwLock<Option<tokio::sync::mpsc::UnboundedSender<UserDataEvent>>>,
    messages_received: &RwLock<u64>,
    messages_processed: &RwLock<u64>,
    order_updates: &RwLock<u64>,
    position_updates: &RwLock<u64>,
    account_updates: &RwLock<u64>,
    errors: &RwLock<u64>,
    last_message_time: &RwLock<f64>,
) -> Result<(), UserDataStreamError> {
    info!("Connecting to user data stream");
    *status.write() = ConnectionStatus::Connecting;

    let (ws_stream, _) = connect_async(ws_url).await?;
    let (_, mut read) = ws_stream.split();

    info!("User data stream connected");
    *status.write() = ConnectionStatus::Connected;

    while let Some(msg_result) = read.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                *messages_received.write() += 1;
                *last_message_time.write() = now_secs();

                match parse_user_data_event(&text) {
                    Ok(event) => {
                        // Update stats based on event type
                        match &event {
                            UserDataEvent::OrderUpdate(_) => *order_updates.write() += 1,
                            UserDataEvent::AccountUpdate(au) => {
                                *account_updates.write() += 1;
                                if !au.positions.is_empty() {
                                    *position_updates.write() += 1;
                                }
                            }
                            UserDataEvent::MarginCall(_) => {
                                error!(raw = %text, "MARGIN CALL RECEIVED");
                            }
                            _ => {}
                        }

                        // Send to event channel
                        let tx_lock = event_tx.read().await;
                        if let Some(tx) = tx_lock.as_ref() {
                            let _ = tx.send(event);
                        }

                        *messages_processed.write() += 1;
                    }
                    Err(e) => {
                        error!(error = %e, "Error parsing user data event");
                        *errors.write() += 1;
                    }
                }
            }
            Ok(Message::Ping(data)) => {
                *last_message_time.write() = now_secs();
                // tungstenite auto-responds to pings
                let _ = data;
            }
            Ok(Message::Pong(_)) => {
                *last_message_time.write() = now_secs();
            }
            Ok(Message::Close(_)) => {
                info!("User data stream closed by server");
                *status.write() = ConnectionStatus::Disconnected;
                break;
            }
            Ok(_) => {}
            Err(e) => {
                error!(error = %e, "User data stream error");
                *errors.write() += 1;
                *status.write() = ConnectionStatus::Failed;
                return Err(UserDataStreamError::WebSocket(e));
            }
        }
    }

    *status.write() = ConnectionStatus::Disconnected;
    Ok(())
}

/// Parse raw JSON into a UserDataEvent
fn parse_user_data_event(text: &str) -> Result<UserDataEvent, serde_json::Error> {
    let data: Value = serde_json::from_str(text)?;
    let event_type = data.get("e").and_then(|v| v.as_str()).unwrap_or("");
    let timestamp = data.get("E").and_then(|v| v.as_u64()).unwrap_or(0);

    match event_type {
        "ORDER_TRADE_UPDATE" => {
            let o = &data["o"];
            Ok(UserDataEvent::OrderUpdate(OrderUpdate {
                symbol: json_str(o, "s"),
                order_id: o.get("i").and_then(|v| v.as_i64()).unwrap_or(0),
                client_order_id: json_str(o, "c"),
                side: json_str(o, "S"),
                order_type: json_str(o, "o"),
                order_status: json_str(o, "X"),
                execution_type: json_str(o, "x"),
                price: json_str(o, "p"),
                quantity: json_str(o, "q"),
                filled_quantity: json_str(o, "z"),
                average_price: json_str(o, "ap"),
                commission: json_str(o, "n"),
                commission_asset: json_str(o, "N"),
                is_maker: o.get("m").and_then(|v| v.as_bool()).unwrap_or(false),
                reduce_only: o.get("R").and_then(|v| v.as_bool()).unwrap_or(false),
                position_side: json_str(o, "ps"),
                realized_pnl: json_str(o, "rp"),
                timestamp,
            }))
        }
        "ACCOUNT_UPDATE" => {
            let a = &data["a"];
            let reason = data.get("m").and_then(|v| v.as_str()).unwrap_or("").to_string();

            let balances = a.get("B")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter().map(|b| BalanceUpdate {
                        asset: json_str(b, "a"),
                        wallet_balance: json_str(b, "wb"),
                        cross_wallet_balance: json_str(b, "cw"),
                    }).collect()
                })
                .unwrap_or_default();

            let positions = a.get("P")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter().map(|p| PositionUpdate {
                        symbol: json_str(p, "s"),
                        position_side: json_str(p, "ps"),
                        position_amount: json_str(p, "pa"),
                        entry_price: json_str(p, "ep"),
                        unrealized_pnl: json_str(p, "up"),
                        margin_type: json_str(p, "mt"),
                        isolated_wallet: json_str(p, "iw"),
                    }).collect()
                })
                .unwrap_or_default();

            Ok(UserDataEvent::AccountUpdate(AccountUpdate {
                reason,
                balances,
                positions,
                timestamp,
            }))
        }
        "MARGIN_CALL" => {
            Ok(UserDataEvent::MarginCall(data))
        }
        "ACCOUNT_CONFIG_UPDATE" => {
            let config_data = data.get("ac").cloned().unwrap_or(Value::Null);
            Ok(UserDataEvent::AccountConfigUpdate(config_data))
        }
        _ => {
            Ok(UserDataEvent::Unknown {
                event_type: event_type.to_string(),
                data,
            })
        }
    }
}

fn json_str(val: &Value, key: &str) -> String {
    val.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

impl Drop for UserDataStreamManager {
    fn drop(&mut self) {
        if let Some(handle) = self.ws_task.write().take() {
            handle.abort();
        }
        if let Some(handle) = self.extend_task.write().take() {
            handle.abort();
        }
        debug!("UserDataStreamManager dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_order_update() {
        let json = r#"{
            "e": "ORDER_TRADE_UPDATE",
            "E": 1234567890,
            "o": {
                "s": "BTCUSDT",
                "i": 12345,
                "c": "abc123",
                "S": "BUY",
                "o": "LIMIT",
                "X": "FILLED",
                "x": "TRADE",
                "p": "50000.00",
                "q": "0.001",
                "z": "0.001",
                "ap": "50000.00",
                "n": "0.01",
                "N": "USDT",
                "m": true,
                "R": false,
                "ps": "BOTH",
                "rp": "0.50"
            }
        }"#;

        let event = parse_user_data_event(json).unwrap();
        match event {
            UserDataEvent::OrderUpdate(ou) => {
                assert_eq!(ou.symbol, "BTCUSDT");
                assert_eq!(ou.order_id, 12345);
                assert_eq!(ou.side, "BUY");
                assert_eq!(ou.order_status, "FILLED");
                assert!(ou.is_maker);
                assert!(!ou.reduce_only);
                assert_eq!(ou.timestamp, 1234567890);
            }
            _ => panic!("Expected OrderUpdate"),
        }
    }

    #[test]
    fn test_parse_account_update() {
        let json = r#"{
            "e": "ACCOUNT_UPDATE",
            "E": 1234567890,
            "m": "ORDER",
            "a": {
                "B": [
                    {"a": "USDT", "wb": "10000.00", "cw": "9500.00"}
                ],
                "P": [
                    {
                        "s": "BTCUSDT",
                        "ps": "BOTH",
                        "pa": "0.001",
                        "ep": "50000.00",
                        "up": "5.00",
                        "mt": "cross",
                        "iw": "0"
                    }
                ]
            }
        }"#;

        let event = parse_user_data_event(json).unwrap();
        match event {
            UserDataEvent::AccountUpdate(au) => {
                assert_eq!(au.reason, "ORDER");
                assert_eq!(au.balances.len(), 1);
                assert_eq!(au.balances[0].asset, "USDT");
                assert_eq!(au.positions.len(), 1);
                assert_eq!(au.positions[0].symbol, "BTCUSDT");
                assert_eq!(au.positions[0].entry_price, "50000.00");
            }
            _ => panic!("Expected AccountUpdate"),
        }
    }

    #[test]
    fn test_parse_margin_call() {
        let json = r#"{
            "e": "MARGIN_CALL",
            "E": 1234567890,
            "p": [{"s": "BTCUSDT", "ps": "BOTH", "pa": "1.0", "mt": "cross"}]
        }"#;

        let event = parse_user_data_event(json).unwrap();
        assert!(matches!(event, UserDataEvent::MarginCall(_)));
    }

    #[test]
    fn test_parse_account_config_update() {
        let json = r#"{
            "e": "ACCOUNT_CONFIG_UPDATE",
            "E": 1234567890,
            "ac": {"s": "BTCUSDT", "l": 10}
        }"#;

        let event = parse_user_data_event(json).unwrap();
        assert!(matches!(event, UserDataEvent::AccountConfigUpdate(_)));
    }

    #[test]
    fn test_parse_unknown_event() {
        let json = r#"{"e": "SOME_NEW_EVENT", "E": 1234567890}"#;

        let event = parse_user_data_event(json).unwrap();
        match event {
            UserDataEvent::Unknown { event_type, .. } => {
                assert_eq!(event_type, "SOME_NEW_EVENT");
            }
            _ => panic!("Expected Unknown"),
        }
    }

    #[test]
    fn test_manager_creation() {
        let client = Arc::new(
            BinanceRestClient::new(
                "https://testnet.binancefuture.com",
                "test_key", "test_secret",
                1200, 300, 10,
            ).unwrap()
        );
        let mgr = UserDataStreamManager::new(
            client,
            "wss://stream.binancefuture.com",
        );

        assert!(!mgr.is_connected());
        assert_eq!(mgr.get_status(), ConnectionStatus::Disconnected);
    }

    #[test]
    fn test_stats_initial() {
        let client = Arc::new(
            BinanceRestClient::new(
                "https://testnet.binancefuture.com",
                "", "", 1200, 300, 10,
            ).unwrap()
        );
        let mgr = UserDataStreamManager::new(
            client,
            "wss://stream.binancefuture.com",
        );

        let stats = mgr.get_stats();
        assert_eq!(stats.messages_received, 0);
        assert_eq!(stats.order_updates, 0);
        assert_eq!(stats.errors, 0);
        assert!(stats.listen_key_age_secs.is_none());
    }

    #[test]
    fn test_order_update_display() {
        let ou = OrderUpdate {
            symbol: "BTCUSDT".to_string(),
            order_id: 123,
            client_order_id: "abc".to_string(),
            side: "BUY".to_string(),
            order_type: "LIMIT".to_string(),
            order_status: "FILLED".to_string(),
            execution_type: "TRADE".to_string(),
            price: "50000".to_string(),
            quantity: "0.001".to_string(),
            filled_quantity: "0.001".to_string(),
            average_price: "50000".to_string(),
            commission: "0.01".to_string(),
            commission_asset: "USDT".to_string(),
            is_maker: true,
            reduce_only: false,
            position_side: "BOTH".to_string(),
            realized_pnl: "0".to_string(),
            timestamp: 0,
        };
        let display = format!("{}", ou);
        assert!(display.contains("BTCUSDT"));
        assert!(display.contains("BUY"));
        assert!(display.contains("FILLED"));
    }
}
