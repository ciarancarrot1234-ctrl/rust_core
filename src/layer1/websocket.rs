// WebSocket Client - Pure Rust Implementation
// 100k+ messages/sec capability with dynamic subscription management

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures::{StreamExt, SinkExt};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::{mpsc, RwLock as AsyncRwLock};
// Note: We use two RwLock types:
// - parking_lot::RwLock: For sync primitives accessed from both sync/async contexts (state, counters)
// - tokio::sync::RwLock (AsyncRwLock): For async-only primitives held across await points (channels)
use parking_lot::RwLock;
use std::time::{Duration, Instant};
use std::fmt;
use tracing::{info, warn, error, debug};

use crate::core::ConnectionStatus;

/// Message received from WebSocket
#[derive(Clone, Debug)]
pub struct WebSocketMessage {
    pub data: String,
    pub timestamp: u64,
}

impl fmt::Display for WebSocketMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WebSocketMessage(timestamp={}, data_len={})",
               self.timestamp, self.data.len())
    }
}

/// Commands sent to the WebSocket event loop
#[derive(Debug)]
enum WsCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

/// High-performance WebSocket client for Binance with dynamic subscription management
pub struct WebSocketClient {
    url: String,
    pub symbol: String,
    state: Arc<RwLock<ConnectionStatus>>,
    message_count: Arc<RwLock<u64>>,
    error_count: Arc<RwLock<u64>>,

    // Channel for receiving messages
    message_rx: Arc<AsyncRwLock<Option<mpsc::UnboundedReceiver<String>>>>,
    message_tx: Arc<AsyncRwLock<Option<mpsc::UnboundedSender<String>>>>,

    // Command channel for subscribe/unsubscribe while connected
    cmd_tx: Arc<AsyncRwLock<Option<mpsc::UnboundedSender<WsCommand>>>>,

    // Active subscriptions (survives reconnects)
    active_subscriptions: Arc<RwLock<HashSet<String>>>,

    // Auto-incrementing request ID for Binance protocol
    next_request_id: Arc<AtomicU32>,

    // Runtime handle for async operations
    runtime: Arc<tokio::runtime::Runtime>,

    // Configurable parameters
    ping_interval_secs: u64,
    health_check_interval_secs: u64,
    stale_timeout_secs: u64,
    max_reconnect_attempts: u32,
    connection_wait_ms: u64,
}

impl WebSocketClient {
    pub fn new(symbol: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_url(symbol, "wss://fstream.binance.com/ws")
    }

    pub fn with_url(symbol: &str, ws_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_config(symbol, ws_url, 2, 20, 30, 60, 10, 500)
    }

    pub fn with_config(
        symbol: &str,
        ws_url: &str,
        worker_threads: usize,
        ping_interval_secs: u64,
        health_check_interval_secs: u64,
        stale_timeout_secs: u64,
        max_reconnect_attempts: u32,
        connection_wait_ms: u64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .build()?;

        info!(symbol = symbol, url = ws_url, "WebSocket client created");

        Ok(Self {
            url: ws_url.to_string(),
            symbol: symbol.to_uppercase(),
            state: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            message_count: Arc::new(RwLock::new(0)),
            error_count: Arc::new(RwLock::new(0)),
            message_rx: Arc::new(AsyncRwLock::new(None)),
            message_tx: Arc::new(AsyncRwLock::new(None)),
            cmd_tx: Arc::new(AsyncRwLock::new(None)),
            active_subscriptions: Arc::new(RwLock::new(HashSet::new())),
            next_request_id: Arc::new(AtomicU32::new(1)),
            runtime: Arc::new(runtime),
            ping_interval_secs,
            health_check_interval_secs,
            stale_timeout_secs,
            max_reconnect_attempts,
            connection_wait_ms,
        })
    }

    pub fn from_binance_config(symbol: &str, config: &crate::core::BinanceConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let ws_url = format!("{}/ws", config.ws_base_url);
        Self::with_config(
            symbol,
            &ws_url,
            config.ws_worker_threads,
            config.ws_ping_interval_secs,
            config.ws_health_check_interval_secs,
            config.ws_stale_timeout_secs,
            config.max_reconnect_attempts,
            config.ws_connection_wait_ms,
        )
    }

    /// Connect to WebSocket and subscribe to stream
    pub fn connect(&self, stream_type: &str) -> Result<(), Box<dyn std::error::Error>> {
        let symbol = self.symbol.to_lowercase();
        let stream_name = format!("{}@{}", symbol, stream_type);

        // Track this as initial subscription
        self.active_subscriptions.write().insert(stream_name);

        self.start_connection()
    }

    /// Connect without an initial subscription
    pub fn connect_raw(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.start_connection()
    }

    fn start_connection(&self) -> Result<(), Box<dyn std::error::Error>> {
        let url = self.url.clone();

        info!(url = %url, "Connecting to WebSocket");

        *self.state.write() = ConnectionStatus::Connecting;

        // Create message channel
        let (msg_tx, msg_rx) = mpsc::unbounded_channel::<String>();

        // Create command channel
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<WsCommand>();

        // Store channels
        let message_tx = self.message_tx.clone();
        let message_rx = self.message_rx.clone();
        let cmd_tx_store = self.cmd_tx.clone();
        self.runtime.block_on(async {
            *message_tx.write().await = Some(msg_tx);
            *message_rx.write().await = Some(msg_rx);
            *cmd_tx_store.write().await = Some(cmd_tx);
        });

        // Clone Arcs for the async task
        let state = self.state.clone();
        let message_count = self.message_count.clone();
        let error_count = self.error_count.clone();
        let msg_tx_clone = self.message_tx.clone();
        let active_subs = self.active_subscriptions.clone();
        let next_id = self.next_request_id.clone();
        let max_reconnects = self.max_reconnect_attempts;
        let ping_interval = self.ping_interval_secs;
        let health_check_interval = self.health_check_interval_secs;
        let stale_timeout = self.stale_timeout_secs;

        // Spawn WebSocket task in the runtime
        self.runtime.spawn(async move {
            if let Err(e) = run_websocket(
                url,
                state.clone(),
                message_count.clone(),
                error_count.clone(),
                msg_tx_clone,
                cmd_rx,
                active_subs,
                next_id,
                max_reconnects,
                ping_interval,
                health_check_interval,
                stale_timeout,
            ).await {
                error!(error = %e, "WebSocket fatal error");
                *state.write() = ConnectionStatus::Failed;
            }
        });

        // Wait a bit for connection
        std::thread::sleep(Duration::from_millis(self.connection_wait_ms));

        Ok(())
    }

    /// Connect asynchronously (for use within tokio runtime)
    pub async fn connect_async(&self, stream_type: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = self.url.clone();
        let symbol = self.symbol.to_lowercase();
        let stream_name = format!("{}@{}", symbol, stream_type);

        // Track as initial subscription
        self.active_subscriptions.write().insert(stream_name);

        info!(url = %url, "Connecting to WebSocket (async)");

        *self.state.write() = ConnectionStatus::Connecting;

        // Create channels
        let (msg_tx, msg_rx) = mpsc::unbounded_channel::<String>();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<WsCommand>();

        *self.message_tx.write().await = Some(msg_tx);
        *self.message_rx.write().await = Some(msg_rx);
        *self.cmd_tx.write().await = Some(cmd_tx);

        let state = self.state.clone();
        let message_count = self.message_count.clone();
        let error_count = self.error_count.clone();
        let msg_tx_clone = self.message_tx.clone();
        let active_subs = self.active_subscriptions.clone();
        let next_id = self.next_request_id.clone();
        let max_reconnects = self.max_reconnect_attempts;
        let ping_interval = self.ping_interval_secs;
        let health_check_interval = self.health_check_interval_secs;
        let stale_timeout = self.stale_timeout_secs;

        tokio::spawn(async move {
            if let Err(e) = run_websocket(
                url,
                state.clone(),
                message_count.clone(),
                error_count.clone(),
                msg_tx_clone,
                cmd_rx,
                active_subs,
                next_id,
                max_reconnects,
                ping_interval,
                health_check_interval,
                stale_timeout,
            ).await {
                error!(error = %e, "WebSocket fatal error");
                *state.write() = ConnectionStatus::Failed;
            }
        });

        tokio::time::sleep(Duration::from_millis(self.connection_wait_ms)).await;

        Ok(())
    }

    /// Subscribe to additional streams while connected
    pub async fn subscribe(&self, streams: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        let stream_names: Vec<String> = streams.iter().map(|s| s.to_string()).collect();

        {
            let mut subs = self.active_subscriptions.write();
            for s in &stream_names {
                subs.insert(s.clone());
            }
        }

        let tx_lock = self.cmd_tx.read().await;
        if let Some(tx) = tx_lock.as_ref() {
            tx.send(WsCommand::Subscribe(stream_names))?;
        } else {
            return Err("WebSocket not connected".into());
        }

        Ok(())
    }

    /// Unsubscribe from streams while connected
    pub async fn unsubscribe(&self, streams: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        let stream_names: Vec<String> = streams.iter().map(|s| s.to_string()).collect();

        {
            let mut subs = self.active_subscriptions.write();
            for s in &stream_names {
                subs.remove(s);
            }
        }

        let tx_lock = self.cmd_tx.read().await;
        if let Some(tx) = tx_lock.as_ref() {
            tx.send(WsCommand::Unsubscribe(stream_names))?;
        } else {
            return Err("WebSocket not connected".into());
        }

        Ok(())
    }

    /// Synchronous subscribe (for use outside tokio)
    pub fn subscribe_sync(&self, streams: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        let stream_names: Vec<String> = streams.iter().map(|s| s.to_string()).collect();

        {
            let mut subs = self.active_subscriptions.write();
            for s in &stream_names {
                subs.insert(s.clone());
            }
        }

        let cmd_tx = self.cmd_tx.clone();
        self.runtime.block_on(async {
            let tx_lock = cmd_tx.read().await;
            if let Some(tx) = tx_lock.as_ref() {
                let _ = tx.send(WsCommand::Subscribe(stream_names));
            }
        });

        Ok(())
    }

    /// Synchronous unsubscribe (for use outside tokio)
    pub fn unsubscribe_sync(&self, streams: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        let stream_names: Vec<String> = streams.iter().map(|s| s.to_string()).collect();

        {
            let mut subs = self.active_subscriptions.write();
            for s in &stream_names {
                subs.remove(s);
            }
        }

        let cmd_tx = self.cmd_tx.clone();
        self.runtime.block_on(async {
            let tx_lock = cmd_tx.read().await;
            if let Some(tx) = tx_lock.as_ref() {
                let _ = tx.send(WsCommand::Unsubscribe(stream_names));
            }
        });

        Ok(())
    }

    /// Get list of active subscriptions
    pub fn active_subscriptions(&self) -> Vec<String> {
        self.active_subscriptions.read().iter().cloned().collect()
    }

    /// Receive next message asynchronously
    pub async fn recv(&self) -> Option<String> {
        let mut rx_lock = self.message_rx.write().await;
        if let Some(rx) = rx_lock.as_mut() {
            rx.recv().await
        } else {
            None
        }
    }

    /// Disconnect from WebSocket
    pub fn disconnect(&self) {
        info!("Disconnecting WebSocket");
        *self.state.write() = ConnectionStatus::Disconnected;
    }

    /// Get connection state
    pub fn is_connected(&self) -> bool {
        *self.state.read() == ConnectionStatus::Connected
    }

    /// Get message count
    pub fn message_count(&self) -> u64 {
        *self.message_count.read()
    }

    /// Get error count
    pub fn error_count(&self) -> u64 {
        *self.error_count.read()
    }

    /// Get statistics
    pub fn get_stats(&self) -> WebSocketStats {
        WebSocketStats {
            state: *self.state.read(),
            message_count: *self.message_count.read(),
            error_count: *self.error_count.read(),
            active_subscriptions: self.active_subscriptions.read().len(),
        }
    }
}

impl Drop for WebSocketClient {
    fn drop(&mut self) {
        *self.state.write() = ConnectionStatus::Disconnected;
        debug!("WebSocket client dropped, state set to Disconnected");
    }
}

/// WebSocket statistics
#[derive(Debug, Clone)]
pub struct WebSocketStats {
    pub state: ConnectionStatus,
    pub message_count: u64,
    pub error_count: u64,
    pub active_subscriptions: usize,
}

impl fmt::Display for WebSocketStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WebSocketStats(state={:?}, messages={}, errors={}, subs={})",
               self.state, self.message_count, self.error_count, self.active_subscriptions)
    }
}

/// Main WebSocket async loop with automatic reconnection
async fn run_websocket(
    url: String,
    state: Arc<RwLock<ConnectionStatus>>,
    message_count: Arc<RwLock<u64>>,
    error_count: Arc<RwLock<u64>>,
    message_tx: Arc<AsyncRwLock<Option<mpsc::UnboundedSender<String>>>>,
    mut cmd_rx: mpsc::UnboundedReceiver<WsCommand>,
    active_subscriptions: Arc<RwLock<HashSet<String>>>,
    next_request_id: Arc<AtomicU32>,
    max_reconnect_attempts: u32,
    ping_interval_secs: u64,
    health_check_interval_secs: u64,
    stale_timeout_secs: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut reconnect_attempt = 0u32;

    loop {
        // Collect current subscriptions to re-subscribe on connect
        let current_subs: Vec<String> = active_subscriptions.read().iter().cloned().collect();

        let (is_ok, error_msg) = match try_connect(
            &url, &current_subs, state.clone(), message_count.clone(),
            error_count.clone(), message_tx.clone(), &mut cmd_rx,
            next_request_id.clone(), ping_interval_secs,
            health_check_interval_secs, stale_timeout_secs,
        ).await {
            Ok(_) => (true, None),
            Err(e) => (false, Some(e.to_string())),
        };

        if is_ok {
            info!("WebSocket connection ended gracefully");
        } else if let Some(msg) = &error_msg {
            error!(error = %msg, "WebSocket connection error");
            *error_count.write() += 1;
            *state.write() = ConnectionStatus::Failed;
        }

        if reconnect_attempt >= max_reconnect_attempts {
            error!(max_attempts = max_reconnect_attempts, "Max reconnection attempts reached");
            *state.write() = ConnectionStatus::Failed;
            break;
        }

        let delay_secs = std::cmp::min(2_u64.pow(reconnect_attempt), 60);
        reconnect_attempt += 1;

        warn!(delay_secs = delay_secs, attempt = reconnect_attempt, max = max_reconnect_attempts, "Reconnecting");
        *state.write() = ConnectionStatus::Disconnected;
        tokio::time::sleep(Duration::from_secs(delay_secs)).await;
    }

    Ok(())
}

/// Try a single connection - subscribes to all active streams and processes commands
async fn try_connect(
    url: &str,
    initial_subscriptions: &[String],
    state: Arc<RwLock<ConnectionStatus>>,
    message_count: Arc<RwLock<u64>>,
    error_count: Arc<RwLock<u64>>,
    message_tx: Arc<AsyncRwLock<Option<mpsc::UnboundedSender<String>>>>,
    cmd_rx: &mut mpsc::UnboundedReceiver<WsCommand>,
    next_request_id: Arc<AtomicU32>,
    ping_interval_secs: u64,
    health_check_interval_secs: u64,
    stale_timeout_secs: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    debug!(url = url, "Connecting to WebSocket");

    let (ws_stream, _) = connect_async(url).await?;

    info!("WebSocket connected");
    *state.write() = ConnectionStatus::Connected;

    let (write, mut read) = ws_stream.split();
    let write = Arc::new(AsyncRwLock::new(write));

    // Subscribe to all active streams
    if !initial_subscriptions.is_empty() {
        let req_id = next_request_id.fetch_add(1, Ordering::Relaxed);
        let subscribe_msg = serde_json::json!({
            "method": "SUBSCRIBE",
            "params": initial_subscriptions,
            "id": req_id
        });

        let mut w = write.write().await;
        w.send(Message::Text(subscribe_msg.to_string())).await?;
        info!(count = initial_subscriptions.len(), streams = ?initial_subscriptions, "Subscribed to streams");
    }

    // Track last message time for health monitoring
    let last_message_time = Arc::new(AsyncRwLock::new(Instant::now()));

    // Start ping task
    let write_ping = write.clone();
    let ping_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(ping_interval_secs));
        loop {
            interval.tick().await;
            let mut w = write_ping.write().await;
            if let Err(e) = w.send(Message::Ping(vec![])).await {
                warn!(error = %e, "Ping failed");
                break;
            }
        }
    });

    // Start health monitor task
    let state_clone = state.clone();
    let last_msg_time = last_message_time.clone();
    let monitor_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(health_check_interval_secs));
        loop {
            interval.tick().await;
            let last_msg = *last_msg_time.read().await;
            let elapsed = last_msg.elapsed();
            if elapsed > Duration::from_secs(stale_timeout_secs) {
                warn!(elapsed_secs = ?elapsed, "Stale connection detected");
                *state_clone.write() = ConnectionStatus::Disconnected;
                break;
            }
        }
    });

    // Main event loop: process both incoming messages and commands
    loop {
        tokio::select! {
            // Incoming WebSocket message
            msg_result = read.next() => {
                match msg_result {
                    Some(Ok(Message::Text(text))) => {
                        *last_message_time.write().await = Instant::now();
                        *message_count.write() += 1;

                        let tx_lock = message_tx.read().await;
                        if let Some(tx) = tx_lock.as_ref() {
                            let _ = tx.send(text);
                        }

                        if *message_count.read() % 10000 == 0 {
                            debug!(count = *message_count.read(), "WebSocket messages received");
                        }
                    }
                    Some(Ok(Message::Binary(data))) => {
                        *last_message_time.write().await = Instant::now();
                        warn!(bytes = data.len(), "Received unexpected binary message");
                    }
                    Some(Ok(Message::Ping(data))) => {
                        *last_message_time.write().await = Instant::now();
                        let mut w = write.write().await;
                        let _ = w.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        *last_message_time.write().await = Instant::now();
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!("WebSocket closed by server");
                        *state.write() = ConnectionStatus::Disconnected;
                        break;
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(e)) => {
                        error!(error = %e, "WebSocket error");
                        *error_count.write() += 1;
                        *state.write() = ConnectionStatus::Failed;
                        break;
                    }
                    None => {
                        info!("WebSocket stream ended");
                        *state.write() = ConnectionStatus::Disconnected;
                        break;
                    }
                }
            }

            // Command from subscribe/unsubscribe calls
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(WsCommand::Subscribe(streams)) => {
                        let req_id = next_request_id.fetch_add(1, Ordering::Relaxed);
                        let msg = serde_json::json!({
                            "method": "SUBSCRIBE",
                            "params": streams,
                            "id": req_id
                        });
                        let mut w = write.write().await;
                        if let Err(e) = w.send(Message::Text(msg.to_string())).await {
                            error!(error = %e, "Failed to send subscribe");
                        } else {
                            info!(streams = ?streams, "Subscribed");
                        }
                    }
                    Some(WsCommand::Unsubscribe(streams)) => {
                        let req_id = next_request_id.fetch_add(1, Ordering::Relaxed);
                        let msg = serde_json::json!({
                            "method": "UNSUBSCRIBE",
                            "params": streams,
                            "id": req_id
                        });
                        let mut w = write.write().await;
                        if let Err(e) = w.send(Message::Text(msg.to_string())).await {
                            error!(error = %e, "Failed to send unsubscribe");
                        } else {
                            info!(streams = ?streams, "Unsubscribed");
                        }
                    }
                    None => {
                        debug!("Command channel closed");
                        break;
                    }
                }
            }
        }
    }

    debug!("WebSocket connection closed");
    *state.write() = ConnectionStatus::Disconnected;

    ping_handle.abort();
    monitor_handle.abort();

    Ok(())
}

/// Create a Binance subscribe message
pub fn create_subscribe_message(stream_name: &str) -> String {
    serde_json::json!({
        "method": "SUBSCRIBE",
        "params": [stream_name],
        "id": 1
    }).to_string()
}

/// Create a Binance unsubscribe message
pub fn create_unsubscribe_message(stream_name: &str) -> String {
    serde_json::json!({
        "method": "UNSUBSCRIBE",
        "params": [stream_name],
        "id": 1
    }).to_string()
}

/// Create a multi-stream subscribe message
pub fn create_multi_subscribe_message(streams: &[&str], id: u32) -> String {
    serde_json::json!({
        "method": "SUBSCRIBE",
        "params": streams,
        "id": id
    }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_websocket_creation() {
        let client = WebSocketClient::new("BTCUSDT").unwrap();
        assert_eq!(client.symbol, "BTCUSDT");
        assert!(!client.is_connected());
    }

    #[test]
    fn test_subscribe_message() {
        let msg = create_subscribe_message("btcusdt@depth");
        assert!(msg.contains("SUBSCRIBE"));
        assert!(msg.contains("btcusdt@depth"));
    }

    #[test]
    fn test_unsubscribe_message() {
        let msg = create_unsubscribe_message("btcusdt@depth");
        assert!(msg.contains("UNSUBSCRIBE"));
        assert!(msg.contains("btcusdt@depth"));
    }

    #[test]
    fn test_multi_subscribe_message() {
        let msg = create_multi_subscribe_message(&["btcusdt@depth", "btcusdt@aggTrade"], 5);
        assert!(msg.contains("SUBSCRIBE"));
        assert!(msg.contains("btcusdt@depth"));
        assert!(msg.contains("btcusdt@aggTrade"));
        assert!(msg.contains("\"id\":5"));
    }

    #[test]
    fn test_websocket_stats() {
        let client = WebSocketClient::new("BTCUSDT").unwrap();
        let stats = client.get_stats();
        assert_eq!(stats.message_count, 0);
        assert_eq!(stats.error_count, 0);
        assert_eq!(stats.active_subscriptions, 0);
        assert_eq!(stats.state, ConnectionStatus::Disconnected);
    }

    #[test]
    fn test_active_subscriptions_tracking() {
        let client = WebSocketClient::new("BTCUSDT").unwrap();
        assert!(client.active_subscriptions().is_empty());

        // Manually insert subscriptions (simulates what connect() does)
        client.active_subscriptions.write().insert("btcusdt@depth".to_string());
        client.active_subscriptions.write().insert("btcusdt@aggTrade".to_string());

        let subs = client.active_subscriptions();
        assert_eq!(subs.len(), 2);
        assert!(subs.contains(&"btcusdt@depth".to_string()));
        assert!(subs.contains(&"btcusdt@aggTrade".to_string()));

        // Remove one
        client.active_subscriptions.write().remove("btcusdt@depth");
        assert_eq!(client.active_subscriptions().len(), 1);
    }
}
