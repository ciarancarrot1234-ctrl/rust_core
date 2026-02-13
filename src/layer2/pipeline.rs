// Integrated Pipeline - Pure Rust Data Processing
// WebSocket -> Parser -> Synchronizer -> OrderBook (all in Rust)

use std::sync::Arc;
use std::collections::HashMap;
use parking_lot::RwLock;
use std::fmt;
use tracing::{info, debug, warn};
use serde::Serialize;

use crate::layer1::websocket::WebSocketClient;
use crate::layer2::synchronizer::DataSynchronizer;
use crate::layer2::orderbook::{OrderBook, OrderBookState};
use crate::layer2::parser::{MessageParser, ParsedMessage};
use crate::layer2::market_data_store::MarketDataStore;
use crate::layer2::metrics::UnifiedMetrics;
use crate::core::events::{publish_event, EventPriority};
use crate::core::config::get_config;

/// Pipeline statistics
#[derive(Debug, Clone)]
pub struct PipelineStats {
    pub symbol: String,
    pub mode: String,
    pub is_running: bool,
    pub updates_processed: u64,
    pub errors: u64,
    pub ws_message_count: Option<u64>,
    pub ws_error_count: Option<u64>,
    pub ws_connected: Option<bool>,
    pub sync_is_synchronized: Option<bool>,
    pub sync_buffer_size: Option<usize>,
    pub ob_bid_levels: Option<usize>,
    pub ob_ask_levels: Option<usize>,
    // Market data counts
    pub agg_trade_count: Option<u64>,
    pub kline_count: Option<u64>,
    pub mark_price_count: Option<u64>,
    pub liquidation_count: Option<u64>,
    pub book_ticker_count: Option<u64>,
    pub last_trade_price: Option<f64>,
    pub last_mark_price: Option<f64>,
}

impl fmt::Display for PipelineStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Pipeline(symbol={}, mode={}, running={}, processed={}, errors={})",
            self.symbol, self.mode, self.is_running, self.updates_processed, self.errors
        )
    }
}

/// Integrated high-performance pipeline
/// Processes market data entirely in Rust for maximum speed
pub struct RustPipeline {
    pub symbol: String,
    mode: String,

    // Components
    websocket: Arc<RwLock<Option<WebSocketClient>>>,
    parser: Arc<RwLock<Option<MessageParser>>>,
    synchronizer: Arc<RwLock<Option<DataSynchronizer>>>,
    orderbook: Arc<RwLock<Option<OrderBook>>>,
    market_data_store: Arc<RwLock<Option<MarketDataStore>>>,

    // State
    is_running: Arc<RwLock<bool>>,
    updates_processed: Arc<RwLock<u64>>,
    errors: Arc<RwLock<u64>>,

    // Latest order book state
    latest_state: Arc<RwLock<Option<OrderBookState>>>,

    // Inline alert thresholds and tracking (from config)
    large_trade_threshold: f64,
    mark_price_change_threshold_pct: f64,
    last_mark_price: Arc<RwLock<f64>>,

    // Background processing thread
    processing_thread: Arc<parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>>,
}

impl RustPipeline {
    pub fn new(symbol: &str, mode: &str) -> Self {
        info!(symbol = symbol, mode = mode, "Creating Rust Pipeline");

        // Load config values
        let config = get_config();
        let config_guard = config.read();
        let thresholds = config_guard.thresholds();
        let large_trade_threshold = thresholds.large_trade_usd;
        let mark_price_change_threshold_pct = thresholds.premium_threshold_pct;
        drop(config_guard);

        Self {
            symbol: symbol.to_uppercase(),
            mode: mode.to_string(),
            websocket: Arc::new(RwLock::new(None)),
            parser: Arc::new(RwLock::new(None)),
            synchronizer: Arc::new(RwLock::new(None)),
            orderbook: Arc::new(RwLock::new(None)),
            market_data_store: Arc::new(RwLock::new(None)),
            is_running: Arc::new(RwLock::new(false)),
            updates_processed: Arc::new(RwLock::new(0)),
            errors: Arc::new(RwLock::new(0)),
            latest_state: Arc::new(RwLock::new(None)),
            large_trade_threshold,
            mark_price_change_threshold_pct,
            last_mark_price: Arc::new(RwLock::new(0.0)),
            processing_thread: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Initialize the pipeline components
    pub fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        debug!("Initializing pipeline components");

        // Load config values for parser
        let config = get_config();
        let config_guard = config.read();
        let binance_config = config_guard.binance();
        let max_latency_ms = binance_config.parser_max_latency_ms;
        let min_price = binance_config.parser_min_price;
        let max_price = binance_config.parser_max_price;
        drop(config_guard);

        // Create WebSocket client
        let ws = WebSocketClient::new(&self.symbol)?;
        *self.websocket.write() = Some(ws);

        // Create parser with config values
        let parser = MessageParser::with_config(&self.symbol, max_latency_ms, min_price, max_price);
        *self.parser.write() = Some(parser);

        // Create synchronizer
        let sync = DataSynchronizer::new(&self.symbol, &self.mode);
        *self.synchronizer.write() = Some(sync);

        // Create order book
        let ob = OrderBook::new(&self.symbol, &self.mode)?;
        *self.orderbook.write() = Some(ob);

        // Create market data store
        let store = MarketDataStore::new(&self.symbol);
        *self.market_data_store.write() = Some(store);

        info!("Pipeline initialized");

        Ok(())
    }

    /// Start the pipeline (connect WebSocket, begin processing)
    /// This will start the background processing loop automatically
    pub fn start(&self, stream_type: &str) -> Result<(), Box<dyn std::error::Error>> {
        debug!(stream_type = stream_type, "Starting pipeline");

        if let Some(ws) = self.websocket.read().as_ref() {
            ws.connect(stream_type)?;
        } else {
            return Err("Pipeline not initialized".into());
        }

        *self.is_running.write() = true;

        // Start background processing loop
        self.start_processing_loop()?;

        info!("Pipeline started with background processing");
        Ok(())
    }

    /// Start the pipeline asynchronously
    pub async fn start_async(&self, stream_type: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!(stream_type = stream_type, "Starting pipeline (async)");

        if let Some(ws) = self.websocket.read().as_ref() {
            ws.connect_async(stream_type).await?;
        } else {
            return Err("Pipeline not initialized".into());
        }

        *self.is_running.write() = true;

        info!("Pipeline started");
        Ok(())
    }

    /// Start background processing loop (polls WebSocket and processes messages)
    /// This spawns a background thread that continuously processes messages until stop() is called
    pub fn start_processing_loop(&self) -> Result<(), Box<dyn std::error::Error>> {
        debug!("Starting background processing loop");

        // Check if already running
        if let Some(_) = self.processing_thread.lock().as_ref() {
            return Err("Processing loop already running".into());
        }

        // Clone Arc references for the thread
        let websocket = Arc::clone(&self.websocket);
        let is_running = Arc::clone(&self.is_running);
        let updates_processed = Arc::clone(&self.updates_processed);
        let errors = Arc::clone(&self.errors);
        let parser = Arc::clone(&self.parser);
        let synchronizer = Arc::clone(&self.synchronizer);
        let orderbook = Arc::clone(&self.orderbook);
        let market_data_store = Arc::clone(&self.market_data_store);
        let latest_state = Arc::clone(&self.latest_state);
        let last_mark_price = Arc::clone(&self.last_mark_price);
        let large_trade_threshold = self.large_trade_threshold;
        let mark_price_change_threshold_pct = self.mark_price_change_threshold_pct;

        // Spawn processing thread
        let handle = std::thread::spawn(move || {
            info!("Processing loop thread started");

            // Create tokio runtime for async recv()
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    warn!("Failed to create runtime in processing loop: {:?}", e);
                    return;
                }
            };

            while *is_running.read() {
                // Receive message with timeout
                let raw_message = {
                    let ws_guard = websocket.read();
                    let ws_ref = match ws_guard.as_ref() {
                        Some(ws) => ws,
                        None => {
                            drop(ws_guard);
                            warn!("WebSocket not available");
                            std::thread::sleep(std::time::Duration::from_millis(100));
                            continue;
                        }
                    };

                    // Call recv synchronously
                    let msg_result = rt.block_on(async {
                        tokio::time::timeout(
                            std::time::Duration::from_secs(2),
                            ws_ref.recv(),
                        ).await
                    });

                    drop(ws_guard);  // Release lock before processing

                    match msg_result {
                        Ok(Some(msg)) => msg,
                        Ok(None) => {
                            continue;
                        }
                        Err(_) => {
                            continue;
                        }
                    }
                };

                // Parse message
                let parsed = match parser.write().as_mut().map(|p| p.parse(&raw_message)) {
                    Some(result) => result,
                    None => {
                        warn!("Parser not available");
                        continue;
                    }
                };

                match parsed {
                    Ok(ParsedMessage::DepthUpdate(update)) => {
                        // Process through synchronizer -> orderbook
                        if let Some(sync) = synchronizer.read().as_ref() {
                            if let Some(sync_msg) = sync.process_update(update) {
                                if let Some(ob) = orderbook.read().as_ref() {
                                    let diff = crate::layer2::orderbook::DiffUpdate {
                                        bids: sync_msg.update.bids,
                                        asks: sync_msg.update.asks,
                                        final_update_id: sync_msg.update.final_update_id,
                                        first_update_id: Some(sync_msg.update.first_update_id),
                                    };

                                    if let Ok(Some(state)) = ob.process_differential(diff) {
                                        *latest_state.write() = Some(state);
                                        *updates_processed.write() += 1;
                                    }
                                }
                            }
                        }
                    }
                    Ok(ParsedMessage::AggTrade(trade)) => {
                        // Store in market data and publish event
                        if let Some(md) = market_data_store.read().as_ref() {
                            md.record_agg_trade(trade.clone());
                        }

                        // Check for large trades
                        let notional = trade.notional_usd();
                        if notional >= large_trade_threshold {
                            debug!("Large trade detected: {} {} @ ${:.2} = ${:.0}",
                                trade.side(), trade.quantity, trade.price, notional);
                        }

                        // Publish event to EventBus
                        let mut data = HashMap::new();
                        data.insert("data".to_string(), serde_json::to_value(&trade).unwrap_or_default());
                        publish_event(
                            "agg_trade",
                            trade.event_time as i64,
                            data,
                            "pipeline",
                            EventPriority::Medium,
                        );
                    }
                    Ok(ParsedMessage::Kline(kline)) => {
                        if let Some(md) = market_data_store.read().as_ref() {
                            md.record_kline(kline);
                        }
                    }
                    Ok(ParsedMessage::MarkPrice(mark_price)) => {
                        if let Some(md) = market_data_store.read().as_ref() {
                            md.record_mark_price(mark_price.clone());
                        }

                        // Check for significant price changes
                        let last = *last_mark_price.read();
                        if last > 0.0 {
                            let change_pct = ((mark_price.mark_price - last) / last).abs() * 100.0;
                            if change_pct >= mark_price_change_threshold_pct {
                                debug!("Mark price change: {:.2}% ({:.2} → {:.2})",
                                    change_pct, last, mark_price.mark_price);
                            }
                        }
                        *last_mark_price.write() = mark_price.mark_price;
                    }
                    Ok(ParsedMessage::Liquidation(liquidation)) => {
                        if let Some(md) = market_data_store.read().as_ref() {
                            md.record_liquidation(liquidation);
                        }
                    }
                    Ok(ParsedMessage::BookTicker(book_ticker)) => {
                        if let Some(md) = market_data_store.read().as_ref() {
                            md.record_book_ticker(book_ticker);
                        }
                    }
                    Ok(ParsedMessage::DepthSnapshot(_snapshot)) => {
                        debug!("Received depth snapshot in processing loop (unexpected)");
                    }
                    Err(e) => {
                        // Ignore subscription confirmation messages
                        if !raw_message.contains("\"result\":null") {
                            warn!("Parse error: {:?}", e);
                            *errors.write() += 1;
                        }
                    }
                }
            }

            info!("Processing loop thread stopped");
        });

        // Store the thread handle
        *self.processing_thread.lock() = Some(handle);

        info!("Background processing loop started");
        Ok(())
    }

    /// Stop the pipeline
    pub fn stop(&self) {
        debug!("Stopping pipeline");

        // Signal the processing thread to stop
        *self.is_running.write() = false;

        // Wait for processing thread to finish
        if let Some(handle) = self.processing_thread.lock().take() {
            debug!("Waiting for processing thread to finish...");
            if let Err(e) = handle.join() {
                warn!("Processing thread panicked: {:?}", e);
            }
        }

        // Disconnect WebSocket
        if let Some(ws) = self.websocket.read().as_ref() {
            ws.disconnect();
        }

        info!("Pipeline stopped");
    }

    /// Process a raw message through the pipeline
    pub fn process_message(&self, raw_message: &str) -> Result<Option<OrderBookState>, Box<dyn std::error::Error>> {
        let mut parser = self.parser.write();
        let parser = parser.as_mut().ok_or("Parser not initialized")?;

        // Parse the message
        match parser.parse(raw_message) {
            Ok(ParsedMessage::DepthUpdate(update)) => {
                // Publish to EventBus for Layer 3 aggregators
                self.publish_parsed_message("depth_update", &update);

                let sync = self.synchronizer.read();
                if let Some(sync) = sync.as_ref() {
                    if let Some(sync_msg) = sync.process_update(update) {
                        *self.updates_processed.write() += 1;

                        // Apply to orderbook
                        let ob = self.orderbook.read();
                        if let Some(ob) = ob.as_ref() {
                            let diff = crate::layer2::orderbook::DiffUpdate {
                                bids: sync_msg.update.bids,
                                asks: sync_msg.update.asks,
                                final_update_id: sync_msg.update.final_update_id,
                                first_update_id: Some(sync_msg.update.first_update_id),
                            };
                            match ob.process_differential(diff) {
                                Ok(state) => {
                                    if let Some(ref s) = state {
                                        *self.latest_state.write() = Some(s.clone());
                                    }
                                    return Ok(state);
                                }
                                Err(e) => {
                                    *self.errors.write() += 1;
                                    return Err(e.into());
                                }
                            }
                        }
                    }
                }
                Ok(None)
            }
            Ok(ParsedMessage::AggTrade(trade)) => {
                let notional = trade.notional_usd();

                // Alert for large trades
                if notional >= self.large_trade_threshold {
                    warn!(
                        symbol = %trade.symbol,
                        side = %trade.side(),
                        price = trade.price,
                        quantity = trade.quantity,
                        notional = notional,
                        "LARGE TRADE"
                    );
                }

                let store = self.market_data_store.read();
                if let Some(store) = store.as_ref() {
                    store.record_agg_trade(trade.clone());
                }

                // Publish to EventBus
                self.publish_parsed_message("agg_trade", &trade);

                Ok(None)
            }
            Ok(ParsedMessage::Kline(kline)) => {
                let store = self.market_data_store.read();
                if let Some(store) = store.as_ref() {
                    store.record_kline(kline.clone());
                }

                self.publish_parsed_message("kline", &kline);

                Ok(None)
            }
            Ok(ParsedMessage::MarkPrice(mp)) => {
                let last_mp = *self.last_mark_price.read();

                // Alert for significant mark price changes
                if last_mp > 0.0 {
                    let change_pct = ((mp.mark_price - last_mp) / last_mp * 100.0).abs();
                    if change_pct >= self.mark_price_change_threshold_pct {
                        info!(
                            symbol = %mp.symbol,
                            mark_price = mp.mark_price,
                            change_pct = format!("{:+.2}%", (mp.mark_price - last_mp) / last_mp * 100.0),
                            funding_rate = mp.funding_rate,
                            "MARK PRICE CHANGE"
                        );
                    }
                }
                *self.last_mark_price.write() = mp.mark_price;

                let store = self.market_data_store.read();
                if let Some(store) = store.as_ref() {
                    store.record_mark_price(mp.clone());
                }

                self.publish_parsed_message("mark_price", &mp);

                Ok(None)
            }
            Ok(ParsedMessage::Liquidation(liq)) => {
                let notional = liq.notional_usd();

                // Always alert liquidations
                warn!(
                    symbol = %liq.symbol,
                    side = %liq.side,
                    price = liq.average_price,
                    quantity = liq.filled_accumulated_quantity,
                    notional = notional,
                    "LIQUIDATION"
                );

                let store = self.market_data_store.read();
                if let Some(store) = store.as_ref() {
                    store.record_liquidation(liq.clone());
                }

                self.publish_parsed_message("liquidation", &liq);

                Ok(None)
            }
            Ok(ParsedMessage::BookTicker(bt)) => {
                let store = self.market_data_store.read();
                if let Some(store) = store.as_ref() {
                    store.record_book_ticker(bt.clone());
                }

                self.publish_parsed_message("book_ticker", &bt);

                Ok(None)
            }
            Ok(ParsedMessage::DepthSnapshot(snapshot)) => {
                self.publish_parsed_message("depth_snapshot", &snapshot);

                Ok(None)
            }
            Err(e) => {
                *self.errors.write() += 1;
                Err(e.into())
            }
        }
    }

    /// Get current order book state
    pub fn get_state(&self) -> Option<OrderBookState> {
        self.latest_state.read().clone()
    }

    /// Get pipeline statistics
    pub fn get_stats(&self) -> PipelineStats {
        let ws_stats = self.websocket.read().as_ref().map(|ws| {
            (ws.message_count(), ws.error_count(), ws.is_connected())
        });

        let sync_stats = self.synchronizer.read().as_ref().map(|sync| {
            (sync.is_synchronized(), sync.buffer_size())
        });

        let ob_stats = self.orderbook.read().as_ref().map(|ob| {
            (ob.bid_levels(), ob.ask_levels())
        });

        let mds_stats = self.market_data_store.read().as_ref().map(|s| s.get_stats());

        PipelineStats {
            symbol: self.symbol.clone(),
            mode: self.mode.clone(),
            is_running: *self.is_running.read(),
            updates_processed: *self.updates_processed.read(),
            errors: *self.errors.read(),
            ws_message_count: ws_stats.map(|s| s.0),
            ws_error_count: ws_stats.map(|s| s.1),
            ws_connected: ws_stats.map(|s| s.2),
            sync_is_synchronized: sync_stats.map(|s| s.0),
            sync_buffer_size: sync_stats.map(|s| s.1),
            ob_bid_levels: ob_stats.map(|s| s.0),
            ob_ask_levels: ob_stats.map(|s| s.1),
            agg_trade_count: mds_stats.as_ref().map(|s| s.agg_trade_count),
            kline_count: mds_stats.as_ref().map(|s| s.kline_count),
            mark_price_count: mds_stats.as_ref().map(|s| s.mark_price_count),
            liquidation_count: mds_stats.as_ref().map(|s| s.liquidation_count),
            book_ticker_count: mds_stats.as_ref().map(|s| s.book_ticker_count),
            last_trade_price: mds_stats.as_ref().and_then(|s| s.last_trade_price),
            last_mark_price: mds_stats.as_ref().and_then(|s| s.last_mark_price),
        }
    }

    /// Check if pipeline is running
    pub fn is_running(&self) -> bool {
        *self.is_running.read()
    }

    /// Get number of updates processed
    pub fn updates_processed(&self) -> u64 {
        *self.updates_processed.read()
    }

    /// Collect unified metrics from all components
    pub fn collect_metrics(&self) -> UnifiedMetrics {
        let ws_stats = self.websocket.read().as_ref().map(|ws| ws.get_stats());
        let parser_stats = self.parser.read().as_ref().map(|p| p.stats.clone());
        let sync_stats = self.synchronizer.read().as_ref().map(|s| s.get_stats());
        let ob_stats = self.orderbook.read().as_ref().map(|ob| ob.get_summary());
        let mds_stats = self.market_data_store.read().as_ref().map(|s| s.get_stats());

        UnifiedMetrics::from_stats(ws_stats, parser_stats, sync_stats, ob_stats, mds_stats)
    }

    /// Subscribe to all available streams for the symbol
    pub fn subscribe_all_streams(&self) -> Result<(), Box<dyn std::error::Error>> {
        let symbol = self.symbol.to_lowercase();
        let streams = vec![
            format!("{}@depth@100ms", symbol),
            format!("{}@aggTrade", symbol),
            format!("{}@kline_1m", symbol),
            format!("{}@markPrice@1s", symbol),
            format!("{}@forceOrder", symbol),
            format!("{}@bookTicker", symbol),
        ];
        let stream_refs: Vec<&str> = streams.iter().map(|s| s.as_str()).collect();

        if let Some(ws) = self.websocket.read().as_ref() {
            ws.subscribe_sync(&stream_refs)?;
        } else {
            return Err("WebSocket not initialized".into());
        }
        Ok(())
    }

    /// Publish a parsed message to EventBus for Layer 3 aggregators
    fn publish_parsed_message<T: Serialize>(&self, event_type: &str, data: &T) {
        let mut event_data = HashMap::new();
        event_data.insert("data".to_string(), serde_json::to_value(data).unwrap());

        publish_event(
            event_type,
            chrono::Utc::now().timestamp_millis(),
            event_data,
            "pipeline",
            EventPriority::Medium,
        );
    }
}

impl Drop for RustPipeline {
    fn drop(&mut self) {
        self.stop();
        debug!("RustPipeline dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_creation() {
        let pipeline = RustPipeline::new("BTCUSDT", "snapshot");
        assert_eq!(pipeline.symbol, "BTCUSDT");
        assert!(!pipeline.is_running());
    }

    #[test]
    fn test_pipeline_initialize() {
        let pipeline = RustPipeline::new("BTCUSDT", "snapshot");
        let result = pipeline.initialize();
        assert!(result.is_ok());
    }

    #[test]
    fn test_pipeline_stats() {
        let pipeline = RustPipeline::new("BTCUSDT", "differential");
        let stats = pipeline.get_stats();
        assert_eq!(stats.symbol, "BTCUSDT");
        assert_eq!(stats.updates_processed, 0);
        assert!(!stats.is_running);
    }

    #[test]
    fn test_pipeline_config_integration() {
        // Create pipeline and verify config values are loaded
        let pipeline = RustPipeline::new("BTCUSDT", "snapshot");

        // Default config values should be:
        // large_trade_usd: 10000.0 (from AggregatorThresholds::default())
        // premium_threshold_pct: 0.1 (from AggregatorThresholds::default())
        assert_eq!(pipeline.large_trade_threshold, 10000.0);
        assert_eq!(pipeline.mark_price_change_threshold_pct, 0.1);
    }
}
