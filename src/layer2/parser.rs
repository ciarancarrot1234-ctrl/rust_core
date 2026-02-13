// Message Parser - Pure Rust Implementation
// All Binance Futures WebSocket message types
// No PyO3 dependency - fully testable with cargo test

use serde::Deserialize;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

use crate::core::types::{Trade, BookLevel, Kline, MarkPrice, Liquidation, OrderSide};

// ============================================================================
// Price/Quantity Level
// ============================================================================

/// A price level [price, quantity]
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct PriceLevel {
    pub price: f64,
    pub quantity: f64,
}

impl PriceLevel {
    pub fn new(price: f64, quantity: f64) -> Self {
        Self { price, quantity }
    }
}

/// Parse a string field as f64, returning ParseError on failure
fn parse_f64_field(value: &str, field_name: &str) -> Result<f64, ParseError> {
    value.parse::<f64>()
        .map_err(|_| ParseError::InvalidJson(format!("Invalid {}: '{}'", field_name, value)))
}

/// Parse a Binance [price_string, qty_string] pair into PriceLevel
fn parse_level(raw: &[String; 2]) -> Result<PriceLevel, ParseError> {
    Ok(PriceLevel {
        price: parse_f64_field(&raw[0], "price")?,
        quantity: parse_f64_field(&raw[1], "quantity")?,
    })
}

// ============================================================================
// Parsed Message Types (Pure Rust)
// ============================================================================

/// Parsed aggregate trade
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ParsedAggTrade {
    pub symbol: String,
    pub agg_trade_id: i64,
    pub price: f64,
    pub quantity: f64,
    pub first_trade_id: i64,
    pub last_trade_id: i64,
    pub timestamp: u64,
    pub is_buyer_maker: bool,
    pub event_time: u64,
}

impl ParsedAggTrade {
    pub fn side(&self) -> &str {
        if self.is_buyer_maker { "SELL" } else { "BUY" }
    }

    pub fn notional_usd(&self) -> f64 {
        self.price * self.quantity
    }
}

/// Parsed depth update (order book diff)
#[derive(Debug, Clone, serde::Serialize)]
pub struct ParsedDepthUpdate {
    pub symbol: String,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub event_time: u64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub prev_final_update_id: Option<u64>,
}

/// Parsed depth snapshot (full order book)
#[derive(Debug, Clone, serde::Serialize)]
pub struct ParsedDepthSnapshot {
    pub symbol: Option<String>,
    pub last_update_id: u64,
    pub event_time: Option<u64>,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

/// Parsed kline/candlestick
#[derive(Debug, Clone, serde::Serialize)]
pub struct ParsedKline {
    pub symbol: String,
    pub interval: String,
    pub open_time: u64,
    pub close_time: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trades: u64,
    pub taker_buy_volume: f64,
    pub taker_buy_quote_volume: f64,
    pub is_closed: bool,
    pub event_time: u64,
}

/// Parsed mark price update
#[derive(Debug, Clone, serde::Serialize)]
pub struct ParsedMarkPrice {
    pub symbol: String,
    pub mark_price: f64,
    pub index_price: f64,
    pub estimated_settle_price: f64,
    pub funding_rate: f64,
    pub next_funding_time: u64,
    pub timestamp: u64,
}

/// Parsed liquidation order
#[derive(Debug, Clone, serde::Serialize)]
pub struct ParsedLiquidation {
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub time_in_force: String,
    pub quantity: f64,
    pub price: f64,
    pub average_price: f64,
    pub order_status: String,
    pub last_filled_quantity: f64,
    pub filled_accumulated_quantity: f64,
    pub timestamp: u64,
    pub event_time: u64,
}

impl ParsedLiquidation {
    pub fn notional_usd(&self) -> f64 {
        self.average_price * self.filled_accumulated_quantity
    }
}

/// Parsed book ticker (best bid/ask)
#[derive(Debug, Clone, serde::Serialize)]
pub struct ParsedBookTicker {
    pub symbol: String,
    pub update_id: u64,
    pub best_bid_price: f64,
    pub best_bid_qty: f64,
    pub best_ask_price: f64,
    pub best_ask_qty: f64,
    pub timestamp: u64,
}

impl ParsedBookTicker {
    pub fn mid_price(&self) -> f64 {
        (self.best_bid_price + self.best_ask_price) / 2.0
    }

    pub fn spread(&self) -> f64 {
        self.best_ask_price - self.best_bid_price
    }

    pub fn spread_bps(&self) -> f64 {
        let mid = self.mid_price();
        if mid > 0.0 {
            (self.spread() / mid) * 10000.0
        } else {
            0.0
        }
    }
}

// ============================================================================
// Unified Parsed Message Enum
// ============================================================================

/// All possible parsed message types from Binance WebSocket
#[derive(Debug, Clone)]
pub enum ParsedMessage {
    AggTrade(ParsedAggTrade),
    DepthUpdate(ParsedDepthUpdate),
    DepthSnapshot(ParsedDepthSnapshot),
    Kline(ParsedKline),
    MarkPrice(ParsedMarkPrice),
    Liquidation(ParsedLiquidation),
    BookTicker(ParsedBookTicker),
}

// ============================================================================
// Serde Structures (Raw Binance JSON)
// ============================================================================

#[derive(Debug, Deserialize)]
struct RawAggTrade {
    #[serde(rename = "e")]
    _event_type: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "a")]
    agg_trade_id: i64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "f")]
    first_trade_id: i64,
    #[serde(rename = "l")]
    last_trade_id: i64,
    #[serde(rename = "T")]
    timestamp: u64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

#[derive(Debug, Deserialize)]
struct RawDepthUpdate {
    #[serde(rename = "e")]
    _event_type: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "pu", default)]
    prev_final_update_id: Option<u64>,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct RawDepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    #[serde(rename = "E", default)]
    event_time: Option<u64>,
    #[serde(rename = "s", default)]
    symbol: Option<String>,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct RawKlineData {
    #[serde(rename = "t")]
    open_time: u64,
    #[serde(rename = "T")]
    close_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "i")]
    interval: String,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "n")]
    trades: u64,
    #[serde(rename = "x")]
    is_closed: bool,
    #[serde(rename = "q")]
    quote_volume: String,
    #[serde(rename = "V")]
    taker_buy_volume: String,
    #[serde(rename = "Q")]
    taker_buy_quote_volume: String,
}

#[derive(Debug, Deserialize)]
struct RawKline {
    #[serde(rename = "e")]
    _event_type: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    _symbol: String,
    #[serde(rename = "k")]
    kline: RawKlineData,
}

#[derive(Debug, Deserialize)]
struct RawMarkPrice {
    #[serde(rename = "e")]
    _event_type: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    mark_price: String,
    #[serde(rename = "i")]
    index_price: String,
    #[serde(rename = "P")]
    estimated_settle_price: String,
    #[serde(rename = "r")]
    funding_rate: String,
    #[serde(rename = "T")]
    next_funding_time: u64,
}

#[derive(Debug, Deserialize)]
struct RawLiquidationOrder {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "o")]
    order_type: String,
    #[serde(rename = "f")]
    time_in_force: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "ap")]
    average_price: String,
    #[serde(rename = "X")]
    order_status: String,
    #[serde(rename = "l")]
    last_filled_quantity: String,
    #[serde(rename = "z")]
    filled_accumulated_quantity: String,
    #[serde(rename = "T")]
    timestamp: u64,
}

#[derive(Debug, Deserialize)]
struct RawLiquidation {
    #[serde(rename = "e")]
    _event_type: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "o")]
    order: RawLiquidationOrder,
}

#[derive(Debug, Deserialize)]
struct RawBookTicker {
    #[serde(rename = "e")]
    _event_type: String,
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    best_bid_price: String,
    #[serde(rename = "B")]
    best_bid_qty: String,
    #[serde(rename = "a")]
    best_ask_price: String,
    #[serde(rename = "A")]
    best_ask_qty: String,
    #[serde(rename = "T")]
    timestamp: u64,
    #[serde(rename = "E")]
    event_time: u64,
}

// ============================================================================
// MessageParser - Stateful parser with validation & stats
// ============================================================================

/// Parser statistics
#[derive(Debug, Clone, Default)]
pub struct ParserStats {
    pub messages_parsed: u64,
    pub parse_errors: u64,
    pub validation_failures: u64,
    pub duplicate_messages: u64,
    pub total_latency_ms: f64,
    pub agg_trade_count: u64,
    pub depth_count: u64,
    pub kline_count: u64,
    pub mark_price_count: u64,
    pub liquidation_count: u64,
    pub book_ticker_count: u64,
}

impl ParserStats {
    pub fn avg_latency_ms(&self) -> f64 {
        if self.messages_parsed > 0 {
            self.total_latency_ms / self.messages_parsed as f64
        } else {
            0.0
        }
    }

    pub fn error_rate(&self) -> f64 {
        if self.messages_parsed > 0 {
            self.parse_errors as f64 / self.messages_parsed as f64
        } else {
            0.0
        }
    }
}

/// Stateful message parser with validation, duplicate detection, and stats
pub struct MessageParser {
    pub symbol: String,
    pub stats: ParserStats,

    // Duplicate detection
    last_trade_id: Option<i64>,
    last_update_id: Option<u64>,

    // Validation thresholds
    max_latency_ms: f64,
    min_price: f64,
    max_price: f64,
}

/// Parser error type
#[derive(Debug)]
pub enum ParseError {
    InvalidJson(String),
    UnknownEventType(String),
    ValidationFailed(String),
    DuplicateMessage(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::InvalidJson(e) => write!(f, "Invalid JSON: {}", e),
            ParseError::UnknownEventType(e) => write!(f, "Unknown event type: {}", e),
            ParseError::ValidationFailed(e) => write!(f, "Validation failed: {}", e),
            ParseError::DuplicateMessage(e) => write!(f, "Duplicate: {}", e),
        }
    }
}

impl std::error::Error for ParseError {}

impl MessageParser {
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_uppercase(),
            stats: ParserStats::default(),
            last_trade_id: None,
            last_update_id: None,
            max_latency_ms: 5000.0,
            min_price: 1000.0,
            max_price: 1_000_000.0,
        }
    }

    /// Create with configurable thresholds
    pub fn with_config(symbol: &str, max_latency_ms: f64, min_price: f64, max_price: f64) -> Self {
        Self {
            symbol: symbol.to_uppercase(),
            stats: ParserStats::default(),
            last_trade_id: None,
            last_update_id: None,
            max_latency_ms,
            min_price,
            max_price,
        }
    }

    /// Get current timestamp in milliseconds
    fn now_ms() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64() * 1000.0
    }

    /// Track latency between exchange timestamp and now
    fn track_latency(&mut self, exchange_time_ms: u64) {
        let now = Self::now_ms();
        let latency = now - exchange_time_ms as f64;
        self.stats.total_latency_ms += latency;

        if latency > self.max_latency_ms {
            warn!(latency_ms = latency, threshold_ms = self.max_latency_ms, "High latency detected");
        }
    }

    /// Validate price is within reasonable bounds
    fn validate_price(&mut self, price: f64) -> bool {
        if price < self.min_price || price > self.max_price {
            self.stats.validation_failures += 1;
            warn!(price = price, min = self.min_price, max = self.max_price, "Price out of bounds");
            return false;
        }
        true
    }

    // ========================================================================
    // Main parse entry point
    // ========================================================================

    /// Parse any Binance WebSocket message (auto-detects type)
    pub fn parse(&mut self, raw_json: &str) -> Result<ParsedMessage, ParseError> {
        // First pass: detect message type
        let value: serde_json::Value = serde_json::from_str(raw_json)
            .map_err(|e| ParseError::InvalidJson(e.to_string()))?;

        // Unwrap combined stream format: {"stream": "...", "data": {...}}
        let data = if value.get("data").is_some() {
            value.get("data").unwrap()
        } else {
            &value
        };

        // Check for depth snapshot (no "e" field, has "lastUpdateId")
        if data.get("lastUpdateId").is_some() && data.get("e").is_none() {
            return self.parse_depth_snapshot_value(data);
        }

        // Get event type
        let event_type = data.get("e")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ParseError::InvalidJson("Missing event type 'e'".into()))?;

        // Validate symbol matches (if present)
        if let Some(sym) = data.get("s").and_then(|v| v.as_str()) {
            if sym.to_uppercase() != self.symbol {
                self.stats.validation_failures += 1;
                return Err(ParseError::ValidationFailed(
                    format!("Symbol mismatch: expected {}, got {}", self.symbol, sym)
                ));
            }
        }

        // Route to specific parser
        let data_str = serde_json::to_string(data)
            .map_err(|e| ParseError::InvalidJson(e.to_string()))?;

        match event_type {
            "aggTrade" => self.parse_agg_trade(&data_str),
            "depthUpdate" => self.parse_depth_update(&data_str),
            "kline" => self.parse_kline(&data_str),
            "markPriceUpdate" => self.parse_mark_price(&data_str),
            "forceOrder" => self.parse_liquidation(&data_str),
            "bookTicker" => self.parse_book_ticker(&data_str),
            _ => Err(ParseError::UnknownEventType(event_type.to_string())),
        }
    }

    // ========================================================================
    // Individual parsers
    // ========================================================================

    pub fn parse_agg_trade(&mut self, raw_json: &str) -> Result<ParsedMessage, ParseError> {
        let raw: RawAggTrade = serde_json::from_str(raw_json)
            .map_err(|e| ParseError::InvalidJson(e.to_string()))?;

        let price = parse_f64_field(&raw.price, "price")?;
        let quantity = parse_f64_field(&raw.quantity, "quantity")?;

        // Validate price
        if !self.validate_price(price) {
            return Err(ParseError::ValidationFailed("Price out of bounds".into()));
        }

        // Duplicate detection
        if self.last_trade_id == Some(raw.agg_trade_id) {
            self.stats.duplicate_messages += 1;
            return Err(ParseError::DuplicateMessage(
                format!("Duplicate trade ID: {}", raw.agg_trade_id)
            ));
        }
        self.last_trade_id = Some(raw.agg_trade_id);

        // Track latency
        self.track_latency(raw.event_time);

        self.stats.messages_parsed += 1;
        self.stats.agg_trade_count += 1;

        Ok(ParsedMessage::AggTrade(ParsedAggTrade {
            symbol: raw.symbol,
            agg_trade_id: raw.agg_trade_id,
            price,
            quantity,
            first_trade_id: raw.first_trade_id,
            last_trade_id: raw.last_trade_id,
            timestamp: raw.timestamp,
            is_buyer_maker: raw.is_buyer_maker,
            event_time: raw.event_time,
        }))
    }

    pub fn parse_depth_update(&mut self, raw_json: &str) -> Result<ParsedMessage, ParseError> {
        let raw: RawDepthUpdate = serde_json::from_str(raw_json)
            .map_err(|e| ParseError::InvalidJson(e.to_string()))?;

        // Gap detection (lightweight - Synchronizer handles authoritative gap detection)
        if let Some(last_id) = self.last_update_id {
            let expected_first = last_id + 1;
            if raw.first_update_id > expected_first {
                let gap = raw.first_update_id - expected_first;
                self.stats.validation_failures += 1;
                // Only warn for very large gaps; small gaps are normal for 100ms streams
                // and the Synchronizer handles proper gap detection + re-sync
                if gap > 10000 {
                    warn!(
                        gap = gap,
                        last_id = last_id,
                        first_id = raw.first_update_id,
                        "Large parser gap detected"
                    );
                }
            }
        }
        self.last_update_id = Some(raw.final_update_id);

        let bids: Result<Vec<PriceLevel>, ParseError> = raw.bids.iter().map(|l| parse_level(l)).collect();
        let asks: Result<Vec<PriceLevel>, ParseError> = raw.asks.iter().map(|l| parse_level(l)).collect();

        self.track_latency(raw.event_time);
        self.stats.messages_parsed += 1;
        self.stats.depth_count += 1;

        Ok(ParsedMessage::DepthUpdate(ParsedDepthUpdate {
            symbol: raw.symbol,
            first_update_id: raw.first_update_id,
            final_update_id: raw.final_update_id,
            event_time: raw.event_time,
            bids: bids?,
            asks: asks?,
            prev_final_update_id: raw.prev_final_update_id,
        }))
    }

    fn parse_depth_snapshot_value(&mut self, value: &serde_json::Value) -> Result<ParsedMessage, ParseError> {
        let raw: RawDepthSnapshot = serde_json::from_value(value.clone())
            .map_err(|e| ParseError::InvalidJson(e.to_string()))?;

        let bids: Result<Vec<PriceLevel>, ParseError> = raw.bids.iter().map(|l| parse_level(l)).collect();
        let asks: Result<Vec<PriceLevel>, ParseError> = raw.asks.iter().map(|l| parse_level(l)).collect();

        self.stats.messages_parsed += 1;
        self.stats.depth_count += 1;

        Ok(ParsedMessage::DepthSnapshot(ParsedDepthSnapshot {
            symbol: raw.symbol,
            last_update_id: raw.last_update_id,
            event_time: raw.event_time,
            bids: bids?,
            asks: asks?,
        }))
    }

    pub fn parse_kline(&mut self, raw_json: &str) -> Result<ParsedMessage, ParseError> {
        let raw: RawKline = serde_json::from_str(raw_json)
            .map_err(|e| ParseError::InvalidJson(e.to_string()))?;

        let k = &raw.kline;

        self.track_latency(raw.event_time);
        self.stats.messages_parsed += 1;
        self.stats.kline_count += 1;

        Ok(ParsedMessage::Kline(ParsedKline {
            symbol: k.symbol.clone(),
            interval: k.interval.clone(),
            open_time: k.open_time,
            close_time: k.close_time,
            open: parse_f64_field(&k.open, "kline.open")?,
            high: parse_f64_field(&k.high, "kline.high")?,
            low: parse_f64_field(&k.low, "kline.low")?,
            close: parse_f64_field(&k.close, "kline.close")?,
            volume: parse_f64_field(&k.volume, "kline.volume")?,
            quote_volume: parse_f64_field(&k.quote_volume, "kline.quote_volume")?,
            trades: k.trades,
            taker_buy_volume: parse_f64_field(&k.taker_buy_volume, "kline.taker_buy_volume")?,
            taker_buy_quote_volume: parse_f64_field(&k.taker_buy_quote_volume, "kline.taker_buy_quote_volume")?,
            is_closed: k.is_closed,
            event_time: raw.event_time,
        }))
    }

    pub fn parse_mark_price(&mut self, raw_json: &str) -> Result<ParsedMessage, ParseError> {
        let raw: RawMarkPrice = serde_json::from_str(raw_json)
            .map_err(|e| ParseError::InvalidJson(e.to_string()))?;

        self.track_latency(raw.event_time);
        self.stats.messages_parsed += 1;
        self.stats.mark_price_count += 1;

        Ok(ParsedMessage::MarkPrice(ParsedMarkPrice {
            symbol: raw.symbol,
            mark_price: parse_f64_field(&raw.mark_price, "mark_price")?,
            index_price: parse_f64_field(&raw.index_price, "index_price")?,
            estimated_settle_price: parse_f64_field(&raw.estimated_settle_price, "estimated_settle_price")?,
            funding_rate: parse_f64_field(&raw.funding_rate, "funding_rate")?,
            next_funding_time: raw.next_funding_time,
            timestamp: raw.event_time,
        }))
    }

    pub fn parse_liquidation(&mut self, raw_json: &str) -> Result<ParsedMessage, ParseError> {
        let raw: RawLiquidation = serde_json::from_str(raw_json)
            .map_err(|e| ParseError::InvalidJson(e.to_string()))?;

        let o = &raw.order;

        self.track_latency(raw.event_time);
        self.stats.messages_parsed += 1;
        self.stats.liquidation_count += 1;

        Ok(ParsedMessage::Liquidation(ParsedLiquidation {
            symbol: o.symbol.clone(),
            side: o.side.clone(),
            order_type: o.order_type.clone(),
            time_in_force: o.time_in_force.clone(),
            quantity: parse_f64_field(&o.quantity, "liquidation.quantity")?,
            price: parse_f64_field(&o.price, "liquidation.price")?,
            average_price: parse_f64_field(&o.average_price, "liquidation.average_price")?,
            order_status: o.order_status.clone(),
            last_filled_quantity: parse_f64_field(&o.last_filled_quantity, "liquidation.last_filled_quantity")?,
            filled_accumulated_quantity: parse_f64_field(&o.filled_accumulated_quantity, "liquidation.filled_accumulated_quantity")?,
            timestamp: o.timestamp,
            event_time: raw.event_time,
        }))
    }

    pub fn parse_book_ticker(&mut self, raw_json: &str) -> Result<ParsedMessage, ParseError> {
        let raw: RawBookTicker = serde_json::from_str(raw_json)
            .map_err(|e| ParseError::InvalidJson(e.to_string()))?;

        self.track_latency(raw.event_time);
        self.stats.messages_parsed += 1;
        self.stats.book_ticker_count += 1;

        Ok(ParsedMessage::BookTicker(ParsedBookTicker {
            symbol: raw.symbol,
            update_id: raw.update_id,
            best_bid_price: parse_f64_field(&raw.best_bid_price, "book_ticker.bid_price")?,
            best_bid_qty: parse_f64_field(&raw.best_bid_qty, "book_ticker.bid_qty")?,
            best_ask_price: parse_f64_field(&raw.best_ask_price, "book_ticker.ask_price")?,
            best_ask_qty: parse_f64_field(&raw.best_ask_qty, "book_ticker.ask_qty")?,
            timestamp: raw.timestamp,
        }))
    }

    /// Reset all statistics
    pub fn reset_stats(&mut self) {
        self.stats = ParserStats::default();
    }
}

// ============================================================================
// Standalone parse functions (stateless, for quick one-off parsing)
// ============================================================================

/// Parse a depth update without stateful tracking
pub fn parse_depth_update_raw(raw_json: &str) -> Result<ParsedDepthUpdate, ParseError> {
    let raw: RawDepthUpdate = serde_json::from_str(raw_json)
        .map_err(|e| ParseError::InvalidJson(e.to_string()))?;

    let bids: Result<Vec<PriceLevel>, ParseError> = raw.bids.iter().map(|l| parse_level(l)).collect();
    let asks: Result<Vec<PriceLevel>, ParseError> = raw.asks.iter().map(|l| parse_level(l)).collect();

    Ok(ParsedDepthUpdate {
        symbol: raw.symbol,
        first_update_id: raw.first_update_id,
        final_update_id: raw.final_update_id,
        event_time: raw.event_time,
        bids: bids?,
        asks: asks?,
        prev_final_update_id: raw.prev_final_update_id,
    })
}

// ============================================================================
// Conversions from Parsed* types to core types
// ============================================================================

impl From<ParsedAggTrade> for Trade {
    fn from(parsed: ParsedAggTrade) -> Self {
        Self {
            symbol: parsed.symbol,
            trade_id: parsed.agg_trade_id,
            price: parsed.price,
            quantity: parsed.quantity,
            quote_quantity: parsed.price * parsed.quantity,
            timestamp: parsed.timestamp as i64,
            is_buyer_maker: parsed.is_buyer_maker,
            event_time: parsed.event_time,
            first_trade_id: parsed.first_trade_id,
            last_trade_id: parsed.last_trade_id,
        }
    }
}

impl From<PriceLevel> for BookLevel {
    fn from(level: PriceLevel) -> Self {
        Self {
            price: level.price,
            quantity: level.quantity,
        }
    }
}

impl From<ParsedKline> for Kline {
    fn from(parsed: ParsedKline) -> Self {
        Self {
            symbol: parsed.symbol,
            interval: parsed.interval,
            open_time: parsed.open_time as i64,
            close_time: parsed.close_time as i64,
            open: parsed.open,
            high: parsed.high,
            low: parsed.low,
            close: parsed.close,
            volume: parsed.volume,
            quote_volume: parsed.quote_volume,
            trades: parsed.trades as i32,
            taker_buy_volume: parsed.taker_buy_volume,
            taker_buy_quote_volume: parsed.taker_buy_quote_volume,
            is_closed: parsed.is_closed,
            event_time: parsed.event_time,
        }
    }
}

impl From<ParsedMarkPrice> for MarkPrice {
    fn from(parsed: ParsedMarkPrice) -> Self {
        Self {
            symbol: parsed.symbol,
            mark_price: parsed.mark_price,
            index_price: parsed.index_price,
            estimated_settle_price: parsed.estimated_settle_price,
            funding_rate: parsed.funding_rate,
            next_funding_time: parsed.next_funding_time as i64,
            timestamp: parsed.timestamp as i64,
        }
    }
}

impl From<ParsedLiquidation> for Liquidation {
    fn from(parsed: ParsedLiquidation) -> Self {
        // Convert String side to OrderSide enum, defaulting to Buy if parsing fails
        let side = OrderSide::from_str(&parsed.side).unwrap_or(OrderSide::Buy);

        Self {
            symbol: parsed.symbol,
            side,
            order_type: parsed.order_type,
            time_in_force: parsed.time_in_force,
            quantity: parsed.quantity,
            price: parsed.price,
            average_price: parsed.average_price,
            order_status: parsed.order_status,
            last_filled_quantity: parsed.last_filled_quantity,
            filled_accumulated_quantity: parsed.filled_accumulated_quantity,
            timestamp: parsed.timestamp as i64,
        }
    }
}
