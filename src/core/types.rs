// Core Type Definitions for Liquidity Hunt System
// Pure Rust - no Python dependencies

use serde::{Deserialize, Serialize};
use std::fmt;

// ============================================================================
// Enums
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Long,
    Short,
    Neutral,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketRegime {
    TrendingUp,
    TrendingDown,
    Ranging,
    Choppy,
    Breakout,
    Reversal,
    LowVolatility,
    HighVolatility,
}

impl fmt::Display for MarketRegime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VolatilityState {
    Low,
    Normal,
    High,
    Extreme,
}

impl fmt::Display for VolatilityState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionStatus {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Failed,
}

impl fmt::Display for ConnectionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl fmt::Display for OrderSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderSide::Buy => write!(f, "BUY"),
            OrderSide::Sell => write!(f, "SELL"),
        }
    }
}

impl std::str::FromStr for OrderSide {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "BUY" => Ok(OrderSide::Buy),
            "SELL" => Ok(OrderSide::Sell),
            _ => Err(format!("Invalid OrderSide: '{}'. Expected 'BUY' or 'SELL'", s)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
    Market,
    StopMarket,
    TakeProfitMarket,
}

impl fmt::Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    GTC, // Good Till Cancel
    IOC, // Immediate or Cancel
    FOK, // Fill or Kill
}

impl fmt::Display for TimeInForce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionSide {
    Both,
    Long,
    Short,
}

impl fmt::Display for PositionSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

// ============================================================================
// Trade
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub symbol: String,
    pub trade_id: i64,
    pub price: f64,
    pub quantity: f64,
    pub quote_quantity: f64,
    pub timestamp: i64, // milliseconds
    pub is_buyer_maker: bool,
    pub event_time: u64, // for latency tracking
    pub first_trade_id: i64, // for gap detection
    pub last_trade_id: i64, // for gap detection
}

impl Trade {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: String,
        trade_id: i64,
        price: f64,
        quantity: f64,
        quote_quantity: f64,
        timestamp: i64,
        is_buyer_maker: bool,
        event_time: u64,
        first_trade_id: i64,
        last_trade_id: i64,
    ) -> Self {
        Self {
            symbol,
            trade_id,
            price,
            quantity,
            quote_quantity,
            timestamp,
            is_buyer_maker,
            event_time,
            first_trade_id,
            last_trade_id,
        }
    }

    pub fn side(&self) -> OrderSide {
        if self.is_buyer_maker {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        }
    }

    pub fn notional_usd(&self) -> f64 {
        self.quote_quantity
    }
}

impl fmt::Display for Trade {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Trade(symbol={}, id={}, price={:.2}, qty={:.4}, side={:?})",
            self.symbol, self.trade_id, self.price, self.quantity, self.side()
        )
    }
}

// ============================================================================
// BookLevel
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: f64,
    pub quantity: f64,
}

impl BookLevel {
    pub fn new(price: f64, quantity: f64) -> Self {
        Self { price, quantity }
    }

    pub fn notional(&self) -> f64 {
        self.price * self.quantity
    }
}

impl fmt::Display for BookLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BookLevel(price={:.2}, qty={:.4})", self.price, self.quantity)
    }
}

// ============================================================================
// OrderBookSnapshot
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub symbol: String,
    pub last_update_id: u64,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub timestamp: i64,
    pub first_update_id: Option<u64>,
}

impl OrderBookSnapshot {
    pub fn new(
        symbol: String,
        last_update_id: u64,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        timestamp: i64,
        first_update_id: Option<u64>,
    ) -> Self {
        Self {
            symbol,
            last_update_id,
            bids,
            asks,
            timestamp,
            first_update_id,
        }
    }

    pub fn best_bid(&self) -> Option<&BookLevel> {
        self.bids.first()
    }

    pub fn best_ask(&self) -> Option<&BookLevel> {
        self.asks.first()
    }

    pub fn mid_price(&self) -> Option<f64> {
        let best_bid = self.best_bid()?;
        let best_ask = self.best_ask()?;
        Some((best_bid.price + best_ask.price) / 2.0)
    }

    pub fn spread(&self) -> Option<f64> {
        let best_bid = self.best_bid()?;
        let best_ask = self.best_ask()?;
        Some(best_ask.price - best_bid.price)
    }
}

impl fmt::Display for OrderBookSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OrderBookSnapshot(symbol={}, update_id={}, bids={}, asks={})",
            self.symbol, self.last_update_id, self.bids.len(), self.asks.len()
        )
    }
}

// ============================================================================
// Kline (Candlestick)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Kline {
    pub symbol: String,
    pub interval: String,
    pub open_time: i64,
    pub close_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trades: i32,
    pub taker_buy_volume: f64,
    pub taker_buy_quote_volume: f64,
    pub is_closed: bool,
    pub event_time: u64, // for latency tracking
}

impl Kline {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: String,
        interval: String,
        open_time: i64,
        close_time: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
        quote_volume: f64,
        trades: i32,
        taker_buy_volume: f64,
        taker_buy_quote_volume: f64,
        is_closed: bool,
        event_time: u64,
    ) -> Self {
        Self {
            symbol,
            interval,
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trades,
            taker_buy_volume,
            taker_buy_quote_volume,
            is_closed,
            event_time,
        }
    }
}

impl fmt::Display for Kline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Kline(symbol={}, interval={}, O={:.2}, H={:.2}, L={:.2}, C={:.2})",
            self.symbol, self.interval, self.open, self.high, self.low, self.close
        )
    }
}

// ============================================================================
// MarkPrice
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkPrice {
    pub symbol: String,
    pub mark_price: f64,
    pub index_price: f64,
    pub estimated_settle_price: f64,
    pub funding_rate: f64,
    pub next_funding_time: i64,
    pub timestamp: i64,
}

impl MarkPrice {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: String,
        mark_price: f64,
        index_price: f64,
        estimated_settle_price: f64,
        funding_rate: f64,
        next_funding_time: i64,
        timestamp: i64,
    ) -> Self {
        Self {
            symbol,
            mark_price,
            index_price,
            estimated_settle_price,
            funding_rate,
            next_funding_time,
            timestamp,
        }
    }
}

impl fmt::Display for MarkPrice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MarkPrice(symbol={}, mark={:.2}, funding={:.6})",
            self.symbol, self.mark_price, self.funding_rate
        )
    }
}

// ============================================================================
// Liquidation
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Liquidation {
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: String,
    pub time_in_force: String,
    pub quantity: f64,
    pub price: f64,
    pub average_price: f64,
    pub order_status: String,
    pub last_filled_quantity: f64,
    pub filled_accumulated_quantity: f64,
    pub timestamp: i64,
}

impl Liquidation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: String,
        side: OrderSide,
        order_type: String,
        time_in_force: String,
        quantity: f64,
        price: f64,
        average_price: f64,
        order_status: String,
        last_filled_quantity: f64,
        filled_accumulated_quantity: f64,
        timestamp: i64,
    ) -> Self {
        Self {
            symbol,
            side,
            order_type,
            time_in_force,
            quantity,
            price,
            average_price,
            order_status,
            last_filled_quantity,
            filled_accumulated_quantity,
            timestamp,
        }
    }
}

impl fmt::Display for Liquidation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Liquidation(symbol={}, side={:?}, qty={:.4}, price={:.2})",
            self.symbol, self.side, self.quantity, self.average_price
        )
    }
}

// ============================================================================
// ExchangeInfo
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeInfo {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub price_precision: i32,
    pub quantity_precision: i32,
    pub base_asset_precision: i32,
    pub quote_asset_precision: i32,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_notional: f64,
    pub min_quantity: f64,
    pub max_quantity: f64,
    pub max_price: f64,
    pub min_price: f64,
}

impl ExchangeInfo {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: String,
        base_asset: String,
        quote_asset: String,
        price_precision: i32,
        quantity_precision: i32,
        base_asset_precision: i32,
        quote_asset_precision: i32,
        tick_size: f64,
        step_size: f64,
        min_notional: f64,
        min_quantity: f64,
        max_quantity: f64,
        max_price: f64,
        min_price: f64,
    ) -> Self {
        Self {
            symbol,
            base_asset,
            quote_asset,
            price_precision,
            quantity_precision,
            base_asset_precision,
            quote_asset_precision,
            tick_size,
            step_size,
            min_notional,
            min_quantity,
            max_quantity,
            max_price,
            min_price,
        }
    }
}

impl fmt::Display for ExchangeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ExchangeInfo(symbol={}, tick={:.8}, step={:.8})",
            self.symbol, self.tick_size, self.step_size
        )
    }
}

// ============================================================================
// AccountInfo
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountInfo {
    pub total_wallet_balance: f64,
    pub total_unrealized_pnl: f64,
    pub total_margin_balance: f64,
    pub available_balance: f64,
    pub max_withdraw_amount: f64,
    pub timestamp: i64,
}

impl AccountInfo {
    pub fn new(
        total_wallet_balance: f64,
        total_unrealized_pnl: f64,
        total_margin_balance: f64,
        available_balance: f64,
        max_withdraw_amount: f64,
        timestamp: i64,
    ) -> Self {
        Self {
            total_wallet_balance,
            total_unrealized_pnl,
            total_margin_balance,
            available_balance,
            max_withdraw_amount,
            timestamp,
        }
    }
}

impl fmt::Display for AccountInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AccountInfo(balance={:.2}, pnl={:.2}, available={:.2})",
            self.total_wallet_balance, self.total_unrealized_pnl, self.available_balance
        )
    }
}

// ============================================================================
// Position
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub position_side: PositionSide,
    pub position_amt: f64,
    pub entry_price: f64,
    pub unrealized_profit: f64,
    pub leverage: i32,
    pub isolated_wallet: f64,
    pub mark_price: f64,
    pub liquidation_price: f64,
    pub timestamp: i64,
}

impl Position {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: String,
        position_side: PositionSide,
        position_amt: f64,
        entry_price: f64,
        unrealized_profit: f64,
        leverage: i32,
        isolated_wallet: f64,
        mark_price: f64,
        liquidation_price: f64,
        timestamp: i64,
    ) -> Self {
        Self {
            symbol,
            position_side,
            position_amt,
            entry_price,
            unrealized_profit,
            leverage,
            isolated_wallet,
            mark_price,
            liquidation_price,
            timestamp,
        }
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Position(symbol={}, side={:?}, amt={:.4}, entry={:.2}, pnl={:.2})",
            self.symbol, self.position_side, self.position_amt, self.entry_price, self.unrealized_profit
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_side() {
        let trade = Trade::new(
            "BTCUSDT".to_string(),
            123456,
            50000.0,
            1.5,
            75000.0,
            1234567890,
            true,
            1234567890,
            123450,
            123460,
        );
        assert_eq!(trade.side(), OrderSide::Sell);
    }

    #[test]
    fn test_book_level_notional() {
        let level = BookLevel::new(50000.0, 2.0);
        assert_eq!(level.notional(), 100000.0);
    }

    #[test]
    fn test_orderbook_mid_price() {
        let snapshot = OrderBookSnapshot::new(
            "BTCUSDT".to_string(),
            123,
            vec![BookLevel::new(50000.0, 1.0)],
            vec![BookLevel::new(50010.0, 1.0)],
            1234567890,
            None,
        );
        assert_eq!(snapshot.mid_price(), Some(50005.0));
        assert_eq!(snapshot.spread(), Some(10.0));
    }

    #[test]
    fn test_display_traits() {
        assert_eq!(format!("{}", OrderSide::Buy), "BUY");
        assert_eq!(format!("{}", OrderSide::Sell), "SELL");
        assert_eq!(format!("{}", Direction::Long), "Long");
    }

    #[test]
    fn test_order_side_from_str() {
        use std::str::FromStr;

        assert_eq!(OrderSide::from_str("BUY").unwrap(), OrderSide::Buy);
        assert_eq!(OrderSide::from_str("buy").unwrap(), OrderSide::Buy);
        assert_eq!(OrderSide::from_str("SELL").unwrap(), OrderSide::Sell);
        assert_eq!(OrderSide::from_str("sell").unwrap(), OrderSide::Sell);
        assert!(OrderSide::from_str("INVALID").is_err());
    }
}
