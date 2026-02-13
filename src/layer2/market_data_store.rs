// Market Data Store - Stores latest data for non-depth streams
// Thread-safe storage for aggTrade, kline, markPrice, liquidation, bookTicker

use parking_lot::RwLock;
use std::collections::VecDeque;
use std::fmt;

use crate::layer2::parser::{
    ParsedAggTrade, ParsedKline, ParsedMarkPrice, ParsedLiquidation, ParsedBookTicker,
};

/// Statistics snapshot from the market data store
#[derive(Debug, Clone)]
pub struct MarketDataStoreStats {
    pub agg_trade_count: u64,
    pub kline_count: u64,
    pub mark_price_count: u64,
    pub liquidation_count: u64,
    pub book_ticker_count: u64,
    pub total_count: u64,
    // Latest value snapshots
    pub last_trade_price: Option<f64>,
    pub last_trade_qty: Option<f64>,
    pub last_trade_is_buyer_maker: Option<bool>,
    pub last_mark_price: Option<f64>,
    pub last_funding_rate: Option<f64>,
    pub last_kline_close: Option<f64>,
    pub last_book_ticker_mid: Option<f64>,
    pub last_book_ticker_spread_bps: Option<f64>,
    pub recent_liquidation_count: usize,
}

impl fmt::Display for MarketDataStoreStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MarketData(trades={}, klines={}, mark={}, liqs={}, tickers={}, total={})",
            self.agg_trade_count, self.kline_count, self.mark_price_count,
            self.liquidation_count, self.book_ticker_count, self.total_count
        )
    }
}

/// Thread-safe store for non-depth market data streams
pub struct MarketDataStore {
    pub symbol: String,

    // Latest values
    latest_agg_trade: RwLock<Option<ParsedAggTrade>>,
    latest_kline: RwLock<Option<ParsedKline>>,
    latest_mark_price: RwLock<Option<ParsedMarkPrice>>,
    latest_book_ticker: RwLock<Option<ParsedBookTicker>>,

    // Liquidation history (keep last N for analysis)
    liquidation_history: RwLock<VecDeque<ParsedLiquidation>>,
    max_liquidation_history: usize,

    // Per-type counts
    agg_trade_count: RwLock<u64>,
    kline_count: RwLock<u64>,
    mark_price_count: RwLock<u64>,
    liquidation_count: RwLock<u64>,
    book_ticker_count: RwLock<u64>,
}

impl MarketDataStore {
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_uppercase(),
            latest_agg_trade: RwLock::new(None),
            latest_kline: RwLock::new(None),
            latest_mark_price: RwLock::new(None),
            latest_book_ticker: RwLock::new(None),
            liquidation_history: RwLock::new(VecDeque::with_capacity(100)),
            max_liquidation_history: 100,
            agg_trade_count: RwLock::new(0),
            kline_count: RwLock::new(0),
            mark_price_count: RwLock::new(0),
            liquidation_count: RwLock::new(0),
            book_ticker_count: RwLock::new(0),
        }
    }

    pub fn record_agg_trade(&self, trade: ParsedAggTrade) {
        *self.latest_agg_trade.write() = Some(trade);
        *self.agg_trade_count.write() += 1;
    }

    pub fn record_kline(&self, kline: ParsedKline) {
        *self.latest_kline.write() = Some(kline);
        *self.kline_count.write() += 1;
    }

    pub fn record_mark_price(&self, mp: ParsedMarkPrice) {
        *self.latest_mark_price.write() = Some(mp);
        *self.mark_price_count.write() += 1;
    }

    pub fn record_liquidation(&self, liq: ParsedLiquidation) {
        let mut history = self.liquidation_history.write();
        history.push_back(liq);
        if history.len() > self.max_liquidation_history {
            history.pop_front();
        }
        *self.liquidation_count.write() += 1;
    }

    pub fn record_book_ticker(&self, bt: ParsedBookTicker) {
        *self.latest_book_ticker.write() = Some(bt);
        *self.book_ticker_count.write() += 1;
    }

    // Accessors

    pub fn latest_agg_trade(&self) -> Option<ParsedAggTrade> {
        self.latest_agg_trade.read().clone()
    }

    pub fn latest_kline(&self) -> Option<ParsedKline> {
        self.latest_kline.read().clone()
    }

    pub fn latest_mark_price(&self) -> Option<ParsedMarkPrice> {
        self.latest_mark_price.read().clone()
    }

    pub fn latest_book_ticker(&self) -> Option<ParsedBookTicker> {
        self.latest_book_ticker.read().clone()
    }

    pub fn recent_liquidations(&self) -> Vec<ParsedLiquidation> {
        self.liquidation_history.read().iter().cloned().collect()
    }

    pub fn get_stats(&self) -> MarketDataStoreStats {
        let agg = *self.agg_trade_count.read();
        let kline = *self.kline_count.read();
        let mark = *self.mark_price_count.read();
        let liq = *self.liquidation_count.read();
        let ticker = *self.book_ticker_count.read();

        let last_trade = self.latest_agg_trade.read();
        let last_mp = self.latest_mark_price.read();
        let last_kl = self.latest_kline.read();
        let last_bt = self.latest_book_ticker.read();

        MarketDataStoreStats {
            agg_trade_count: agg,
            kline_count: kline,
            mark_price_count: mark,
            liquidation_count: liq,
            book_ticker_count: ticker,
            total_count: agg + kline + mark + liq + ticker,
            last_trade_price: last_trade.as_ref().map(|t| t.price),
            last_trade_qty: last_trade.as_ref().map(|t| t.quantity),
            last_trade_is_buyer_maker: last_trade.as_ref().map(|t| t.is_buyer_maker),
            last_mark_price: last_mp.as_ref().map(|m| m.mark_price),
            last_funding_rate: last_mp.as_ref().map(|m| m.funding_rate),
            last_kline_close: last_kl.as_ref().map(|k| k.close),
            last_book_ticker_mid: last_bt.as_ref().map(|b| b.mid_price()),
            last_book_ticker_spread_bps: last_bt.as_ref().map(|b| b.spread_bps()),
            recent_liquidation_count: self.liquidation_history.read().len(),
        }
    }
}

impl fmt::Display for MarketDataStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let stats = self.get_stats();
        write!(f, "MarketDataStore({}, {})", self.symbol, stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_trade(price: f64, qty: f64) -> ParsedAggTrade {
        ParsedAggTrade {
            symbol: "BTCUSDT".to_string(),
            agg_trade_id: 1,
            price,
            quantity: qty,
            first_trade_id: 1,
            last_trade_id: 1,
            timestamp: 1000,
            is_buyer_maker: false,
            event_time: 1000,
        }
    }

    fn make_kline(close: f64) -> ParsedKline {
        ParsedKline {
            symbol: "BTCUSDT".to_string(),
            interval: "1m".to_string(),
            open_time: 1000,
            close_time: 2000,
            open: 95000.0,
            high: 95100.0,
            low: 94900.0,
            close,
            volume: 100.0,
            quote_volume: 9500000.0,
            trades: 500,
            taker_buy_volume: 50.0,
            taker_buy_quote_volume: 4750000.0,
            is_closed: false,
            event_time: 1500,
        }
    }

    fn make_mark_price(price: f64, rate: f64) -> ParsedMarkPrice {
        ParsedMarkPrice {
            symbol: "BTCUSDT".to_string(),
            mark_price: price,
            index_price: price - 1.0,
            estimated_settle_price: price,
            funding_rate: rate,
            next_funding_time: 2000,
            timestamp: 1000,
        }
    }

    fn make_liquidation(side: &str, price: f64, qty: f64) -> ParsedLiquidation {
        ParsedLiquidation {
            symbol: "BTCUSDT".to_string(),
            side: side.to_string(),
            order_type: "LIMIT".to_string(),
            time_in_force: "IOC".to_string(),
            quantity: qty,
            price,
            average_price: price,
            order_status: "FILLED".to_string(),
            last_filled_quantity: qty,
            filled_accumulated_quantity: qty,
            timestamp: 1000,
            event_time: 1000,
        }
    }

    fn make_book_ticker(bid: f64, ask: f64) -> ParsedBookTicker {
        ParsedBookTicker {
            symbol: "BTCUSDT".to_string(),
            update_id: 1,
            best_bid_price: bid,
            best_bid_qty: 1.0,
            best_ask_price: ask,
            best_ask_qty: 1.0,
            timestamp: 1000,
        }
    }

    #[test]
    fn test_store_creation() {
        let store = MarketDataStore::new("BTCUSDT");
        assert_eq!(store.symbol, "BTCUSDT");
        let stats = store.get_stats();
        assert_eq!(stats.total_count, 0);
    }

    #[test]
    fn test_record_agg_trade() {
        let store = MarketDataStore::new("BTCUSDT");
        store.record_agg_trade(make_trade(95000.0, 0.5));
        store.record_agg_trade(make_trade(95100.0, 1.0));

        let stats = store.get_stats();
        assert_eq!(stats.agg_trade_count, 2);
        assert_eq!(stats.last_trade_price, Some(95100.0));
        assert_eq!(stats.last_trade_qty, Some(1.0));
    }

    #[test]
    fn test_record_kline() {
        let store = MarketDataStore::new("BTCUSDT");
        store.record_kline(make_kline(95050.0));

        let stats = store.get_stats();
        assert_eq!(stats.kline_count, 1);
        assert_eq!(stats.last_kline_close, Some(95050.0));
    }

    #[test]
    fn test_record_mark_price() {
        let store = MarketDataStore::new("BTCUSDT");
        store.record_mark_price(make_mark_price(95000.0, 0.0001));

        let stats = store.get_stats();
        assert_eq!(stats.mark_price_count, 1);
        assert_eq!(stats.last_mark_price, Some(95000.0));
        assert_eq!(stats.last_funding_rate, Some(0.0001));
    }

    #[test]
    fn test_record_book_ticker() {
        let store = MarketDataStore::new("BTCUSDT");
        store.record_book_ticker(make_book_ticker(95000.0, 95001.0));

        let stats = store.get_stats();
        assert_eq!(stats.book_ticker_count, 1);
        assert_eq!(stats.last_book_ticker_mid, Some(95000.5));
        assert!(stats.last_book_ticker_spread_bps.unwrap() > 0.0);
    }

    #[test]
    fn test_liquidation_history_capping() {
        let store = MarketDataStore::new("BTCUSDT");

        // Add 110 liquidations (cap is 100)
        for i in 0..110 {
            store.record_liquidation(make_liquidation("SELL", 95000.0 + i as f64, 0.1));
        }

        let stats = store.get_stats();
        assert_eq!(stats.liquidation_count, 110); // Total count tracks all
        assert_eq!(stats.recent_liquidation_count, 100); // History capped at 100

        let recent = store.recent_liquidations();
        assert_eq!(recent.len(), 100);
        // First item should be from i=10 (first 10 were evicted)
        assert!((recent[0].price - 95010.0).abs() < 0.01);
    }

    #[test]
    fn test_total_count() {
        let store = MarketDataStore::new("BTCUSDT");
        store.record_agg_trade(make_trade(95000.0, 1.0));
        store.record_kline(make_kline(95000.0));
        store.record_mark_price(make_mark_price(95000.0, 0.0001));
        store.record_liquidation(make_liquidation("BUY", 95000.0, 0.5));
        store.record_book_ticker(make_book_ticker(95000.0, 95001.0));

        let stats = store.get_stats();
        assert_eq!(stats.total_count, 5);
    }
}
