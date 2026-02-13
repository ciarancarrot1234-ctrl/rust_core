// Unified Metrics - Collects and formats stats from all pipeline components
// Aggregates existing per-component stats into a single view

use std::fmt;
use tracing::info;

use crate::layer1::websocket::WebSocketStats;
use crate::layer2::parser::ParserStats;
use crate::layer2::synchronizer::SynchronizerStats;
use crate::layer2::orderbook::OrderBookSummary;
use crate::layer2::market_data_store::MarketDataStoreStats;

/// Unified metrics snapshot from all pipeline components
#[derive(Debug, Clone)]
pub struct UnifiedMetrics {
    pub ws: Option<WebSocketStats>,
    pub parser: Option<ParserStats>,
    pub sync: Option<SynchronizerStats>,
    pub orderbook: Option<OrderBookSummary>,
    pub market_data: Option<MarketDataStoreStats>,
}

impl UnifiedMetrics {
    /// Create from component stats
    pub fn from_stats(
        ws: Option<WebSocketStats>,
        parser: Option<ParserStats>,
        sync: Option<SynchronizerStats>,
        orderbook: Option<OrderBookSummary>,
        market_data: Option<MarketDataStoreStats>,
    ) -> Self {
        Self { ws, parser, sync, orderbook, market_data }
    }

    /// Total messages received by WebSocket
    pub fn total_ws_messages(&self) -> u64 {
        self.ws.as_ref().map(|w| w.message_count).unwrap_or(0)
    }

    /// Total messages parsed
    pub fn total_parsed(&self) -> u64 {
        self.parser.as_ref().map(|p| p.messages_parsed).unwrap_or(0)
    }

    /// Total errors across all components
    pub fn total_errors(&self) -> u64 {
        let parse_errs = self.parser.as_ref().map(|p| p.parse_errors).unwrap_or(0);
        let ws_errs = self.ws.as_ref().map(|w| w.error_count).unwrap_or(0);
        let sync_dropped = self.sync.as_ref().map(|s| s.messages_dropped).unwrap_or(0);
        parse_errs + ws_errs + sync_dropped
    }

    /// Print detailed multi-line report
    pub fn print_report(&self) {
        info!("=== UNIFIED METRICS ===");

        // WebSocket
        if let Some(ws) = &self.ws {
            info!("  WebSocket:    msgs={} errs={} subs={} state={:?}",
                ws.message_count, ws.error_count, ws.active_subscriptions, ws.state);
        }

        // Parser
        if let Some(p) = &self.parser {
            info!("  Parser:       parsed={} errs={} avg_latency={:.1}ms",
                p.messages_parsed, p.parse_errors, p.avg_latency_ms());
            info!("                depth={} trades={} klines={} mark={} liqs={} tickers={}",
                p.depth_count, p.agg_trade_count, p.kline_count,
                p.mark_price_count, p.liquidation_count, p.book_ticker_count);
        }

        // Synchronizer
        if let Some(s) = &self.sync {
            info!("  Synchronizer: state={} passed={} gaps={} dropped={}",
                s.state, s.messages_passed, s.gap_count, s.messages_dropped);
        }

        // OrderBook
        if let Some(ob) = &self.orderbook {
            let mid = ob.best_bid.unwrap_or(0.0) / 2.0 + ob.best_ask.unwrap_or(0.0) / 2.0;
            let spread_bps = ob.spread_bps.unwrap_or(0.0);
            info!("  OrderBook:    mid={:.2} spread={:.2}bps bids={} asks={}",
                mid, spread_bps, ob.bid_levels, ob.ask_levels);
        }

        // Market Data
        if let Some(md) = &self.market_data {
            let trade_info = md.last_trade_price
                .map(|p| format!(" (last ${:.2})", p))
                .unwrap_or_default();
            let mark_info = md.last_mark_price
                .map(|p| format!(" mark=${:.2}", p))
                .unwrap_or_default();
            let funding_info = md.last_funding_rate
                .map(|r| format!(" funding={:.6}", r))
                .unwrap_or_default();
            info!("  Market Data:  trades={}{}{}{}",
                md.agg_trade_count, trade_info, mark_info, funding_info);
            info!("                klines={} tickers={} liqs={}",
                md.kline_count, md.book_ticker_count, md.liquidation_count);
        }
    }

    /// Print compact single-line stats for periodic logging
    pub fn print_compact(&self, elapsed_secs: u64) {
        let mid = self.orderbook.as_ref()
            .map(|ob| {
                let bid = ob.best_bid.unwrap_or(0.0);
                let ask = ob.best_ask.unwrap_or(0.0);
                (bid + ask) / 2.0
            })
            .unwrap_or(0.0);

        let spread = self.orderbook.as_ref()
            .and_then(|ob| ob.spread_bps)
            .unwrap_or(0.0);

        let depth = self.parser.as_ref().map(|p| p.depth_count).unwrap_or(0);
        let trades = self.parser.as_ref().map(|p| p.agg_trade_count).unwrap_or(0);
        let klines = self.parser.as_ref().map(|p| p.kline_count).unwrap_or(0);
        let mark = self.parser.as_ref().map(|p| p.mark_price_count).unwrap_or(0);
        let liqs = self.parser.as_ref().map(|p| p.liquidation_count).unwrap_or(0);
        let tickers = self.parser.as_ref().map(|p| p.book_ticker_count).unwrap_or(0);

        let gaps = self.sync.as_ref().map(|s| s.gap_count).unwrap_or(0);
        let errs = self.total_errors();
        let latency = self.parser.as_ref().map(|p| p.avg_latency_ms()).unwrap_or(0.0);

        info!("[{:>3}s] mid={:.2} sprd={:.2}bp | depth={} trade={} kline={} mark={} liq={} tick={} | gaps={} errs={} lat={:.1}ms",
            elapsed_secs, mid, spread,
            depth, trades, klines, mark, liqs, tickers,
            gaps, errs, latency);
    }
}

impl fmt::Display for UnifiedMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total = self.total_parsed();
        let errors = self.total_errors();
        write!(f, "UnifiedMetrics(parsed={}, errors={})", total, errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ConnectionStatus;

    #[test]
    fn test_empty_metrics() {
        let metrics = UnifiedMetrics::from_stats(None, None, None, None, None);
        assert_eq!(metrics.total_ws_messages(), 0);
        assert_eq!(metrics.total_parsed(), 0);
        assert_eq!(metrics.total_errors(), 0);
    }

    #[test]
    fn test_metrics_with_ws_stats() {
        let ws = WebSocketStats {
            state: ConnectionStatus::Connected,
            message_count: 1000,
            error_count: 2,
            active_subscriptions: 6,
        };
        let metrics = UnifiedMetrics::from_stats(Some(ws), None, None, None, None);
        assert_eq!(metrics.total_ws_messages(), 1000);
        assert_eq!(metrics.total_errors(), 2);
    }

    #[test]
    fn test_metrics_with_parser_stats() {
        let parser = ParserStats {
            messages_parsed: 500,
            parse_errors: 3,
            validation_failures: 0,
            duplicate_messages: 1,
            total_latency_ms: 1000.0,
            agg_trade_count: 200,
            depth_count: 250,
            kline_count: 10,
            mark_price_count: 20,
            liquidation_count: 0,
            book_ticker_count: 20,
        };
        let metrics = UnifiedMetrics::from_stats(None, Some(parser), None, None, None);
        assert_eq!(metrics.total_parsed(), 500);
        assert_eq!(metrics.total_errors(), 3); // parse errors only
    }

    #[test]
    fn test_metrics_total_errors() {
        let ws = WebSocketStats {
            state: ConnectionStatus::Connected,
            message_count: 100,
            error_count: 1,
            active_subscriptions: 1,
        };
        let parser = ParserStats {
            messages_parsed: 90,
            parse_errors: 2,
            ..Default::default()
        };
        let sync = SynchronizerStats {
            state: "Synchronized".to_string(),
            total_messages: 88,
            messages_passed: 85,
            messages_buffered: 0,
            messages_dropped: 3,
            gap_count: 0,
            small_gap_count: 0,
            gap_rate: 0.0,
        };
        let metrics = UnifiedMetrics::from_stats(Some(ws), Some(parser), Some(sync), None, None);
        // 1 ws err + 2 parse err + 3 sync dropped = 6
        assert_eq!(metrics.total_errors(), 6);
    }

    #[test]
    fn test_display() {
        let metrics = UnifiedMetrics::from_stats(None, None, None, None, None);
        let s = format!("{}", metrics);
        assert!(s.contains("parsed=0"));
        assert!(s.contains("errors=0"));
    }
}
