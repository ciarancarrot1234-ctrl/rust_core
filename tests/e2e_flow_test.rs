// End-to-End Flow Tests for Liquidity Hunt
//
// These tests exercise the full data pipeline without network connections:
//   Core types → Layer 2 (Parser → OrderBook) → EventBus → Layer 3 (TradeAggregator)
//
// Run with: cargo test --test e2e_flow_test

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use liquidity_hunt::core::{
    Event, EventBus, EventPriority,
    AggregatorThresholds,
};
use liquidity_hunt::layer2::{
    MessageParser, ParsedMessage,
    OrderBook, SnapshotData, DiffUpdate,
};
use liquidity_hunt::layer2::parser::PriceLevel;

// ============================================================================
// Helpers
// ============================================================================

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Build a valid aggTrade JSON for BTCUSDT at the given price.
fn make_agg_trade_json(price: f64, qty: f64, is_buyer_maker: bool, trade_id: i64) -> String {
    let ts = now_ms();
    format!(
        r#"{{"e":"aggTrade","E":{ts},"s":"BTCUSDT","a":{trade_id},"p":"{price}","q":"{qty}","f":{trade_id},"l":{trade_id},"T":{ts},"m":{maker}}}"#,
        ts = ts,
        trade_id = trade_id,
        price = price,
        qty = qty,
        maker = if is_buyer_maker { "true" } else { "false" },
    )
}

/// Build a valid markPriceUpdate JSON.
fn make_mark_price_json(price: f64, funding_rate: f64) -> String {
    let ts = now_ms();
    format!(
        r#"{{"e":"markPriceUpdate","E":{ts},"s":"BTCUSDT","p":"{price}","i":"{price}","P":"{price}","r":"{fr}","T":{next}}}"#,
        ts = ts,
        price = price,
        fr = funding_rate,
        next = ts + 28_800_000,
    )
}

/// Build a valid kline JSON.
fn make_kline_json(open: f64, high: f64, low: f64, close: f64, volume: f64, closed: bool) -> String {
    let ts = now_ms();
    format!(
        r#"{{"e":"kline","E":{ts},"s":"BTCUSDT","k":{{"t":{open_t},"T":{close_t},"s":"BTCUSDT","i":"1m","f":1,"L":2,"o":"{open}","c":"{close}","h":"{high}","l":"{low}","v":"{vol}","n":10,"x":{closed},"q":"{qvol}","V":"{tvol}","Q":"{tqvol}","B":"0"}}}}"#,
        ts = ts,
        open_t = ts - 60_000,
        close_t = ts,
        open = open,
        close = close,
        high = high,
        low = low,
        vol = volume,
        closed = closed,
        qvol = close * volume,
        tvol = volume / 2.0,
        tqvol = close * volume / 2.0,
    )
}

/// Build a forceOrder (liquidation) JSON.
fn make_liquidation_json(side: &str, price: f64, qty: f64) -> String {
    let ts = now_ms();
    format!(
        r#"{{"e":"forceOrder","E":{ts},"o":{{"s":"BTCUSDT","S":"{side}","o":"LIMIT","f":"IOC","q":"{qty}","p":"{price}","ap":"{price}","X":"FILLED","l":"{qty}","z":"{qty}","T":{ts}}}}}"#,
        ts = ts,
        side = side,
        price = price,
        qty = qty,
    )
}

// ============================================================================
// TEST 1 – Core: Event creation and EventBus pub/sub
// ============================================================================

#[test]
fn test_core_event_bus_pubsub() {
    let bus = EventBus::new();
    let received: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));

    let recv_clone = received.clone();
    bus.subscribe("test_event", move |evt| {
        recv_clone.lock().unwrap().push(evt);
    });

    // Publish 3 events
    for i in 0..3 {
        let mut data = HashMap::new();
        data.insert("index".to_string(), serde_json::json!(i));
        bus.publish(Event::new(
            "test_event".to_string(),
            now_ms() as i64,
            data,
            "test".to_string(),
            EventPriority::Medium,
        ));
    }

    // Callbacks are synchronous in publish(), so check immediately
    let events = received.lock().unwrap();
    assert_eq!(events.len(), 3, "Should receive exactly 3 events");
    assert_eq!(events[0].event_type, "test_event");
    assert_eq!(events[0].source, "test");
}

#[test]
fn test_core_event_bus_wildcard_subscriber() {
    let bus = EventBus::new();
    let count: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    let count_clone = count.clone();
    bus.subscribe("*", move |_| {
        *count_clone.lock().unwrap() += 1;
    });

    for event_type in &["agg_trade", "depth_update", "kline"] {
        bus.publish(Event::new(
            event_type.to_string(),
            now_ms() as i64,
            HashMap::new(),
            "test".to_string(),
            EventPriority::Info,
        ));
    }

    assert_eq!(*count.lock().unwrap(), 3, "Wildcard should receive all 3 events");
}

#[test]
fn test_core_event_bus_history() {
    let bus = EventBus::new();

    for i in 0..5 {
        let mut data = HashMap::new();
        data.insert("i".to_string(), serde_json::json!(i));
        bus.publish(Event::new(
            "hist_event".to_string(),
            i,
            data,
            "src".to_string(),
            EventPriority::Low,
        ));
    }

    let history = bus.get_recent_events(Some("hist_event"), Some(10));
    assert_eq!(history.len(), 5);

    let all = bus.get_recent_events(None, Some(100));
    assert_eq!(all.len(), 5);
}

// ============================================================================
// TEST 2 – Layer 2: MessageParser parses all message types
// ============================================================================

#[test]
fn test_parser_agg_trade() {
    let mut parser = MessageParser::with_config("BTCUSDT", 60_000.0, 1000.0, 1_000_000.0);
    let json = make_agg_trade_json(42_000.0, 1.5, false, 100);

    let result = parser.parse(&json).expect("Should parse agg trade");
    match result {
        ParsedMessage::AggTrade(t) => {
            assert_eq!(t.symbol, "BTCUSDT");
            assert!((t.price - 42_000.0).abs() < 0.01);
            assert!((t.quantity - 1.5).abs() < 0.0001);
            assert!(!t.is_buyer_maker);
            assert_eq!(t.side(), "BUY");
            assert!((t.notional_usd() - 63_000.0).abs() < 1.0);
        }
        _ => panic!("Expected AggTrade variant"),
    }

    assert_eq!(parser.stats.messages_parsed, 1);
    assert_eq!(parser.stats.agg_trade_count, 1);
}

#[test]
fn test_parser_agg_trade_buyer_maker_is_sell() {
    let mut parser = MessageParser::with_config("BTCUSDT", 60_000.0, 1000.0, 1_000_000.0);
    let json = make_agg_trade_json(42_000.0, 0.5, true, 200);

    let result = parser.parse(&json).expect("Should parse");
    if let ParsedMessage::AggTrade(t) = result {
        assert!(t.is_buyer_maker);
        assert_eq!(t.side(), "SELL");
    } else {
        panic!("Expected AggTrade");
    }
}

#[test]
fn test_parser_duplicate_trade_rejected() {
    let mut parser = MessageParser::with_config("BTCUSDT", 60_000.0, 1000.0, 1_000_000.0);
    let json = make_agg_trade_json(42_000.0, 1.0, false, 300);

    let first = parser.parse(&json);
    assert!(first.is_ok(), "First parse should succeed");

    let second = parser.parse(&json);
    assert!(second.is_err(), "Duplicate should be rejected");
    assert_eq!(parser.stats.duplicate_messages, 1);
}

#[test]
fn test_parser_price_out_of_bounds_rejected() {
    let mut parser = MessageParser::with_config("BTCUSDT", 60_000.0, 100_000.0, 200_000.0);
    let json = make_agg_trade_json(42_000.0, 1.0, false, 400); // price below min

    let result = parser.parse(&json);
    assert!(result.is_err(), "Out-of-bounds price should fail validation");
    assert_eq!(parser.stats.validation_failures, 1);
}

#[test]
fn test_parser_mark_price() {
    let mut parser = MessageParser::with_config("BTCUSDT", 60_000.0, 1000.0, 1_000_000.0);
    let json = make_mark_price_json(42_500.0, 0.0001);

    let result = parser.parse(&json).expect("Should parse mark price");
    match result {
        ParsedMessage::MarkPrice(mp) => {
            assert_eq!(mp.symbol, "BTCUSDT");
            assert!((mp.mark_price - 42_500.0).abs() < 0.01);
            assert!((mp.funding_rate - 0.0001).abs() < 0.000001);
        }
        _ => panic!("Expected MarkPrice variant"),
    }
    assert_eq!(parser.stats.mark_price_count, 1);
}

#[test]
fn test_parser_kline() {
    let mut parser = MessageParser::with_config("BTCUSDT", 60_000.0, 1000.0, 1_000_000.0);
    let json = make_kline_json(41_000.0, 43_000.0, 40_500.0, 42_500.0, 100.0, true);

    let result = parser.parse(&json).expect("Should parse kline");
    match result {
        ParsedMessage::Kline(k) => {
            assert_eq!(k.symbol, "BTCUSDT");
            assert!((k.open - 41_000.0).abs() < 0.01);
            assert!((k.high - 43_000.0).abs() < 0.01);
            assert!((k.low - 40_500.0).abs() < 0.01);
            assert!((k.close - 42_500.0).abs() < 0.01);
            assert!(k.is_closed);
        }
        _ => panic!("Expected Kline variant"),
    }
    assert_eq!(parser.stats.kline_count, 1);
}

#[test]
fn test_parser_liquidation() {
    let mut parser = MessageParser::with_config("BTCUSDT", 60_000.0, 1000.0, 1_000_000.0);
    let json = make_liquidation_json("SELL", 41_000.0, 2.5);

    let result = parser.parse(&json).expect("Should parse liquidation");
    match result {
        ParsedMessage::Liquidation(liq) => {
            assert_eq!(liq.symbol, "BTCUSDT");
            assert_eq!(liq.side, "SELL");
            assert!((liq.price - 41_000.0).abs() < 0.01);
            assert!((liq.notional_usd() - 102_500.0).abs() < 1.0);
        }
        _ => panic!("Expected Liquidation variant"),
    }
    assert_eq!(parser.stats.liquidation_count, 1);
}

#[test]
fn test_parser_depth_snapshot() {
    let mut parser = MessageParser::with_config("BTCUSDT", 60_000.0, 1000.0, 1_000_000.0);
    let json = r#"{"lastUpdateId":12345,"bids":[["42000.0","1.5"],["41999.0","2.0"]],"asks":[["42001.0","1.0"],["42002.0","3.0"]]}"#;

    let result = parser.parse(json).expect("Should parse depth snapshot");
    match result {
        ParsedMessage::DepthSnapshot(snap) => {
            assert_eq!(snap.last_update_id, 12345);
            assert_eq!(snap.bids.len(), 2);
            assert_eq!(snap.asks.len(), 2);
            assert!((snap.bids[0].price - 42_000.0).abs() < 0.01);
        }
        _ => panic!("Expected DepthSnapshot variant"),
    }
}

#[test]
fn test_parser_symbol_mismatch_rejected() {
    let mut parser = MessageParser::with_config("BTCUSDT", 60_000.0, 1000.0, 1_000_000.0);
    let ts = now_ms();
    let json = format!(
        r#"{{"e":"aggTrade","E":{ts},"s":"ETHUSDT","a":999,"p":"3000.0","q":"1.0","f":999,"l":999,"T":{ts},"m":false}}"#,
        ts = ts,
    );

    let result = parser.parse(&json);
    assert!(result.is_err(), "Wrong symbol should fail");
}

// ============================================================================
// TEST 3 – Layer 2: OrderBook snapshot and differential updates
// ============================================================================

#[test]
fn test_orderbook_snapshot_mode() {
    let ob = OrderBook::new("BTCUSDT", "snapshot").expect("Should create order book");

    let snapshot = SnapshotData {
        bids: vec![
            PriceLevel::new(42_000.0, 1.0),
            PriceLevel::new(41_999.0, 2.0),
            PriceLevel::new(41_998.0, 3.0),
        ],
        asks: vec![
            PriceLevel::new(42_001.0, 1.5),
            PriceLevel::new(42_002.0, 2.5),
        ],
        last_update_id: 1000,
    };

    let state = ob.initialize_from_snapshot(snapshot).expect("Should initialize");

    assert!(state.is_synced);
    assert!((state.best_bid - 42_000.0).abs() < 0.01);
    assert!((state.best_ask - 42_001.0).abs() < 0.01);
    assert!((state.mid_price - 42_000.5).abs() < 0.1);
    assert!(state.spread > 0.0);
    assert!(state.total_bid_volume > 0.0);
    assert!(state.total_ask_volume > 0.0);
    assert_eq!(state.last_update_id, 1000);

    let summary = ob.get_summary();
    assert!(summary.is_initialized);
    assert_eq!(summary.bid_levels, 3);
    assert_eq!(summary.ask_levels, 2);
}

#[test]
fn test_orderbook_differential_mode() {
    let ob = OrderBook::new("BTCUSDT", "differential").expect("Should create order book");

    // Initialize with snapshot first
    let snapshot = SnapshotData {
        bids: vec![
            PriceLevel::new(42_000.0, 1.0),
            PriceLevel::new(41_999.0, 2.0),
        ],
        asks: vec![
            PriceLevel::new(42_001.0, 1.5),
        ],
        last_update_id: 500,
    };
    ob.initialize_from_snapshot(snapshot).expect("Should initialize");

    // Apply diff: add a bid, remove ask, add new ask
    let diff = DiffUpdate {
        bids: vec![
            PriceLevel::new(41_998.0, 5.0), // add new level
            PriceLevel::new(41_999.0, 0.0), // remove existing level
        ],
        asks: vec![
            PriceLevel::new(42_001.0, 0.0), // remove
            PriceLevel::new(42_002.0, 3.0), // add new level
        ],
        final_update_id: 501,
        first_update_id: Some(501),
    };

    let state = ob.process_differential(diff).expect("Should process diff");
    let state = state.expect("Should return state");

    assert!((state.best_bid - 42_000.0).abs() < 0.01, "Best bid should still be 42000");
    assert!((state.best_ask - 42_002.0).abs() < 0.01, "Best ask should now be 42002");
    assert_eq!(state.last_update_id, 501);
}

#[test]
fn test_orderbook_invalid_mode() {
    let result = OrderBook::new("BTCUSDT", "invalid_mode");
    assert!(result.is_err(), "Invalid mode should return error");
}

#[test]
fn test_orderbook_spread_calculation() {
    let ob = OrderBook::new("BTCUSDT", "snapshot").expect("Should create order book");
    let snapshot = SnapshotData {
        bids: vec![PriceLevel::new(40_000.0, 1.0)],
        asks: vec![PriceLevel::new(40_004.0, 1.0)],
        last_update_id: 1,
    };
    let state = ob.initialize_from_snapshot(snapshot).expect("Should initialize");

    // spread = 4, mid = 40002, spread_bps = 4/40002 * 10000 ≈ 1.0 bps
    assert!((state.spread - 4.0).abs() < 0.01);
    assert!(state.spread_bps > 0.0 && state.spread_bps < 5.0);
}

// ============================================================================
// TEST 4 – EventBus + Parser integration: parse → publish → receive
// ============================================================================

#[test]
fn test_parse_and_publish_to_event_bus() {
    let bus = EventBus::new();
    let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let recv_clone = received.clone();
    bus.subscribe("agg_trade", move |evt| {
        recv_clone.lock().unwrap().push(evt.event_type.clone());
    });

    let mut parser = MessageParser::with_config("BTCUSDT", 60_000.0, 1000.0, 1_000_000.0);
    let json = make_agg_trade_json(42_000.0, 1.0, false, 1000);

    let result = parser.parse(&json).expect("Parse should succeed");

    if let ParsedMessage::AggTrade(trade) = result {
        let mut data = HashMap::new();
        data.insert("data".to_string(), serde_json::to_value(&trade).unwrap());
        bus.publish(Event::new(
            "agg_trade".to_string(),
            trade.event_time as i64,
            data,
            "pipeline".to_string(),
            EventPriority::Medium,
        ));
    }

    let events = received.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], "agg_trade");
}

// ============================================================================
// TEST 5 – Full pipeline: parse multiple messages → publish all → verify bus
// ============================================================================

#[test]
fn test_full_pipeline_multiple_message_types() {
    let bus = EventBus::new();

    let event_log: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new())); // (type, source)
    let log_clone = event_log.clone();
    bus.subscribe("*", move |evt| {
        log_clone.lock().unwrap().push((evt.event_type.clone(), evt.source.clone()));
    });

    let mut parser = MessageParser::with_config("BTCUSDT", 60_000.0, 1000.0, 1_000_000.0);

    let messages: Vec<(&str, String)> = vec![
        ("agg_trade",   make_agg_trade_json(42_000.0, 1.0, false, 2001)),
        ("agg_trade",   make_agg_trade_json(42_100.0, 0.5, true,  2002)),
        ("mark_price",  make_mark_price_json(42_050.0, 0.0001)),
        ("kline",       make_kline_json(41_800.0, 42_200.0, 41_700.0, 42_100.0, 50.0, false)),
        ("liquidation", make_liquidation_json("BUY", 41_500.0, 1.5)),
    ];

    for (event_type, json) in &messages {
        match parser.parse(json) {
            Ok(msg) => {
                let (bus_event_type, data) = match msg {
                    ParsedMessage::AggTrade(ref t) => (
                        "agg_trade",
                        {
                            let mut d = HashMap::new();
                            d.insert("data".to_string(), serde_json::to_value(t).unwrap());
                            d
                        }
                    ),
                    ParsedMessage::MarkPrice(ref mp) => (
                        "mark_price",
                        {
                            let mut d = HashMap::new();
                            d.insert("data".to_string(), serde_json::to_value(mp).unwrap());
                            d
                        }
                    ),
                    ParsedMessage::Kline(ref k) => (
                        "kline",
                        {
                            let mut d = HashMap::new();
                            d.insert("data".to_string(), serde_json::to_value(k).unwrap());
                            d
                        }
                    ),
                    ParsedMessage::Liquidation(ref liq) => (
                        "liquidation",
                        {
                            let mut d = HashMap::new();
                            d.insert("data".to_string(), serde_json::to_value(liq).unwrap());
                            d
                        }
                    ),
                    _ => continue,
                };

                bus.publish(Event::new(
                    bus_event_type.to_string(),
                    now_ms() as i64,
                    data,
                    "pipeline".to_string(),
                    EventPriority::Medium,
                ));
            }
            Err(e) => panic!("Unexpected parse error for {}: {:?}", event_type, e),
        }
    }

    let log = event_log.lock().unwrap();
    assert_eq!(log.len(), 5, "All 5 messages should reach the bus");

    let types: Vec<&str> = log.iter().map(|(t, _)| t.as_str()).collect();
    assert_eq!(types[0], "agg_trade");
    assert_eq!(types[1], "agg_trade");
    assert_eq!(types[2], "mark_price");
    assert_eq!(types[3], "kline");
    assert_eq!(types[4], "liquidation");

    // All should come from pipeline
    assert!(log.iter().all(|(_, src)| src == "pipeline"));

    // Parser stats
    assert_eq!(parser.stats.agg_trade_count, 2);
    assert_eq!(parser.stats.mark_price_count, 1);
    assert_eq!(parser.stats.kline_count, 1);
    assert_eq!(parser.stats.liquidation_count, 1);
    assert_eq!(parser.stats.messages_parsed, 5);
    assert_eq!(parser.stats.parse_errors, 0);
}

// ============================================================================
// TEST 6 – Layer 3: TradeAggregator subscribes and processes agg_trade events
// ============================================================================

#[test]
fn test_trade_aggregator_receives_events_via_bus() {
    use liquidity_hunt::layer3::TradeAggregator;
    use liquidity_hunt::core::get_event_bus;

    // Use the global event bus (TradeAggregator subscribes to it internally)
    let bus = get_event_bus();

    let thresholds = AggregatorThresholds::default();
    let _aggregator = TradeAggregator::new("BTCUSDT".to_string(), thresholds);

    // Give the background thread a moment to start
    std::thread::sleep(std::time::Duration::from_millis(50));

    let mut parser = MessageParser::with_config("BTCUSDT", 60_000.0, 1000.0, 1_000_000.0);

    // Publish 10 trades via the global bus
    for i in 0..10 {
        let json = make_agg_trade_json(42_000.0 + i as f64 * 10.0, 1.0, i % 2 == 0, 5000 + i);
        let msg = parser.parse(&json).expect("Parse should succeed");

        if let ParsedMessage::AggTrade(trade) = msg {
            let mut data = HashMap::new();
            data.insert("data".to_string(), serde_json::to_value(&trade).unwrap());
            bus.publish(Event::new(
                "agg_trade".to_string(),
                trade.event_time as i64,
                data,
                "pipeline".to_string(),
                EventPriority::Medium,
            ));
        }
    }

    // Allow background thread to process
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Verify the events were stored in history
    let history = bus.get_recent_events(Some("agg_trade"), Some(20));
    assert!(history.len() >= 10, "Bus should have at least 10 agg_trade events, got {}", history.len());
}

// ============================================================================
// TEST 7 – OrderBook + Parser integration: parse snapshot → apply diffs
// ============================================================================

#[test]
fn test_orderbook_parse_snapshot_then_diffs() {
    let ob = OrderBook::new("BTCUSDT", "differential").expect("Order book creation");

    // Simulate receiving a REST snapshot
    let snapshot = SnapshotData {
        bids: vec![
            PriceLevel::new(42_000.0, 5.0),
            PriceLevel::new(41_990.0, 3.0),
            PriceLevel::new(41_980.0, 8.0),
        ],
        asks: vec![
            PriceLevel::new(42_010.0, 4.0),
            PriceLevel::new(42_020.0, 6.0),
            PriceLevel::new(42_030.0, 2.0),
        ],
        last_update_id: 10_000,
    };

    let state = ob.initialize_from_snapshot(snapshot).expect("Initialize");
    assert!(state.is_synced);
    assert_eq!(state.last_update_id, 10_000);

    // Apply a series of diffs (simulating WS stream)
    let diffs = vec![
        DiffUpdate {
            bids: vec![PriceLevel::new(42_000.0, 6.0)], // update qty
            asks: vec![PriceLevel::new(42_010.0, 3.5)], // update qty
            final_update_id: 10_001,
            first_update_id: Some(10_001),
        },
        DiffUpdate {
            bids: vec![PriceLevel::new(42_005.0, 2.0)], // add new level above best
            asks: vec![PriceLevel::new(42_010.0, 0.0)], // remove best ask
            final_update_id: 10_002,
            first_update_id: Some(10_002),
        },
        DiffUpdate {
            bids: vec![PriceLevel::new(41_980.0, 0.0)], // remove level
            asks: vec![PriceLevel::new(42_008.0, 1.0)], // insert between levels
            final_update_id: 10_003,
            first_update_id: Some(10_003),
        },
    ];

    let mut last_state = None;
    for diff in diffs {
        let update_id = diff.final_update_id;
        last_state = ob.process_differential(diff)
            .expect("Diff should succeed");
        assert!(last_state.is_some(), "Should return state for update {}", update_id);
    }

    let final_state = last_state.expect("Should have final state");
    // After all diffs: best bid should be 42_005 (new level inserted), best ask 42_008
    assert!((final_state.best_bid - 42_005.0).abs() < 0.01,
        "Expected best bid 42005, got {}", final_state.best_bid);
    assert!((final_state.best_ask - 42_008.0).abs() < 0.01,
        "Expected best ask 42008, got {}", final_state.best_ask);
    assert_eq!(final_state.last_update_id, 10_003);

    let summary = ob.get_summary();
    assert_eq!(summary.last_update_id, 10_003);
}

// ============================================================================
// TEST 8 – EventPriority ordering
// ============================================================================

#[test]
fn test_event_priority_ordering() {
    assert!(EventPriority::Critical < EventPriority::High);
    assert!(EventPriority::High < EventPriority::Medium);
    assert!(EventPriority::Medium < EventPriority::Low);
    assert!(EventPriority::Low < EventPriority::Info);
}

// ============================================================================
// TEST 9 – ParsedBookTicker helpers
// ============================================================================

#[test]
fn test_book_ticker_helpers() {
    let mut parser = MessageParser::with_config("BTCUSDT", 60_000.0, 1000.0, 1_000_000.0);
    let ts = now_ms();
    let json = format!(
        r#"{{"e":"bookTicker","u":9999,"s":"BTCUSDT","b":"42000.0","B":"1.5","a":"42002.0","A":"2.0","T":{ts},"E":{ts}}}"#,
        ts = ts,
    );

    let result = parser.parse(&json).expect("Should parse book ticker");
    if let ParsedMessage::BookTicker(bt) = result {
        assert!((bt.mid_price() - 42_001.0).abs() < 0.01);
        assert!((bt.spread() - 2.0).abs() < 0.01);
        assert!(bt.spread_bps() > 0.0);
    } else {
        panic!("Expected BookTicker");
    }
}

// ============================================================================
// TEST 10 – Parser stats accuracy across message types
// ============================================================================

#[test]
fn test_parser_stats_accuracy() {
    let mut parser = MessageParser::with_config("BTCUSDT", 60_000.0, 1000.0, 1_000_000.0);

    // Parse 3 trades (one duplicate)
    parser.parse(&make_agg_trade_json(42_000.0, 1.0, false, 7001)).ok();
    parser.parse(&make_agg_trade_json(42_100.0, 1.0, false, 7002)).ok();
    parser.parse(&make_agg_trade_json(42_100.0, 1.0, false, 7002)).ok(); // duplicate

    // Parse 2 mark prices
    parser.parse(&make_mark_price_json(42_000.0, 0.0001)).ok();
    parser.parse(&make_mark_price_json(42_100.0, 0.0002)).ok();

    // Parse 1 kline
    parser.parse(&make_kline_json(41_000.0, 43_000.0, 40_500.0, 42_500.0, 100.0, true)).ok();

    assert_eq!(parser.stats.agg_trade_count, 2, "2 unique trades");
    assert_eq!(parser.stats.mark_price_count, 2, "2 mark prices");
    assert_eq!(parser.stats.kline_count, 1, "1 kline");
    assert_eq!(parser.stats.duplicate_messages, 1, "1 duplicate");
    assert_eq!(parser.stats.messages_parsed, 5, "5 successfully parsed");
    assert_eq!(parser.stats.parse_errors, 0);
    assert!(parser.stats.error_rate() == 0.0);
    assert!(parser.stats.avg_latency_ms() >= 0.0);
}
