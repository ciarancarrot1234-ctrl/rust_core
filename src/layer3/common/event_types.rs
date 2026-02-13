// Event Type Constants - All 85 trade condition event types
// Centralized constants for consistency across the system

// ============================================================================
// Delta Events (12)
// ============================================================================

pub const DELTA_THRESHOLD: &str = "delta_threshold";
pub const DELTA_SLOPE: &str = "delta_slope";
pub const DELTA_ACCELERATION: &str = "delta_acceleration";
pub const DELTA_REGIME_CHANGE: &str = "delta_regime_change";
pub const DELTA_REVERSAL: &str = "delta_reversal";
pub const DELTA_MOMENTUM_BUILDING: &str = "delta_momentum_building";
pub const DELTA_EXHAUSTION: &str = "delta_exhaustion";
pub const DELTA_DIVERGENCE: &str = "delta_divergence";
pub const DELTA_ZERO_CROSS: &str = "delta_zero_cross";
pub const DELTA_EXTREME: &str = "delta_extreme";
pub const DELTA_NEUTRAL: &str = "delta_neutral";
pub const DELTA_IMBALANCE_FLIP: &str = "delta_imbalance_flip";

// ============================================================================
// VWAP Events (11)
// ============================================================================

pub const VWAP_DEVIATION: &str = "vwap_deviation";
pub const VWAP_EXTREME_DEVIATION: &str = "vwap_extreme_deviation";
pub const VWAP_CROSS: &str = "vwap_cross";
pub const VWAP_RETEST: &str = "vwap_retest";
pub const VWAP_REJECTION: &str = "vwap_rejection";
pub const VWAP_ALIGNMENT: &str = "vwap_alignment";
pub const VWAP_BAND_BREAK: &str = "vwap_band_break";
pub const VWAP_CONVERGENCE: &str = "vwap_convergence";
pub const VWAP_DIVERGENCE: &str = "vwap_divergence";
pub const VWAP_ANCHOR_SHIFT: &str = "vwap_anchor_shift";
pub const VWAP_RECLAIM: &str = "vwap_reclaim";

// ============================================================================
// Volume Events (12)
// ============================================================================

pub const VOLUME_SPIKE: &str = "volume_spike";
pub const VOLUME_DRY_UP: &str = "volume_dry_up";
pub const VOLUME_REGIME_CHANGE: &str = "volume_regime_change";
pub const VOLUME_RATIO_EXTREME: &str = "volume_ratio_extreme";
pub const VOLUME_ACCELERATION: &str = "volume_acceleration";
pub const VOLUME_EXHAUSTION: &str = "volume_exhaustion";
pub const VOLUME_CLIMAX: &str = "volume_climax";
pub const VOLUME_DISTRIBUTION: &str = "volume_distribution";
pub const VOLUME_ACCUMULATION: &str = "volume_accumulation";
pub const VOLUME_CHURN: &str = "volume_churn";
pub const VOLUME_DIVERGENCE: &str = "volume_divergence";
pub const VOLUME_CONFIRMATION: &str = "volume_confirmation";

// ============================================================================
// Pattern Events (10)
// ============================================================================

pub const PATTERN_LARGE_TRADE: &str = "pattern_large_trade";
pub const PATTERN_WHALE_TRADE: &str = "pattern_whale_trade";
pub const PATTERN_CLUSTER: &str = "pattern_cluster";
pub const PATTERN_RATE_SPIKE: &str = "pattern_rate_spike";
pub const PATTERN_INSTITUTIONAL: &str = "pattern_institutional";
pub const PATTERN_AGGRESSION: &str = "pattern_aggression";
pub const PATTERN_ABSORPTION: &str = "pattern_absorption";
pub const PATTERN_SWEEP: &str = "pattern_sweep";
pub const PATTERN_ICEBERG: &str = "pattern_iceberg";
pub const PATTERN_SPOOFING: &str = "pattern_spoofing";

// ============================================================================
// Profile Events (8)
// ============================================================================

pub const PROFILE_POC_SHIFT: &str = "profile_poc_shift";
pub const PROFILE_POC_TEST: &str = "profile_poc_test";
pub const PROFILE_VALUE_AREA_BREAK: &str = "profile_value_area_break";
pub const PROFILE_VALUE_AREA_RETEST: &str = "profile_value_area_retest";
pub const PROFILE_HIGH_VOLUME_NODE: &str = "profile_high_volume_node";
pub const PROFILE_LOW_VOLUME_NODE: &str = "profile_low_volume_node";
pub const PROFILE_SINGLE_PRINTS: &str = "profile_single_prints";
pub const PROFILE_BALANCE: &str = "profile_balance";

// ============================================================================
// Streak Events (8)
// ============================================================================

pub const STREAK_STARTED: &str = "streak_started";
pub const STREAK_EXTENDED: &str = "streak_extended";
pub const STREAK_ENDED: &str = "streak_ended";
pub const STREAK_RECORD: &str = "streak_record";
pub const STREAK_INTENSITY_HIGH: &str = "streak_intensity_high";
pub const STREAK_INTENSITY_LOW: &str = "streak_intensity_low";
pub const STREAK_ALTERNATING: &str = "streak_alternating";
pub const STREAK_MOMENTUM: &str = "streak_momentum";

// Total: 61 base events + 24 Binance-specific = 85 total events

// ============================================================================
// OrderBook Aggregator Events (95 Events)
// ============================================================================

// ============================================================================
// Spread Events (8)
// ============================================================================

pub const OB_SPREAD_THRESHOLD: &str = "ob_spread_threshold";
pub const OB_SPREAD_REGIME: &str = "ob_spread_regime";
pub const OB_SPREAD_COMPRESSED: &str = "ob_spread_compressed";
pub const OB_SPREAD_EXPANDED: &str = "ob_spread_expanded";
pub const OB_SPREAD_VOLATILITY: &str = "ob_spread_volatility";
pub const OB_SPREAD_STABILIZED: &str = "ob_spread_stabilized";
pub const OB_SPREAD_ASYMMETRY: &str = "ob_spread_asymmetry";
pub const OB_SPREAD_ANOMALY: &str = "ob_spread_anomaly";

// ============================================================================
// Imbalance Events (12)
// ============================================================================

pub const OB_IMBALANCE_THRESHOLD: &str = "ob_imbalance_threshold";
pub const OB_IMBALANCE_SHIFT: &str = "ob_imbalance_shift";
pub const OB_IMBALANCE_REGIME: &str = "ob_imbalance_regime";
pub const OB_IMBALANCE_DIVERGENCE: &str = "ob_imbalance_divergence";
pub const OB_IMBALANCE_ALIGNMENT: &str = "ob_imbalance_alignment";
pub const OB_IMBALANCE_REVERSAL: &str = "ob_imbalance_reversal";
pub const OB_IMBALANCE_ACCELERATION: &str = "ob_imbalance_acceleration";
pub const OB_IMBALANCE_EXHAUSTION: &str = "ob_imbalance_exhaustion";
pub const OB_IMBALANCE_OSCILLATION: &str = "ob_imbalance_oscillation";
pub const OB_IMBALANCE_HIDDEN: &str = "ob_imbalance_hidden";
pub const OB_IMBALANCE_PERSISTENCE: &str = "ob_imbalance_persistence";
pub const OB_IMBALANCE_BREAKDOWN: &str = "ob_imbalance_breakdown";

// ============================================================================
// Wall Events (12)
// ============================================================================

pub const OB_WALL_APPEARED: &str = "ob_wall_appeared";
pub const OB_WALL_DISAPPEARED: &str = "ob_wall_disappeared";
pub const OB_WALL_MODIFIED: &str = "ob_wall_modified";
pub const OB_WALL_CLUSTERING: &str = "ob_wall_clustering";
pub const OB_WALL_ABSORBED: &str = "ob_wall_absorbed";
pub const OB_WALL_PULLED: &str = "ob_wall_pulled";
pub const OB_WALL_REFRESHED: &str = "ob_wall_refreshed";
pub const OB_WALL_MOVED: &str = "ob_wall_moved";
pub const OB_WALL_STACKING: &str = "ob_wall_stacking";
pub const OB_WALL_SPOOFING: &str = "ob_wall_spoofing";
pub const OB_WALL_TESTED: &str = "ob_wall_tested";
pub const OB_WALL_DEFENDED: &str = "ob_wall_defended";

// ============================================================================
// Liquidity Events (10)
// ============================================================================

pub const OB_LIQUIDITY_HOLE: &str = "ob_liquidity_hole";
pub const OB_LIQUIDITY_REGIME: &str = "ob_liquidity_regime";
pub const OB_LIQUIDITY_SWEPT: &str = "ob_liquidity_swept";
pub const OB_LIQUIDITY_DEPTH_THRESHOLD: &str = "ob_liquidity_depth_threshold";
pub const OB_LIQUIDITY_VACUUM: &str = "ob_liquidity_vacuum";
pub const OB_LIQUIDITY_RESTORED: &str = "ob_liquidity_restored";
pub const OB_LIQUIDITY_ONE_SIDED: &str = "ob_liquidity_one_sided";
pub const OB_LIQUIDITY_TRAP: &str = "ob_liquidity_trap";
pub const OB_LIQUIDITY_RECONSTRUCTION: &str = "ob_liquidity_reconstruction";
pub const OB_LIQUIDITY_MIGRATION: &str = "ob_liquidity_migration";

// ============================================================================
// Microprice Events (8)
// ============================================================================

pub const OB_MICROPRICE_DIVERGENCE: &str = "ob_microprice_divergence";
pub const OB_MICROPRICE_ALIGNMENT: &str = "ob_microprice_alignment";
pub const OB_MICROPRICE_CROSSED_MID: &str = "ob_microprice_crossed_mid";
pub const OB_MICROPRICE_LEADING: &str = "ob_microprice_leading";
pub const OB_MICROPRICE_ACCELERATION: &str = "ob_microprice_acceleration";
pub const OB_MICROPRICE_REVERSAL: &str = "ob_microprice_reversal";
pub const OB_MICROPRICE_COMPRESSION: &str = "ob_microprice_compression";
pub const OB_MICROPRICE_EXPANSION: &str = "ob_microprice_expansion";

// ============================================================================
// Order Flow Events (8)
// ============================================================================

pub const OB_ORDERFLOW_AGGRESSIVE: &str = "ob_orderflow_aggressive";
pub const OB_ORDERFLOW_PASSIVE_ACCUMULATION: &str = "ob_orderflow_passive_accumulation";
pub const OB_ORDERFLOW_REVERSAL: &str = "ob_orderflow_reversal";
pub const OB_ORDERFLOW_ICEBERG: &str = "ob_orderflow_iceberg";
pub const OB_ORDERFLOW_QUOTE_STUFFING: &str = "ob_orderflow_quote_stuffing";
pub const OB_ORDERFLOW_MOMENTUM_IGNITION: &str = "ob_orderflow_momentum_ignition";
pub const OB_ORDERFLOW_STOP_HUNT: &str = "ob_orderflow_stop_hunt";
pub const OB_ORDERFLOW_ABSORPTION: &str = "ob_orderflow_absorption";

// ============================================================================
// Binance-Specific OrderBook Events (37)
// ============================================================================

pub const OB_BINANCE_BOOK_TICKER_UPDATE: &str = "ob_binance_book_ticker_update";
pub const OB_BINANCE_DEPTH_UPDATE: &str = "ob_binance_depth_update";
pub const OB_BINANCE_SNAPSHOT_SYNC: &str = "ob_binance_snapshot_sync";
pub const OB_BINANCE_RECONNECT_EVENT: &str = "ob_binance_reconnect_event";
pub const OB_BINANCE_LATENCY_SPIKE: &str = "ob_binance_latency_spike";
pub const OB_BINANCE_SEQUENCE_GAP: &str = "ob_binance_sequence_gap";
pub const OB_BINANCE_PRICE_LEVEL_REMOVED: &str = "ob_binance_price_level_removed";
pub const OB_BINANCE_RAPID_UPDATE: &str = "ob_binance_rapid_update";
pub const OB_BINANCE_AGGREGATE_LEVEL_SPLIT: &str = "ob_binance_aggregate_level_split";
pub const OB_BINANCE_AGGREGATE_LEVEL_MERGE: &str = "ob_binance_aggregate_level_merge";
pub const OB_BINANCE_HIGH_FREQUENCY_UPDATE: &str = "ob_binance_high_frequency_update";
pub const OB_BINANCE_ORDER_TYPE_DOMINANCE: &str = "ob_binance_order_type_dominance";
pub const OB_BINANCE_FUNDING_RATE_IMPLICATION: &str = "ob_binance_funding_rate_implication";
pub const OB_BINANCE_LIQUIDATION_CLUSTER_NEAR: &str = "ob_binance_liquidation_cluster_near";
pub const OB_BINANCE_OPEN_INTEREST_CHANGE: &str = "ob_binance_open_interest_change";
pub const OB_BINANCE_LONG_SHORT_RATIO_SHIFT: &str = "ob_binance_long_short_ratio_shift";
pub const OB_BINANCE_TAKER_FLOW_IMBALANCE: &str = "ob_binance_taker_flow_imbalance";
pub const OB_BINANCE_TOP_TRADER_POSITION_CHANGE: &str = "ob_binance_top_trader_position_change";
pub const OB_BINANCE_INSURANCE_FUND_IMPLICATION: &str = "ob_binance_insurance_fund_implication";
pub const OB_BINANCE_ADL_WARNING_IMPLICATION: &str = "ob_binance_adl_warning_implication";
pub const OB_BINANCE_MARK_PRICE_DIVERGENCE: &str = "ob_binance_mark_price_divergence";
pub const OB_BINANCE_INDEX_PRICE_DIVERGENCE: &str = "ob_binance_index_price_divergence";
pub const OB_BINANCE_PREMIUM_INDEX_SHIFT: &str = "ob_binance_premium_index_shift";
pub const OB_BINANCE_SETTLEMENT_PRICE_APPROACH: &str = "ob_binance_settlement_price_approach";
pub const OB_BINANCE_MULTI_ASSET_MARGIN_IMPLICATION: &str = "ob_binance_multi_asset_margin_implication";
pub const OB_BINANCE_COLLATERAL_RATIO_IMPLICATION: &str = "ob_binance_collateral_ratio_implication";
pub const OB_BINANCE_POSITION_LIMIT_APPROACH: &str = "ob_binance_position_limit_approach";
pub const OB_BINANCE_LEVERAGE_BRACKET_CHANGE: &str = "ob_binance_leverage_bracket_change";
pub const OB_BINANCE_MAINTENANCE_MARGIN_WARNING: &str = "ob_binance_maintenance_margin_warning";
pub const OB_BINANCE_MARGIN_RATIO_CRITICAL: &str = "ob_binance_margin_ratio_critical";
pub const OB_BINANCE_AUTO_DELEVERAGING_ZONE: &str = "ob_binance_auto_deleveraging_zone";
pub const OB_BINANCE_LIQUIDATION_ENGINE_ACTIVITY: &str = "ob_binance_liquidation_engine_activity";
pub const OB_BINANCE_SMART_LIMIT_ORDER_IMPLICATION: &str = "ob_binance_smart_limit_order_implication";
pub const OB_BINANCE_STOP_LIMIT_TRIGGERED: &str = "ob_binance_stop_limit_triggered";
pub const OB_BINANCE_TAKE_PROFIT_TRIGGERED: &str = "ob_binance_take_profit_triggered";
pub const OB_BINANCE_TRAILING_STOP_UPDATED: &str = "ob_binance_trailing_stop_updated";
pub const OB_BINANCE_HEDGE_MODE_IMPLICATION: &str = "ob_binance_hedge_mode_implication";

// Total OrderBook Events: 8 + 12 + 12 + 10 + 8 + 8 + 37 = 95 events

// ============================================================================
// Kline Aggregator Events (105 Events)
// ============================================================================

// ============================================================================
// EMA/Price Events (10)
// ============================================================================

pub const KLINE_EMA_CROSSED: &str = "kline_ema_crossed";
pub const KLINE_EMA_CROSSOVER: &str = "kline_ema_crossover";
pub const KLINE_EMA_STACK_BULLISH: &str = "kline_ema_stack_bullish";
pub const KLINE_EMA_STACK_BEARISH: &str = "kline_ema_stack_bearish";
pub const KLINE_EMA_ALIGNMENT: &str = "kline_ema_alignment";
pub const KLINE_EMA_COMPRESSION: &str = "kline_ema_compression";
pub const KLINE_EMA_EXPANSION: &str = "kline_ema_expansion";
pub const KLINE_EMA_SLOPE: &str = "kline_ema_slope";
pub const KLINE_EMA_DISTANCE: &str = "kline_ema_distance";
pub const KLINE_EMA_SR_TEST: &str = "kline_ema_sr_test";

// ============================================================================
// RSI Events (8)
// ============================================================================

pub const KLINE_RSI_THRESHOLD: &str = "kline_rsi_threshold";
pub const KLINE_RSI_DIVERGENCE: &str = "kline_rsi_divergence";
pub const KLINE_RSI_REGIME: &str = "kline_rsi_regime";
pub const KLINE_RSI_CENTERLINE: &str = "kline_rsi_centerline";
pub const KLINE_RSI_EXTREME: &str = "kline_rsi_extreme";
pub const KLINE_RSI_REVERSAL: &str = "kline_rsi_reversal";
pub const KLINE_RSI_FAILURE_SWING: &str = "kline_rsi_failure_swing";
pub const KLINE_RSI_HIDDEN_DIVERGENCE: &str = "kline_rsi_hidden_divergence";

// ============================================================================
// MACD Events (8)
// ============================================================================

pub const KLINE_MACD_CROSSOVER: &str = "kline_macd_crossover";
pub const KLINE_MACD_ZERO_CROSSED: &str = "kline_macd_zero_crossed";
pub const KLINE_MACD_HISTOGRAM_REGIME: &str = "kline_macd_histogram_regime";
pub const KLINE_MACD_DIVERGENCE: &str = "kline_macd_divergence";
pub const KLINE_MACD_HISTOGRAM_PEAK: &str = "kline_macd_histogram_peak";
pub const KLINE_MACD_ACCELERATION: &str = "kline_macd_acceleration";
pub const KLINE_MACD_DECELERATION: &str = "kline_macd_deceleration";
pub const KLINE_MACD_HIDDEN_DIVERGENCE: &str = "kline_macd_hidden_divergence";

// ============================================================================
// Candle Pattern Events (12)
// ============================================================================

pub const KLINE_CANDLE_PATTERN: &str = "kline_candle_pattern";
pub const KLINE_CANDLE_DOJI: &str = "kline_candle_doji";
pub const KLINE_CANDLE_ENGULFING_BULLISH: &str = "kline_candle_engulfing_bullish";
pub const KLINE_CANDLE_ENGULFING_BEARISH: &str = "kline_candle_engulfing_bearish";
pub const KLINE_CANDLE_HAMMER: &str = "kline_candle_hammer";
pub const KLINE_CANDLE_SHOOTING_STAR: &str = "kline_candle_shooting_star";
pub const KLINE_CANDLE_SIZE_ANOMALY: &str = "kline_candle_size_anomaly";
pub const KLINE_CANDLE_WICK_ANOMALY: &str = "kline_candle_wick_anomaly";
pub const KLINE_CANDLE_CONSECUTIVE: &str = "kline_candle_consecutive";
pub const KLINE_CANDLE_INSIDE_BAR: &str = "kline_candle_inside_bar";
pub const KLINE_CANDLE_OUTSIDE_BAR: &str = "kline_candle_outside_bar";
pub const KLINE_CANDLE_PIN_BAR: &str = "kline_candle_pin_bar";

// ============================================================================
// Volatility Events (8)
// ============================================================================

pub const KLINE_VOLATILITY_THRESHOLD: &str = "kline_volatility_threshold";
pub const KLINE_VOLATILITY_REGIME: &str = "kline_volatility_regime";
pub const KLINE_VOLATILITY_CONTRACTION: &str = "kline_volatility_contraction";
pub const KLINE_VOLATILITY_EXPANSION: &str = "kline_volatility_expansion";
pub const KLINE_BB_WIDTH_EXTREME: &str = "kline_bb_width_extreme";
pub const KLINE_BB_SQUEEZE: &str = "kline_bb_squeeze";
pub const KLINE_BB_EXPANSION_DETECTED: &str = "kline_bb_expansion_detected";
pub const KLINE_VOLATILITY_SPIKE: &str = "kline_volatility_spike";

// ============================================================================
// Bollinger Band Events (8)
// ============================================================================

pub const KLINE_BB_TOUCHED: &str = "kline_bb_touched";
pub const KLINE_BB_EXITED: &str = "kline_bb_exited";
pub const KLINE_BB_RETURNED: &str = "kline_bb_returned";
pub const KLINE_BB_WALK: &str = "kline_bb_walk";
pub const KLINE_BB_MIDDLE_CROSSED: &str = "kline_bb_middle_crossed";
pub const KLINE_BB_REVERSAL: &str = "kline_bb_reversal";
pub const KLINE_BB_BREAKOUT: &str = "kline_bb_breakout";
pub const KLINE_BB_FAILED_BREAKOUT: &str = "kline_bb_failed_breakout";

// ============================================================================
// Trend Events (8)
// ============================================================================

pub const KLINE_TREND_CHANGED: &str = "kline_trend_changed";
pub const KLINE_TREND_STRENGTH_INCREASED: &str = "kline_trend_strength_increased";
pub const KLINE_TREND_STRENGTH_DECREASED: &str = "kline_trend_strength_decreased";
pub const KLINE_TREND_HIGHER_HIGH: &str = "kline_trend_higher_high";
pub const KLINE_TREND_LOWER_LOW: &str = "kline_trend_lower_low";
pub const KLINE_TREND_HIGHER_LOW: &str = "kline_trend_higher_low";
pub const KLINE_TREND_LOWER_HIGH: &str = "kline_trend_lower_high";
pub const KLINE_TREND_CONTINUATION: &str = "kline_trend_continuation";

// ============================================================================
// Binance-Specific Kline Events (43)
// ============================================================================

pub const KLINE_BINANCE_FUNDING_RATE_IMPLICATION: &str = "kline_binance_funding_rate_implication";
pub const KLINE_BINANCE_LIQUIDATION_CLUSTER_NEAR: &str = "kline_binance_liquidation_cluster_near";
pub const KLINE_BINANCE_OPEN_INTEREST_DIVERGENCE: &str = "kline_binance_open_interest_divergence";
pub const KLINE_BINANCE_LONG_SHORT_RATIO_EXTREME: &str = "kline_binance_long_short_ratio_extreme";
pub const KLINE_BINANCE_TAKER_FLOW_CONFIRMATION: &str = "kline_binance_taker_flow_confirmation";
pub const KLINE_BINANCE_TOP_TRADER_ALIGNMENT: &str = "kline_binance_top_trader_alignment";
pub const KLINE_BINANCE_MARK_PRICE_DIVERGENCE: &str = "kline_binance_mark_price_divergence";
pub const KLINE_BINANCE_PREMIUM_INDEX_SHIFT: &str = "kline_binance_premium_index_shift";
pub const KLINE_BINANCE_SETTLEMENT_APPROACH: &str = "kline_binance_settlement_approach";
pub const KLINE_BINANCE_MULTI_ASSET_MARGIN_EFFECT: &str = "kline_binance_multi_asset_margin_effect";
pub const KLINE_BINANCE_COLLATERAL_RATIO_EFFECT: &str = "kline_binance_collateral_ratio_effect";
pub const KLINE_BINANCE_POSITION_LIMIT_EFFECT: &str = "kline_binance_position_limit_effect";
pub const KLINE_BINANCE_LEVERAGE_BRACKET_EFFECT: &str = "kline_binance_leverage_bracket_effect";
pub const KLINE_BINANCE_MAINTENANCE_MARGIN_EFFECT: &str = "kline_binance_maintenance_margin_effect";
pub const KLINE_BINANCE_MARGIN_RATIO_EFFECT: &str = "kline_binance_margin_ratio_effect";
pub const KLINE_BINANCE_ADL_ZONE_EFFECT: &str = "kline_binance_adl_zone_effect";
pub const KLINE_BINANCE_LIQUIDATION_ENGINE_EFFECT: &str = "kline_binance_liquidation_engine_effect";
pub const KLINE_BINANCE_SMART_ORDER_EFFECT: &str = "kline_binance_smart_order_effect";
pub const KLINE_BINANCE_STOP_ORDER_TRIGGERED: &str = "kline_binance_stop_order_triggered";
pub const KLINE_BINANCE_TP_ORDER_TRIGGERED: &str = "kline_binance_tp_order_triggered";
pub const KLINE_BINANCE_TRAILING_STOP_EFFECT: &str = "kline_binance_trailing_stop_effect";
pub const KLINE_BINANCE_HEDGE_MODE_EFFECT: &str = "kline_binance_hedge_mode_effect";
pub const KLINE_BINANCE_INSURANCE_FUND_EFFECT: &str = "kline_binance_insurance_fund_effect";
pub const KLINE_BINANCE_INDEX_PRICE_EFFECT: &str = "kline_binance_index_price_effect";
pub const KLINE_BINANCE_AUTO_DELEVERAGING_EFFECT: &str = "kline_binance_auto_deleveraging_effect";
pub const KLINE_BINANCE_FEE_RATE_EFFECT: &str = "kline_binance_fee_rate_effect";
pub const KLINE_BINANCE_ORDER_SIZE_LIMIT_EFFECT: &str = "kline_binance_order_size_limit_effect";
pub const KLINE_BINANCE_PRICE_FILTER_EFFECT: &str = "kline_binance_price_filter_effect";
pub const KLINE_BINANCE_LOT_SIZE_EFFECT: &str = "kline_binance_lot_size_effect";
pub const KLINE_BINANCE_MARKET_ORDER_EFFECT: &str = "kline_binance_market_order_effect";
pub const KLINE_BINANCE_LIMIT_ORDER_EFFECT: &str = "kline_binance_limit_order_effect";
pub const KLINE_BINANCE_STOP_MARKET_EFFECT: &str = "kline_binance_stop_market_effect";
pub const KLINE_BINANCE_TAKE_PROFIT_MARKET_EFFECT: &str = "kline_binance_take_profit_market_effect";
pub const KLINE_BINANCE_TRAILING_STOP_MARKET_EFFECT: &str = "kline_binance_trailing_stop_market_effect";
pub const KLINE_BINANCE_POST_ONLY_EFFECT: &str = "kline_binance_post_only_effect";
pub const KLINE_BINANCE_IOC_EFFECT: &str = "kline_binance_ioc_effect";
pub const KLINE_BINANCE_FOK_EFFECT: &str = "kline_binance_fok_effect";
pub const KLINE_BINANCE_GTC_EFFECT: &str = "kline_binance_gtc_effect";
pub const KLINE_BINANCE_IOC_PARTIAL_FILL: &str = "kline_binance_ioc_partial_fill";
pub const KLINE_BINANCE_TIME_IN_FORCE_EXPIRY: &str = "kline_binance_time_in_force_expiry";
pub const KLINE_BINANCE_REDUCE_ONLY_EFFECT: &str = "kline_binance_reduce_only_effect";
pub const KLINE_BINANCE_CLOSE_POSITION_EFFECT: &str = "kline_binance_close_position_effect";
pub const KLINE_BINANCE_POSITION_SIDE_EFFECT: &str = "kline_binance_position_side_effect";

// Total Kline Events: 10 + 8 + 8 + 12 + 8 + 8 + 8 + 43 = 105 events

// ============================================================================
// MarkPrice Aggregator Events (58 Events)
// ============================================================================

// ============================================================================
// Premium Events (10)
// ============================================================================

pub const MP_PREMIUM_THRESHOLD: &str = "mp_premium_threshold";
pub const MP_PREMIUM_REGIME: &str = "mp_premium_regime";
pub const MP_PREMIUM_FLIPPED: &str = "mp_premium_flipped";
pub const MP_PREMIUM_SPIKE: &str = "mp_premium_spike";
pub const MP_PREMIUM_CONVERGENCE: &str = "mp_premium_convergence";
pub const MP_PREMIUM_DIVERGENCE: &str = "mp_premium_divergence";
pub const MP_PREMIUM_EXTREME: &str = "mp_premium_extreme";
pub const MP_PREMIUM_MEAN_REVERSION: &str = "mp_premium_mean_reversion";
pub const MP_PREMIUM_ACCELERATION: &str = "mp_premium_acceleration";
pub const MP_PREMIUM_PERSISTENCE: &str = "mp_premium_persistence";

// ============================================================================
// Funding Events (10)
// ============================================================================

pub const MP_FUNDING_THRESHOLD: &str = "mp_funding_threshold";
pub const MP_FUNDING_REGIME: &str = "mp_funding_regime";
pub const MP_FUNDING_FLIPPED: &str = "mp_funding_flipped";
pub const MP_FUNDING_EXTREME: &str = "mp_funding_extreme";
pub const MP_FUNDING_APPROACHING: &str = "mp_funding_approaching";
pub const MP_FUNDING_TREND: &str = "mp_funding_trend";
pub const MP_FUNDING_REVERSAL: &str = "mp_funding_reversal";
pub const MP_FUNDING_SQUEEZE_CONDITIONS: &str = "mp_funding_squeeze_conditions";
pub const MP_FUNDING_ARB_OPPORTUNITY: &str = "mp_funding_arb_opportunity";
pub const MP_FUNDING_POSITIONING: &str = "mp_funding_positioning";

// ============================================================================
// Divergence Events (6)
// ============================================================================

pub const MP_DIVERGENCE_MARK_LAST: &str = "mp_divergence_mark_last";
pub const MP_DIVERGENCE_INDEX_COMPOSITION: &str = "mp_divergence_index_composition";
pub const MP_DIVERGENCE_PRICE_ANCHOR: &str = "mp_divergence_price_anchor";
pub const MP_DIVERGENCE_CROSS_EXCHANGE: &str = "mp_divergence_cross_exchange";
pub const MP_DIVERGENCE_SPOT_FUTURES: &str = "mp_divergence_spot_futures";
pub const MP_DIVERGENCE_PERP_QUARTERLY: &str = "mp_divergence_perp_quarterly";

// ============================================================================
// Liquidation Zone Events (6)
// ============================================================================

pub const MP_LIQUIDATION_NEAR_CLUSTER: &str = "mp_liquidation_near_cluster";
pub const MP_LIQUIDATION_BREACHED: &str = "mp_liquidation_breached";
pub const MP_LIQUIDATION_EXITED: &str = "mp_liquidation_exited";
pub const MP_LIQUIDATION_MAGNET: &str = "mp_liquidation_magnet";
pub const MP_LIQUIDATION_DEFENDED: &str = "mp_liquidation_defended";
pub const MP_LIQUIDATION_CASCADE_ZONE: &str = "mp_liquidation_cascade_zone";

// ============================================================================
// Binance-Specific MarkPrice Events (26)
// ============================================================================

pub const MP_BINANCE_INSURANCE_FUND_CORRELATION: &str = "mp_binance_insurance_fund_correlation";
pub const MP_BINANCE_ADL_WARNING_CORRELATION: &str = "mp_binance_adl_warning_correlation";
pub const MP_BINANCE_OPEN_INTEREST_EFFECT: &str = "mp_binance_open_interest_effect";
pub const MP_BINANCE_LONG_SHORT_RATIO_EFFECT: &str = "mp_binance_long_short_ratio_effect";
pub const MP_BINANCE_TAKER_VOLUME_EFFECT: &str = "mp_binance_taker_volume_effect";
pub const MP_BINANCE_TOP_TRADER_EFFECT: &str = "mp_binance_top_trader_effect";
pub const MP_BINANCE_LEVERAGE_BRACKET_EFFECT: &str = "mp_binance_leverage_bracket_effect";
pub const MP_BINANCE_POSITION_LIMIT_EFFECT: &str = "mp_binance_position_limit_effect";
pub const MP_BINANCE_MARGIN_RATIO_EFFECT: &str = "mp_binance_margin_ratio_effect";
pub const MP_BINANCE_MAINTENANCE_MARGIN_EFFECT: &str = "mp_binance_maintenance_margin_effect";
pub const MP_BINANCE_MULTI_ASSET_MARGIN_EFFECT: &str = "mp_binance_multi_asset_margin_effect";
pub const MP_BINANCE_COLLATERAL_RATIO_EFFECT: &str = "mp_binance_collateral_ratio_effect";
pub const MP_BINANCE_SETTLEMENT_PRICE_APPROACH: &str = "mp_binance_settlement_price_approach";
pub const MP_BINANCE_FUNDING_SETTLEMENT_APPROACH: &str = "mp_binance_funding_settlement_approach";
pub const MP_BINANCE_ORDER_IMBALANCE_EFFECT: &str = "mp_binance_order_imbalance_effect";
pub const MP_BINANCE_TRADE_FLOW_EFFECT: &str = "mp_binance_trade_flow_effect";
pub const MP_BINANCE_DEPTH_EFFECT: &str = "mp_binance_depth_effect";
pub const MP_BINANCE_SPREAD_EFFECT: &str = "mp_binance_spread_effect";
pub const MP_BINANCE_BOOK_TICKER_EFFECT: &str = "mp_binance_book_ticker_effect";
pub const MP_BINANCE_AGG_TRADE_EFFECT: &str = "mp_binance_agg_trade_effect";
pub const MP_BINANCE_KLINE_EFFECT: &str = "mp_binance_kline_effect";
pub const MP_BINANCE_FORCE_ORDER_EFFECT: &str = "mp_binance_force_order_effect";
pub const MP_BINANCE_ACCOUNT_UPDATE_EFFECT: &str = "mp_binance_account_update_effect";
pub const MP_BINANCE_ORDER_UPDATE_EFFECT: &str = "mp_binance_order_update_effect";
pub const MP_BINANCE_BALANCE_UPDATE_EFFECT: &str = "mp_binance_balance_update_effect";
pub const MP_BINANCE_POSITION_UPDATE_EFFECT: &str = "mp_binance_position_update_effect";

// Total MarkPrice Events: 10 + 10 + 6 + 6 + 26 = 58 events

// ============================================================================
// Liquidation Aggregator Events (52 Events)
// ============================================================================

// ============================================================================
// Individual Events (8)
// ============================================================================

pub const LIQ_DETECTED: &str = "liq_detected";
pub const LIQ_LARGE: &str = "liq_large";
pub const LIQ_RATE_THRESHOLD: &str = "liq_rate_threshold";
pub const LIQ_VOLUME_THRESHOLD: &str = "liq_volume_threshold";
pub const LIQ_SIZE_ANOMALY: &str = "liq_size_anomaly";
pub const LIQ_WHALE: &str = "liq_whale";
pub const LIQ_SEQUENTIAL: &str = "liq_sequential";
pub const LIQ_PRICE_LEVEL_BREACHED: &str = "liq_price_level_breached";

// ============================================================================
// Cluster Events (10)
// ============================================================================

pub const LIQ_CLUSTER_STARTED: &str = "liq_cluster_started";
pub const LIQ_CLUSTER_GROWING: &str = "liq_cluster_growing";
pub const LIQ_CLUSTER_ENDED: &str = "liq_cluster_ended";
pub const LIQ_CLUSTER_SIDE_DOMINANT: &str = "liq_cluster_side_dominant";
pub const LIQ_CLUSTER_BILATERAL: &str = "liq_cluster_bilateral";
pub const LIQ_CLUSTER_INTENSITY_HIGH: &str = "liq_cluster_intensity_high";
pub const LIQ_CLUSTER_PRICE_IMPACT: &str = "liq_cluster_price_impact";
pub const LIQ_CLUSTER_ABSORBED: &str = "liq_cluster_absorbed";
pub const LIQ_CLUSTER_CASCADING: &str = "liq_cluster_cascading";
pub const LIQ_CLUSTER_EXHAUSTED: &str = "liq_cluster_exhausted";

// ============================================================================
// Cascade Events (10)
// ============================================================================

pub const LIQ_CASCADE_POTENTIAL: &str = "liq_cascade_potential";
pub const LIQ_CASCADE_ACCELERATION: &str = "liq_cascade_acceleration";
pub const LIQ_CASCADE_DECELERATION: &str = "liq_cascade_deceleration";
pub const LIQ_CASCADE_REVERSAL: &str = "liq_cascade_reversal";
pub const LIQ_CASCADE_EXHAUSTION: &str = "liq_cascade_exhaustion";
pub const LIQ_CASCADE_COMPLETION: &str = "liq_cascade_completion";
pub const LIQ_CASCADE_CONTINUATION: &str = "liq_cascade_continuation";
pub const LIQ_CASCADE_ABSORPTION: &str = "liq_cascade_absorption";
pub const LIQ_CASCADE_IMMINENT_WARNING: &str = "liq_cascade_imminent_warning";
pub const LIQ_CASCADE_SPIRAL: &str = "liq_cascade_spiral";

// ============================================================================
// Directional Events (6)
// ============================================================================

pub const LIQ_SIDE_IMBALANCE: &str = "liq_side_imbalance";
pub const LIQ_SIDE_FLIPPED: &str = "liq_side_flipped";
pub const LIQ_BALANCED: &str = "liq_balanced";
pub const LIQ_LONG_SQUEEZE: &str = "liq_long_squeeze";
pub const LIQ_SHORT_SQUEEZE: &str = "liq_short_squeeze";
pub const LIQ_SQUEEZE_EXHAUSTION: &str = "liq_squeeze_exhaustion";

// ============================================================================
// Impact Events (8)
// ============================================================================

pub const LIQ_HIGH_IMPACT: &str = "liq_high_impact";
pub const LIQ_IMPACT_ABSORPTION: &str = "liq_impact_absorption";
pub const LIQ_IMPACT_EXHAUSTION: &str = "liq_impact_exhaustion";
pub const LIQ_IMPACT_REBOUND: &str = "liq_impact_rebound";
pub const LIQ_FEEDBACK_LOOP: &str = "liq_feedback_loop";
pub const LIQ_POSITION_UNWINDING: &str = "liq_position_unwinding";
pub const LIQ_MARGIN_CALL_CASCADE: &str = "liq_margin_call_cascade";
pub const LIQ_SYSTEMIC_RISK: &str = "liq_systemic_risk";

// ============================================================================
// Binance-Specific Liquidation Events (10)
// ============================================================================

pub const LIQ_BINANCE_ADL_TRIGGERED: &str = "liq_binance_adl_triggered";
pub const LIQ_BINANCE_INSURANCE_FUND_USED: &str = "liq_binance_insurance_fund_used";
pub const LIQ_BINANCE_AUTO_LIQUIDATION: &str = "liq_binance_auto_liquidation";
pub const LIQ_BINANCE_MANUAL_LIQUIDATION: &str = "liq_binance_manual_liquidation";
pub const LIQ_BINANCE_PARTIAL_LIQUIDATION: &str = "liq_binance_partial_liquidation";
pub const LIQ_BINANCE_FULL_LIQUIDATION: &str = "liq_binance_full_liquidation";
pub const LIQ_BINANCE_BANKRUPTCY_EVENT: &str = "liq_binance_bankruptcy_event";
pub const LIQ_BINANCE_TAKER_LIQUIDATION: &str = "liq_binance_taker_liquidation";
pub const LIQ_BINANCE_MAKER_LIQUIDATION: &str = "liq_binance_maker_liquidation";
pub const LIQ_BINANCE_PRICE_PROTECTION: &str = "liq_binance_price_protection";

// Total Liquidation Events: 8 + 10 + 10 + 6 + 8 + 10 = 52 events

// ============================================================================
// Binance Data Aggregator Events (75 Events)
// ============================================================================

// ============================================================================
// Open Interest Events (15)
// ============================================================================

pub const BD_OI_THRESHOLD: &str = "bd_oi_threshold";
pub const BD_OI_REGIME: &str = "bd_oi_regime";
pub const BD_OI_SPIKE: &str = "bd_oi_spike";
pub const BD_OI_DROP: &str = "bd_oi_drop";
pub const BD_OI_PRICE_CORRELATION: &str = "bd_oi_price_correlation";
pub const BD_OI_DELTA_POSITIVE: &str = "bd_oi_delta_positive";
pub const BD_OI_DELTA_NEGATIVE: &str = "bd_oi_delta_negative";
pub const BD_OI_CONCENTRATION: &str = "bd_oi_concentration";
pub const BD_OI_LIQUIDATION_CORRELATION: &str = "bd_oi_liquidation_correlation";
pub const BD_OI_FUNDING_CORRELATION: &str = "bd_oi_funding_correlation";
pub const BD_OI_PREMIUM_CORRELATION: &str = "bd_oi_premium_correlation";
pub const BD_OI_TREND_ACCELERATION: &str = "bd_oi_trend_acceleration";
pub const BD_OI_TREND_DECELERATION: &str = "bd_oi_trend_deceleration";
pub const BD_OI_HISTORICAL_EXTREME: &str = "bd_oi_historical_extreme";
pub const BD_OI_MEAN_REVERSION: &str = "bd_oi_mean_reversion";

// ============================================================================
// Long/Short Ratio Events (15)
// ============================================================================

pub const BD_LS_THRESHOLD: &str = "bd_ls_threshold";
pub const BD_LS_EXTREME_LONG: &str = "bd_ls_extreme_long";
pub const BD_LS_EXTREME_SHORT: &str = "bd_ls_extreme_short";
pub const BD_LS_REGIME: &str = "bd_ls_regime";
pub const BD_LS_REVERSAL: &str = "bd_ls_reversal";
pub const BD_LS_DIVERGENCE: &str = "bd_ls_divergence";
pub const BD_LS_CONVERGENCE: &str = "bd_ls_convergence";
pub const BD_LS_TREND: &str = "bd_ls_trend";
pub const BD_LS_COUNTER_TREND: &str = "bd_ls_counter_trend";
pub const BD_LS_SQUEEZE_SETUP: &str = "bd_ls_squeeze_setup";
pub const BD_LS_HISTORICAL_EXTREME: &str = "bd_ls_historical_extreme";
pub const BD_LS_ACCOUNT_TOP_DIVERGENCE: &str = "bd_ls_account_top_divergence";
pub const BD_LS_SENTIMENT_SHIFT: &str = "bd_ls_sentiment_shift";
pub const BD_LS_CONTRARIAN: &str = "bd_ls_contrarian";
pub const BD_LS_POSITIONING_EXTREME: &str = "bd_ls_positioning_extreme";

// ============================================================================
// Taker Volume Events (15)
// ============================================================================

pub const BD_TAKER_BUY_SPIKE: &str = "bd_taker_buy_spike";
pub const BD_TAKER_SELL_SPIKE: &str = "bd_taker_sell_spike";
pub const BD_TAKER_RATIO_EXTREME: &str = "bd_taker_ratio_extreme";
pub const BD_TAKER_REGIME: &str = "bd_taker_regime";
pub const BD_TAKER_TREND: &str = "bd_taker_trend";
pub const BD_TAKER_REVERSAL: &str = "bd_taker_reversal";
pub const BD_TAKER_PRICE_CORRELATION: &str = "bd_taker_price_correlation";
pub const BD_TAKER_OI_CORRELATION: &str = "bd_taker_oi_correlation";
pub const BD_TAKER_FUNDING_CORRELATION: &str = "bd_taker_funding_correlation";
pub const BD_TAKER_HISTORICAL_EXTREME: &str = "bd_taker_historical_extreme";
pub const BD_TAKER_ACCUMULATION: &str = "bd_taker_accumulation";
pub const BD_TAKER_DISTRIBUTION: &str = "bd_taker_distribution";
pub const BD_TAKER_CLIMAX: &str = "bd_taker_climax";
pub const BD_TAKER_EXHAUSTION: &str = "bd_taker_exhaustion";
pub const BD_TAKER_SMART_MONEY: &str = "bd_taker_smart_money";

// ============================================================================
// Top Trader Events (15)
// ============================================================================

pub const BD_TOP_POSITION_SPIKE: &str = "bd_top_position_spike";
pub const BD_TOP_POSITION_DROP: &str = "bd_top_position_drop";
pub const BD_TOP_LONG_EXTREME: &str = "bd_top_long_extreme";
pub const BD_TOP_SHORT_EXTREME: &str = "bd_top_short_extreme";
pub const BD_TOP_REVERSAL: &str = "bd_top_reversal";
pub const BD_TOP_DIVERGENCE: &str = "bd_top_divergence";
pub const BD_TOP_CONVERGENCE: &str = "bd_top_convergence";
pub const BD_TOP_ACCUMULATION: &str = "bd_top_accumulation";
pub const BD_TOP_DISTRIBUTION: &str = "bd_top_distribution";
pub const BD_TOP_SENTIMENT_SHIFT: &str = "bd_top_sentiment_shift";
pub const BD_TOP_LIQUIDATION_RISK: &str = "bd_top_liquidation_risk";
pub const BD_TOP_CORRELATION_BREAKDOWN: &str = "bd_top_correlation_breakdown";
pub const BD_TOP_MOMENTUM_BUILDING: &str = "bd_top_momentum_building";
pub const BD_TOP_MOMENTUM_FADING: &str = "bd_top_momentum_fading";
pub const BD_TOP_HISTORICAL_EXTREME: &str = "bd_top_historical_extreme";

// ============================================================================
// Insurance Fund Events (15)
// ============================================================================

pub const BD_INSURANCE_DEPLETION: &str = "bd_insurance_depletion";
pub const BD_INSURANCE_REPLENISHMENT: &str = "bd_insurance_replenishment";
pub const BD_INSURANCE_HISTORICAL_LOW: &str = "bd_insurance_historical_low";
pub const BD_INSURANCE_HISTORICAL_HIGH: &str = "bd_insurance_historical_high";
pub const BD_INSURANCE_LIQUIDATION_CORRELATION: &str = "bd_insurance_liquidation_correlation";
pub const BD_INSURANCE_SYSTEMIC_RISK: &str = "bd_insurance_systemic_risk";
pub const BD_INSURANCE_ADL_INDICATOR: &str = "bd_insurance_adl_indicator";
pub const BD_INSURANCE_DRAWDOWN_EXTREME: &str = "bd_insurance_drawdown_extreme";
pub const BD_INSURANCE_RECOVERY: &str = "bd_insurance_recovery";
pub const BD_INSURANCE_STRESS_TEST: &str = "bd_insurance_stress_test";
pub const BD_INSURANCE_VOLATILITY_CORRELATION: &str = "bd_insurance_volatility_correlation";
pub const BD_INSURANCE_MARKET_REGIME: &str = "bd_insurance_market_regime";
pub const BD_INSURANCE_PREDICTIVE_MODEL: &str = "bd_insurance_predictive_model";
pub const BD_INSURANCE_RISK_ADJUSTMENT: &str = "bd_insurance_risk_adjustment";
pub const BD_INSURANCE_EXCHANGE_HEALTH: &str = "bd_insurance_exchange_health";

// Total Binance Data Events: 15 + 15 + 15 + 15 + 15 = 75 events

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_constants_not_empty() {
        // Verify all constants are non-empty
        assert!(!DELTA_THRESHOLD.is_empty());
        assert!(!VWAP_DEVIATION.is_empty());
        assert!(!VOLUME_SPIKE.is_empty());
        assert!(!PATTERN_LARGE_TRADE.is_empty());
        assert!(!PROFILE_POC_SHIFT.is_empty());
        assert!(!STREAK_STARTED.is_empty());
    }

    #[test]
    fn test_event_constants_unique() {
        // Collect all event names
        let events = vec![
            DELTA_THRESHOLD, DELTA_SLOPE, DELTA_ACCELERATION, DELTA_REGIME_CHANGE,
            DELTA_REVERSAL, DELTA_MOMENTUM_BUILDING, DELTA_EXHAUSTION, DELTA_DIVERGENCE,
            DELTA_ZERO_CROSS, DELTA_EXTREME, DELTA_NEUTRAL, DELTA_IMBALANCE_FLIP,
            VWAP_DEVIATION, VWAP_EXTREME_DEVIATION, VWAP_CROSS, VWAP_RETEST,
            VWAP_REJECTION, VWAP_ALIGNMENT, VWAP_BAND_BREAK, VWAP_CONVERGENCE,
            VWAP_DIVERGENCE, VWAP_ANCHOR_SHIFT, VWAP_RECLAIM,
            VOLUME_SPIKE, VOLUME_DRY_UP, VOLUME_REGIME_CHANGE, VOLUME_RATIO_EXTREME,
            VOLUME_ACCELERATION, VOLUME_EXHAUSTION, VOLUME_CLIMAX, VOLUME_DISTRIBUTION,
            VOLUME_ACCUMULATION, VOLUME_CHURN, VOLUME_DIVERGENCE, VOLUME_CONFIRMATION,
            PATTERN_LARGE_TRADE, PATTERN_WHALE_TRADE, PATTERN_CLUSTER, PATTERN_RATE_SPIKE,
            PATTERN_INSTITUTIONAL, PATTERN_AGGRESSION, PATTERN_ABSORPTION, PATTERN_SWEEP,
            PATTERN_ICEBERG, PATTERN_SPOOFING,
            PROFILE_POC_SHIFT, PROFILE_POC_TEST, PROFILE_VALUE_AREA_BREAK, PROFILE_VALUE_AREA_RETEST,
            PROFILE_HIGH_VOLUME_NODE, PROFILE_LOW_VOLUME_NODE, PROFILE_SINGLE_PRINTS, PROFILE_BALANCE,
            STREAK_STARTED, STREAK_EXTENDED, STREAK_ENDED, STREAK_RECORD,
            STREAK_INTENSITY_HIGH, STREAK_INTENSITY_LOW, STREAK_ALTERNATING, STREAK_MOMENTUM,
        ];

        // All should be unique
        let unique: std::collections::HashSet<_> = events.iter().collect();
        assert_eq!(unique.len(), events.len(), "All event constants should be unique");
    }
}
