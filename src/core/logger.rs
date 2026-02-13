// Structured Logging for Liquidity Hunt System
// Pure Rust using tracing crate - no Python dependencies

use tracing::Level;
use tracing_subscriber::EnvFilter;
use std::sync::Once;

static INIT: Once = Once::new();

/// Setup structured logging for the entire application
pub fn setup_logging(
    log_level: Option<&str>,
    log_file: Option<&str>,
    json_format: Option<bool>,
    console_output: Option<bool>,
) {
    let log_level_str = log_level.unwrap_or("INFO");
    let json_format = json_format.unwrap_or(true);
    let console_output = console_output.unwrap_or(true);

    // Parse log level
    let level = match log_level_str.to_uppercase().as_str() {
        "TRACE" => Level::TRACE,
        "DEBUG" => Level::DEBUG,
        "INFO" => Level::INFO,
        "WARN" | "WARNING" => Level::WARN,
        "ERROR" => Level::ERROR,
        _ => Level::INFO,
    };

    INIT.call_once(|| {
        // Create environment filter
        let filter = EnvFilter::from_default_env()
            .add_directive(level.into())
            // Suppress noisy libraries
            .add_directive("websockets=warn".parse().unwrap())
            .add_directive("tokio_tungstenite=warn".parse().unwrap())
            .add_directive("tungstenite=warn".parse().unwrap())
            .add_directive("hyper=warn".parse().unwrap())
            .add_directive("reqwest=warn".parse().unwrap());

        if console_output {
            if json_format {
                tracing_subscriber::fmt()
                    .json()
                    .with_target(true)
                    .with_thread_ids(true)
                    .with_env_filter(filter)
                    .init();
            } else {
                tracing_subscriber::fmt()
                    .with_target(true)
                    .with_env_filter(filter)
                    .init();
            }
        } else {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .init();
        }

        tracing::info!(
            log_level = %log_level_str,
            log_file = ?log_file,
            "Logging initialized"
        );
    });
}

// Note: ContextLogger removed - unused in codebase. All code uses tracing macros directly.

// Rust-native logging macros (for use throughout the codebase)

#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        tracing::debug!($($arg)*);
    };
}

#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        tracing::info!($($arg)*);
    };
}

#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => {
        tracing::warn!($($arg)*);
    };
}

#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        tracing::error!($($arg)*);
    };
}

// Helper functions for structured logging

/// Create a span for a function/operation
pub fn create_span(name: &str, level: Level) -> tracing::Span {
    match level {
        Level::TRACE => tracing::trace_span!(target: "liquidity_hunt", "{}", name),
        Level::DEBUG => tracing::debug_span!(target: "liquidity_hunt", "{}", name),
        Level::INFO => tracing::info_span!(target: "liquidity_hunt", "{}", name),
        Level::WARN => tracing::warn_span!(target: "liquidity_hunt", "{}", name),
        Level::ERROR => tracing::error_span!(target: "liquidity_hunt", "{}", name),
    }
}

/// Log with fields
pub fn log_with_fields(level: Level, message: &str, fields: &[(&str, &str)]) {
    let span = create_span("log_with_fields", level);
    let _enter = span.enter();

    match level {
        Level::TRACE => {
            for (key, value) in fields {
                tracing::trace!(key = %key, value = %value);
            }
            tracing::trace!("{}", message);
        }
        Level::DEBUG => {
            for (key, value) in fields {
                tracing::debug!(key = %key, value = %value);
            }
            tracing::debug!("{}", message);
        }
        Level::INFO => {
            for (key, value) in fields {
                tracing::info!(key = %key, value = %value);
            }
            tracing::info!("{}", message);
        }
        Level::WARN => {
            for (key, value) in fields {
                tracing::warn!(key = %key, value = %value);
            }
            tracing::warn!("{}", message);
        }
        Level::ERROR => {
            for (key, value) in fields {
                tracing::error!(key = %key, value = %value);
            }
            tracing::error!("{}", message);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_setup_logging() {
        setup_logging(
            Some("DEBUG"),
            None,
            Some(false),
            Some(true),
        );
    }
}
