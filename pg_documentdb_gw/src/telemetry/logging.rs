/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/telemetry/logging.rs
 *
 *-------------------------------------------------------------------------
 */

use std::time::Duration;

use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    logs::{BatchConfigBuilder, BatchLogProcessor, SdkLoggerProvider},
    Resource,
};
use serde::Deserialize;
use tracing_subscriber::{EnvFilter, Layer, Registry};

use crate::{
    error::{DocumentDBError, Result},
    telemetry::config::{env_var, DEFAULT_EXPORT_TIMEOUT_MS, DEFAULT_OTLP_ENDPOINT},
};

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_LOGGING_ENABLED: bool = true;
const DEFAULT_CONSOLE_ENABLED: bool = false;
const DEFAULT_MAX_QUEUE_SIZE: usize = 4096;
const DEFAULT_LOG_MAX_EXPORT_BATCH_SIZE: usize = 256;
const DEFAULT_LOG_EXPORT_INTERVAL_MS: u64 = 5000;
const DEFAULT_LOG_LEVEL: &str = "info";

// ============================================================================
// JSON Configuration
// ============================================================================

/// JSON configuration for logging (matches SetupConfiguration.json TelemetryOptions.Logging)
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct LoggingOptions {
    /// Whether OTLP log export is enabled
    pub enabled: Option<bool>,
    /// OTLP endpoint for log export
    pub otlp_endpoint: Option<String>,
    /// Log level filter (e.g., "info", "debug", "warn")
    pub level: Option<String>,
    /// Whether console logging is enabled
    pub console_enabled: Option<bool>,
    /// Maximum queue size for log batching
    pub max_queue_size: Option<usize>,
    /// Maximum batch size for export
    pub max_export_batch_size: Option<usize>,
    /// Export interval in milliseconds
    pub export_interval_ms: Option<u64>,
    /// Export timeout in milliseconds
    pub export_timeout_ms: Option<u64>,
}

// ============================================================================
// Runtime Configuration
// ============================================================================

/// Runtime configuration for logging with OTLP export.
///
/// Stores JSON configuration values and provides accessor methods that implement
/// the fallback logic: JSON value > environment variable > default constant.
#[derive(Debug, Clone)]
pub struct LoggingConfig {
    enabled: Option<bool>,
    otlp_endpoint: Option<String>,
    level: Option<String>,
    console_enabled: Option<bool>,
    max_queue_size: Option<usize>,
    max_export_batch_size: Option<usize>,
    export_interval_ms: Option<u64>,
    export_timeout_ms: Option<u64>,
}

impl LoggingConfig {
    /// Creates logging config from optional JSON configuration.
    ///
    /// Accessor methods implement fallback: JSON > env vars > defaults.
    pub fn new(json_config: Option<&LoggingOptions>) -> Self {
        let json = json_config.cloned().unwrap_or_default();

        Self {
            enabled: json.enabled,
            otlp_endpoint: json.otlp_endpoint,
            level: json.level,
            console_enabled: json.console_enabled,
            max_queue_size: json.max_queue_size,
            max_export_batch_size: json.max_export_batch_size,
            export_interval_ms: json.export_interval_ms,
            export_timeout_ms: json.export_timeout_ms,
        }
    }

    /// Whether OTLP logging is enabled. Fallback: JSON > OTEL_LOGGING_ENABLED > true.
    pub fn logging_enabled(&self) -> bool {
        self.enabled
            .or_else(|| env_var("OTEL_LOGGING_ENABLED"))
            .unwrap_or(DEFAULT_LOGGING_ENABLED)
    }

    /// OTLP endpoint for logs. Fallback: JSON > OTEL_EXPORTER_OTLP_LOGS_ENDPOINT > OTEL_EXPORTER_OTLP_ENDPOINT > default.
    pub fn otlp_endpoint(&self) -> String {
        self.otlp_endpoint
            .clone()
            .or_else(|| env_var("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"))
            .or_else(|| env_var("OTEL_EXPORTER_OTLP_ENDPOINT"))
            .unwrap_or_else(|| DEFAULT_OTLP_ENDPOINT.to_string())
    }

    /// Log level filter. Fallback: JSON > RUST_LOG > "info".
    pub fn level(&self) -> String {
        self.level
            .clone()
            .or_else(|| env_var("RUST_LOG"))
            .unwrap_or_else(|| DEFAULT_LOG_LEVEL.to_string())
    }

    /// Whether console logging is enabled. Fallback: JSON > OTEL_LOGS_CONSOLE_ENABLED > false.
    pub fn console_enabled(&self) -> bool {
        self.console_enabled
            .or_else(|| env_var("OTEL_LOGS_CONSOLE_ENABLED"))
            .unwrap_or(DEFAULT_CONSOLE_ENABLED)
    }

    /// Max queue size for log batching. Fallback: JSON > OTEL_BLRP_MAX_QUEUE_SIZE > 4096.
    pub fn max_queue_size(&self) -> usize {
        self.max_queue_size
            .or_else(|| env_var("OTEL_BLRP_MAX_QUEUE_SIZE"))
            .unwrap_or(DEFAULT_MAX_QUEUE_SIZE)
    }

    /// Max export batch size. Fallback: JSON > OTEL_BLRP_MAX_EXPORT_BATCH_SIZE > 256.
    pub fn max_export_batch_size(&self) -> usize {
        self.max_export_batch_size
            .or_else(|| env_var("OTEL_BLRP_MAX_EXPORT_BATCH_SIZE"))
            .unwrap_or(DEFAULT_LOG_MAX_EXPORT_BATCH_SIZE)
    }

    /// Export interval in ms. Fallback: JSON > OTEL_BLRP_SCHEDULE_DELAY > 5000.
    pub fn export_interval_ms(&self) -> u64 {
        self.export_interval_ms
            .or_else(|| env_var("OTEL_BLRP_SCHEDULE_DELAY"))
            .unwrap_or(DEFAULT_LOG_EXPORT_INTERVAL_MS)
    }

    /// Export timeout in ms. Fallback: JSON > OTEL_EXPORTER_OTLP_LOGS_TIMEOUT > OTEL_EXPORTER_OTLP_TIMEOUT > 10000.
    pub fn export_timeout_ms(&self) -> u64 {
        self.export_timeout_ms
            .or_else(|| env_var("OTEL_EXPORTER_OTLP_LOGS_TIMEOUT"))
            .or_else(|| env_var("OTEL_EXPORTER_OTLP_TIMEOUT"))
            .unwrap_or(DEFAULT_EXPORT_TIMEOUT_MS)
    }

    /// Creates an OTLP export configuration for logs.
    pub fn create_export_config(&self) -> opentelemetry_otlp::ExportConfig {
        opentelemetry_otlp::ExportConfig {
            endpoint: Some(self.otlp_endpoint()),
            protocol: opentelemetry_otlp::Protocol::Grpc,
            timeout: Some(std::time::Duration::from_millis(self.export_timeout_ms())),
        }
    }
}

// ============================================================================
// Provider Creation
// ============================================================================

/// Type alias for a boxed tracing subscriber layer
type BoxedLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;

/// Creates an OpenTelemetry logging provider with OTLP export and tracing layers.
///
/// Returns provider and subscriber layers.
/// Returns `None` provider if logging is disabled in config.
///
/// # Errors
///
/// Returns an error if the OTLP log exporter fails to build.
///
/// # Example
/// ```rust,ignore
/// use documentdb_gateway::telemetry::{config::LoggingConfig, logging::create_logging_provider};
///
/// let config = LoggingConfig::default();
/// let resource = opentelemetry_sdk::Resource::default();
/// let (provider, layers) = create_logging_provider(&config, resource)?;
/// ```
pub fn create_logging_provider(
    config: &LoggingConfig,
    resource: Resource,
) -> Result<(Option<SdkLoggerProvider>, Vec<BoxedLayer>)> {
    let mut layers: Vec<BoxedLayer> = Vec::new();

    let logger_provider = if config.logging_enabled() {
        let exporter = opentelemetry_otlp::LogExporter::builder()
            .with_tonic()
            .with_export_config(config.create_export_config())
            .build()
            .map_err(|e| {
                DocumentDBError::internal_error(format!("failed to build log exporter: {e}"))
            })?;

        let batch_config = BatchConfigBuilder::default()
            .with_max_queue_size(config.max_queue_size())
            .with_max_export_batch_size(config.max_export_batch_size())
            .with_scheduled_delay(Duration::from_millis(config.export_interval_ms()))
            .build();

        let log_processor = BatchLogProcessor::builder(exporter)
            .with_batch_config(batch_config)
            .build();

        let provider = SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_log_processor(log_processor)
            .build();

        let otel_layer = OpenTelemetryTracingBridge::new(&provider)
            .with_filter(get_env_filter(&config.level()))
            .boxed();

        layers.push(otel_layer);
        Some(provider)
    } else {
        None
    };

    Ok((logger_provider, layers))
}

/// Creates an `EnvFilter` from `RUST_LOG` env var, falling back to the provided level.
pub fn get_env_filter(log_level: &str) -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::builder().parse(log_level).unwrap_or_else(|e| {
            eprintln!("Warning: Invalid log level '{log_level}': {e}. Falling back to 'info'");
            EnvFilter::builder()
                .parse("info")
                .expect("'info' should be a valid log level")
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::KeyValue;
    use std::env;

    /// Helper to temporarily set env vars, restoring on drop.
    struct EnvGuard(Vec<(String, Option<String>)>);

    impl EnvGuard {
        fn set(key: &str, value: &str) -> Self {
            let original = env::var(key).ok();
            env::set_var(key, value);
            Self(vec![(key.to_string(), original)])
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, original) in self.0.drain(..) {
                match original {
                    Some(val) => env::set_var(&key, val),
                    None => env::remove_var(&key),
                }
            }
        }
    }

    #[test]
    fn test_logging_config_uses_env_var() {
        let _guard = EnvGuard::set("RUST_LOG", "debug");
        let config = LoggingConfig::new(None);
        assert_eq!(config.level(), "debug");
    }

    #[test]
    fn test_logging_config_json_overrides_env() {
        let _guard = EnvGuard::set("RUST_LOG", "warn");

        // Test with JSON config - should override env var
        let json_config = LoggingOptions {
            level: Some("debug".to_string()),
            console_enabled: Some(true),
            ..Default::default()
        };
        let config = LoggingConfig::new(Some(&json_config));
        assert_eq!(config.level(), "debug");
        assert!(config.console_enabled());

        // Test with no JSON config - should use env var
        let config = LoggingConfig::new(None);
        assert_eq!(config.level(), "warn");
    }

    #[tokio::test]
    async fn test_create_logging_provider_when_disabled() {
        let json_config = LoggingOptions {
            enabled: Some(false),
            ..Default::default()
        };
        let config = LoggingConfig::new(Some(&json_config));

        let attributes = vec![
            KeyValue::new("service.name", "test-service"),
            KeyValue::new("service.version", "1.0.0"),
        ];
        let resource = Resource::builder().with_attributes(attributes).build();

        let result = create_logging_provider(&config, resource);
        assert!(result.is_ok());
        let (provider, layers) = result.unwrap();
        assert!(provider.is_none());
        assert!(layers.is_empty());
    }

    #[tokio::test]
    async fn test_create_logging_provider_when_enabled() {
        let json_config = LoggingOptions {
            enabled: Some(true),
            level: Some("DEBUG".to_string()),
            ..Default::default()
        };
        let config = LoggingConfig::new(Some(&json_config));

        let attributes = vec![
            KeyValue::new("service.name", "logging-service"),
            KeyValue::new("environment", "test"),
        ];
        let resource = Resource::builder().with_attributes(attributes).build();

        let result = create_logging_provider(&config, resource);
        assert!(result.is_ok());
        let (provider, layers) = result.unwrap();
        assert!(provider.is_some());
        assert_eq!(layers.len(), 1);
    }

    #[test]
    fn test_get_env_filter_invalid_level_does_not_panic() {
        // Invalid log level should not panic - falls back to "info"
        let _filter = get_env_filter("not_a_valid_level!!!");
        // If we get here without panicking, the fallback worked
    }
}
