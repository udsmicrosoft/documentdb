/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/telemetry/config.rs
 *
 * Shared telemetry configuration types and helpers.
 * Follows SetupConfiguration pattern: accessor methods resolve JSON > env > default.
 *
 *-------------------------------------------------------------------------
 */

use std::env;

use opentelemetry::KeyValue;
use serde::Deserialize;

use crate::telemetry::{
    logging::{LoggingConfig, LoggingOptions},
    metrics::{MetricsConfig, MetricsOptions},
    tracing::{TracingConfig, TracingOptions},
};

// ============================================================================
// Shared Constants
// ============================================================================

pub(crate) const DEFAULT_OTLP_ENDPOINT: &str = "http://localhost:4317";
pub(crate) const DEFAULT_EXPORT_TIMEOUT_MS: u64 = 10000;
const DEFAULT_SERVICE_NAME: &str = env!("CARGO_CRATE_NAME");
const DEFAULT_SERVICE_VERSION: &str = env!("CARGO_PKG_VERSION");

// ============================================================================
// Shared Helper Functions
// ============================================================================

/// Parse env var into Option<T>, returning None if missing or invalid.
pub(crate) fn env_var<T: std::str::FromStr>(var: &str) -> Option<T> {
    env::var(var).ok().and_then(|v| v.parse().ok())
}

/// Parse OTEL_RESOURCE_ATTRIBUTES into KeyValue pairs.
pub(crate) fn parse_resource_attributes() -> Vec<KeyValue> {
    env::var("OTEL_RESOURCE_ATTRIBUTES")
        .unwrap_or_default()
        .split(',')
        .filter_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            Some(KeyValue::new(
                key.trim().to_owned(),
                value.trim().to_owned(),
            ))
        })
        .collect()
}

// ============================================================================
// JSON Configuration
// ============================================================================

/// JSON configuration for telemetry (matches SetupConfiguration.json TelemetryOptions section)
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct TelemetryOptions {
    /// Service name for telemetry identification
    pub service_name: Option<String>,
    /// Service version for telemetry identification
    pub service_version: Option<String>,
    /// Tracing configuration
    pub tracing: Option<TracingOptions>,
    /// Metrics configuration
    pub metrics: Option<MetricsOptions>,
    /// Logging configuration
    pub logging: Option<LoggingOptions>,
}

// ============================================================================
// Runtime Configuration
// ============================================================================

/// Unified runtime configuration for all telemetry signals.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    service_name: Option<String>,
    service_version: Option<String>,
    tracing: TracingConfig,
    metrics: MetricsConfig,
    logging: LoggingConfig,
}

impl TelemetryConfig {
    pub fn new(json_config: Option<&TelemetryOptions>) -> Self {
        let json = json_config.cloned().unwrap_or_default();

        Self {
            service_name: json.service_name,
            service_version: json.service_version,
            tracing: TracingConfig::new(json.tracing.as_ref()),
            metrics: MetricsConfig::new(json.metrics.as_ref()),
            logging: LoggingConfig::new(json.logging.as_ref()),
        }
    }

    pub fn service_name(&self) -> String {
        self.service_name
            .clone()
            .or_else(|| env_var("OTEL_SERVICE_NAME"))
            .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_string())
    }

    pub fn service_version(&self) -> String {
        self.service_version
            .clone()
            .or_else(|| env_var("OTEL_SERVICE_VERSION"))
            .unwrap_or_else(|| DEFAULT_SERVICE_VERSION.to_string())
    }

    pub fn tracing(&self) -> &TracingConfig {
        &self.tracing
    }

    pub fn metrics(&self) -> &MetricsConfig {
        &self.metrics
    }

    pub fn logging(&self) -> &LoggingConfig {
        &self.logging
    }

    /// Returns true if any telemetry signal (tracing, metrics, or logging) is enabled.
    pub fn any_signal_enabled(&self) -> bool {
        self.tracing.tracing_enabled()
            || self.metrics.metrics_enabled()
            || self.logging.logging_enabled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    /// Helper to temporarily set env vars, restoring on drop.
    struct EnvGuard(Vec<(String, Option<String>)>);

    impl EnvGuard {
        fn set(key: &str, value: &str) -> Self {
            let original = env::var(key).ok();
            env::set_var(key, value);
            Self(vec![(key.to_string(), original)])
        }

        fn remove(key: &str) -> Self {
            let original = env::var(key).ok();
            env::remove_var(key);
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
    fn test_env_var_parses_value() {
        let _guard = EnvGuard::set("TEST_ENV_VAR", "100");
        assert_eq!(env_var::<u64>("TEST_ENV_VAR"), Some(100));
        assert_eq!(env_var::<u64>("TEST_MISSING"), None);
    }

    #[test]
    fn test_parse_resource_attributes() {
        let _guard = EnvGuard::set("OTEL_RESOURCE_ATTRIBUTES", "key1=val1,key2=val2");
        let result = parse_resource_attributes();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].key.as_str(), "key1");
    }

    #[test]
    fn test_telemetry_config_uses_json_value() {
        let json_config = TelemetryOptions {
            service_name: Some("json-service".to_string()),
            service_version: Some("1.0.0".to_string()),
            ..Default::default()
        };
        let config = TelemetryConfig::new(Some(&json_config));
        assert_eq!(config.service_name(), "json-service");
        assert_eq!(config.service_version(), "1.0.0");
    }

    #[test]
    fn test_telemetry_config_uses_env_when_json_missing() {
        let _guard = EnvGuard::set("OTEL_SERVICE_NAME", "env-service");
        let config = TelemetryConfig::new(None);
        assert_eq!(config.service_name(), "env-service");
    }

    #[test]
    fn test_telemetry_config_uses_default_when_both_missing() {
        let _guard = EnvGuard::remove("OTEL_SERVICE_NAME");
        let config = TelemetryConfig::new(None);
        assert_eq!(config.service_name(), DEFAULT_SERVICE_NAME);
    }

    #[test]
    fn test_telemetry_config_json_overrides_env() {
        let _guard = EnvGuard::set("OTEL_SERVICE_NAME", "env-service");
        let json_config = TelemetryOptions {
            service_name: Some("json-service".to_string()),
            ..Default::default()
        };
        let config = TelemetryConfig::new(Some(&json_config));
        // JSON should take priority over env
        assert_eq!(config.service_name(), "json-service");
    }

    #[test]
    fn test_telemetry_config_with_full_json() {
        let json_config = TelemetryOptions {
            service_name: Some("json-service".to_string()),
            service_version: Some("1.0.0".to_string()),
            tracing: Some(TracingOptions {
                enabled: Some(true),
                sampling_ratio: Some(0.5),
                ..Default::default()
            }),
            metrics: Some(MetricsOptions {
                enabled: Some(false),
                ..Default::default()
            }),
            logging: Some(LoggingOptions {
                level: Some("error".to_string()),
                ..Default::default()
            }),
        };
        let config = TelemetryConfig::new(Some(&json_config));
        assert_eq!(config.service_name(), "json-service");
        assert_eq!(config.service_version(), "1.0.0");
        assert!((config.tracing().sampling_ratio() - 0.5).abs() < f64::EPSILON);
        assert!(!config.metrics().metrics_enabled());
        assert_eq!(config.logging().level(), "error");
    }

    #[test]
    fn test_telemetry_config_partial_json() {
        let _guard = EnvGuard::set("OTEL_METRIC_EXPORT_INTERVAL", "90000");

        let json_config = TelemetryOptions {
            service_name: Some("partial-service".to_string()),
            // No tracing, metrics, or logging sections
            ..Default::default()
        };
        let config = TelemetryConfig::new(Some(&json_config));

        // Service name from JSON
        assert_eq!(config.service_name(), "partial-service");
        // Metrics interval from env var
        assert_eq!(config.metrics().export_interval_ms(), 90000);
        // Tracing defaults to disabled
        assert!(!config.tracing().tracing_enabled());
    }
}
