/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/telemetry/tracing.rs
 *
 *-------------------------------------------------------------------------
 */

use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    trace::{Sampler, SdkTracerProvider},
    Resource,
};
use serde::Deserialize;

use crate::{
    error::{DocumentDBError, Result},
    telemetry::config::{
        env_var, parse_resource_attributes, DEFAULT_EXPORT_TIMEOUT_MS, DEFAULT_OTLP_ENDPOINT,
    },
};

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_TRACING_ENABLED: bool = false;
const DEFAULT_SAMPLING_RATIO: f64 = 0.1;
const DEFAULT_EXPORT_INTERVAL_MS: u64 = 5000;
const DEFAULT_MAX_EXPORT_BATCH_SIZE: usize = 512;

// ============================================================================
// JSON Configuration
// ============================================================================

/// JSON configuration for tracing (matches SetupConfiguration.json TelemetryOptions.Tracing)
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct TracingOptions {
    /// Whether tracing is enabled
    pub enabled: Option<bool>,
    /// OTLP endpoint for trace export
    pub otlp_endpoint: Option<String>,
    /// Sampling ratio (0.0 to 1.0)
    pub sampling_ratio: Option<f64>,
    /// Export interval in milliseconds
    pub export_interval_ms: Option<u64>,
    /// Maximum batch size for export
    pub max_export_batch_size: Option<usize>,
    /// Export timeout in milliseconds
    pub export_timeout_ms: Option<u64>,
}

// ============================================================================
// Runtime Configuration
// ============================================================================

/// Runtime configuration for distributed tracing with OTLP export.
///
/// Stores JSON configuration values and provides accessor methods that implement
/// the fallback logic: JSON value > environment variable > default constant.
#[derive(Debug, Clone)]
pub struct TracingConfig {
    enabled: Option<bool>,
    otlp_endpoint: Option<String>,
    sampling_ratio: Option<f64>,
    export_interval_ms: Option<u64>,
    max_export_batch_size: Option<usize>,
    export_timeout_ms: Option<u64>,
}

impl TracingConfig {
    /// Creates tracing config from optional JSON configuration.
    ///
    /// Accessor methods implement fallback: JSON > env vars > defaults.
    pub fn new(json_config: Option<&TracingOptions>) -> Self {
        let json = json_config.cloned().unwrap_or_default();

        Self {
            enabled: json.enabled,
            otlp_endpoint: json.otlp_endpoint,
            sampling_ratio: json.sampling_ratio,
            export_interval_ms: json.export_interval_ms,
            max_export_batch_size: json.max_export_batch_size,
            export_timeout_ms: json.export_timeout_ms,
        }
    }

    /// Whether tracing is enabled. Fallback: JSON > OTEL_TRACING_ENABLED > true.
    pub fn tracing_enabled(&self) -> bool {
        self.enabled
            .or_else(|| env_var("OTEL_TRACING_ENABLED"))
            .unwrap_or(DEFAULT_TRACING_ENABLED)
    }

    /// OTLP endpoint for traces. Fallback: JSON > OTEL_EXPORTER_OTLP_TRACES_ENDPOINT > OTEL_EXPORTER_OTLP_ENDPOINT > default.
    pub fn otlp_endpoint(&self) -> String {
        self.otlp_endpoint
            .clone()
            .or_else(|| env_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
            .or_else(|| env_var("OTEL_EXPORTER_OTLP_ENDPOINT"))
            .unwrap_or_else(|| DEFAULT_OTLP_ENDPOINT.to_string())
    }

    /// Sampling ratio (0.0 to 1.0). Fallback: JSON > OTEL_TRACES_SAMPLER_ARG > 0.1.
    pub fn sampling_ratio(&self) -> f64 {
        self.sampling_ratio
            .or_else(|| env_var("OTEL_TRACES_SAMPLER_ARG"))
            .unwrap_or(DEFAULT_SAMPLING_RATIO)
            .clamp(0.0, 1.0)
    }

    /// Resource attributes from OTEL_RESOURCE_ATTRIBUTES env var.
    pub fn resource_attributes(&self) -> Vec<KeyValue> {
        parse_resource_attributes()
    }

    /// Export interval in ms. Fallback: JSON > OTEL_BSP_SCHEDULE_DELAY > 5000.
    pub fn export_interval_ms(&self) -> u64 {
        self.export_interval_ms
            .or_else(|| env_var("OTEL_BSP_SCHEDULE_DELAY"))
            .unwrap_or(DEFAULT_EXPORT_INTERVAL_MS)
    }

    /// Max export batch size. Fallback: JSON > OTEL_BSP_MAX_EXPORT_BATCH_SIZE > 512.
    pub fn max_export_batch_size(&self) -> usize {
        self.max_export_batch_size
            .or_else(|| env_var("OTEL_BSP_MAX_EXPORT_BATCH_SIZE"))
            .unwrap_or(DEFAULT_MAX_EXPORT_BATCH_SIZE)
    }

    /// Export timeout in ms. Fallback: JSON > OTEL_EXPORTER_OTLP_TRACES_TIMEOUT > OTEL_EXPORTER_OTLP_TIMEOUT > 10000.
    pub fn export_timeout_ms(&self) -> u64 {
        self.export_timeout_ms
            .or_else(|| env_var("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT"))
            .or_else(|| env_var("OTEL_EXPORTER_OTLP_TIMEOUT"))
            .unwrap_or(DEFAULT_EXPORT_TIMEOUT_MS)
    }

    /// Creates an OTLP export configuration for traces.
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

/// Creates an OpenTelemetry tracer provider with OTLP export.
///
/// Returns `None` if tracing is disabled in config.
///
/// # Errors
///
/// Returns an error if the OTLP span exporter fails to build.
///
/// # Example
/// ```rust,ignore
/// use opentelemetry::KeyValue;
/// use opentelemetry_sdk::Resource;
/// use documentdb_gateway::telemetry::config::TracingConfig;
///
/// let config = TracingConfig::default();
/// let attrs = vec![KeyValue::new("service.name", "my-gateway")];
/// let resource = Resource::builder().with_attributes(attrs).build();
/// let provider = create_tracer_provider(&config, resource)?;
/// ```
pub fn create_tracer_provider(
    config: &TracingConfig,
    resource: Resource,
) -> Result<Option<SdkTracerProvider>> {
    let tracer_provider = if config.tracing_enabled() {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_export_config(config.create_export_config())
            .build()
            .map_err(|e| {
                DocumentDBError::internal_error(format!("failed to build tracer exporter: {e}"))
            })?;

        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            // Use parent-based sampling: honors upstream sampling decisions when client provides
            // trace context, otherwise samples at configured ratio for root spans
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                config.sampling_ratio(),
            ))))
            .with_resource(resource)
            .build();

        Some(provider)
    } else {
        None
    };

    Ok(tracer_provider)
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
    fn test_tracing_config_uses_env_var() {
        let _guard = EnvGuard::set("OTEL_TRACES_SAMPLER_ARG", "0.5");
        let config = TracingConfig::new(None);
        assert!((config.sampling_ratio() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_tracing_config_sampling_ratio_clamped() {
        let _guard = EnvGuard::set("OTEL_TRACES_SAMPLER_ARG", "2.0");
        let config = TracingConfig::new(None);
        assert!((config.sampling_ratio() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_tracing_config_json_overrides_env() {
        let _guard = EnvGuard::set("OTEL_TRACES_SAMPLER_ARG", "0.3");

        // Test with JSON config present - should override env var
        let json_config = TracingOptions {
            enabled: Some(true),
            sampling_ratio: Some(0.8),
            otlp_endpoint: Some("http://json-endpoint:4317".to_string()),
            ..Default::default()
        };
        let config = TracingConfig::new(Some(&json_config));
        assert!((config.sampling_ratio() - 0.8).abs() < f64::EPSILON);
        assert_eq!(config.otlp_endpoint(), "http://json-endpoint:4317");

        // Test with no JSON config - should use env var
        let config = TracingConfig::new(None);
        assert!((config.sampling_ratio() - 0.3).abs() < f64::EPSILON);
    }

    #[test]
    fn test_tracing_config_json_clamps_sampling_ratio() {
        // Test that sampling ratio is clamped even from JSON
        let json_config = TracingOptions {
            sampling_ratio: Some(2.0), // Invalid: > 1.0
            ..Default::default()
        };
        let config = TracingConfig::new(Some(&json_config));
        assert!((config.sampling_ratio() - 1.0).abs() < f64::EPSILON);

        let json_config = TracingOptions {
            sampling_ratio: Some(-0.5), // Invalid: < 0.0
            ..Default::default()
        };
        let config = TracingConfig::new(Some(&json_config));
        assert!((config.sampling_ratio() - 0.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_create_tracer_provider_when_disabled() {
        let json_config = TracingOptions {
            enabled: Some(false),
            ..Default::default()
        };
        let config = TracingConfig::new(Some(&json_config));

        let attributes = vec![
            KeyValue::new("service.name", "test-service"),
            KeyValue::new("service.version", "1.0.0"),
        ];
        let resource = Resource::builder().with_attributes(attributes).build();

        let result = create_tracer_provider(&config, resource);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_create_tracer_provider_when_enabled() {
        let json_config = TracingOptions {
            enabled: Some(true),
            sampling_ratio: Some(0.5),
            ..Default::default()
        };
        let config = TracingConfig::new(Some(&json_config));

        let attributes = vec![
            KeyValue::new("service.name", "test-service"),
            KeyValue::new("environment", "test"),
        ];
        let resource = Resource::builder().with_attributes(attributes).build();

        let result = create_tracer_provider(&config, resource);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }
}
