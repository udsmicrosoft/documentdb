/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/telemetry/metrics.rs
 *
 *-------------------------------------------------------------------------
 */

use std::time::Duration;

use either::Either;
use opentelemetry::{global, metrics::Counter, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider, Temporality},
    Resource,
};
use serde::Deserialize;

use crate::{
    context::ConnectionContext,
    error::{DocumentDBError, Result},
    protocol::header::Header,
    requests::{request_tracker::RequestTracker, Request, RequestIntervalKind},
    responses::{CommandError, Response},
    telemetry::config::{
        env_var, parse_resource_attributes, DEFAULT_EXPORT_TIMEOUT_MS, DEFAULT_OTLP_ENDPOINT,
    },
};

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_METRICS_ENABLED: bool = true;
const DEFAULT_COLLECTION_INTERVAL_MS: u64 = 15000;

// ============================================================================
// JSON Configuration
// ============================================================================

/// JSON configuration for metrics (matches SetupConfiguration.json TelemetryOptions.Metrics)
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsOptions {
    /// Whether metrics are enabled
    pub enabled: Option<bool>,
    /// OTLP endpoint for metrics export
    pub otlp_endpoint: Option<String>,
    /// Export interval in milliseconds
    pub export_interval_ms: Option<u64>,
    /// Export timeout in milliseconds
    pub export_timeout_ms: Option<u64>,
}

// ============================================================================
// Runtime Configuration
// ============================================================================

/// Runtime configuration for metrics collection with OTLP export.
///
/// Stores JSON configuration values and provides accessor methods that implement
/// the fallback logic: JSON value > environment variable > default constant.
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    enabled: Option<bool>,
    otlp_endpoint: Option<String>,
    export_interval_ms: Option<u64>,
    export_timeout_ms: Option<u64>,
}

impl MetricsConfig {
    /// Creates metrics config from optional JSON configuration.
    ///
    /// Accessor methods implement fallback: JSON > env vars > defaults.
    pub fn new(json_config: Option<&MetricsOptions>) -> Self {
        let json = json_config.cloned().unwrap_or_default();

        Self {
            enabled: json.enabled,
            otlp_endpoint: json.otlp_endpoint,
            export_interval_ms: json.export_interval_ms,
            export_timeout_ms: json.export_timeout_ms,
        }
    }

    /// Whether metrics are enabled. Fallback: JSON > OTEL_METRICS_ENABLED > true.
    pub fn metrics_enabled(&self) -> bool {
        self.enabled
            .or_else(|| env_var("OTEL_METRICS_ENABLED"))
            .unwrap_or(DEFAULT_METRICS_ENABLED)
    }

    /// OTLP endpoint for metrics. Fallback: JSON > OTEL_EXPORTER_OTLP_METRICS_ENDPOINT > OTEL_EXPORTER_OTLP_ENDPOINT > default.
    pub fn otlp_endpoint(&self) -> String {
        self.otlp_endpoint
            .clone()
            .or_else(|| env_var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"))
            .or_else(|| env_var("OTEL_EXPORTER_OTLP_ENDPOINT"))
            .unwrap_or_else(|| DEFAULT_OTLP_ENDPOINT.to_string())
    }

    /// Export interval in ms. Fallback: JSON > OTEL_METRIC_EXPORT_INTERVAL > 60000.
    pub fn export_interval_ms(&self) -> u64 {
        self.export_interval_ms
            .or_else(|| env_var("OTEL_METRIC_EXPORT_INTERVAL"))
            .unwrap_or(DEFAULT_COLLECTION_INTERVAL_MS)
    }

    /// Export timeout in ms. Fallback: JSON > OTEL_EXPORTER_OTLP_METRICS_TIMEOUT > OTEL_EXPORTER_OTLP_TIMEOUT > 10000.
    pub fn export_timeout_ms(&self) -> u64 {
        self.export_timeout_ms
            .or_else(|| env_var("OTEL_EXPORTER_OTLP_METRICS_TIMEOUT"))
            .or_else(|| env_var("OTEL_EXPORTER_OTLP_TIMEOUT"))
            .unwrap_or(DEFAULT_EXPORT_TIMEOUT_MS)
    }

    /// Resource attributes from OTEL_RESOURCE_ATTRIBUTES env var.
    pub fn resource_attributes(&self) -> Vec<KeyValue> {
        parse_resource_attributes()
    }

    /// Creates an OTLP export configuration for metrics.
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

/// Creates an OpenTelemetry meter provider with periodic OTLP export.
///
/// Returns `None` if metrics are disabled in config.
///
/// # Errors
///
/// Returns an error if the OTLP metrics exporter fails to build.
///
/// # Example
/// ```rust,ignore
/// use opentelemetry::KeyValue;
/// use opentelemetry_sdk::Resource;
/// use documentdb_gateway::telemetry::config::MetricsConfig;
/// let config = MetricsConfig::default();
/// let attrs = vec![KeyValue::new("service.name", "my-gateway")];
/// let resource = Resource::builder().with_attributes(attrs).build();
/// let provider = create_metrics_provider(&config, resource)?;
/// ```
pub fn create_metrics_provider(
    config: &MetricsConfig,
    resource: Resource,
) -> Result<Option<SdkMeterProvider>> {
    if !config.metrics_enabled() {
        return Ok(None);
    }

    // Build the OTLP exporter with:
    // - Delta temporality: Counters emit delta values (change since last export).
    //   The OTel Collector should aggregate deltas into cumulative for Prometheus.
    // - Tonic: Use gRPC via tonic library for transport
    // - Export config: Endpoint and timeout settings
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_temporality(Temporality::Delta)
        .with_tonic()
        .with_export_config(config.create_export_config())
        .build()
        .map_err(|e| {
            DocumentDBError::internal_error(format!("failed to build metrics exporter: {e}"))
        })?;

    // Create a periodic reader that exports metrics at regular intervals
    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_millis(config.export_interval_ms()))
        .build();

    // Build the meter provider with the resource and reader
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader)
        .build();

    Ok(Some(meter_provider))
}

/// Records request-level metrics using low-memory Counters.
///
/// See: https://opentelemetry.io/docs/specs/semconv/database/database-metrics/
///
/// Emits metrics for each request including:
/// - `db.client.operation.duration.total` (seconds) - sum of all durations
/// - `db.client.operations` (count) - number of operations
/// - `db.client.request.size.total` (bytes) - sum of request sizes
/// - `db.client.response.size.total` (bytes) - sum of response sizes
///
/// Aggregation (averages, percentiles) is delegated to the collector.
#[derive(Clone)]
pub struct OtelTelemetryProvider {
    /// Total duration of all operations (seconds). Divide by operations count for average.
    operation_duration_total: Counter<f64>,
    /// Count of operations. Use with duration_total to compute average latency.
    operations_count: Counter<u64>,
    /// Total request payload bytes.
    request_size_total: Counter<u64>,
    /// Total response payload bytes.
    response_size_total: Counter<u64>,
}

impl OtelTelemetryProvider {
    pub fn new() -> Self {
        let meter = global::meter("documentdb_gateway");

        Self {
            operation_duration_total: meter
                .f64_counter("db.client.operation.duration.total")
                .with_description("Total duration of database client operations (sum)")
                .with_unit("s")
                .build(),
            operations_count: meter
                .u64_counter("db.client.operations")
                .with_description("Count of database client operations")
                .with_unit("{operation}")
                .build(),
            request_size_total: meter
                .u64_counter("db.client.request.size.total")
                .with_description("Total size of database client request payloads")
                .with_unit("By")
                .build(),
            response_size_total: meter
                .u64_counter("db.client.response.size.total")
                .with_description("Total size of database client response payloads")
                .with_unit("By")
                .build(),
        }
    }

    fn record_request_metrics(
        &self,
        header: &Header,
        request: Option<&Request<'_>>,
        response: Either<&Response, (&CommandError, usize)>,
        collection: &str,
        request_tracker: &RequestTracker,
    ) {
        let operation = request
            .map(|r| r.request_type().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let db_name = request.and_then(|r| r.db().ok()).unwrap_or("unknown");
        let db_system = KeyValue::new("db.system.name", "documentdb");
        let db_operation = KeyValue::new("db.operation.name", operation);
        let db_collection = KeyValue::new("db.collection.name", collection.to_string());
        let db_namespace = KeyValue::new("db.namespace", db_name.to_string());

        let duration_to_secs =
            |ns: i64| -> f64 { Duration::from_nanos(ns.max(0) as u64).as_secs_f64() };

        let duration_ns =
            request_tracker.get_interval_elapsed_time(RequestIntervalKind::HandleRequest);

        // Build attributes based on success/failure
        let base_attrs: Vec<KeyValue> = match &response {
            Either::Left(_) => {
                vec![
                    db_system.clone(),
                    db_operation.clone(),
                    db_collection.clone(),
                    db_namespace.clone(),
                ]
            }
            Either::Right((err, _)) => {
                vec![
                    db_system.clone(),
                    db_operation.clone(),
                    db_collection.clone(),
                    db_namespace.clone(),
                    KeyValue::new("error.type", err.code.to_string()),
                ]
            }
        };

        // Record operation count and total duration
        self.operations_count.add(1, &base_attrs);
        self.operation_duration_total
            .add(duration_to_secs(duration_ns), &base_attrs);

        // Record request/response sizes
        self.request_size_total
            .add(header.length as u64, &base_attrs);

        let response_size_bytes = match &response {
            Either::Left(resp) => resp
                .as_raw_document()
                .map(|doc| doc.as_bytes().len() as u64)
                .unwrap_or(0),
            Either::Right((_, size)) => *size as u64,
        };
        self.response_size_total
            .add(response_size_bytes, &base_attrs);

        // Record PostgreSQL phase breakdown (duration totals)
        let pg_begin_ns = request_tracker
            .get_interval_elapsed_time(RequestIntervalKind::PostgresBeginTransaction);
        if pg_begin_ns > 0 {
            let mut attrs = base_attrs.clone();
            attrs.push(KeyValue::new(
                "db.operation.phase",
                "postgres_begin_transaction",
            ));
            self.operation_duration_total
                .add(duration_to_secs(pg_begin_ns), &attrs);
        }

        let pg_exec_ns =
            request_tracker.get_interval_elapsed_time(RequestIntervalKind::ProcessRequest);
        if pg_exec_ns > 0 {
            let mut attrs = base_attrs.clone();
            attrs.push(KeyValue::new("db.operation.phase", "postgres_execution"));
            self.operation_duration_total
                .add(duration_to_secs(pg_exec_ns), &attrs);
        }

        let pg_commit_ns = request_tracker
            .get_interval_elapsed_time(RequestIntervalKind::PostgresCommitTransaction);
        if pg_commit_ns > 0 {
            let mut attrs = base_attrs.clone();
            attrs.push(KeyValue::new("db.operation.phase", "postgres_commit"));
            self.operation_duration_total
                .add(duration_to_secs(pg_commit_ns), &attrs);
        }
    }
}

impl Default for OtelTelemetryProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl crate::telemetry::TelemetryProvider for OtelTelemetryProvider {
    async fn emit_request_event(
        &self,
        _connection_context: &ConnectionContext,
        header: &Header,
        request: Option<&Request<'_>>,
        response: Either<&Response, (&CommandError, usize)>,
        collection: String,
        request_tracker: &RequestTracker,
        activity_id: &str,
        user_agent: &str,
    ) {
        // Record activity_id and user_agent on the current span for correlation
        // activity_id: Gateway-internal correlation ID (for Geneva log compatibility)
        // user_agent: Client driver info (e.g., "PyMongo/4.14.1")
        let span = tracing::Span::current();
        span.record("activity_id", activity_id);
        span.record("user_agent", user_agent);

        // Delegate to the inherent method for metrics recording
        self.record_request_metrics(header, request, response, &collection, request_tracker);
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
    fn test_metrics_config_uses_env_var() {
        let _guard = EnvGuard::set("OTEL_METRIC_EXPORT_INTERVAL", "30000");
        let config = MetricsConfig::new(None);
        assert_eq!(config.export_interval_ms(), 30000);
    }

    #[test]
    fn test_metrics_config_json_overrides_env() {
        let _guard = EnvGuard::set("OTEL_METRIC_EXPORT_INTERVAL", "45000");

        // Test with JSON config - should override env var
        let json_config = MetricsOptions {
            enabled: Some(false),
            export_interval_ms: Some(30000),
            ..Default::default()
        };
        let config = MetricsConfig::new(Some(&json_config));
        assert!(!config.metrics_enabled());
        assert_eq!(config.export_interval_ms(), 30000);

        // Test with no JSON config - should use env var
        let config = MetricsConfig::new(None);
        assert_eq!(config.export_interval_ms(), 45000);
    }

    #[tokio::test]
    async fn test_create_metrics_provider_when_disabled() {
        let json_config = MetricsOptions {
            enabled: Some(false),
            ..Default::default()
        };
        let config = MetricsConfig::new(Some(&json_config));

        let attributes = vec![
            KeyValue::new("service.name", "test-service"),
            KeyValue::new("service.version", "1.0.0"),
        ];
        let resource = Resource::builder().with_attributes(attributes).build();

        let result = create_metrics_provider(&config, resource);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_create_metrics_provider_when_enabled() {
        let json_config = MetricsOptions {
            enabled: Some(true),
            ..Default::default()
        };
        let config = MetricsConfig::new(Some(&json_config));

        let attributes = vec![
            KeyValue::new("service.name", "metrics-service"),
            KeyValue::new("environment", "test"),
        ];
        let resource = Resource::builder().with_attributes(attributes).build();

        let result = create_metrics_provider(&config, resource);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_request_metrics_creation() {
        // Verify OtelTelemetryProvider can be created and instruments are initialized
        let metrics = OtelTelemetryProvider::new();

        // Verify we can clone the metrics
        let _cloned = metrics.clone();
    }
}
