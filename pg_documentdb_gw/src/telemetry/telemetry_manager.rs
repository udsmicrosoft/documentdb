/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/telemetry/telemetry_manager.rs
 *
 *-------------------------------------------------------------------------
 */

use crate::{
    error::{DocumentDBError, Result},
    telemetry::{
        config::TelemetryConfig,
        logging::{self, create_logging_provider},
        metrics::create_metrics_provider,
        tracing::create_tracer_provider,
    },
};
use opentelemetry::trace::TracerProvider;
use opentelemetry::{global, KeyValue};
use opentelemetry_sdk::{
    logs::SdkLoggerProvider, metrics::SdkMeterProvider, trace::SdkTracerProvider,
};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, Layer};

/// Manages OpenTelemetry providers for tracing, metrics, and logging.
///
/// Handles initialization, configuration, and cleanup of all telemetry components.
///
/// # Example
/// ```rust,ignore
/// use documentdb_gateway::telemetry::{TelemetryConfig, TelemetryManager};
/// use opentelemetry::KeyValue;
///
/// # fn example() -> Result<()> {
/// let config = TelemetryConfig::from_env();
/// let attributes = vec![KeyValue::new("service.instance.id", "gateway-1")];
///
/// let telemetry = TelemetryManager::init_telemetry(config, attributes)?;
/// // ... application code ...
/// telemetry.shutdown()?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// [`init_telemetry`](Self::init_telemetry) returns an error if any telemetry provider fails to initialize.
/// [`shutdown`](Self::shutdown) returns an error if any provider fails to shutdown cleanly.
pub struct TelemetryManager {
    meter_provider: Option<SdkMeterProvider>,
    tracer_provider: Option<SdkTracerProvider>,
    logger_provider: Option<SdkLoggerProvider>,

    /// Worker guards for non-blocking logging.
    /// These guards must be kept alive to ensure any remaining logs are flushed when the program terminates.
    /// Dropping the guards will flush and close the underlying writer.
    _guards: Vec<WorkerGuard>,
}

impl TelemetryManager {
    pub fn init_telemetry(config: TelemetryConfig, attributes: Vec<KeyValue>) -> Result<Self> {
        let mut guards = Vec::new();

        let resource = opentelemetry_sdk::Resource::builder()
            .with_attributes(attributes)
            .build();

        let (logger_provider, log_layers) =
            create_logging_provider(config.logging(), resource.clone())?;

        let tracer_provider = create_tracer_provider(config.tracing(), resource.clone())?;

        let meter_provider = create_metrics_provider(config.metrics(), resource)?;

        let mut all_layers = log_layers;

        if let Some(ref provider) = tracer_provider {
            global::set_tracer_provider(provider.clone());
            let tracer = provider.tracer(env!("CARGO_CRATE_NAME"));
            let otel_trace_layer = OpenTelemetryLayer::new(tracer)
                .with_filter(logging::get_env_filter(&config.logging().level()))
                .boxed();
            all_layers.push(otel_trace_layer);
        }

        if config.logging().console_enabled() {
            let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stdout());
            let console_layer = fmt::layer()
                .with_writer(non_blocking)
                .with_filter(logging::get_env_filter(&config.logging().level()))
                .boxed();
            all_layers.push(console_layer);
            guards.push(guard);
        }

        if let Some(ref provider) = meter_provider {
            global::set_meter_provider(provider.clone());
        }

        tracing_subscriber::registry()
            .with(all_layers)
            .try_init()
            .map_err(|e| {
                DocumentDBError::internal_error(format!(
                    "failed to initialize tracing subscriber: {e}"
                ))
            })?;

        Ok(Self {
            meter_provider,
            tracer_provider,
            logger_provider,
            _guards: guards,
        })
    }

    pub fn shutdown(self) -> Result<()> {
        let mut first_error: Option<DocumentDBError> = None;

        if let Some(tracer_provider) = self.tracer_provider {
            if let Err(e) = tracer_provider.shutdown() {
                let err = DocumentDBError::internal_error(format!(
                    "failed to shutdown tracer provider: {e}"
                ));
                if first_error.is_none() {
                    first_error = Some(err);
                } else {
                    tracing::warn!("additional shutdown error (tracer): {e}");
                }
            }
        }

        if let Some(meter_provider) = self.meter_provider {
            if let Err(e) = meter_provider.shutdown() {
                let err = DocumentDBError::internal_error(format!(
                    "failed to shutdown meter provider: {e}"
                ));
                if first_error.is_none() {
                    first_error = Some(err);
                } else {
                    tracing::warn!("additional shutdown error (meter): {e}");
                }
            }
        }

        if let Some(logger_provider) = self.logger_provider {
            if let Err(e) = logger_provider.shutdown() {
                let err = DocumentDBError::internal_error(format!(
                    "failed to shutdown logger provider: {e}"
                ));
                if first_error.is_none() {
                    first_error = Some(err);
                } else {
                    tracing::warn!("additional shutdown error (logger): {e}");
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}
