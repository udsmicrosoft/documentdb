/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/telemetry/mod.rs
 *
 * Telemetry infrastructure for the DocumentDB gateway.
 * Provides OpenTelemetry-based metrics, tracing, and logging.
 *
 *-------------------------------------------------------------------------
 */

pub mod client_info;
pub mod config;
pub mod context_propagation;
pub mod event_id;
pub mod logging;
pub mod metrics;
pub mod telemetry_manager;
pub mod tracing;

// Re-export commonly used types
pub use config::{TelemetryConfig, TelemetryOptions};
pub use context_propagation::{extract_context_from_comment, format_trace_comment, parse_traceparent};
pub use logging::{LoggingConfig, LoggingOptions};
pub use metrics::{MetricsConfig, MetricsOptions, OtelTelemetryProvider};
pub use telemetry_manager::TelemetryManager;
pub use tracing::{TracingConfig, TracingOptions};

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use dyn_clone::{clone_trait_object, DynClone};
use either::Either;

static TRACING_ENABLED: AtomicBool = AtomicBool::new(false);

/// Returns whether distributed tracing is enabled.
/// Used to skip trace context extraction when tracing is off.
pub fn is_tracing_enabled() -> bool {
    TRACING_ENABLED.load(Ordering::Relaxed)
}

pub(crate) fn set_tracing_enabled(enabled: bool) {
    TRACING_ENABLED.store(enabled, Ordering::Relaxed);
}

use crate::{
    context::ConnectionContext,
    error::ErrorCode,
    protocol::header::Header,
    requests::{request_tracker::RequestTracker, Request},
    responses::{CommandError, Response},
};

/// Telemetry provider for request metrics and events.
///
/// Implementations:
/// - [`OtelTelemetryProvider`] (OSS): OpenTelemetry metrics + span attributes
/// - `GenevaTelemetryProvider` (Native): Geneva/Fluent logging
#[expect(clippy::too_many_arguments)]
#[async_trait]
pub trait TelemetryProvider: Send + Sync + DynClone {
    /// Emit telemetry for a completed request.
    ///
    /// * `activity_id` - Gateway correlation ID. OSS: span attribute. Native: log field.
    /// * `user_agent` - Client driver info from handshake. OSS: span attribute. Native: log + metric.
    async fn emit_request_event(
        &self,
        connection_context: &ConnectionContext,
        header: &Header,
        request: Option<&Request<'_>>,
        response: Either<&Response, (&CommandError, usize)>,
        collection: String,
        request_tracker: &RequestTracker,
        activity_id: &str,
        user_agent: &str,
    );
}

clone_trait_object!(TelemetryProvider);

// In case of no error (success), error_code passed here should be None and status code returned is 200
pub fn error_code_to_status_code(error: Option<i32>) -> u16 {
    match error {
        None => 200,
        Some(code) => match ErrorCode::from_i32(code) {
            Some(ErrorCode::AuthenticationFailed | ErrorCode::Unauthorized) => 401,
            Some(ErrorCode::InternalError) => 500,
            Some(ErrorCode::ExceededTimeLimit) => 408,
            Some(ErrorCode::DuplicateKey) => 409,
            _ => 400,
        },
    }
}
