/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/telemetry/context_propagation.rs
 *
 * W3C Trace Context propagation for distributed tracing.
 * - Inbound: Extract trace context from request comment field
 * - Outbound: Format trace context as SQL comment for PostgreSQL
 *
 *-------------------------------------------------------------------------
 */

use std::borrow::Cow;

use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId};
use opentelemetry::Context;
use serde_json::Value;

// =============================================================================
// Inbound: Client → Gateway
// =============================================================================

/// Extract trace context from request comment field.
///
/// The wire protocol doesn't support HTTP-style trace headers, so clients
/// can pass W3C trace context via the `comment` field in queries.
///
/// Expected format: `{"traceparent": "00-{trace_id}-{span_id}-{flags}"}`
///
/// Returns `None` for invalid or missing trace context (backward compatible).
///
/// # Example
/// ```rust,ignore
/// let comment = r#"{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}"#;
/// if let Some(ctx) = extract_context_from_comment(comment) {
///     // Use ctx as parent for new spans
/// }
/// ```
pub fn extract_context_from_comment(comment: &str) -> Option<Context> {
    // Handle parsing errors gracefully - don't fail requests for malformed trace context
    let json: Value = serde_json::from_str(comment).ok()?;
    let traceparent = json.get("traceparent")?.as_str()?;
    parse_traceparent(traceparent)
}

/// Parse a W3C traceparent string into an OpenTelemetry context.
///
/// Format: `00-{trace_id}-{span_id}-{flags}`
/// Reference: <https://www.w3.org/TR/trace-context/#traceparent-header>
///
/// Returns `None` for invalid traceparent values.
pub fn parse_traceparent(traceparent: &str) -> Option<Context> {
    let parts: Vec<&str> = traceparent.split('-').collect();
    if parts.len() != 4 || parts[0] != "00" {
        return None;
    }

    let trace_id = TraceId::from_hex(parts[1]).ok()?;
    let span_id = SpanId::from_hex(parts[2]).ok()?;
    let flags = TraceFlags::new(u8::from_str_radix(parts[3], 16).ok()?);

    if trace_id == TraceId::INVALID || span_id == SpanId::INVALID {
        return None;
    }

    let span_context = SpanContext::new(trace_id, span_id, flags, true, Default::default());
    Some(Context::current().with_remote_span_context(span_context))
}

// =============================================================================
// Outbound: Gateway → PostgreSQL
// =============================================================================

/// Format trace context as SQL comment for PostgreSQL correlation.
///
/// Prepends W3C traceparent as a SQL comment so PostgreSQL logs can be
/// correlated with gateway traces.
///
/// Format: `/* traceparent='00-{trace_id}-{span_id}-{flags}' */ {sql}`
///
/// Returns original SQL unchanged if context is invalid or not sampled.
/// Uses `Cow<str>` to avoid allocation when unsampled (90% of requests).
///
/// # Example
/// ```rust,ignore
/// let sql = "SELECT * FROM users";
/// let traced_sql = format_trace_comment(sql, &context);
/// // Result: "/* traceparent='00-abc...-def...-01' */ SELECT * FROM users"
/// ```
pub fn format_trace_comment<'a>(sql: &'a str, context: &Context) -> Cow<'a, str> {
    let span = context.span();
    let span_context = span.span_context();

    if span_context.is_valid() && span_context.is_sampled() {
        Cow::Owned(format!(
            "/* traceparent='00-{}-{}-{:02x}' */ {sql}",
            span_context.trace_id(),
            span_context.span_id(),
            span_context.trace_flags().to_u8()
        ))
    } else {
        Cow::Borrowed(sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TraceState;

    // -------------------------------------------------------------------------
    // extract_context_from_comment tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_extract_invalid_traceparent() {
        let comment = r#"{"traceparent": "invalid"}"#;
        let result = extract_context_from_comment(comment);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_no_traceparent() {
        let comment = r#"{"other": "field"}"#;
        let result = extract_context_from_comment(comment);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_malformed_json() {
        let comment = "not json";
        let result = extract_context_from_comment(comment);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_valid_with_extra_fields() {
        let comment = r#"{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "other": "data"}"#;
        let result = extract_context_from_comment(comment);
        assert!(result.is_some());
    }

    // -------------------------------------------------------------------------
    // format_trace_comment tests
    // -------------------------------------------------------------------------

    fn create_test_context(trace_id: &str, span_id: &str, flags: u8) -> Context {
        let trace_id = TraceId::from_hex(trace_id).unwrap();
        let span_id = SpanId::from_hex(span_id).unwrap();
        let trace_flags = TraceFlags::new(flags);
        let span_context =
            SpanContext::new(trace_id, span_id, trace_flags, true, TraceState::default());
        Context::current().with_remote_span_context(span_context)
    }

    #[test]
    fn test_format_with_valid_context() {
        let context =
            create_test_context("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", 0x01);

        let sql = "SELECT * FROM users WHERE id = $1";
        let result = format_trace_comment(sql, &context);

        assert_eq!(
            result,
            "/* traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' */ SELECT * FROM users WHERE id = $1"
        );
    }

    #[test]
    fn test_format_with_invalid_context() {
        let context = Context::current();
        let sql = "SELECT * FROM users";
        let result = format_trace_comment(sql, &context);

        assert_eq!(result, "SELECT * FROM users");
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn test_format_with_unsampled_context() {
        // flags=0x00 means NOT sampled - this is 90% of requests
        let context = create_test_context(
            "4bf92f3577b34da6a3ce929d0e0e4736",
            "00f067aa0ba902b7",
            0x00, // not sampled
        );
        let sql = "SELECT * FROM users";
        let result = format_trace_comment(sql, &context);

        assert_eq!(result, "SELECT * FROM users");
        assert!(matches!(result, Cow::Borrowed(_))); // verify zero-copy
    }

    #[test]
    fn test_format_with_sampled_context_returns_owned() {
        let context =
            create_test_context("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", 0x01);
        let sql = "SELECT * FROM users";
        let result = format_trace_comment(sql, &context);

        assert!(matches!(result, Cow::Owned(_)));
    }

    #[test]
    fn test_format_sql_with_existing_comments() {
        let context =
            create_test_context("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", 0x01);

        let sql = "/* existing comment */ SELECT * FROM users";
        let result = format_trace_comment(sql, &context);

        assert!(result.starts_with("/* traceparent="));
        assert!(result.contains("/* existing comment */"));
    }

    // -------------------------------------------------------------------------
    // extract_context_from_comment - value verification tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_extract_verifies_trace_ids() {
        let comment =
            r#"{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}"#;
        let ctx = extract_context_from_comment(comment).unwrap();
        let span = ctx.span();
        let span_ctx = span.span_context();

        assert_eq!(
            span_ctx.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(span_ctx.span_id().to_string(), "00f067aa0ba902b7");
        assert!(span_ctx.trace_flags().is_sampled());
    }

    #[test]
    fn test_extract_wrong_version() {
        // W3C version must be "00"
        let comment =
            r#"{"traceparent": "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}"#;
        assert!(extract_context_from_comment(comment).is_none());
    }

    #[test]
    fn test_extract_empty_string() {
        assert!(extract_context_from_comment("").is_none());
    }

    #[test]
    fn test_extract_zero_trace_id() {
        // All-zero trace_id is invalid per W3C spec
        let comment =
            r#"{"traceparent": "00-00000000000000000000000000000000-00f067aa0ba902b7-01"}"#;
        assert!(extract_context_from_comment(comment).is_none());
    }

    #[test]
    fn test_extract_zero_span_id() {
        // All-zero span_id is invalid per W3C spec
        let comment =
            r#"{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"}"#;
        assert!(extract_context_from_comment(comment).is_none());
    }
}
