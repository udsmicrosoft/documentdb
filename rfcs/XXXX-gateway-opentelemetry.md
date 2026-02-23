---
rfc: XXXX
title: "OpenTelemetry Instrumentation for DocumentDB Gateway"
status: Draft
owner: "@urismiley"
issue: "https://github.com/microsoft/documentdb/issues/XXX"
version-target: 1.0
implementations:
  - "https://github.com/microsoft/documentdb/pull/XXX"
---

# RFC-XXXX: OpenTelemetry Instrumentation for DocumentDB Gateway

## Problem

The DocumentDB gateway (`pg_documentdb_gw`) translates MongoDB wire protocol requests into PostgreSQL SQL queries. In production, operators have limited visibility into gateway behavior: there are no structured metrics for request throughput/latency, no distributed traces linking client requests through the gateway to PostgreSQL, and no centralized log export.

### Impact

- **Operators** cannot monitor gateway health, identify slow queries, or diagnose failures without custom log parsing.
- **Developers** debugging end-to-end request flows must manually correlate logs across the MongoDB client, gateway, and PostgreSQL — a time-consuming process with no trace continuity.
- **Downstream consumers** (e.g., managed service providers) cannot integrate gateway telemetry into their existing observability pipelines without forking and modifying the gateway.

### Current Workarounds

- Manual log file inspection (`gateway.log`)
- Custom monitoring scripts parsing unstructured logs
- No distributed tracing capability

### Success Criteria

1. Gateway exports metrics, traces, and logs via OTLP gRPC to any OpenTelemetry-compatible backend.
2. Each signal (tracing, metrics, logging) can be independently enabled/disabled.
3. Telemetry is zero-impact when disabled and low-overhead when enabled.
4. W3C trace context propagates from MongoDB clients through the gateway to PostgreSQL.
5. Console logging remains available by default, independent of OTLP export.

### Non-Goals

- Instrumenting PostgreSQL internals (out of scope; PostgreSQL has its own observability).
- Providing a bundled observability backend (operators choose their own).
- Replacing the existing `TelemetryProvider` trait (downstream consumers continue using it for custom telemetry).

---

## Approach

### Solution Overview

Add OpenTelemetry instrumentation to the gateway using the standard Rust OpenTelemetry SDK. All three signals are exported over OTLP gRPC:

```
MongoDB Client → DocumentDB Gateway → OTel Collector → {Tempo, Prometheus, Loki, ...}
                         ↓
                    PostgreSQL
```

### Key Design Decisions

**1. OTLP gRPC as the single export protocol.**
All signals use gRPC to a collector. This avoids maintaining per-backend exporters and lets operators choose any OTLP-compatible backend (Grafana, Datadog, Azure Monitor, etc.).

**2. Independent signal control.**
Each signal (tracing, metrics, logging) has its own `Enabled` flag and endpoint override. Metrics and logging are enabled by default; tracing is opt-in because it has higher overhead (sampling, span creation, SQL comment injection).

**3. W3C trace context via MongoDB `comment` field.**
The MongoDB wire protocol has no header mechanism for trace propagation. The `comment` field is the standard extension point. The gateway accepts traceparent as either a BSON document (`{ traceparent: "00-..." }`) or a JSON string (`'{"traceparent": "00-..."}'`).

**4. Console logging remains default-on.**
OTLP log export is additive. Console logging (`stdout`) is enabled by default so the gateway always produces logs even without a collector.

**5. Configuration priority: JSON > Environment Variables > Defaults.**
This matches the existing gateway configuration pattern (`SetupConfiguration.json`). Environment variables follow OTel conventions (`OTEL_EXPORTER_OTLP_ENDPOINT`, etc.).

**6. TelemetryProvider trait preserved for extensibility.**
The existing `TelemetryProvider` trait continues to serve as a hook for downstream consumers who need custom telemetry. The OTel metrics are recorded through this trait via `OtelTelemetryProvider`.

### Tradeoffs

| Decision | Benefit | Cost |
|----------|---------|------|
| gRPC-only export | Single protocol, lower maintenance | Requires collector for non-gRPC backends |
| Tracing off by default | Zero overhead for operators who don't need traces | Must opt-in for distributed tracing |
| Comment-based trace propagation | Works within wire protocol constraints | Requires client cooperation; adds parsing cost |
| Parent-based sampling | Honors client sampling decisions | Sampled traces create unique SQL comments, slightly reducing prepared statement cache hit rates |

---

## Detailed Design

### Telemetry Module Structure

```
src/telemetry/
├── mod.rs                    # TelemetryProvider trait, public API
├── config.rs                 # TelemetryConfig (JSON + env var + defaults)
├── telemetry_manager.rs      # Provider lifecycle (init, shutdown)
├── tracing.rs                # TracerProvider setup, sampling config
├── metrics.rs                # OtelTelemetryProvider, db.client.* counters
├── logging.rs                # LoggerProvider, console + OTLP layers
├── context_propagation.rs    # W3C traceparent extraction/injection
├── client_info.rs            # MongoDB client metadata parsing
└── event_id.rs               # Event ID generation
```

### Configuration

Configuration follows the fallback chain: `SetupConfiguration.json` → environment variables → compiled defaults.

**Environment Variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP endpoint (gRPC) |
| `OTEL_SERVICE_NAME` | `documentdb_gateway` | Service name |
| `OTEL_TRACING_ENABLED` | `false` | Enable distributed tracing |
| `OTEL_METRICS_ENABLED` | `true` | Enable metrics export |
| `OTEL_LOGGING_ENABLED` | `true` | Enable OTLP log export |
| `OTEL_LOGS_CONSOLE_ENABLED` | `true` | Enable console logging |
| `OTEL_TRACES_SAMPLER_ARG` | `0.1` | Trace sampling ratio (0.0–1.0) |
| `OTEL_METRIC_EXPORT_INTERVAL` | `15000` | Metrics export interval (ms) |
| `RUST_LOG` | `info` | Log level filter |

Each signal also supports a dedicated endpoint override (e.g., `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`).

When all three signals are disabled, telemetry initialization is skipped entirely — no providers are created, no global state is set, and no `TelemetryProvider` is passed to the gateway.

### Metrics

Metrics follow [OTel Database Client Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/database/database-metrics/):

| Metric | Type | Unit |
|--------|------|------|
| `db.client.operation.duration.total` | Counter | seconds |
| `db.client.operations` | Counter | count |
| `db.client.request.size.total` | Counter | bytes |
| `db.client.response.size.total` | Counter | bytes |

Attributes: `db.system.name`, `db.operation.name`, `db.collection.name`, `db.namespace`, `error.type`.

Metrics are recorded via `OtelTelemetryProvider::emit_request_event()`, called after each request completes. Attributes are built once per request without cloning.

### Traces

Each request creates a `handle_message` span (via `#[instrument]`) with attributes: `activity_id`, `operation`, `db`, `collection`, `user_agent`.

**Trace Context Propagation:**

1. Client sends W3C `traceparent` in the MongoDB `comment` field (BSON document or JSON string).
2. Gateway extracts the context and sets it as parent on the current span (only when `OTEL_TRACING_ENABLED=true`).
3. Gateway injects `traceparent` into SQL queries as a comment: `/* traceparent='00-...' */ SELECT ...`

This links traces from client → gateway → PostgreSQL.

**Sampling:** Parent-based with configurable ratio. Root spans are sampled at the configured ratio; child spans honor the client's sampling decision.

### Logging

Two independent log outputs:

1. **Console (stdout):** Default-on. Uses `tracing-subscriber` `fmt` layer with non-blocking writer.
2. **OTLP export:** Default-on. Uses `opentelemetry-appender-tracing` bridge. Batch export with configurable queue size, batch size, and interval.

Both outputs share the same `EnvFilter` (from `RUST_LOG` or config), ensuring consistent log levels.

### Reliability

- Telemetry failures never propagate to request handling.
- OTLP batch exporters drop data silently when the collector is unreachable.
- A global `is_tracing_enabled()` flag avoids trace context parsing overhead when tracing is off.

### Testing Strategy

- **Unit tests:** `context_propagation.rs` (traceparent parsing/formatting), `config.rs` (fallback chain), `logging.rs` (provider creation), `metrics.rs` (config defaults).
- **Integration tests:** Local observability stack via docker-compose (OTel Collector, Tempo, Loki, Prometheus, Grafana) with traffic generation scripts.

### Migration Path

This is an additive change with no breaking modifications:

- Existing deployments see metrics and logs exported by default (to `localhost:4317`). If no collector is listening, exports silently fail.
- Console logging is now default-on, ensuring log output even without a collector.
- Tracing is opt-in and requires explicit `OTEL_TRACING_ENABLED=true`.
- The `TelemetryProvider` trait interface is unchanged; downstream consumers' implementations continue to work.

### Documentation Updates

- `docs/v1/telemetry.md`: User-facing configuration guide, metric reference, trace context examples, local testing instructions.

---

## Implementation Tracking

### Implementation PRs

- [ ] PR: Gateway OpenTelemetry instrumentation (tracing, metrics, logging, context propagation, configuration)

### Open Questions

- [ ] Question: Should `OtelTelemetryProvider` metrics be recorded directly via `global::meter()` in the request path instead of through the `TelemetryProvider` trait?
  - Discussion: This would make OTel metrics automatic for all downstream consumers without requiring them to wrap `OtelTelemetryProvider`. Deferred to follow-up.

- [ ] Question: Should `#[instrument]` spans be suppressed when tracing is disabled, or is the current no-op behavior sufficient?
  - Discussion: Currently spans are created but not exported when tracing is off. The overhead is minimal (no allocation for unsampled spans).
