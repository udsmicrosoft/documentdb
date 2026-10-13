# Contributing to pg_documentdb_gw

This document describes the project structure and conventions for the `pg_documentdb_gw` Rust workspace.

## Conventions

### Coding conventions

Do follow the Rust API guidelines and idiomatic Rust practices. We use `rustfmt` for code formatting and `clippy` for linting. Please ensure your code is formatted and free of warnings before submitting a PR.
For reference see:

- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/about.html)
- [Pragmatic Rust Guidelines](https://microsoft.github.io/rust-guidelines/guidelines/index.html)

### Workspace Dependencies

All third-party dependencies are declared in the root `Cargo.toml` under `[workspace.dependencies]` and referenced by member crates with `.workspace = true`. Do not add dependency versions directly in member crate `Cargo.toml` files.

### Test Organization

Test helper functions in `documentdb_tests/src/commands/` are organized as one module per command (e.g., `insert.rs`, `find.rs`, `aggregate.rs`). These are consumed by integration tests in `documentdb_tests/tests/`.

### Telemetry

The gateway exposes OpenTelemetry-compatible **metrics** and **traces** via OTLP/gRPC. Both signals are opt-in. Enable them in `SetupConfiguration.json`:

```json
"TelemetryOptions": {
  "Metrics": { "Enabled": true, "OtlpEndpoint": "http://localhost:4317" },
  "Tracing": { "Enabled": true, "OtlpEndpoint": "http://localhost:4317", "SamplerRatio": 1.0 }
}
```

Or via the standard OpenTelemetry environment variables (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_TRACES_ENABLED`, `OTEL_TRACES_SAMPLER_ARG`, `OTEL_METRICS_ENABLED`, `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_SERVICE_NAME`). JSON values take precedence over env vars; env vars take precedence over defaults.

Tracing emits a `gateway.request` root span per request with `db.system.name`, `db.operation.name`, `db.collection.name`, `db.namespace`, `connection.id`, `network.protocol`, and `network.transport.tls` attributes, plus nested spans (`gateway.read_request`, `gateway.format_request`, `gateway.auth`, `gateway.process_request`, `postgres.transaction`, `postgres.acquire_connection`, `postgres.execute`, `gateway.write_response`) that mirror the metric phase breakdown.

Sampling defaults to `ParentBased(TraceIdRatioBased(1.0))` — once tracing is enabled, every root span is sampled. Lower `SamplerRatio` to ratio-sample in production. The `OTEL_TRACES_SAMPLER` env var is intentionally ignored; only the ratio is configurable in v1.

#### SQLCommenter migration

The gateway no longer appends trace comments to PostgreSQL statements.
Parameterized query calls use the cached prepared-statement path regardless
of tracing. Gateway spans and incoming parent-context extraction are unchanged.

Remove the `postgres-sql-commenter` feature from custom build commands.
The retired `TelemetryOptions.Tracing.SqlCommenterEnabled` and
`DOCUMENTDB_SQL_COMMENTER_ENABLED` settings are ignored. To propagate context
in command BSON, opt in to UDF trace-context logging as described below.

#### UDF trace-context logging

The standalone gateway propagates a sampled `postgres.execute` context in the
private `$documentdbTrace` field of an existing command-BSON argument. The
backend logs receipt at the command's C execution boundary. This provides
correlation, not backend spans, timing, or a backend trace exporter.

The binary's default features include `postgres-udf-trace-context`. Runtime
selection is opt-in:

```json
"TelemetryOptions": {
  "Tracing": {
    "Enabled": true,
    "UdfTraceContextEnabled": true,
    "SamplerRatio": 1.0
  }
}
```

`DOCUMENTDB_UDF_TRACE_CONTEXT_ENABLED=true` selects the same carrier.
JSON overrides the environment. Both startup and `check` validate the carrier
selection. Invalid selections remain startup errors. Telemetry initialization
failures are logged, and the gateway continues without command-BSON propagation
even when it was selected.

Use matched gateway and extension builds, restart PostgreSQL after installing
the extension, and restart the gateway to open new backend connections.
Before enabling propagation, startup checks the read-only
`documentdb.udf_trace_context_version` capability from the loaded binary's
`pg_settings`, along with matching schema and binary versions. The `check`
command validates these requirements without initializing telemetry.
File overrides cannot supply the capability. A refresh that loses
or cannot verify capability disables trace propagation until restart, even
if a later refresh succeeds. Supported data commands continue without trace
metadata. Rolling upgrades and mixed-version operation are not supported by
this feature.

| Command | Backend execution boundary |
| --- | --- |
| `aggregate` | Aggregate cursor first page |
| `find` | Find cursor first page |
| `getMore` | Cursor continuation request, with a fresh execution parent |
| `count` | `count_query` |
| `distinct` | `distinct_query` |
| `insert` | Shared insert processing, including function, transaction procedure, bulk procedure, and document sequences |
| `update` | Shared update processing, including function, transaction procedure, bulk procedure, and document sequences |
| `delete` | Shared delete processing, including function, transaction procedure, and document sequences |
| `findAndModify` | Command execution before field deduplication |

The gateway-facing count and distinct UDFs are volatile C functions. They are
not the planner-rewritten aggregation helpers. Logging needs no planner
changes, forced generic plans, or additional SQL arguments.

Administrative and diagnostic commands, schema and index operations, user and
role management, authentication, cursor deletion, and connection housekeeping
are outside this data-command scope. Some bind a command document and others
bind separate scalar arguments; they are not silently treated as carriers.
Worker helpers and distributed propagation are also excluded. The gateway
never attaches metadata to explain and rejects caller-supplied carrier fields
in its targets.

The version-1 envelope contains `version`, a sampled W3C version-00
`traceparent`, and optional `tracestate`. Trace state is limited to 512 bytes
and 32 entries; the envelope budget is 1 KiB. Injection preserves all original
BSON bytes except the document length and terminal marker, including command
order, duplicate keys, and every application `comment` type. Caller-supplied
reserved fields and case aliases are rejected on supported commands, including
when tracing is disabled. No client envelope is used as a fallback.

Disabled or unsampled operations retain borrowed BSON. Sampled operations
make one command copy with capacity for the appended field. Metadata is
omitted if the resulting command would exceed the 16 MiB carrier budget;
an otherwise valid operation is not rejected. The payload-free
`docdb.gateway.udf_trace_context.skipped` counter records limit or validation
skips, with a bounded `docdb.trace_context.skip.reason` attribute. Enable
metrics export to observe it. No SQL text, UDF signatures, application
comments, pooled-session settings, or stored documents are changed. The
backend removes the field from first-page cursor specifications before they
can be saved in a continuation or forwarded. Continuation requests carry
their own context, not the opening request's parent.

Common backend command parsing ignores the carrier field, including case
aliases, even when extraction is disabled. Only the data-command entrypoints
listed above validate and log the context; other parsers using the common
ignored-field list tolerate the metadata without processing it.
`documentdb.enable_udf_trace_context` enables bounded validation and server-only
logging. Records contain only a fixed operation name and a validated
`traceparent`; statement text, error context, command contents, and
`tracestate` are not included. The setting is off by default.
`documentdb.udf_trace_context_log_limit_per_second` limits records per backend
process in one-second windows, defaults to 100, and accepts 0 through 10000.
Zero suppresses all records. Budget drops increment
`udf_trace_context_log_dropped` without affecting the command.

Malformed, duplicate, or unsupported
metadata is ignored for tracing and counted as `udf_trace_context_rejected`;
business validation still applies.

These are best-effort diagnostic records, not a complete audit trail.
Unsampled requests, budget drops, and failures before the execution boundary
do not produce a record. Enabled logging adds validation, formatting, and
server-log I/O; the per-process limit is not a cluster-wide volume limit.
Keep the feature opt-in and choose a suitable sampling ratio and log budget.

Run the ignored acceptance test against a disposable server with backend
logging enabled, `documentdb.test_udf_trace_context=off`, and
`UDF_TRACE_TEST_PG_LOG` pointing to its server log. Use a sufficiently large
log budget for this small synthetic workload:

```bash
UDF_TRACE_TEST_RUNTIME=v2 UDF_TRACE_TEST_SAMPLED=true \
  UDF_TRACE_TEST_ENABLED=true UDF_TRACE_TEST_PG_LOG=/path/to/test-postgres.log \
  cargo test -p documentdb_gateway_otel --all-features \
  gateway_to_real_udf_entrypoints -- --ignored --test-threads=1
```

Repeat for `v1`, with sampling disabled, and with transport disabled. For a
backend-disabled run, turn off the backend setting and set
`UDF_TRACE_TEST_BACKEND_ENABLED=false`. The test matches server-log parent IDs
to in-memory exported execution spans and exercises every supported command,
comments, both insert encodings, batches, transactions, cursor reuse, errors,
and spoofed metadata. It forces one retry by terminating only the find backend
blocked on its fixture's table lock and checks both attempts against the same
execution span.

The separate `documentdb.test_udf_trace_context` NOTICE probe is for SQL
regression tests only. It reports validation status and a validated parent
identifier, never command contents or trace state. Do not enable it for
production logging or performance measurements. Backend coverage in
`commands_udf_trace_context` includes generic/custom prepared plans,
procedure variants, collation, malformed envelopes, and clean continuations.
