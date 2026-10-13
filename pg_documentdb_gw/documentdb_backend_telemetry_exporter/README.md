# DocumentDB backend telemetry exporter

This standalone workspace member collects completed native Find spans and exports OTLP.
It is **not part of gateway request execution** and does not require Kubernetes.
It reuses the workspace's pinned PostgreSQL client, Tokio runtime, and OTLP
dependencies. It does not start a tracer, sample a second time, or generate
replacement IDs or timestamps for completed backend records.

The exporter discovers and reads only the restricted telemetry SQL API. It does
not run application queries, explain requests, or index-catalog diagnostics.
Downstream investigation skills query already-emitted telemetry in a telemetry
backend, not DocumentDB. Database reader credentials stay with the exporter.

Capabilities:

- A bounded v1 native-record decoder and direct OTLP/gRPC export, including a
  loopback receiver test of the actual exported IDs, nanoseconds, flags, and
  status. Retries of the same immutable record produce identical payloads.
- A read-only native SQL collector with serial polling, one bounded pending
  batch, and cursor advancement only after successful export.
- Actual scan classification and measured first-batch document counts from
  supported native Find executions, preserved through SQL and OTLP export.
- Bounded native plan summaries with explicit unknown/incomplete states.

The matching engine has the opt-in native producer and SQL read API described below.

**Not implemented:** native collection/query-shape and logical index-name mapping,
native plan fingerprints, CPU measurement, durable cursor persistence,
telemetry-backend query integration, packaged investigation skills, or
native-mode demo gates. No gateway request-path code or metrics are added by
this crate. Active diagnostics and application-query replay are out of scope.

## Native producer demo path

The matching engine source records existing local, non-view single-batch Find execution
(including point reads) in `pg_documentdb/src/infrastructure/backend_telemetry.c`.
The scope starts after query translation and collection lookup, immediately
before first-page execution. It does not measure earlier outer planning or
translation failures. Internal Find helpers, remote/sharded dispatch, GetMore,
catalog queries, and planning-only explain do not acquire this scope.

Each scope owns a copy of its accepted trace/parent IDs and a new child ID.
There is no backend-global current parent to leak into nested or pooled calls.
The record contains timings and outcome, plus a bounded actual scan/work summary
when execution completes. A query cancellation records `cancelled`; other
execution errors record the safe `other` category and are rethrown.
Failed executions do not publish partial plan/work as completed measurements.
Only sampled v1 flag `01` is admitted; other contexts retain their existing
command behavior without native recording.

### Native scan and document evidence

The engine enables row-only executor instrumentation for these sampled scopes
and copies scalar evidence before executor cleanup. It does not rerun the query,
invoke explain, or resolve logical index names through catalog queries.
No CPU, buffer, or per-node timer instrumentation is added.

The native walk recognizes sequential, index, index-only, and bitmap-heap
collection scans through supported single-input wrappers and bitmap children.
It visits at most 64 nodes at depth 16 and requires exactly one scan of the
resolved target relation. Unknown/custom, multi-scan, subplan, and parallel
shapes are explicitly incomplete, with `scan_kind: "unknown"` and a null
examined count. Index-only scans also leave the examined count unknown.

`returned_documents` counts BSON documents actually emitted into the first
batch. `examined_documents` sums target-scan tuples returned or removed by
filters/rechecks, using executor counters rather than estimates. It can include
read-ahead not emitted in the response. It does not count physical index entries,
invisible heap tuples, incidental catalog rows, or CPU work. Zero is a measured
value; unavailable measurements remain null or absent.

`plan.complete` describes scan-classification coverage.
`plan.index_names_complete` independently describes logical index-name coverage.
Native index scans currently have complete classification but incomplete name
coverage, an empty `used_index_name_ids` list, and no fingerprint. That empty
list is **not evidence that no index was used**. A supported sequential scan
has complete name coverage with no used indexes. Collection/query-shape IDs
remain absent. Consumers must not infer a query-family or collection match
without compatible identities supplied by the producer.

### Native storage and restricted reads

Both storage and recording are default-off system settings. A disposable
instance with the matching extension binary must reserve storage at startup:

```conf
documentdb.enable_backend_telemetry_storage = on
documentdb.enable_udf_trace_context = on
documentdb.enable_backend_telemetry = on
```

Changing the storage setting requires a restart. Recording and context settings
are superuser-controlled; configure them consistently for workload and reader
connections. Capability flags report the calling connection's effective
settings, not the state of every other connection.

The buffer has 2,000 fixed slots within a 4 MiB allocation ceiling, 2 KiB per
record, and at most 64 completed-span admissions per one-second window. Writers
try the lock once and drop telemetry on contention rather than waiting. A read
copies a bounded batch before constructing JSON. Producer instance and epoch
identities change on server restart; no historical buffer is persisted.

The SQL objects ship in the `1.2-0` schema upgrade. A consumer must discover the
deployed signatures before calling them, then inspect the binary protocol and
storage/configuration flags:

```sql
SELECT to_regprocedure('documentdb_api_internal.backend_telemetry_capabilities()'),
       to_regprocedure('documentdb_api_internal.backend_telemetry_read(uuid,bigint,integer)');
SELECT documentdb_api_internal.backend_telemetry_capabilities();
```

Both functions are security-invoker functions with execution revoked from
`PUBLIC`. The harness grants only schema usage and those functions to a
dedicated telemetry role, then gives its separate reader login that role:

```sql
CREATE ROLE telemetry_demo_reader NOLOGIN;
GRANT USAGE ON SCHEMA documentdb_api_internal, documentdb_core TO telemetry_demo_reader;
GRANT EXECUTE ON FUNCTION documentdb_api_internal.backend_telemetry_capabilities(),
    documentdb_api_internal.backend_telemetry_read(uuid, bigint, integer)
    TO telemetry_demo_reader;
```

The core-schema grant is needed by the planner's type lookup on a fresh reader
connection, even though these functions return JSON. It grants no table access.
The server returns records only for the current PostgreSQL database. This is
not document-database or collection-level isolation within that database.
Neither the telemetry backend nor the investigating agent receives this role's
connection credentials.

Call `backend_telemetry_read(expected_epoch, after_sequence, max_records)` with
the discovered epoch, initially sequence zero, and a limit from 1 to 128.
The non-destructive v1 JSON response contains `producer_instance_id`, `epoch`,
`database_oid`, `oldest_sequence`, `latest_sequence`, `epoch_reset`, `gap`,
`next_cursor: {epoch, after_sequence}`, `loss`, and `records`.
Responses are capped at 256 KiB. Advance only after accepting the batch; an
unchanged retained record can be read repeatedly without changing its values.

An epoch mismatch explicitly returns `epoch_reset: true` and no records. The
caller must acknowledge the new epoch with another read, not treat this as an
ordinary empty batch. `gap: true` means the cursor predates retained history.
Sequences and loss counters are instance-wide, so sequence jumps can also
reflect records filtered out from other PostgreSQL databases. The `overwrite`
counter counts retention evictions, including already-read records; it does
not prove delivery loss. Admission, contention, oversize, clock, and identity
losses are separate. No buffer means unknown identities/counters and an explicit
read error, not an empty success.

## Native export command

Build the standalone crate in the gateway workspace with
`cargo build -p documentdb_backend_telemetry_exporter --locked`.
Provision the dedicated reader login and grants above, then supply two private
environment settings through the harness:

- `DOCUMENTDB_TELEMETRY_PG_URI`: the approved PostgreSQL database and reader login,
  with explicit `sslmode=disable`. This local PoC does not implement TLS and rejects
  other modes rather than silently downgrading. Use only an isolated local endpoint.
- `DOCUMENTDB_TELEMETRY_OTLP_ENDPOINT`: the approved OTLP/gRPC receiver endpoint.

Run with explicit deployment labels:

```bash
target/debug/documentdb_backend_telemetry_exporter export-native local native-demo
```

Start collection on a fresh demo instance before the workload. The collector
discovers both SQL signatures, checks protocol/configuration/storage, and binds
to the actual database OID, producer ID, and epoch. It rechecks capabilities
before each new batch. All SQL uses one read-only connection with a two-second
statement timeout and client deadlines. Telemetry reads do not create Find spans.

Polling is approximately once per second, without overlapping reads or exports.
An OTLP failure retains the same immutable batch and cursor. The CLI allows three
consecutive delivery attempts, then exits unsuccessfully. Empty batches skip
OTLP and may advance across sequences belonging to other PostgreSQL databases.
SQL errors, missing capabilities, source/epoch changes, and retention gaps stop
collection explicitly. There is no reconnect, automatic epoch rebinding, or
silent skipping of lost history.

Each successful poll writes one sanitized JSON acknowledgement to stdout:
`origin: "native_collection"`, source identity, database OID, `after_sequence`,
`exported_records`, and the producer's `loss` counters from the read-batch snapshot.
An empty poll is not proof that the workload recorded spans. This acknowledgement
is not a span or a plan. Acknowledgements and loss counters are CLI status only,
not exported OTel signals; telemetry-only analysis must not assume they are
available from its backend.
Stderr contains fixed error categories, never the URI or raw driver errors.

Ctrl+C drops the connection and any pending in-memory batch. Cursors are not
persisted: restarting begins at zero, so retained records can be delivered again.
Ambiguous delivery can also duplicate a span. Consumers must deduplicate original
identities rather than assume exactly-once delivery. Restarting the collector
does not bypass a retention gap; use a fresh owned demo instance for a new run.

## Long-running local export

`serve-native NAMESPACE CLUSTER` uses the same decoder, restricted SQL API, and
OTLP exporter as `export-native`, with a long-running recovery policy:

- SQL must explicitly name a loopback host, database, reader, and
  `sslmode=disable`. Remote hosts, redirected host addresses, implicit database
  selection, and oversized connection settings are rejected.
- There is one SQL connection and at most one pending batch, with the same
  record, byte, and operation-deadline limits as strict export.
- Failed delivery retries the same immutable batch without advancing its cursor.
  Backoff increases from one second to a maximum of 30 seconds.
- SQL reconnection preserves the last in-memory cursor while the source and
  retained history still match. A changed source/epoch or retention gap binds
  to the oldest available records and reports the discontinuity explicitly.
- Process startup has no durable checkpoint and can replay retained records.
  Original span IDs are preserved for downstream deduplication. Delivery is
  neither durable nor exactly once.
- SIGTERM and Ctrl+C interrupt polling, export, and backoff. Pending delivery
  can be ambiguous at shutdown; the final status reports unconfirmed records.

The process writes bounded JSON status objects with origin
`native_exporter_status` at startup, binding/recovery, failures, state changes,
and approximately every 30 seconds while running. They contain the last source
binding, acknowledged collection report, successful-export time, and process-local
counters. They contain no connection URI, password, query text, or span payloads.
`running` means a successful source poll, not proof of delivery when the batch is
empty. Check `last_export_unix_ms`, delivered counts, and the receiving backend
to establish actual export. These statuses are stdout diagnostics, not SQL
objects or additional OTLP signals.

Both source data and counters have explicit limits: producer loss counters are
instance-wide, and sequence gaps do not establish an exact count of lost spans
for the configured database. A process restart resets exporter counters.
Neither mode expands native command coverage or enables recording.

### Container packaging

Build the binary with the repository toolchain for the target Linux architecture,
then package it from the gateway workspace:

```bash
cargo build --release --locked -p documentdb_backend_telemetry_exporter
docker build -f documentdb_backend_telemetry_exporter/Dockerfile \
  -t documentdb-native-trace-exporter:local .
```

The image packages the built artifact, not a compiler. Its Debian 13 runtime must
be compatible with the binary's linked libraries. It runs as a non-root user,
needs no writable root filesystem, and does not mount PostgreSQL data.
Supply the two environment settings above through the deployment's secret and
configuration mechanisms. A sidecar can run the following arguments:

```text
serve-native <namespace> <cluster>
```

Keep startup independent of PostgreSQL availability. Do not make database
readiness depend on this process's delivery status.

## Version 1 native evidence contract

The protocol version is independent of command-BSON context propagation.
`tests/fixtures/native-find-v1.json` is the canonical minimal span fixture.
The decoder rejects unknown fields, versions, unsampled contexts, oversized
records, zero/malformed identities, self-parenting spans, and invalid outcomes.

| Native field | Meaning |
| --- | --- |
| `producer_instance_id`, `epoch` | Nonzero lowercase 128-bit hex instance and buffer-allocation identities. A buffer restart changes the epoch. |
| `sequence` | Positive signed-64-bit-range completed-record sequence, monotonically increasing within the epoch. It is not a trace ID. |
| `trace_id`, `span_id`, `parent_span_id` | Inherited trace, new backend child, and accepted gateway execute parent. Export does not regenerate them. |
| `trace_flags` | Exactly `1` for the initial sampled v1 producer contract. |
| `operation`, `origin` | Exactly `Find` and `actual_execution`. Span name is always `documentdb.command`. |
| `start_time_unix_ns` | Producer wall-clock anchor, not collector arrival time. |
| `duration_ns`, `end_time_unix_ns` | Monotonic elapsed duration and checked anchor-plus-duration end, within the signed-64-bit nanosecond range. Zero duration is valid. Overflow, reversed or inconsistent times are rejected, not repaired to fit a parent. |
| `outcome`, `error_class` | `ok`, `error`, or `cancelled`, with an enumerated safe classification on failures. No raw error messages. |
| `collection_id` | Optional scoped opaque collection identity. Missing means unknown. |
| `query_shape_id`, `query_shape_version` | Optional established native document-query shape, not a SQL query ID. Both must be present together. |
| `plan` | Optional bounded summary: scan kind, logical index-name IDs, fingerprint, and explicit completeness/unsupported/truncation flags. `complete` describes scan classification; `index_names_complete` separately describes logical name coverage and defaults to the original v1 assumption of true when omitted by older records. |
| `work` | Optional genuinely measured document counts. Null/missing does not become zero. Returned documents mean first-batch documents emitted; examined documents mean target-collection documents considered by the instrumented read, never incidental catalog rows or plan estimates. |

The OTLP resource identifies `documentdb-backend`, the configured real deployment
scope, and the producer instance. Its library scope is `documentdb.backend` with
the exporter library version; the record schema version is a separate attribute.
No default SDK resource/environment detectors or invented Kubernetes attributes
are used. Each sink accepts one producer and one epoch per batch. Exports are
single-flight, have a two-second deadline, and retain no retry queue.
Delivery is not promised exactly once. A caller must resend the original
immutable record, not reconstruct attributes under the same span ID.

### Identity and privacy

The exporter validates and preserves producer-supplied identities. It does not
derive collection IDs, query-shape IDs, index-name IDs, or plan fingerprints and
does not accept an identity key. Missing identities remain unknown. An operation
name alone does not establish that two spans refer to the same query or collection.

The native schema excludes raw statements, literals, collection names, index
names, and raw error messages. Unknown fields are rejected rather than forwarded.
Deployment labels must be explicitly chosen, bounded, non-sensitive values.
An observed scan change and increased examined-document counts can establish
changed execution behavior, but do not independently prove an index was dropped
or identify the cause.

## Collector bounds and remaining integration

| Surface | Enforced here |
| --- | --- |
| Native record | At most 2 KiB serialized JSON. |
| Export batch | 1-128 records and at most 256 KiB serialized JSON, with ordered sequences. |
| Native SQL | One read-only connection; two-second connect/query deadlines and statement timeout. Capability data and connection configuration are at most 16 KiB each. |
| Native polling | One immutable pending batch and serial approximately one-second polls. Strict `export-native` stops after three consecutive delivery attempts; `serve-native` retries with bounded backoff. No durable queue or cursor. |
| Plan summary | At most eight used index-name IDs; malformed identities and inconsistent coverage flags are rejected. |

These are payload and concurrency bounds, **not a total-memory guarantee**.
The PostgreSQL client can allocate a server response before the collector checks
its payload.

A standard OTLP receiver is the ingestion target. Querying retained metrics and
traces and packaging read-only investigation skills are separate integration
work. Gateway-only analysis must remain useful without native spans.
Recording configured, storage allocated, and data received remain separate
states. Neither a gateway flag, a binary version string, nor a context receipt
log proves that the deployed SQL API exists or that a native span was recorded.

This is a local demo PoC, not production-hardened telemetry infrastructure.
Focused checks cover the actual native path and contract; broad stress,
multi-version, and performance campaigns are not claimed.

## Focused validation

```bash
cargo test -p documentdb_backend_telemetry_exporter --locked
cargo clippy -p documentdb_backend_telemetry_exporter --all-targets --locked -- -D warnings
cargo make format-check
```

For the native compatibility smoke check, the harness saves a real SQL batch
as bounded JSON and sets `DOCUMENTDB_TELEMETRY_TEST_NATIVE_BATCH` to that file:

```bash
cargo test -p documentdb_backend_telemetry_exporter --test live_native --locked -- --ignored
```

For the SQL-to-OTLP smoke check, also set `DOCUMENTDB_TELEMETRY_TEST_PG_URI` to
the dedicated reader URI, including `sslmode=disable`. Enable storage, recording,
and context for both the reader and workload connections, then execute sampled,
successful Finds and capture their native batch. The test starts its own loopback
OTLP receiver, rejects the first delivery acknowledgement, and checks that the
retry is identical and a subsequent poll does not resend acknowledged records.
It also checks scan/work values, coverage flags, and missing identities on the
OTLP wire:

```bash
cargo test -p documentdb_backend_telemetry_exporter --test live_export --locked -- --ignored
```

Fixture tests exercise identity/timestamp preservation on the OTLP wire,
duplicate delivery, failed destinations, malformed/oversized records,
source/epoch/cursor validation, missing or measured work, incomplete plan
coverage, and native-only CLI errors without argument disclosure.
Native producer behavior is covered separately by `commands_backend_telemetry`
in the engine's SQL regression suite.
The focused PostgreSQL 16 scan fixture uses explicit planner controls on 64
documents. Its indexed/absent-index/restored executions return one document and
examine 1/64/1, respectively; a point read examines one, and a filter rejecting
every document returns zero while examining 64. A separate real-export fixture
also exercises an unsupported parallel plan with an unknown examined count.
These checks establish measured values for those actual executions, not default
planner choices, complete traces, production overhead, or an agent diagnosis.
