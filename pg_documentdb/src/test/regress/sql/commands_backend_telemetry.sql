-- Copyright (c) Microsoft Corporation. All rights reserved.
-- SPDX-License-Identifier: MIT

SET search_path TO documentdb_api, documentdb_core, documentdb_api_catalog;
SET documentdb.next_collection_id TO 29701000;
SET documentdb.next_collection_index_id TO 29701000;

SHOW documentdb.enable_backend_telemetry;
SELECT capabilities->>'schema_version' AS schema_version,
       capabilities->>'storage_allocated' AS storage_allocated,
       capabilities->>'recording_enabled' AS recording_enabled
FROM documentdb_api_internal.backend_telemetry_capabilities() AS capabilities;
CREATE TEMP TABLE telemetry_start AS
SELECT (capabilities->>'epoch')::uuid AS epoch,
       (capabilities->>'latest_sequence')::bigint AS sequence
FROM documentdb_api_internal.backend_telemetry_capabilities() AS capabilities;

SELECT documentdb_api.insert_one('backend_telemetry_tests', 'reads', '{"_id":1,"value":1,"zero":0}');
SET documentdb.enable_udf_trace_context TO on;
SET documentdb.udf_trace_context_log_limit_per_second TO 0;
SET documentdb.enable_backend_telemetry TO on;

SELECT cursorPage IS NOT NULL AS find_succeeded
FROM documentdb_api.find_cursor_first_page('backend_telemetry_tests',
    '{"find":"reads","filter":{"value":1},"limit":1,"singleBatch":true,"$documentdbTrace":{"version":1,"traceparent":"00-11112222333344445555666677778888-8877665544332211-01"}}', 0);

CREATE TEMP TABLE telemetry_first AS
SELECT documentdb_api_internal.backend_telemetry_read(epoch, sequence, 128) AS batch
FROM telemetry_start;
SELECT jsonb_array_length(batch->'records') = 1 AS one_span,
       batch->>'epoch_reset' AS epoch_reset,
       batch->>'gap' AS gap,
       batch->'records'->0->>'operation' AS operation,
       batch->'records'->0->>'origin' AS origin,
       batch->'records'->0->>'trace_id' AS trace_id,
       batch->'records'->0->>'parent_span_id' AS parent_span_id,
       batch->'records'->0->>'outcome' AS outcome
FROM telemetry_first;
SELECT record->>'span_id' ~ '^[0-9a-f]{16}$'
           AND record->>'span_id' NOT IN ('0000000000000000', '8877665544332211') AS valid_child,
       (record->>'start_time_unix_ns')::bigint > 0
           AND (record->>'end_time_unix_ns')::bigint - (record->>'start_time_unix_ns')::bigint
               = (record->>'duration_ns')::bigint
           AND (record->>'duration_ns')::bigint >= 0 AS valid_times,
       record->>'trace_flags' AS trace_flags,
       length(record::text) <= 2048 AS bounded_record,
       record->'plan'->>'complete' = 'true'
           AND (record->'work'->>'returned_documents')::bigint = 1
           AND (record->'work'->>'examined_documents')::bigint >= 1 AS measured_plan_and_work,
       record->'error_class' = 'null'::jsonb AS no_error
FROM telemetry_first, LATERAL jsonb_array_elements(batch->'records') AS record;
SELECT batch->'records' =
       documentdb_api_internal.backend_telemetry_read(epoch, sequence, 128)->'records'
           AS retry_preserves_record
FROM telemetry_first CROSS JOIN telemetry_start;

BEGIN;
SAVEPOINT telemetry_error;
SELECT cursorPage FROM documentdb_api.find_cursor_first_page('backend_telemetry_tests',
    '{"find":"reads","filter":{"$expr":{"$gt":[{"$divide":[1,"$zero"]},0]}},"limit":1,"singleBatch":true,"$documentdbTrace":{"version":1,"traceparent":"00-11112222333344445555666677778888-2222222222222222-01"}}', 0);
ROLLBACK TO SAVEPOINT telemetry_error;
SELECT cursorPage IS NOT NULL AS next_untraced_find_succeeded
FROM documentdb_api.find_cursor_first_page('backend_telemetry_tests',
    '{"find":"reads","filter":{"value":1},"limit":1,"singleBatch":true}', 0);
COMMIT;

SELECT cursorPage IS NOT NULL AS unsampled_find_succeeded
FROM documentdb_api.find_cursor_first_page('backend_telemetry_tests',
    '{"find":"reads","filter":{"value":1},"limit":1,"singleBatch":true,"$documentdbTrace":{"version":1,"traceparent":"00-11112222333344445555666677778888-3333333333333333-00"}}', 0);
SELECT cursorPage IS NOT NULL AS diagnostic_catalog_succeeded
FROM documentdb_api.list_indexes_cursor_first_page('backend_telemetry_tests', '{"listIndexes":"reads"}');
SET documentdb.enable_backend_telemetry TO off;
SELECT cursorPage IS NOT NULL AS disabled_find_succeeded
FROM documentdb_api.find_cursor_first_page('backend_telemetry_tests',
    '{"find":"reads","filter":{"value":1},"limit":1,"singleBatch":true,"$documentdbTrace":{"version":1,"traceparent":"00-11112222333344445555666677778888-4444444444444444-01"}}', 0);

SELECT jsonb_array_length(batch->'records') = 2 AS only_admitted_finds_recorded,
       batch->'records'->1->>'parent_span_id' AS failed_parent,
       batch->'records'->1->>'outcome' AS failed_outcome,
       batch->'records'->1->>'error_class' AS safe_error_class,
       NOT (batch->'records'->1) ? 'work' AND NOT (batch->'records'->1) ? 'plan'
           AS failed_execution_does_not_claim_completed_work,
       batch->'records'->0->>'span_id' <> batch->'records'->1->>'span_id' AS distinct_children,
       batch::text NOT LIKE '%$divide%' AND batch::text NOT LIKE '%backend_telemetry_tests%'
           AS no_statement_or_namespace
FROM telemetry_start,
LATERAL documentdb_api_internal.backend_telemetry_read(epoch, sequence, 128) AS batch;

SELECT documentdb_api_internal.backend_telemetry_read('00000000-0000-0000-0000-000000000000', 0, 128)->>'epoch_reset' AS explicit_epoch_reset;
SELECT documentdb_api_internal.backend_telemetry_read(epoch, -1, 128) FROM telemetry_start;
SELECT documentdb_api_internal.backend_telemetry_read(epoch, sequence, 129) FROM telemetry_start;

CREATE ROLE telemetry_demo_reader;
GRANT USAGE ON SCHEMA documentdb_api_internal, documentdb_core TO telemetry_demo_reader;
SET ROLE telemetry_demo_reader;
SELECT documentdb_api_internal.backend_telemetry_capabilities();
RESET ROLE;
GRANT EXECUTE ON FUNCTION documentdb_api_internal.backend_telemetry_capabilities(),
    documentdb_api_internal.backend_telemetry_read(uuid, bigint, integer) TO telemetry_demo_reader;
SET ROLE telemetry_demo_reader;
SELECT capabilities->>'schema_version' AS reader_schema_version
FROM documentdb_api_internal.backend_telemetry_capabilities() AS capabilities;
SELECT jsonb_array_length(documentdb_api_internal.backend_telemetry_read(
    (documentdb_api_internal.backend_telemetry_capabilities()->>'epoch')::uuid, 0, 1)->'records') AS bounded_reader_batch;
RESET ROLE;
REVOKE EXECUTE ON FUNCTION documentdb_api_internal.backend_telemetry_capabilities(),
    documentdb_api_internal.backend_telemetry_read(uuid, bigint, integer) FROM telemetry_demo_reader;
REVOKE USAGE ON SCHEMA documentdb_api_internal, documentdb_core FROM telemetry_demo_reader;
DROP ROLE telemetry_demo_reader;

SET documentdb.enable_backend_telemetry TO on;
SET max_parallel_workers_per_gather TO 0;
CREATE TEMP TABLE telemetry_enrichment_start AS
SELECT (capabilities->>'epoch')::uuid AS epoch,
       (capabilities->>'latest_sequence')::bigint AS sequence
FROM documentdb_api_internal.backend_telemetry_capabilities() AS capabilities;
SELECT count(documentdb_api.insert_one('backend_telemetry_tests', 'scan_work',
    format('{"_id":%s,"value":%s}', value, value)::bson)) AS inserted
FROM generate_series(1, 64) AS value;
SELECT documentdb_api_internal.create_indexes_non_concurrently('backend_telemetry_tests',
    '{"createIndexes":"scan_work","indexes":[{"key":{"value":1},"name":"value_idx"}]}', true);

-- Select known scan paths on a small fixture, not a performance benchmark.
SET enable_seqscan TO off;
SELECT cursorPage IS NOT NULL AS indexed_find_succeeded
FROM documentdb_api.find_cursor_first_page('backend_telemetry_tests',
    '{"find":"scan_work","filter":{"value":64},"limit":1,"singleBatch":true,"$documentdbTrace":{"version":1,"traceparent":"00-11112222333344445555666677778888-5555555555555555-01"}}', 0);
CALL documentdb_api.drop_indexes('backend_telemetry_tests',
    '{"dropIndexes":"scan_work","index":["value_idx"]}');
RESET enable_seqscan;
SET enable_indexscan TO off;
SET enable_bitmapscan TO off;
SELECT cursorPage IS NOT NULL AS sequential_find_succeeded
FROM documentdb_api.find_cursor_first_page('backend_telemetry_tests',
    '{"find":"scan_work","filter":{"value":64},"limit":1,"singleBatch":true,"$documentdbTrace":{"version":1,"traceparent":"00-11112222333344445555666677778888-6666666666666666-01"}}', 0);
RESET enable_indexscan;
RESET enable_bitmapscan;
SELECT documentdb_api_internal.create_indexes_non_concurrently('backend_telemetry_tests',
    '{"createIndexes":"scan_work","indexes":[{"key":{"value":1},"name":"value_idx"}]}', true);
SET enable_seqscan TO off;
SELECT cursorPage IS NOT NULL AS restored_find_succeeded
FROM documentdb_api.find_cursor_first_page('backend_telemetry_tests',
    '{"find":"scan_work","filter":{"value":64},"limit":1,"singleBatch":true,"$documentdbTrace":{"version":1,"traceparent":"00-11112222333344445555666677778888-7777777777777777-01"}}', 0);
SELECT cursorPage IS NOT NULL AS point_read_succeeded
FROM documentdb_api.find_cursor_first_page('backend_telemetry_tests',
    '{"find":"scan_work","filter":{"_id":64},"limit":1,"singleBatch":true,"$documentdbTrace":{"version":1,"traceparent":"00-11112222333344445555666677778888-8888888888888888-01"}}', 0);
RESET enable_seqscan;
SELECT cursorPage IS NOT NULL AS constant_false_find_succeeded
FROM documentdb_api.find_cursor_first_page('backend_telemetry_tests',
    '{"find":"scan_work","filter":{"$expr":false},"limit":1,"singleBatch":true,"$documentdbTrace":{"version":1,"traceparent":"00-11112222333344445555666677778888-9999999999999999-01"}}', 0);

CREATE TEMP TABLE telemetry_enriched AS
SELECT record
FROM telemetry_enrichment_start,
LATERAL documentdb_api_internal.backend_telemetry_read(epoch, sequence, 128) AS batch,
LATERAL jsonb_array_elements(batch->'records') AS record;
SELECT record->>'parent_span_id' AS parent,
       record->'plan'->>'scan_kind' AS scan_kind,
       record->'plan'->>'complete' AS scan_complete,
       record->'plan'->>'index_names_complete' AS index_names_complete,
       record->'work'->>'returned_documents' AS returned_documents,
       record->'work'->>'examined_documents' AS examined_documents
FROM telemetry_enriched ORDER BY (record->>'sequence')::bigint;
SELECT count(*) = 5 AS all_find_records,
       bool_and(length(record::text) <= 2048
           AND record->'plan'->'used_index_name_ids' = '[]'::jsonb
           AND record->'plan'->'fingerprint' = 'null'::jsonb
           AND NOT record ? 'collection_id'
           AND NOT record ? 'query_shape_id'
           AND record::text NOT LIKE '%value_idx%'
           AND record::text NOT LIKE '%scan_work%') AS bounded_without_invented_identities
FROM telemetry_enriched;

SELECT documentdb_api.drop_collection('backend_telemetry_tests', 'scan_work');
SELECT documentdb_api.drop_collection('backend_telemetry_tests', 'reads');
DROP TABLE telemetry_first, telemetry_start, telemetry_enrichment_start, telemetry_enriched;
RESET max_parallel_workers_per_gather;
RESET documentdb.enable_backend_telemetry;
RESET documentdb.enable_udf_trace_context;
RESET documentdb.udf_trace_context_log_limit_per_second;
