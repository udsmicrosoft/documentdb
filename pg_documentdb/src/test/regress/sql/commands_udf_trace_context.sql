-- Copyright (c) Microsoft Corporation. All rights reserved.
-- SPDX-License-Identifier: MIT

SET search_path TO documentdb_api, documentdb_core, documentdb_api_catalog;
SET documentdb.next_collection_id TO 29601000;
SET documentdb.next_collection_index_id TO 29601000;
SHOW documentdb.udf_trace_context_version;
SET documentdb.udf_trace_context_version TO 2;

SET documentdb.test_udf_trace_context TO on;
SET documentdb.enable_udf_trace_context TO on;
SHOW documentdb.udf_trace_context_log_limit_per_second;
SET documentdb.udf_trace_context_log_limit_per_second TO -1;
SET documentdb.udf_trace_context_log_limit_per_second TO 10001;
SET documentdb.udf_trace_context_log_limit_per_second TO 10000;
-- Exercise the gateway-facing UDFs with reusable generic prepared statements.
SET plan_cache_mode TO force_generic_plan;
PREPARE udf_trace_count(text, bson) AS SELECT document FROM documentdb_api.count_query($1, $2);
PREPARE udf_trace_insert(text, bson) AS SELECT * FROM documentdb_api.insert($1, $2, NULL, NULL);

EXECUTE udf_trace_insert('udf_trace_tests', '{"insert":"events","documents":[{"_id":1}],"comment":{"application":[1,"two"]},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01","tracestate":"vendor=value"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"comment":"application","$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$DOCUMENTDBTRACE":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01","tracestate":"1tenant@vendor=value, other=two"}}');

EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"comment":""}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"comment":{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}}');

EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":1}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":[]}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":2,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1.0,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},"$DOCUMENTDBTRACE":{}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{},"$documentdbTrace":{}}');

EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"invalid"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-00000000000000000000000000000000-b7ad6b7169203331-01"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-0AF7651916CD43DD8448EB211C80319C-B7AD6B7169203331-01"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01","tracestate":1}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01","tracestate":"a=1,a=2"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01","tracestate":"a=1,"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01","tracestate":"a=1=2"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01","baggage":"not accepted"}}');
EXECUTE udf_trace_count('udf_trace_tests', ('{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01","tracestate":"a=' || repeat('x', 513) || '"}}')::bson);
EXECUTE udf_trace_count('udf_trace_tests', ('{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01","padding":"' || repeat('x', 1024) || '"}}')::bson);

SET documentdb.enable_udf_trace_context TO off;
EXECUTE udf_trace_insert('udf_trace_tests', '{"insert":"events","documents":[{"_id":2}],"comment":"","$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":2},"$documentdbTrace":{"version":99}}');
SET documentdb.enable_udf_trace_context TO on;

EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":1}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":1,"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}}');
EXECUTE udf_trace_insert('udf_trace_tests', '{"insert":"events","documents":1}');
EXECUTE udf_trace_insert('udf_trace_tests', '{"insert":"events","documents":1,"$documentdbTrace":{}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1}}');

BEGIN;
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}}');
SAVEPOINT trace_error;
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":1,"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}}');
ROLLBACK TO SAVEPOINT trace_error;
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1}}');
ROLLBACK;

-- Shared command processing covers functions, transaction procedures, and bulk procedures.
SELECT * FROM documentdb_api.update('udf_trace_tests', '{"update":"events","updates":[{"q":{"_id":1},"u":{"$set":{"label":"Alpha"}}}],"comment":[1,"two"],"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-1111111111111111-01"}}', NULL, NULL);
CALL documentdb_api.update_txn_proc('udf_trace_tests', '{"update":"events","updates":[{"q":{"_id":2},"u":{"$set":{"label":"alpha"}}}],"comment":"","$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-2222222222222222-01"}}', NULL, NULL);
CALL documentdb_api.update_bulk('udf_trace_tests', '{"update":"events","updates":[{"q":{},"u":{"$set":{"active":true}},"multi":true}],"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-3333333333333333-01"}}', NULL, NULL);
CALL documentdb_api.insert_txn_proc('udf_trace_tests', '{"insert":"events","documents":[{"_id":3}],"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-4444444444444444-01"}}', NULL, NULL);
CALL documentdb_api.insert_bulk('udf_trace_tests', '{"insert":"events","documents":[{"_id":4},{"_id":5}],"ordered":false,"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-5555555555555555-01"}}', NULL, NULL);
SELECT * FROM documentdb_api.find_and_modify('udf_trace_tests', '{"findAndModify":"events","query":{"_id":3},"update":{"$set":{"active":true}},"new":true,"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-6666666666666666-01"}}', NULL);
SELECT * FROM documentdb_api.find_and_modify('udf_trace_tests', '{"findAndModify":"events","query":{"_id":3},"update":{"$set":{"active":true}},"$documentdbTrace":{},"$documentdbTrace":{}}', NULL);
SELECT * FROM documentdb_api.delete('udf_trace_tests', '{"delete":"events","deletes":[{"q":{"_id":5},"limit":1}],"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-7777777777777777-01"}}', NULL, NULL);
CALL documentdb_api.delete_txn_proc('udf_trace_tests', '{"delete":"events","deletes":[{"q":{"_id":4},"limit":1}],"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-8888888888888888-01"}}', NULL, NULL);

-- Reused prepared statements must use the current parent's ID in both planning modes.
PREPARE udf_trace_distinct(text, bson) AS SELECT document FROM documentdb_api.distinct_query($1, $2);
EXECUTE udf_trace_distinct('udf_trace_tests', '{"distinct":"events","key":"active","$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-1111111111111111-01"}}');
SET plan_cache_mode TO force_custom_plan;
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-2222222222222222-01"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":2},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-3333333333333333-01"}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":999},"$documentdbTrace":{}}');
EXECUTE udf_trace_distinct('udf_trace_tests', '{"distinct":"events","key":"active","$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-4444444444444444-01"}}');
SET documentdb_core.enableCollation TO on;
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"label":"ALPHA"},"collation":{"locale":"en","strength":2},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-5555555555555555-01"}}');
RESET documentdb_core.enableCollation;

-- Cursor continuations must not carry the opening request's context.
CREATE TEMP TABLE udf_trace_cursor AS
SELECT cursorPage, continuation FROM documentdb_api.find_cursor_first_page('udf_trace_tests', '{"find":"events","sort":{"_id":1},"batchSize":1,"comment":{"application":"unchanged"},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-1111111111111111-01"}}', 4294967294);
SELECT cursorPage::text NOT LIKE '%$documentdbTrace%' AS clean_page,
       continuation IS NOT NULL AND continuation::text NOT LIKE '%$documentdbTrace%' AS clean_continuation
FROM udf_trace_cursor;
SELECT cursorPage FROM documentdb_api.cursor_get_more('udf_trace_tests', '{"getMore":{"$numberLong":"4294967294"},"collection":"events","batchSize":10,"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-2222222222222222-01"}}', (SELECT continuation FROM udf_trace_cursor));
DROP TABLE udf_trace_cursor;
CREATE TEMP TABLE udf_trace_cursor AS
SELECT cursorPage, continuation FROM documentdb_api.aggregate_cursor_first_page('udf_trace_tests', '{"aggregate":"events","pipeline":[{"$sort":{"_id":1}}],"cursor":{"batchSize":1},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-3333333333333333-01"}}', 4294967293);
SELECT cursorPage::text NOT LIKE '%$documentdbTrace%' AS clean_page,
       continuation IS NOT NULL AND continuation::text NOT LIKE '%$documentdbTrace%' AS clean_continuation
FROM udf_trace_cursor;
SELECT cursorPage FROM documentdb_api.cursor_get_more('udf_trace_tests', '{"getMore":{"$numberLong":"4294967293"},"collection":"events","batchSize":10,"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-4444444444444444-01"}}', (SELECT continuation FROM udf_trace_cursor));
DROP TABLE udf_trace_cursor;

-- Check deltas without resetting counters shared with other tests.
CREATE TEMP TABLE udf_trace_counter_before AS
SELECT wanted.feature_name,
       COALESCE(stats.usage_count, 0)::bigint AS usage_count
FROM (VALUES
    ('udf_trace_context_log_dropped'),
    ('udf_trace_context_rejected')
) AS wanted(feature_name)
LEFT JOIN documentdb_api_internal.command_feature_counter_stats(false)
    AS stats USING (feature_name);

SET documentdb.udf_trace_context_log_limit_per_second TO 0;
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{}}');
EXECUTE udf_trace_count('udf_trace_tests', '{"count":"events","query":{"_id":1},"$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-5555555555555555-01"}}');
SELECT before.feature_name,
       COALESCE(stats.usage_count, 0)::bigint - before.usage_count AS increment
FROM udf_trace_counter_before AS before
LEFT JOIN documentdb_api_internal.command_feature_counter_stats(false)
    AS stats USING (feature_name)
ORDER BY before.feature_name;
DROP TABLE udf_trace_counter_before;
RESET documentdb.udf_trace_context_log_limit_per_second;

-- Common command parsing ignores carrier metadata without adding trace entrypoints.
SELECT documentdb_api.create_collection_view('udf_trace_tests', '{"create":"untraced","$documentdbTrace":{"version":1,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-5555555555555555-01"}}');
SELECT cursorPage IS NOT NULL AS list_indexes_ignores_trace
FROM documentdb_api.list_indexes_cursor_first_page('udf_trace_tests', '{"listIndexes":"untraced","$DOCUMENTDBTRACE":42}');

CREATE FUNCTION pg_temp.udf_trace_explain(p_explain_spec bson, p_append_cursor_params boolean DEFAULT false)
RETURNS bson
LANGUAGE c VOLATILE
AS 'pg_documentdb', 'documentdb_explain';
SELECT pg_temp.udf_trace_explain('{"explain":{"find":"events"},"$db":"udf_trace_tests","verbosity":"queryPlanner","$documentdbTrace":{}}') IS NOT NULL AS explain_ignores_trace;
DROP FUNCTION pg_temp.udf_trace_explain(bson, boolean);

SET documentdb.enable_udf_trace_context TO off;
SELECT cursorPage IS NOT NULL AS list_indexes_ignores_disabled_trace
FROM documentdb_api.list_indexes_cursor_first_page('udf_trace_tests', '{"listIndexes":"untraced","$documentdbTrace":{}}');
SET documentdb.enable_udf_trace_context TO on;
SELECT cursorPage FROM documentdb_api.list_indexes_cursor_first_page('udf_trace_tests', '{"listIndexes":"untraced","$documentdbTraceExtra":{}}');
SELECT documentdb_api.drop_collection('udf_trace_tests', 'untraced');

SELECT document FROM documentdb_api.collection('udf_trace_tests', 'events') ORDER BY object_id;
SELECT documentdb_api.drop_collection('udf_trace_tests', 'events');
DEALLOCATE udf_trace_count;
DEALLOCATE udf_trace_insert;
DEALLOCATE udf_trace_distinct;
RESET plan_cache_mode;
RESET documentdb.enable_udf_trace_context;
RESET documentdb.test_udf_trace_context;
