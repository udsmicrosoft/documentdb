/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

SET search_path TO documentdb_api, documentdb_core, documentdb_api_catalog;
SET documentdb.next_collection_id TO 8940000;
SET documentdb.next_collection_index_id TO 8940000;

-- Planner statistics for an indexed path, comparing a scalar path with an
-- array path that holds the same values.
--
-- Both collections have 1000 documents and a single field index on "a":
--   * The value 1000 is in "a" for (nearly) every document, so
--     { "a": { "$eq": 1000 } } should be estimated as not selective (~1.0).
--   * The value 500 is in "a" for only 2 documents, so
--     { "a": { "$eq": 500 } } should be estimated as very selective (~0.002).
--
-- Equality on an array path matches when any element is equal. By default
-- the statistics describe whole array values, so the estimates for the array
-- path do not track the data. When array value unpacking is enabled, the
-- statistics are collected over the distinct elements of each array and scaled
-- back to fractions of the sampled documents, so the array estimates match the
-- data. Array value unpacking is enabled when either:
--   * documentdb_core.bson_stats_enable_array_value_unpack is on, or
--   * documentdb.enable_array_bson_stats_with_planner_statistics and
--     documentdb.enablePlannerStatisticsNewCollections are both on.
--
-- The selectivity is read from the extended EXPLAIN cost line of the "a_1"
-- index and classified into coarse buckets so the output does not depend on
-- exact floating point values or the PostgreSQL version.

SET documentdb.enablePerCollectionPlannerStatistics TO on;
SET documentdb.enableExplainScanIndexCosts TO on;
SET documentdb.enableExtendedExplainPlans TO on;

CREATE SCHEMA bson_array_stats_selectivity_tests;

CREATE FUNCTION bson_array_stats_selectivity_tests.classify_a_1_selectivity(
    p_collection text, p_filter text) RETURNS text
LANGUAGE plpgsql AS $fn$
DECLARE
    v_row text;
    v_selectivity numeric := NULL;
BEGIN
    -- Force the a_1 index scan so its extended cost line is emitted.
    PERFORM set_config('enable_seqscan', 'off', true);
    PERFORM set_config('enable_bitmapscan', 'off', true);

    FOR v_row IN
        EXECUTE format(
            $q$EXPLAIN (COSTS OFF) SELECT document FROM documentdb_api_catalog.bson_aggregation_find(%L, %L)$q$,
            'array_stats_db',
            format('{ "find": "%s", "filter": %s }', p_collection, p_filter))
    LOOP
        IF v_row LIKE '%a_1: (%startup cost=%selectivity=%' THEN
            v_selectivity := substring(v_row from 'selectivity=([0-9.eE+-]+)')::numeric;
            EXIT;
        END IF;
    END LOOP;

    IF v_selectivity IS NULL THEN
        RETURN 'a_1 selectivity not found in EXPLAIN output';
    END IF;

    RETURN CASE
        WHEN v_selectivity >= 0.5 THEN 'not selective (selectivity >= 0.5)'
        WHEN v_selectivity < 0.01 THEN 'very selective (selectivity < 0.01)'
        ELSE 'moderately selective (0.01 <= selectivity < 0.5)'
    END;
END;
$fn$;

-- Scalar "a": 998 documents have a = 1000 and 2 documents have a = 500.
SELECT documentdb_api.create_collection_view('array_stats_db',
    '{ "create": "scalar_path", "statsEnabled": true }');
SELECT documentdb_api_internal.create_indexes_non_concurrently('array_stats_db',
    '{ "createIndexes": "scalar_path", "indexes": [ { "key": { "a": 1 }, "name": "a_1" } ] }', TRUE);

SELECT COUNT(documentdb_api.insert_one('array_stats_db', 'scalar_path',
    bson_build_document('_id', i, 'a', CASE WHEN i IN (499, 500) THEN 500 ELSE 1000 END)))
FROM generate_series(1, 1000) i;
ANALYZE documentdb_data.documents_8940001;

-- Array "a": every document has a = [ 1000, i, i + 1 ], so 1000 is in all 1000
-- documents and 500 is only in the documents with i = 499 and i = 500.
SELECT documentdb_api.create_collection_view('array_stats_db',
    '{ "create": "array_path", "statsEnabled": true }');
SELECT documentdb_api_internal.create_indexes_non_concurrently('array_stats_db',
    '{ "createIndexes": "array_path", "indexes": [ { "key": { "a": 1 }, "name": "a_1" } ] }', TRUE);

SELECT COUNT(documentdb_api.insert_one('array_stats_db', 'array_path',
    FORMAT('{ "_id": %s, "a": [ 1000, %s, %s ] }', i, i, i + 1)::bson))
FROM generate_series(1, 1000) i;
ANALYZE documentdb_data.documents_8940002;

-- The actual number of matching documents is the same for both collections.
SELECT documentdb_api.count_query('array_stats_db', '{ "count": "scalar_path", "query": { "a": { "$eq": 1000 } } }');
SELECT documentdb_api.count_query('array_stats_db', '{ "count": "scalar_path", "query": { "a": { "$eq": 500 } } }');
SELECT documentdb_api.count_query('array_stats_db', '{ "count": "array_path", "query": { "a": { "$eq": 1000 } } }');
SELECT documentdb_api.count_query('array_stats_db', '{ "count": "array_path", "query": { "a": { "$eq": 500 } } }');

-- The collected statistics for "a" with array value unpacking off (default).
-- The scalar path has 2 distinct values with 1000 as the most common one. The
-- array path is collected as whole array values, which are all distinct
-- (n_distinct = -1), so the common element 1000 is not a most common value.
SELECT tablename, n_distinct, (most_common_vals::text::bson[])[1:2] AS top_mcv
FROM pg_stats_ext_exprs
WHERE tablename IN ('documents_8940001', 'documents_8940002') AND expr LIKE '%''a''%'
ORDER BY tablename;

-- Scalar path: the estimates match the data.
-- 1000 matches 998 of 1000 documents: not selective.
SELECT bson_array_stats_selectivity_tests.classify_a_1_selectivity(
    'scalar_path', '{ "a": { "$eq": 1000 } }') AS scalar_eq_common;

-- 500 matches 2 of 1000 documents: very selective.
SELECT bson_array_stats_selectivity_tests.classify_a_1_selectivity(
    'scalar_path', '{ "a": { "$eq": 500 } }') AS scalar_eq_rare;

-- Array path with unpacking off: the estimates do not match the data.
-- 1000 matches all 1000 documents, so this should be not selective, but it is
-- estimated as very selective.
SELECT bson_array_stats_selectivity_tests.classify_a_1_selectivity(
    'array_path', '{ "a": { "$eq": 1000 } }') AS array_eq_common_unpack_off;

-- 500 matches 2 of 1000 documents: very selective.
SELECT bson_array_stats_selectivity_tests.classify_a_1_selectivity(
    'array_path', '{ "a": { "$eq": 500 } }') AS array_eq_rare_unpack_off;

-- The feature flag alone does not unpack array values when planner
-- statistics for new collections are disabled: n_distinct stays -1 with no
-- most common values.
SET documentdb.enable_array_bson_stats_with_planner_statistics TO on;
SET documentdb.enablePlannerStatisticsNewCollections TO off;
ANALYZE documentdb_data.documents_8940002;
SELECT tablename, n_distinct, (most_common_vals::text::bson[])[1:2] AS top_mcv
FROM pg_stats_ext_exprs
WHERE tablename = 'documents_8940002' AND expr LIKE '%''a''%';

-- Enable array value unpacking through the feature flag with planner
-- statistics for new collections enabled, and re-collect the statistics.
SET documentdb.enablePlannerStatisticsNewCollections TO on;
ANALYZE documentdb_data.documents_8940001;
ANALYZE documentdb_data.documents_8940002;

-- The scalar path is unchanged. The array path is now collected per element:
-- it has about 1001 distinct element values (stored as an absolute count) and
-- 1000 is the most common one.
SELECT tablename, n_distinct, (most_common_vals::text::bson[])[1:2] AS top_mcv
FROM pg_stats_ext_exprs
WHERE tablename IN ('documents_8940001', 'documents_8940002') AND expr LIKE '%''a''%'
ORDER BY tablename;

-- Scalar path estimates are unchanged.
SELECT bson_array_stats_selectivity_tests.classify_a_1_selectivity(
    'scalar_path', '{ "a": { "$eq": 1000 } }') AS scalar_eq_common_unpack_on;
SELECT bson_array_stats_selectivity_tests.classify_a_1_selectivity(
    'scalar_path', '{ "a": { "$eq": 500 } }') AS scalar_eq_rare_unpack_on;

-- Array path with unpacking on: the estimates match the data.
-- 1000 matches all 1000 documents: not selective.
SELECT bson_array_stats_selectivity_tests.classify_a_1_selectivity(
    'array_path', '{ "a": { "$eq": 1000 } }') AS array_eq_common_unpack_on;

-- 500 matches 2 of 1000 documents: very selective.
SELECT bson_array_stats_selectivity_tests.classify_a_1_selectivity(
    'array_path', '{ "a": { "$eq": 500 } }') AS array_eq_rare_unpack_on;

-- Turn off the feature flag and re-collect: array values are no longer
-- unpacked.
RESET documentdb.enable_array_bson_stats_with_planner_statistics;
ANALYZE documentdb_data.documents_8940002;
SELECT tablename, n_distinct, (most_common_vals::text::bson[])[1:2] AS top_mcv
FROM pg_stats_ext_exprs
WHERE tablename = 'documents_8940002' AND expr LIKE '%''a''%';

-- The core GUC alone also enables array value unpacking.
SET documentdb_core.bson_stats_enable_array_value_unpack TO on;
ANALYZE documentdb_data.documents_8940002;
SELECT tablename, n_distinct, (most_common_vals::text::bson[])[1:2] AS top_mcv
FROM pg_stats_ext_exprs
WHERE tablename = 'documents_8940002' AND expr LIKE '%''a''%';
SELECT bson_array_stats_selectivity_tests.classify_a_1_selectivity(
    'array_path', '{ "a": { "$eq": 1000 } }') AS array_eq_common_core_guc_on;

DROP SCHEMA bson_array_stats_selectivity_tests CASCADE;
SELECT documentdb_api.drop_collection('array_stats_db', 'scalar_path');
SELECT documentdb_api.drop_collection('array_stats_db', 'array_path');

RESET documentdb.enablePerCollectionPlannerStatistics;
RESET documentdb.enableExplainScanIndexCosts;
RESET documentdb.enableExtendedExplainPlans;
RESET documentdb_core.bson_stats_enable_array_value_unpack;
RESET documentdb.enable_array_bson_stats_with_planner_statistics;
RESET documentdb.enablePlannerStatisticsNewCollections;
