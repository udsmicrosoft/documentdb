SET search_path TO documentdb_api,documentdb_api_internal,documentdb_core;
SET citus.next_shard_id TO 230000;
SET documentdb.next_collection_id TO 2300;
SET documentdb.next_collection_index_id TO 2300;

SELECT '{ "": [ { "a": 1 }, { "a": 2 } ] }'::bsonsequence;

SELECT bsonsequence_get_bson('{ "": [ { "a": 1 }, { "a": 2 } ] }'::bsonsequence);

SELECT bsonsequence_get_bson(bsonsequence_in(bsonsequence_out('{ "": [ { "a": 1 }, { "a": 2 } ] }'::bsonsequence)));

PREPARE q1(bytea) AS SELECT bsonsequence_get_bson($1);

EXECUTE q1('{ "": [ { "a": 1 }, { "a": 2 } ] }'::bsonsequence::bytea);

SELECT '{ "": [ { "a": 1 }, { "a": 2 } ] }'::bsonsequence::bytea::bsonsequence;

SELECT bsonsequence_get_bson('{ "a": 1 }'::bson::bsonsequence);

-- generate a long string and ensure we have the docs.
SELECT COUNT(*) FROM bsonsequence_get_bson(('{ "": [ ' || rtrim(REPEAT('{ "a": 1, "b": 2 },', 100), ',') || ' ] }')::bsonsequence);

-- sequences whose documents end exactly 5 bytes before a power-of-two buffer boundary must not write past their allocation.
SELECT length(s::bytea), (SELECT COUNT(*) FROM bsonsequence_get_bson(s))
FROM (SELECT ('{ "": [ { "a": "' || repeat('x', n) || '" }' || repeat(', { }', k) || ' ] }')::bsonsequence AS s
      FROM (VALUES (46, 1), (46, 2), (41, 2), (110, 1)) AS t(n, k)) q;
