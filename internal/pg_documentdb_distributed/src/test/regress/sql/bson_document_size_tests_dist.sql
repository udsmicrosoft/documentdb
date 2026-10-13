SET search_path TO documentdb_api,documentdb_core,documentdb_api_catalog;
SET citus.next_shard_id TO 271500000;
SET documentdb.next_collection_id TO 27150000;
SET documentdb.next_collection_index_id TO 27150000;

-- Inline document compression with distributed execution. Stored size of a
-- document of about 1.5 KB of data with the threshold set to 1024 (about 620
-- to 690 bytes means compressed).
--
-- "remote" means local shard execution is off
-- (documentdb.useLocalExecutionShardQueries and citus.enable_local_execution),
-- so the write is sent to the shard through the distributed executor. "local"
-- means the write goes directly to the shard table on this node. All cases
-- are remote unless they say local.
--
-- | Operation                                         | Stored size | Compressed? |
-- |---------------------------------------------------|-------------|-------------|
-- | insertOne local                                   | 629         | yes         |
-- | insertOne remote (TODO)                           | 1539        | no          |
-- | insertMany local                                  | 633         | yes         |
-- | insertMany remote (TODO)                          | 1538        | no          |
-- | update updateByObjectId                           | 635-685     | yes         |
-- | update updateOne (filter not on _id)              | 628         | yes         |
-- | update updateMany (TODO, not implemented)         | 1538        | no          |
-- | update upsert                                     | 626         | yes         |
-- | update_txn_proc updateByObjectId                  | 622         | yes         |
-- | update_txn_proc updateOne (filter not on _id)     | 631         | yes         |
-- | update_txn_proc updateMany (TODO, not implemented)| 1537        | no          |
-- | update_txn_proc upsert                            | 624         | yes         |
-- | Raw UPDATE through the test function              | 623         | yes         |
-- | Raw INSERT through the test function (TODO)       | 1537        | no          |
--
-- With the threshold set to -1 the documents are stored uncompressed.
--
-- How the write paths differ:
-- * insert_one(db, collection, document) is a SQL wrapper that builds an
--   insert command with a single document and calls insert(). insertMany is
--   insert() with several documents. All of them are the same insert command:
--   one document goes through ProcessInsertion and a batch goes through
--   DoMultiInsertWithoutTransactionId.
-- * Single document updates and upserts (update() and update_txn_proc with
--   multi false) are routed to update_worker on the shard. The write then
--   runs on the node that owns the shard, with the shard table known, and the
--   document is compressed right before it is written.
--
-- Why remote inserts are not compressed (TODO):
-- * Without local shard execution there is no shard table, so the insert is
--   built as an INSERT on the distributed table (CreateInsertQuery) and run
--   through the distributed executor (RunInsertQuery). The coordinator
--   compresses the document parameter, but the executor sends the value to
--   the shard over the connection in its serialized form, which is always the
--   full uncompressed document. The shard stores what it receives and nothing
--   compresses it again. This applies to single document and batch inserts.
-- * Possible fix: compress on the shard, for example by routing inserts to
--   insert_worker like updates are routed to update_worker. Inserts with a
--   transaction id already call insert_worker, which is not covered here.

-- Run every write without local shard execution so that the commands are sent
-- to the shards through the distributed executor.
SET documentdb.useLocalExecutionShardQueries TO off;
SET citus.enable_local_execution TO off;

-- Session level settings are not sent to the connections that run the shard
-- writes. Run the test as a role that has the compression threshold set, so
-- that the shard connections opened for the role use it. Cases that need a
-- different threshold use SET LOCAL, which citus.propagate_set_commands
-- forwards to the shard connections.
SELECT current_user AS original_test_user \gset
CREATE ROLE doc_size_compress_user WITH LOGIN SUPERUSER IN ROLE :original_test_user;
ALTER ROLE doc_size_compress_user SET documentdb.document_toast_compression_threshold TO 1024;
SET ROLE doc_size_compress_user;
SET citus.propagate_set_commands TO 'local';

-- Documents of roughly 1.5 KB with nested documents, arrays, dates, and a
-- string or compound _id. Verify the stored size after insert and after an
-- in place single document update.
-- TODO: The insert_one calls below are stored uncompressed (see the header
-- for why). The updates that follow run on the shard and are stored
-- compressed.
SELECT documentdb_api.create_collection('doc_size_db', 'doc_size_string_id');

SELECT documentdb_api.insert_one('doc_size_db', 'doc_size_string_id', '{"_id": "hijklmnopqrstuvwxyz0123456789abcdefg", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}');

SELECT pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150001 ORDER BY object_id;

SELECT document FROM bson_aggregation_find('doc_size_db', '{ "find": "doc_size_string_id", "filter": { "_id": "hijklmnopqrstuvwxyz0123456789abcdefg" } }');

-- Same length replacement keeps the size unchanged.
SELECT documentdb_api.update('doc_size_db', '{ "update": "doc_size_string_id", "updates": [ { "q": { "_id": "hijklmnopqrstuvwxyz0123456789abcdefg" }, "u": { "$set": { "f35ab": "zyxwvutsrqponmlkjihgfedc" } }, "multi": false } ] }');

SELECT pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150001 ORDER BY object_id;

-- A longer value grows the document.
SELECT documentdb_api.update('doc_size_db', '{ "update": "doc_size_string_id", "updates": [ { "q": { "_id": "hijklmnopqrstuvwxyz0123456789abcdefg" }, "u": { "$set": { "f40abc.f43abcdef": "0123456789012345678901234567890123456789" } }, "multi": false } ] }');

SELECT pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150001 ORDER BY object_id;

SELECT document FROM bson_aggregation_find('doc_size_db', '{ "find": "doc_size_string_id", "filter": { "_id": "hijklmnopqrstuvwxyz0123456789abcdefg" }, "projection": { "f35ab": 1, "f40abc": 1 } }');

-- Compound _id document.
SELECT documentdb_api.create_collection('doc_size_db', 'doc_size_compound_id');

SELECT documentdb_api.insert_one('doc_size_db', 'doc_size_compound_id', '{"_id": {"f01abcdefghijkl": "hijklmn", "f02abcde": "opqrstuvwxyz0123456789abcdefghijklmn"}, "f03abcdef": "vwxyz012345678", "f04abcdefghijklmnopqr": {}, "f05abcdefghi": ["23456789abcdefghijklmnop"], "f06abcdefghijklmno": "9abcdef", "f07abcdef": {"$date": {"$numberLong": "1700000006000"}}, "f08abcdefghijklmn": "nopqrstuvwxyz0", "f09abcd": true, "f10abcdefghijkl": {"f11abcd": "uvwxyz0123456789abcdefghijklmnopqrstuvwxyz01", "f12a": "123456789abcdefghijklmno", "f13ab": "89abcdefghijklmnopqrstuv", "f14abcd": "fghijklmnopqrstuvwxyz012", "f15ab": [{"f16a": "mnopqrstuvwx", "f17ab": "tuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0"}, {"f16a": "012345", "f17ab": "789abcdefghijklmnopqrstuvwxyz0123456789abcde"}, {"f16a": "efghijklmnop", "f17ab": "lmnopqrstuvwxyz012345678"}]}, "f18abcdefghijklm": "stuv", "f19abcdefghijklmno": "z012", "f20abcdefgh": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f21ab": true, "f22abcdefghijk": true, "f23abcdefghi": true, "f24abcdefghijklmnopqrs": true, "f25abcdefghi": true, "f26abcdefghijklm": {"$date": {"$numberLong": "1700000021000"}}, "f27abcdefghijkl": "klmnopqrstuvwxyz0123456789abcdefghij", "f28abcdef": "rstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghi", "f29abcdefghij": [], "f30abcdefghi": {"f16a": "yz012345678", "f17ab": "567"}, "f31ab": {"f32abcdef": "cdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123", "f33ab": "jklmnopqrstuvwxyz0123456"}, "f34abcdefghijk": ["qrstuvwxy", "xyz0"], "f35abcdefghi": [{"f16a": "456789abc", "f36abcdefgh": [{"f37a": {"$numberInt": "31"}}, {"f37a": {"$numberInt": "32"}}, {"f37a": {"$numberInt": "33"}}, {"f37a": {"$numberInt": "34"}}]}], "f38abcdefghij": {"$date": {"$numberLong": "1700000035000"}}, "f39abcdefgh": "abcdefghijklmnopqrstuv", "f40abcdefghijkl": [{"f41a": "hijklmnopqrst", "f42abcd": false, "f43a": "opqrs"}], "f44abc": "vwxyz0", "f45ab": "23456789abcdefghijklmnop", "f46abcdef": {"$date": {"$numberLong": "1700000041000"}}}');

SELECT pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150002 ORDER BY object_id;

SELECT document FROM bson_aggregation_find('doc_size_db', '{ "find": "doc_size_compound_id", "filter": { "_id": {"f01abcdefghijkl": "hijklmn", "f02abcde": "opqrstuvwxyz0123456789abcdefghijklmn"} } }');

-- Same length replacement keeps the size unchanged.
SELECT documentdb_api.update('doc_size_db', '{ "update": "doc_size_compound_id", "updates": [ { "q": { "_id": {"f01abcdefghijkl": "hijklmn", "f02abcde": "opqrstuvwxyz0123456789abcdefghijklmn"} }, "u": { "$set": { "f44abc": "zyxwvu" } }, "multi": false } ] }');

SELECT pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150002 ORDER BY object_id;

-- Appending to a nested array grows the document.
SELECT documentdb_api.update('doc_size_db', '{ "update": "doc_size_compound_id", "updates": [ { "q": { "_id": {"f01abcdefghijkl": "hijklmn", "f02abcde": "opqrstuvwxyz0123456789abcdefghijklmn"} }, "u": { "$push": { "f10abcdefghijkl.f15ab": { "f16a": "abcdef", "f17ab": "0123456789012345678901234567890123456789abcd" } } }, "multi": false } ] }');

SELECT pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150002 ORDER BY object_id;

SELECT document FROM bson_aggregation_find('doc_size_db', '{ "find": "doc_size_compound_id", "filter": { "_id": {"f01abcdefghijkl": "hijklmn", "f02abcde": "opqrstuvwxyz0123456789abcdefghijklmn"} }, "projection": { "f44abc": 1, "f10abcdefghijkl.f15ab": 1 } }');

SELECT documentdb_api.drop_collection('doc_size_db', 'doc_size_string_id');
SELECT documentdb_api.drop_collection('doc_size_db', 'doc_size_compound_id');

-- Upserts write the inserted document compressed as well.
SELECT documentdb_api.create_collection('doc_size_db', 'doc_size_upsert');

-- Upsert with only an _id filter.
SELECT documentdb_api.update('doc_size_db', '{ "update": "doc_size_upsert", "updates": [ { "q": { "_id": "upsertbyidabcdefghijklmnopqrstuvwxyz0" }, "u": {"_id": "upsertbyidabcdefghijklmnopqrstuvwxyz0", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}, "upsert": true } ] }');

SELECT pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150003 ORDER BY object_id;

-- Upsert with a non _id filter.
SELECT documentdb_api.update('doc_size_db', '{ "update": "doc_size_upsert", "updates": [ { "q": { "f01abcdefghijkl": "nomatchvalue" }, "u": {"_id": "upsertbyfilterabcdefghijklmnopqrstuv", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}, "upsert": true } ] }');

SELECT pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150003 ORDER BY object_id;

-- Upsert with an _id filter that matches an existing document replaces it in place.
SELECT documentdb_api.update('doc_size_db', '{ "update": "doc_size_upsert", "updates": [ { "q": { "_id": "upsertbyidabcdefghijklmnopqrstuvwxyz0" }, "u": { "$set": { "f01abcdefghijkl": "zyxwvut" } }, "upsert": true } ] }');

SELECT pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150003 ORDER BY object_id;

-- Without compression the upserted document is stored uncompressed.

BEGIN;
SET LOCAL documentdb.document_toast_compression_threshold TO -1;
SELECT documentdb_api.update('doc_size_db', '{ "update": "doc_size_upsert", "updates": [ { "q": { "_id": "upsertnocompressabcdefghijklmnopqrst" }, "u": {"_id": "upsertnocompressabcdefghijklmnopqrst", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}, "upsert": true } ] }');
COMMIT;

SELECT pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150003 ORDER BY object_id;

SELECT document FROM bson_aggregation_find('doc_size_db', '{ "find": "doc_size_upsert", "projection": { "f01abcdefghijkl": 1 }, "sort": { "_id": 1 } }');

SELECT documentdb_api.drop_collection('doc_size_db', 'doc_size_upsert');

-- Raw writes through a test function that compresses the document. The
-- function is declared immutable since distributed UPDATE queries only allow
-- immutable functions with column arguments. It is pushed down to the shard
-- for the raw updates below.
-- TODO: Raw inserts through the function are not compressed. The INSERT ...
-- SELECT is evaluated on the coordinator, which compresses the value, and the
-- row is then sent to the shard in its serialized uncompressed form. The raw
-- UPDATE is pushed down and the function runs on the shard, so it compresses.
CREATE FUNCTION documentdb_api_internal.test_compress_bson_if_needed(documentdb_core.bson)
RETURNS documentdb_core.bson LANGUAGE C IMMUTABLE STRICT AS 'pg_documentdb', $$test_compress_bson_if_needed$$;

SELECT documentdb_api.create_collection('doc_size_db', 'doc_size_raw');

-- A raw insert of the document as is is stored uncompressed.
INSERT INTO documentdb_data.documents_27150004 (shard_key_value, object_id, document)
SELECT 27150004, bson_get_value(document, '_id'), document FROM (SELECT '{"_id": "rawinsertplainabcdefghijklmnopqrstu", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}'::documentdb_core.bson AS document) d;

-- A raw insert through the function (see the TODO above).
INSERT INTO documentdb_data.documents_27150004 (shard_key_value, object_id, document)
SELECT 27150004, bson_get_value(document, '_id'), documentdb_api_internal.test_compress_bson_if_needed(document) FROM (SELECT '{"_id": "rawinsertcompressedabcdefghijklmno", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}'::documentdb_core.bson AS document) d;

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150004 ORDER BY object_id;

-- A raw update through the function compresses the uncompressed document.
UPDATE documentdb_data.documents_27150004
SET document = documentdb_api_internal.test_compress_bson_if_needed(document)
WHERE document->>'_id' = 'rawinsertplainabcdefghijklmnopqrstu';

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150004 ORDER BY object_id;

-- Without a threshold the function returns the document uncompressed.

BEGIN;
SET LOCAL documentdb.document_toast_compression_threshold TO -1;
INSERT INTO documentdb_data.documents_27150004 (shard_key_value, object_id, document)
SELECT 27150004, bson_get_value(document, '_id'), documentdb_api_internal.test_compress_bson_if_needed(document) FROM (SELECT '{"_id": "rawinsertnothresholdabcdefghijklmn", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}'::documentdb_core.bson AS document) d;
COMMIT;

-- Documents smaller than the threshold are not compressed.

BEGIN;
SET LOCAL documentdb.document_toast_compression_threshold TO 4096;
UPDATE documentdb_data.documents_27150004
SET document = documentdb_api_internal.test_compress_bson_if_needed(documentdb_core.bson_from_bytea(document::bytea))
WHERE document->>'_id' = 'rawinsertcompressedabcdefghijklmno';
COMMIT;

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150004 ORDER BY object_id;

SELECT document FROM bson_aggregation_find('doc_size_db', '{ "find": "doc_size_raw", "projection": { "f01abcdefghijkl": 1 }, "sort": { "_id": 1 } }');

SELECT documentdb_api.drop_collection('doc_size_db', 'doc_size_raw');
DROP FUNCTION documentdb_api_internal.test_compress_bson_if_needed(documentdb_core.bson);

-- insertMany batches.
-- TODO: insertMany batches are stored uncompressed. A batch is a single
-- multi row INSERT on the distributed table, and each document reaches the
-- shard uncompressed for the same reason as single document inserts (see the
-- header). The retry of the remaining documents after a duplicate key error
-- inserts them one at a time and is not compressed either.
SELECT documentdb_api.create_collection('doc_size_db', 'doc_size_insert_many');

SELECT documentdb_api.insert('doc_size_db', '{ "insert": "doc_size_insert_many", "ordered": true, "documents": [ {"_id": "insertmanyorderedabcdefghijklmnop01", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}, {"_id": "insertmanyorderedabcdefghijklmnop02", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}, {"_id": "insertmanyorderedabcdefghijklmnop03", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}} ] }');

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150005 ORDER BY object_id;

-- Unordered insertMany.
SELECT documentdb_api.insert('doc_size_db', '{ "insert": "doc_size_insert_many", "ordered": false, "documents": [ {"_id": "insertmanyunorderedabcdefghijklm01", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}, {"_id": "insertmanyunorderedabcdefghijklm02", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}} ] }');

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150005 ORDER BY object_id;

-- A duplicate _id fails the batch, and the remaining documents are retried one at a time.
SELECT documentdb_api.insert('doc_size_db', '{ "insert": "doc_size_insert_many", "ordered": false, "documents": [ {"_id": "insertmanyretryabcdefghijklmnopq01", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}, {"_id": "insertmanyorderedabcdefghijklmnop01", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}, {"_id": "insertmanyretryabcdefghijklmnopq02", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}} ] }');

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150005 ORDER BY object_id;

SELECT document FROM bson_aggregation_find('doc_size_db', '{ "find": "doc_size_insert_many", "projection": { "f01abcdefghijkl": 1 }, "sort": { "_id": 1 } }');

-- updateMany over all documents in the collection.
-- TODO: updateMany is stored uncompressed because compression is not
-- implemented for multi document updates yet.
SELECT documentdb_api.update('doc_size_db', '{ "update": "doc_size_insert_many", "updates": [ { "q": {}, "u": { "$set": { "f01abcdefghijkl": "updmany" } }, "multi": true } ] }');

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150005 ORDER BY object_id;

SELECT document FROM bson_aggregation_find('doc_size_db', '{ "find": "doc_size_insert_many", "projection": { "f01abcdefghijkl": 1 }, "sort": { "_id": 1 } }');

SELECT documentdb_api.drop_collection('doc_size_db', 'doc_size_insert_many');

-- Updates through update_txn_proc. The procedure cannot run inside a
-- transaction block, so it relies on the threshold set for the role.
SELECT documentdb_api.create_collection('doc_size_db', 'doc_size_txn_proc');

-- The inserted document is stored uncompressed (see the TODO in the header).
SELECT documentdb_api.insert_one('doc_size_db', 'doc_size_txn_proc', '{"_id": "txnprocupdateabcdefghijklmnopqrstu", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}');

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150006 ORDER BY object_id;

-- An in place update through the procedure compresses the document.
CALL documentdb_api.update_txn_proc('doc_size_db', '{ "update": "doc_size_txn_proc", "updates": [ { "q": { "_id": "txnprocupdateabcdefghijklmnopqrstu" }, "u": { "$set": { "f01abcdefghijkl": "txnproc" } } } ] }');

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150006 ORDER BY object_id;

-- An upsert through the procedure writes the inserted document compressed.
CALL documentdb_api.update_txn_proc('doc_size_db', '{ "update": "doc_size_txn_proc", "updates": [ { "q": { "_id": "txnprocupsertabcdefghijklmnopqrstu" }, "u": {"_id": "txnprocupsertabcdefghijklmnopqrstu", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}, "upsert": true } ] }');

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150006 ORDER BY object_id;

-- Without compression the upserted document is stored uncompressed. The
-- original role does not have the threshold set.
RESET ROLE;
CALL documentdb_api.update_txn_proc('doc_size_db', '{ "update": "doc_size_txn_proc", "updates": [ { "q": { "_id": "txnprocnocompressabcdefghijklmnopq" }, "u": {"_id": "txnprocnocompressabcdefghijklmnopq", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}, "upsert": true } ] }');
SET ROLE doc_size_compress_user;

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150006 ORDER BY object_id;

SELECT document FROM bson_aggregation_find('doc_size_db', '{ "find": "doc_size_txn_proc", "projection": { "f01abcdefghijkl": 1 }, "sort": { "_id": 1 } }');

SELECT documentdb_api.drop_collection('doc_size_db', 'doc_size_txn_proc');

-- updateOne with a filter that is not on _id, through update() and through
-- update_txn_proc, and updateMany through update_txn_proc. Each document has
-- a unique value in f01abcdefghijkl that the filters match on. The inserts
-- are stored uncompressed (see the TODO in the header).
SELECT documentdb_api.create_collection('doc_size_db', 'doc_size_update_kinds');
SELECT documentdb_api.insert_one('doc_size_db', 'doc_size_update_kinds', '{"_id": "updatekindsfilterabcdefghijklmnopq", "f01abcdefghijkl": "markera", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}');
SELECT documentdb_api.insert_one('doc_size_db', 'doc_size_update_kinds', '{"_id": "updatekindstxnfilterabcdefghijklmn", "f01abcdefghijkl": "markerb", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}');
SELECT documentdb_api.insert_one('doc_size_db', 'doc_size_update_kinds', '{"_id": "updatekindstxnmanyabcdefghijklmnop", "f01abcdefghijkl": "markerc", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}');

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150007 ORDER BY object_id;

-- updateOne through update() with a non _id filter compresses the document.
SELECT documentdb_api.update('doc_size_db', '{ "update": "doc_size_update_kinds", "updates": [ { "q": { "f01abcdefghijkl": "markera" }, "u": { "$set": { "f01abcdefghijkl": "updatea" } } } ] }');

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150007 ORDER BY object_id;

-- updateOne through update_txn_proc with a non _id filter compresses the document.
CALL documentdb_api.update_txn_proc('doc_size_db', '{ "update": "doc_size_update_kinds", "updates": [ { "q": { "f01abcdefghijkl": "markerb" }, "u": { "$set": { "f01abcdefghijkl": "updateb" } } } ] }');

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150007 ORDER BY object_id;

-- TODO: updateMany through update_txn_proc is stored uncompressed because
-- compression is not implemented for multi document updates yet.
CALL documentdb_api.update_txn_proc('doc_size_db', '{ "update": "doc_size_update_kinds", "updates": [ { "q": { "f01abcdefghijkl": "markerc" }, "u": { "$set": { "f01abcdefghijkl": "updatec" } }, "multi": true } ] }');

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150007 ORDER BY object_id;

SELECT document FROM bson_aggregation_find('doc_size_db', '{ "find": "doc_size_update_kinds", "projection": { "f01abcdefghijkl": 1 }, "sort": { "_id": 1 } }');

SELECT documentdb_api.drop_collection('doc_size_db', 'doc_size_update_kinds');

RESET ROLE;
REASSIGN OWNED BY doc_size_compress_user TO :original_test_user;
DROP ROLE doc_size_compress_user;

-- Inserts with local shard execution. The shard is on this node, so the
-- insert is written directly to the shard table and the document is
-- compressed. The threshold is set for the session since the write runs in
-- this backend.
SET documentdb.useLocalExecutionShardQueries TO on;
SET citus.enable_local_execution TO on;
SET documentdb.document_toast_compression_threshold TO 1024;
SELECT documentdb_api.create_collection('doc_size_db', 'doc_size_local_insert');

-- insertOne with local execution.
SELECT documentdb_api.insert_one('doc_size_db', 'doc_size_local_insert', '{"_id": "localinsertoneabcdefghijklmnopqrst", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}');

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150008 ORDER BY object_id;

-- insertMany with local execution.
SELECT documentdb_api.insert('doc_size_db', '{ "insert": "doc_size_local_insert", "documents": [ {"_id": "localinsertmanyabcdefghijklmnopq01", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}}, {"_id": "localinsertmanyabcdefghijklmnopq02", "f01abcdefghijkl": "opqrstu", "f02abcde": "vwxyz0123456789abcdefghijklmnopqrstu", "f03abcdefghijk": {"f04abcd": "23456789abcdefghijklmnopqrstuvwxyz0123456789", "f05a": "9abcdefghijklmnopqrstuvw", "f06ab": "ghijklmnopqrstuvwxyz0123", "f07abcd": "nopqrstuvwxyz0123456789a"}, "f08abcdefghi": ["uvwxyz0123456789abcdefgh"], "f09abcd": "12", "f10abcdef": {"$date": {"$numberLong": "1700000010000"}}, "f11abcdefghijklmn": "fghijklmnopqrs", "f12abcd": true, "f13abcdefghijkl": {"f04abcd": "mnopqrstuvwxyz0123456789abcdefghijklmnopqrst", "f05a": "tuvwxyz0123456789abcdefg", "f06ab": "0123456789abcdefghijklmn", "f07abcd": "789abcdefghijklmnopqrstu"}, "f14abcdefghijklm": "efgh", "f15abcdefghijklmno": "lmno", "f16abcdefghijk": true, "f17abcdefghi": true, "f18abcdefghijklmnopqrs": true, "f19abcdefghi": true, "f20abcdefghij": [], "f21abcdefghi": {"f22a": "stuvwxyz012", "f23ab": "z01"}, "f24a": "6789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwx", "f25ab": {"f26abcdef": "defghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234", "f27ab": "klmnopqrstuvwxyz01234567"}, "f28abcdefghijk": ["rstuvwxyz"], "f29abcdefghi": [{"f22a": "yz0123456", "f30abcdefgh": [{"f31a": {"$numberInt": "25"}}]}], "f32abcdefghijkl": [], "f33abcdefghijklmnop": {"f24a": "cdefghijklmnop"}, "f34abc": "jklmnop", "f35ab": "qrstuvwxyz0123456789abcd", "f36abcdef": {"$date": {"$numberLong": "1700000029000"}}, "f37ab": true, "f38abcdefghij": {"$date": {"$numberLong": "1700000030000"}}, "f39abcdefgh": "bcdefghijklmnopqrstuvwxyz0123456789abcdef", "f40abc": {"f41abcd": "i", "f42abcd": "p", "f43abcdef": "wxyz", "f44ab": "3456", "f36abcdef": {"$date": {"$numberLong": "1700000036000"}}, "f12abcd": true, "f45abcdefgh": "h"}} ] }');

SELECT document->>'_id' AS id, pg_column_size(document) AS column_size, length(document::bytea) AS bytea_length
FROM documentdb_data.documents_27150008 ORDER BY object_id;

SELECT documentdb_api.drop_collection('doc_size_db', 'doc_size_local_insert');
RESET documentdb.document_toast_compression_threshold;
RESET citus.enable_local_execution;
RESET documentdb.useLocalExecutionShardQueries;
