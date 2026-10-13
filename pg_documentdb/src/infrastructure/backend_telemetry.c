/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

#include <postgres.h>
#include <fmgr.h>
#include <math.h>
#include <miscadmin.h>
#include <common/int.h>
#include <executor/executor.h>
#include <executor/instrument.h>
#include <lib/stringinfo.h>
#include <parser/parsetree.h>
#include <port/atomics.h>
#include <storage/lwlock.h>
#include <storage/shmem.h>
#include <utils/builtins.h>
#include <utils/jsonb.h>
#include <utils/rel.h>
#include <utils/timestamp.h>
#include <utils/uuid.h>

#include "infrastructure/backend_telemetry.h"

#define TELEMETRY_SCHEMA_VERSION 1
#define TELEMETRY_RECORD_BYTES 2048
#define TELEMETRY_BATCH_RECORDS 128
#define TELEMETRY_BATCH_BYTES (256 * 1024)
#define TELEMETRY_MEMORY_BYTES (4 * 1024 * 1024)
#define TELEMETRY_ADMISSION_PER_SECOND 64
#define TELEMETRY_PLAN_NODES 64
#define TELEMETRY_PLAN_DEPTH 16

/* Leave room for slot and buffer metadata within the shared-memory budget. */
#define TELEMETRY_SLOTS 2000
#define TELEMETRY_RESPONSE_RESERVE_BYTES 2048
#define TELEMETRY_JSON_SPACING_BYTES 128

typedef enum TelemetryLoss
{
	TelemetryLoss_Admission,
	TelemetryLoss_Contention,
	TelemetryLoss_Overwrite,
	TelemetryLoss_Oversize,
	TelemetryLoss_Clock,
	TelemetryLoss_Identity,
	TelemetryLoss_Count
} TelemetryLoss;

typedef struct TelemetrySlot
{
	int64 sequence;
	Oid databaseId;
	int length;
	char json[TELEMETRY_RECORD_BYTES];
} TelemetrySlot;

typedef struct TelemetrySharedState
{
	int lockTrancheId;
	LWLock lock;
	pg_uuid_t epoch;
	char epochHex[33];
	char producerInstanceId[33];
	int64 lastSequence;
	instr_time admissionWindowStartMonotonic;
	int admittedInWindow;
	pg_atomic_uint64 losses[TelemetryLoss_Count];
	TelemetrySlot slots[TELEMETRY_SLOTS];
} TelemetrySharedState;

typedef struct TelemetrySnapshot
{
	char epoch[33];
	char producerInstanceId[33];
	int64 oldestSequence;
	int64 latestSequence;
	uint64 losses[TelemetryLoss_Count];
} TelemetrySnapshot;

typedef struct TelemetryPlanWalk
{
	BackendFindSpan *span;
	PlannedStmt *statement;
	int nodes;
	int collectionScans;
} TelemetryPlanWalk;

StaticAssertDecl(sizeof(TelemetrySharedState) <= TELEMETRY_MEMORY_BYTES,
				 "backend telemetry exceeds its shared-memory budget");

extern bool EnableBackendTelemetry;
extern bool EnableBackendTelemetryStorage;
extern bool EnableUdfTraceContext;

static TelemetrySharedState *SharedState = NULL;
static const char *TelemetryShmemName = "DocumentDB Backend Telemetry";

static bool RandomHex(char *output, int bytes);
static void EncodeHex(const unsigned char *input, int bytes, char *output);
static bool ElapsedNanoseconds(instr_time start, instr_time end, int64 *durationNs);
static void CopyTelemetrySnapshot(TelemetrySnapshot *snapshot);
static void AppendSnapshot(StringInfo output, const TelemetrySnapshot *snapshot);
static Datum TelemetryJson(StringInfo output);
static void CapturePlanState(PlanState *state, TelemetryPlanWalk *walk, int depth);
static void CaptureCollectionScan(ScanState *state, TelemetryPlanWalk *walk);

PG_FUNCTION_INFO_V1(backend_telemetry_capabilities);
PG_FUNCTION_INFO_V1(backend_telemetry_read);


Size
BackendTelemetryShmemSize(void)
{
	return EnableBackendTelemetryStorage ? MAXALIGN(sizeof(TelemetrySharedState)) : 0;
}


void
InitializeBackendTelemetryShmem(void)
{
	if (!EnableBackendTelemetryStorage)
	{
		return;
	}
	bool found = false;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	SharedState = ShmemInitStruct(TelemetryShmemName, BackendTelemetryShmemSize(),
								  &found);
	if (!found)
	{
		MemSet(SharedState, 0, sizeof(*SharedState));
#if PG_VERSION_NUM >= 190000
		SharedState->lockTrancheId = LWLockNewTrancheId(TelemetryShmemName);
#else
		SharedState->lockTrancheId = LWLockNewTrancheId();
#endif
		LWLockInitialize(&SharedState->lock, SharedState->lockTrancheId);
		if (!RandomHex(SharedState->producerInstanceId, 16) ||
			!pg_strong_random(SharedState->epoch.data, UUID_LEN))
		{
			LWLockRelease(AddinShmemInitLock);
			ereport(ERROR, (errmsg("Could not initialize backend telemetry identities")));
		}
		EncodeHex(SharedState->epoch.data, UUID_LEN, SharedState->epochHex);
		if (strcmp(SharedState->epochHex, "00000000000000000000000000000000") == 0)
		{
			LWLockRelease(AddinShmemInitLock);
			ereport(ERROR, (errmsg("Invalid backend telemetry epoch")));
		}
		for (int i = 0; i < TelemetryLoss_Count; i++)
		{
			pg_atomic_init_u64(&SharedState->losses[i], 0);
		}
	}
#if PG_VERSION_NUM < 190000
	LWLockRegisterTranche(SharedState->lockTrancheId, TelemetryShmemName);
#endif
	LWLockRelease(AddinShmemInitLock);
}


BackendFindSpan *
BeginBackendFindSpan(const UdfTraceIdentifiers *parent, Oid relationId)
{
	if (!EnableBackendTelemetry || SharedState == NULL || parent == NULL)
	{
		return NULL;
	}

	/* The executor updates this object across PG_TRY/PG_CATCH boundaries. */
	BackendFindSpan *span = palloc0(sizeof(BackendFindSpan));
	span->parent = *parent;
	span->relationId = relationId;
	if (!RandomHex(span->spanId, 8) ||
		strcmp(span->spanId, parent->parentSpanId) == 0)
	{
		pg_atomic_fetch_add_u64(&SharedState->losses[TelemetryLoss_Identity], 1);
		pfree(span);
		return NULL;
	}
	int64 unixTimeUs;
	int64 epochOffsetUs = (int64) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) *
						  SECS_PER_DAY * USECS_PER_SEC;
	if (pg_add_s64_overflow(GetCurrentTimestamp(), epochOffsetUs, &unixTimeUs) ||
		pg_mul_s64_overflow(unixTimeUs, 1000, &span->startTimeUnixNs) ||
		span->startTimeUnixNs <= 0)
	{
		pg_atomic_fetch_add_u64(&SharedState->losses[TelemetryLoss_Clock], 1);
		pfree(span);
		return NULL;
	}
	INSTR_TIME_SET_CURRENT(span->startTimeMonotonic);
	return span;
}


void
CaptureBackendFindExecution(BackendFindSpan *span, QueryDesc *queryDesc)
{
	TelemetryPlanWalk walk = { .span = span, .statement = queryDesc->plannedstmt };
	CapturePlanState(queryDesc->planstate, &walk, 0);
	if (walk.collectionScans != 1)
	{
		span->planUnsupported = true;
	}
	if (span->planUnsupported || span->planTruncated)
	{
		span->scanKind = BackendFindScan_Unknown;
		span->examinedDocumentsKnown = false;
	}
	span->executionCaptured = true;
}


/* This completion path also runs during error unwinding: no allocation or waiting. */
void
EndBackendFindSpan(const BackendFindSpan *span, int sqlErrorCode)
{
	instr_time endTimeMonotonic;
	INSTR_TIME_SET_CURRENT(endTimeMonotonic);
	int64 durationNs;
	int64 endTimeUnixNs;
	if (!ElapsedNanoseconds(span->startTimeMonotonic, endTimeMonotonic, &durationNs) ||
		pg_add_s64_overflow(span->startTimeUnixNs, durationNs, &endTimeUnixNs))
	{
		pg_atomic_fetch_add_u64(&SharedState->losses[TelemetryLoss_Clock], 1);
		return;
	}
	if (!LWLockConditionalAcquire(&SharedState->lock, LW_EXCLUSIVE))
	{
		pg_atomic_fetch_add_u64(&SharedState->losses[TelemetryLoss_Contention], 1);
		return;
	}
	instr_time elapsed = endTimeMonotonic;
	INSTR_TIME_SUBTRACT(elapsed, SharedState->admissionWindowStartMonotonic);
	if (INSTR_TIME_IS_ZERO(SharedState->admissionWindowStartMonotonic) ||
		INSTR_TIME_GET_DOUBLE(elapsed) >= 1.0)
	{
		SharedState->admissionWindowStartMonotonic = endTimeMonotonic;
		SharedState->admittedInWindow = 0;
	}
	if (SharedState->admittedInWindow >= TELEMETRY_ADMISSION_PER_SECOND ||
		SharedState->lastSequence == PG_INT64_MAX)
	{
		pg_atomic_fetch_add_u64(&SharedState->losses[TelemetryLoss_Admission], 1);
		LWLockRelease(&SharedState->lock);
		return;
	}
	const char *outcome = "ok";
	const char *errorClass = "null";
	if (sqlErrorCode == ERRCODE_QUERY_CANCELED)
	{
		outcome = "cancelled";
		errorClass = "\"cancelled\"";
	}
	else if (sqlErrorCode != ERRCODE_SUCCESSFUL_COMPLETION)
	{
		outcome = "error";
		errorClass = "\"other\"";
	}
	char details[512] = { 0 };
	if (sqlErrorCode == ERRCODE_SUCCESSFUL_COMPLETION && span->executionCaptured)
	{
		const char *scanKind = span->scanKind == BackendFindScan_Index ? "index" :
							   span->scanKind == BackendFindScan_Sequential ?
							   "sequential" :
							   "unknown";
		char examined[32] = "null";
		if (span->examinedDocumentsKnown)
		{
			snprintf(examined, sizeof(examined), INT64_FORMAT, span->examinedDocuments);
		}
		int detailsLength = snprintf(details, sizeof(details),
									 ",\"plan\":{\"scan_kind\":\"%s\","
									 "\"used_index_name_ids\":[],\"index_names_complete\":%s,"
									 "\"fingerprint\":null,\"complete\":%s,"
									 "\"unsupported_nodes\":%s,\"truncated\":%s},"
									 "\"work\":{\"returned_documents\":" UINT64_FORMAT
									 ",\"examined_documents\":%s}",
									 scanKind,
									 span->scanKind == BackendFindScan_Sequential ?
									 "true" : "false",
									 span->scanKind != BackendFindScan_Unknown ? "true" :
									 "false",
									 span->planUnsupported ? "true" : "false",
									 span->planTruncated ? "true" : "false",
									 span->returnedDocuments, examined);
		if (detailsLength < 0 || detailsLength >= (int) sizeof(details))
		{
			pg_atomic_fetch_add_u64(&SharedState->losses[TelemetryLoss_Oversize], 1);
			LWLockRelease(&SharedState->lock);
			return;
		}
	}
	int64 sequence = SharedState->lastSequence + 1;
	char json[TELEMETRY_RECORD_BYTES];
	int length = snprintf(json, sizeof(json),
						  "{\"schema_version\":1,\"producer_instance_id\":\"%s\","
						  "\"epoch\":\"%s\",\"sequence\":" INT64_FORMAT ","
																		"\"trace_id\":\"%s\",\"span_id\":\"%s\",\"parent_span_id\":\"%s\","
																		"\"trace_flags\":1,\"operation\":\"Find\",\"origin\":\"actual_execution\","
																		"\"start_time_unix_ns\":"
						  INT64_FORMAT ","
									   "\"end_time_unix_ns\":"
						  INT64_FORMAT ",\"duration_ns\":" INT64_FORMAT ","
																		"\"outcome\":\"%s\",\"error_class\":%s%s}",
						  SharedState->producerInstanceId, SharedState->epochHex,
						  sequence,
						  span->parent.traceId, span->spanId, span->parent.parentSpanId,
						  span->startTimeUnixNs, endTimeUnixNs, durationNs, outcome,
						  errorClass, details);
	if (length < 0 || length >= TELEMETRY_RECORD_BYTES)
	{
		pg_atomic_fetch_add_u64(&SharedState->losses[TelemetryLoss_Oversize], 1);
		LWLockRelease(&SharedState->lock);
		return;
	}
	TelemetrySlot *slot = &SharedState->slots[(sequence - 1) % TELEMETRY_SLOTS];
	if (slot->sequence != 0)
	{
		pg_atomic_fetch_add_u64(&SharedState->losses[TelemetryLoss_Overwrite], 1);
	}
	slot->databaseId = MyDatabaseId;
	slot->sequence = sequence;
	slot->length = length;
	memcpy(slot->json, json, length + 1);
	SharedState->lastSequence = sequence;
	SharedState->admittedInWindow++;
	LWLockRelease(&SharedState->lock);
}


Datum
backend_telemetry_capabilities(PG_FUNCTION_ARGS)
{
	StringInfoData output;
	initStringInfo(&output);
	appendStringInfo(&output,
					 "{\"schema_version\":%d,\"database_oid\":%u,"
					 "\"recording_enabled\":%s,\"context_enabled\":%s,"
					 "\"storage_allocated\":%s,\"capacity\":%d,\"max_record_bytes\":%d,"
					 "\"max_batch_records\":%d,\"max_batch_bytes\":%d,"
					 "\"admission_per_second\":%d",
					 TELEMETRY_SCHEMA_VERSION, MyDatabaseId,
					 EnableBackendTelemetry ? "true" : "false",
					 EnableUdfTraceContext ? "true" : "false",
					 SharedState != NULL ? "true" : "false", TELEMETRY_SLOTS,
					 TELEMETRY_RECORD_BYTES, TELEMETRY_BATCH_RECORDS,
					 TELEMETRY_BATCH_BYTES,
					 TELEMETRY_ADMISSION_PER_SECOND);
	if (SharedState != NULL)
	{
		TelemetrySnapshot snapshot;
		LWLockAcquire(&SharedState->lock, LW_SHARED);
		CopyTelemetrySnapshot(&snapshot);
		LWLockRelease(&SharedState->lock);
		AppendSnapshot(&output, &snapshot);
	}
	else
	{
		appendStringInfoString(&output,
							   ",\"producer_instance_id\":null,\"epoch\":null,"
							   "\"oldest_sequence\":null,\"latest_sequence\":null,\"loss\":null");
	}
	appendStringInfoChar(&output, '}');
	PG_RETURN_DATUM(TelemetryJson(&output));
}


Datum
backend_telemetry_read(PG_FUNCTION_ARGS)
{
	pg_uuid_t *epoch = PG_GETARG_UUID_P(0);
	int64 afterSequence = PG_GETARG_INT64(1);
	int limit = PG_GETARG_INT32(2);
	if (afterSequence < 0 || limit < 1 || limit > TELEMETRY_BATCH_RECORDS)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("Invalid backend telemetry cursor or batch limit")));
	}
	if (SharedState == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg(
							"Backend telemetry storage was not allocated at startup")));
	}
	TelemetrySlot *records = palloc0(sizeof(TelemetrySlot) * limit);
	TelemetrySnapshot snapshot;
	int count = 0;
	int bytes = TELEMETRY_RESPONSE_RESERVE_BYTES;
	int64 nextSequence = 0;
	bool gap = false;
	bool epochReset;

	LWLockAcquire(&SharedState->lock, LW_SHARED);
	CopyTelemetrySnapshot(&snapshot);
	epochReset = memcmp(epoch->data, SharedState->epoch.data, UUID_LEN) != 0;
	if (!epochReset && afterSequence <= snapshot.latestSequence)
	{
		gap = snapshot.oldestSequence > 0 && afterSequence < snapshot.oldestSequence - 1;
		nextSequence = Max(afterSequence, snapshot.oldestSequence - 1);
		while (nextSequence < snapshot.latestSequence && count < limit)
		{
			int64 sequence = nextSequence + 1;
			TelemetrySlot *slot = &SharedState->slots[(sequence - 1) % TELEMETRY_SLOTS];
			if (slot->databaseId == MyDatabaseId)
			{
				if (bytes + slot->length + TELEMETRY_JSON_SPACING_BYTES >
					TELEMETRY_BATCH_BYTES)
				{
					break;
				}
				records[count++] = *slot;
				bytes += slot->length + TELEMETRY_JSON_SPACING_BYTES;
			}
			nextSequence = sequence;
		}
	}
	LWLockRelease(&SharedState->lock);

	if (!epochReset && afterSequence > snapshot.latestSequence)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("Backend telemetry cursor exceeds the latest sequence")));
	}
	StringInfoData output;
	initStringInfo(&output);
	appendStringInfo(&output, "{\"schema_version\":%d,\"database_oid\":%u",
					 TELEMETRY_SCHEMA_VERSION, MyDatabaseId);
	AppendSnapshot(&output, &snapshot);
	appendStringInfo(&output,
					 ",\"epoch_reset\":%s,\"gap\":%s,\"next_cursor\":"
					 "{\"epoch\":\"%s\",\"after_sequence\":" INT64_FORMAT
					 "},\"records\":[",
					 epochReset ? "true" : "false", gap ? "true" : "false",
					 snapshot.epoch, nextSequence);
	for (int i = 0; i < count; i++)
	{
		CHECK_FOR_INTERRUPTS();
		if (i > 0)
		{
			appendStringInfoChar(&output, ',');
		}
		appendBinaryStringInfo(&output, records[i].json, records[i].length);
	}
	appendStringInfoString(&output, "]}");
	PG_RETURN_DATUM(TelemetryJson(&output));
}


/* A bounded walk of the actual executor tree, with no catalog/name lookups. */
static void
CapturePlanState(PlanState *state, TelemetryPlanWalk *walk, int depth)
{
	if (state == NULL)
	{
		return;
	}
	if (depth > TELEMETRY_PLAN_DEPTH || walk->nodes == TELEMETRY_PLAN_NODES)
	{
		walk->span->planTruncated = true;
		return;
	}
	walk->nodes++;
	check_stack_depth();
	CHECK_FOR_INTERRUPTS();
	if (state->initPlan != NIL || state->subPlan != NIL ||
		innerPlanState(state) != NULL || state->plan->parallel_aware)
	{
		walk->span->planUnsupported = true;
		return;
	}
	switch (nodeTag(state))
	{
		case T_SeqScanState:
		case T_IndexScanState:
		case T_IndexOnlyScanState:
		case T_BitmapHeapScanState:
		{
			CaptureCollectionScan((ScanState *) state, walk);
			break;
		}

		case T_BitmapIndexScanState:
		{
			Index relationIndex = ((BitmapIndexScan *) state->plan)->scan.scanrelid;
			if (relationIndex == 0 ||
				relationIndex > (Index) list_length(walk->statement->rtable) ||
				rt_fetch(relationIndex, walk->statement->rtable)->relid !=
				walk->span->relationId)
			{
				walk->span->planUnsupported = true;
			}
			break;
		}

		case T_BitmapAndState:
		case T_BitmapOrState:
		{
			int count = IsA(state, BitmapAndState) ?
						((BitmapAndState *) state)->nplans :
						((BitmapOrState *) state)->nplans;
			PlanState **children = IsA(state, BitmapAndState) ?
								   ((BitmapAndState *) state)->bitmapplans :
								   ((BitmapOrState *) state)->bitmapplans;
			for (int i = 0; i < count && !walk->span->planTruncated; i++)
			{
				CapturePlanState(children[i], walk, depth + 1);
			}
			return;
		}

		case T_SubqueryScanState:
		{
			CapturePlanState(((SubqueryScanState *) state)->subplan, walk, depth + 1);
			return;
		}

		case T_LimitState:
		case T_ResultState:
		case T_SortState:
		case T_IncrementalSortState:
		case T_MaterialState:
		{
			break;
		}

		default:
		{
			walk->span->planUnsupported = true;
			return;
		}
	}
	CapturePlanState(outerPlanState(state), walk, depth + 1);
}


static void
CaptureCollectionScan(ScanState *state, TelemetryPlanWalk *walk)
{
	BackendFindSpan *span = walk->span;
	if (state->ss_currentRelation == NULL ||
		RelationGetRelid(state->ss_currentRelation) != span->relationId)
	{
		span->planUnsupported = true;
		return;
	}
	walk->collectionScans++;
	span->scanKind = IsA(state, SeqScanState) ?
					 BackendFindScan_Sequential : BackendFindScan_Index;

	/* Index-only tuples are not a measurement of examined collection documents. */
	if (state->ps.instrument == NULL || IsA(state, IndexOnlyScanState))
	{
		return;
	}
	InstrEndLoop(state->ps.instrument);
	double examined = state->ps.instrument->ntuples + state->ps.instrument->nfiltered1;
	if (IsA(state, IndexScanState) || IsA(state, BitmapHeapScanState))
	{
		examined += state->ps.instrument->nfiltered2;
	}
	if (isfinite(examined) && examined >= 0 && examined < 0x1p63 &&
		trunc(examined) == examined)
	{
		span->examinedDocuments = (int64) examined;
		span->examinedDocumentsKnown = true;
	}
}


static bool
RandomHex(char *output, int bytes)
{
	unsigned char value[16];
	if (!pg_strong_random(value, bytes))
	{
		return false;
	}
	bool nonzero = false;
	for (int i = 0; i < bytes; i++)
	{
		nonzero |= value[i] != 0;
	}
	EncodeHex(value, bytes, output);
	return nonzero;
}


static void
EncodeHex(const unsigned char *input, int bytes, char *output)
{
	static const char digits[] = "0123456789abcdef";
	for (int i = 0; i < bytes; i++)
	{
		output[2 * i] = digits[input[i] >> 4];
		output[2 * i + 1] = digits[input[i] & 15];
	}
	output[2 * bytes] = '\0';
}


static bool
ElapsedNanoseconds(instr_time start, instr_time end, int64 *durationNs)
{
	INSTR_TIME_SUBTRACT(end, start);
#ifdef INSTR_TIME_GET_NANOSEC
	*durationNs = INSTR_TIME_GET_NANOSEC(end);
	return *durationNs >= 0;
#else
	double nanoseconds = INSTR_TIME_GET_DOUBLE(end) * 1000000000.0;
	if (!isfinite(nanoseconds) || nanoseconds < 0 || nanoseconds >= (double) PG_INT64_MAX)
	{
		return false;
	}
	*durationNs = (int64) nanoseconds;
	return true;
#endif
}


/* Caller holds the shared-state lock; these counters are instance-wide. */
static void
CopyTelemetrySnapshot(TelemetrySnapshot *snapshot)
{
	memcpy(snapshot->epoch, SharedState->epochHex, sizeof(snapshot->epoch));
	memcpy(snapshot->producerInstanceId, SharedState->producerInstanceId,
		   sizeof(snapshot->producerInstanceId));
	snapshot->latestSequence = SharedState->lastSequence;
	snapshot->oldestSequence = SharedState->lastSequence == 0 ? 0 :
							   Max(INT64CONST(1), SharedState->lastSequence -
								   TELEMETRY_SLOTS + 1);
	for (int i = 0; i < TelemetryLoss_Count; i++)
	{
		snapshot->losses[i] = pg_atomic_read_u64(&SharedState->losses[i]);
	}
}


static void
AppendSnapshot(StringInfo output, const TelemetrySnapshot *snapshot)
{
	appendStringInfo(output,
					 ",\"producer_instance_id\":\"%s\",\"epoch\":\"%s\"",
					 snapshot->producerInstanceId, snapshot->epoch);
	appendStringInfo(output, ",\"oldest_sequence\":" INT64_FORMAT,
					 snapshot->oldestSequence);
	appendStringInfo(output, ",\"latest_sequence\":" INT64_FORMAT,
					 snapshot->latestSequence);
	appendStringInfo(output, ",\"loss\":{\"admission\":" UINT64_FORMAT,
					 snapshot->losses[TelemetryLoss_Admission]);
	appendStringInfo(output, ",\"contention\":" UINT64_FORMAT,
					 snapshot->losses[TelemetryLoss_Contention]);
	appendStringInfo(output, ",\"overwrite\":" UINT64_FORMAT,
					 snapshot->losses[TelemetryLoss_Overwrite]);
	appendStringInfo(output, ",\"oversize\":" UINT64_FORMAT,
					 snapshot->losses[TelemetryLoss_Oversize]);
	appendStringInfo(output, ",\"clock\":" UINT64_FORMAT,
					 snapshot->losses[TelemetryLoss_Clock]);
	appendStringInfo(output, ",\"identity\":" UINT64_FORMAT "}",
					 snapshot->losses[TelemetryLoss_Identity]);
}


static Datum
TelemetryJson(StringInfo output)
{
	Datum result = DirectFunctionCall1(jsonb_in, CStringGetDatum(output->data));
	Jsonb *json = DatumGetJsonbP(result);
	char *serialized = JsonbToCString(NULL, &json->root, VARSIZE(json));
	if (strlen(serialized) > TELEMETRY_BATCH_BYTES)
	{
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("Backend telemetry response exceeds its byte limit")));
	}
	pfree(serialized);
	return result;
}
