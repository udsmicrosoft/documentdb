/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "portability/instr_time.h"

#include "utils/feature_counter.h"
#include "utils/udf_trace_context.h"

#define TRACEPARENT_LENGTH 55
#define MAX_TRACESTATE_LENGTH 512
#define MAX_TRACE_ENVELOPE_LENGTH 1024
#define MAX_TRACESTATE_MEMBERS 32

typedef enum UdfTraceContextStatus
{
	UdfTraceContext_Disabled,
	UdfTraceContext_Missing,
	UdfTraceContext_Invalid,
	UdfTraceContext_UnsupportedVersion,
	UdfTraceContext_Duplicate,
	UdfTraceContext_Valid
} UdfTraceContextStatus;

typedef struct UdfTraceContext
{
	const char *traceparent;
	const char *tracestate;
	uint32_t tracestateLength;
} UdfTraceContext;

extern bool EnableUdfTraceContext;
extern bool TestUdfTraceContext;
extern int UdfTraceContextLogLimitPerSecond;

static instr_time LogWindowStart;
static int LogEntriesInWindow;

static UdfTraceContextStatus ExtractUdfTraceContext(pgbson *command,
													UdfTraceContext *context);
static UdfTraceContextStatus ParseTraceEnvelope(const bson_value_t *envelope,
												UdfTraceContext *context);
static bool ValidSampledTraceparent(const char *value, uint32_t length);
static bool ValidTracestate(const char *value, uint32_t length);
static bool ValidTracestateKey(const char *key, uint32_t length);
static bool IsLowerHex(char value);
static bool IsLowerAlpha(char value);
static bool IsDigit(char value);
static void MarkCorruptEnvelope(const bson_iter_t *iter, void *data);
static bool AdmitTraceContextLog(void);
static void ReportTraceContext(UdfTraceContextStatus status, const char *traceparent,
							   const char *operation);

bool
IsUdfTraceContextField(const char *fieldName)
{
	return pg_strcasecmp(fieldName, UDF_TRACE_CONTEXT_FIELD) == 0;
}


void
ProcessUdfTraceContextAtEntry(pgbson *command, const char *operation)
{
	ProcessUdfTraceContextForSpan(command, operation, NULL);
}


bool
ProcessUdfTraceContextForSpan(pgbson *command, const char *operation,
							  UdfTraceIdentifiers *identifiers)
{
	if (!EnableUdfTraceContext && !TestUdfTraceContext)
	{
		return false;
	}

	UdfTraceContext context = { 0 };
	UdfTraceContextStatus status = EnableUdfTraceContext ?
								   ExtractUdfTraceContext(command, &context) :
								   UdfTraceContext_Disabled;
	ReportTraceContext(status, context.traceparent, operation);

	/* Recording v1 supports flag 01; receipt logging keeps its existing behavior. */
	if (status != UdfTraceContext_Valid ||
		context.traceparent[53] != '0' || context.traceparent[54] != '1')
	{
		return false;
	}
	if (identifiers != NULL)
	{
		memcpy(identifiers->traceId, context.traceparent + 3, 32);
		identifiers->traceId[32] = '\0';
		memcpy(identifiers->parentSpanId, context.traceparent + 36, 16);
		identifiers->parentSpanId[16] = '\0';
	}
	return true;
}


/* Cursor continuations and forwarded cursor specifications must not retain a parent. */
pgbson *
RemoveUdfTraceContext(pgbson *command)
{
	bson_iter_t iter;
	PgbsonInitIterator(command, &iter);
	bool found = false;
	while (bson_iter_next(&iter))
	{
		CHECK_FOR_INTERRUPTS();
		if (IsUdfTraceContextField(bson_iter_key(&iter)))
		{
			found = true;
			break;
		}
	}
	if (!found)
	{
		return command;
	}

	pgbson_writer writer;
	PgbsonWriterInit(&writer);
	PgbsonInitIterator(command, &iter);
	while (bson_iter_next(&iter))
	{
		CHECK_FOR_INTERRUPTS();
		if (!IsUdfTraceContextField(bson_iter_key(&iter)))
		{
			PgbsonWriterAppendIter(&writer, &iter);
		}
	}
	return PgbsonWriterGetPgbson(&writer);
}


static bool
AdmitTraceContextLog(void)
{
	if (UdfTraceContextLogLimitPerSecond == 0)
	{
		return false;
	}

	instr_time now;
	INSTR_TIME_SET_CURRENT(now);
	instr_time elapsed = now;
	INSTR_TIME_SUBTRACT(elapsed, LogWindowStart);
	if (INSTR_TIME_IS_ZERO(LogWindowStart) || INSTR_TIME_GET_DOUBLE(elapsed) >= 1.0)
	{
		LogWindowStart = now;
		LogEntriesInWindow = 0;
	}
	if (LogEntriesInWindow >= UdfTraceContextLogLimitPerSecond)
	{
		return false;
	}
	LogEntriesInWindow++;
	return true;
}


static void
ReportTraceContext(UdfTraceContextStatus status, const char *traceparent,
				   const char *operation)
{
	if (status == UdfTraceContext_Invalid ||
		status == UdfTraceContext_UnsupportedVersion ||
		status == UdfTraceContext_Duplicate)
	{
		ReportFeatureUsage(FEATURE_UDF_TRACE_CONTEXT_REJECTED);
	}

	if (status == UdfTraceContext_Valid)
	{
		if (AdmitTraceContextLog())
		{
			ereport(LOG_SERVER_ONLY,
					(errmsg_internal("udf_trace_context operation=%s traceparent=%s",
									 operation, traceparent),
					 errhidestmt(true),
					 errhidecontext(true)));
		}
		else
		{
			ReportFeatureUsage(FEATURE_UDF_TRACE_CONTEXT_LOG_DROPPED);
		}
	}

	if (TestUdfTraceContext)
	{
		const char *statusNames[] = {
			"disabled", "missing", "invalid", "unsupported_version", "duplicate", "valid"
		};
		ereport(NOTICE, (errmsg(
							 "udf_trace_context operation=%s status=%s traceparent=%s",
							 operation, statusNames[status],
							 status == UdfTraceContext_Valid ? traceparent :
							 "none")));
	}
}


static UdfTraceContextStatus
ExtractUdfTraceContext(pgbson *command, UdfTraceContext *context)
{
	bson_iter_t iter;
	PgbsonInitIterator(command, &iter);
	bool found = false;
	bson_value_t envelope = { 0 };

	while (bson_iter_next(&iter))
	{
		CHECK_FOR_INTERRUPTS();
		if (!IsUdfTraceContextField(bson_iter_key(&iter)))
		{
			continue;
		}
		if (found)
		{
			return UdfTraceContext_Duplicate;
		}
		found = true;
		envelope = *bson_iter_value(&iter);
	}
	return found ? ParseTraceEnvelope(&envelope, context) : UdfTraceContext_Missing;
}


static UdfTraceContextStatus
ParseTraceEnvelope(const bson_value_t *envelope, UdfTraceContext *context)
{
	if (envelope->value_type != BSON_TYPE_DOCUMENT ||
		envelope->value.v_doc.data_len > MAX_TRACE_ENVELOPE_LENGTH)
	{
		return UdfTraceContext_Invalid;
	}

	bson_iter_t iter;
	if (!bson_iter_init_from_data(&iter, envelope->value.v_doc.data,
								  envelope->value.v_doc.data_len))
	{
		return UdfTraceContext_Invalid;
	}
	bson_iter_t validationIter = iter;
	bson_visitor_t visitor = { 0 };
	visitor.visit_corrupt = MarkCorruptEnvelope;
	bool corrupt = false;
	bson_iter_visit_all(&validationIter, &visitor, &corrupt);
	if (corrupt)
	{
		return UdfTraceContext_Invalid;
	}
	bool hasVersion = false;
	int32_t version = 0;
	while (bson_iter_next(&iter))
	{
		const char *key = bson_iter_key(&iter);
		if (strcmp(key, "version") == 0 && !hasVersion && BSON_ITER_HOLDS_INT32(&iter))
		{
			version = bson_iter_int32(&iter);
			hasVersion = true;
		}
		else if (strcmp(key, "traceparent") == 0 && context->traceparent == NULL &&
				 BSON_ITER_HOLDS_UTF8(&iter))
		{
			uint32_t length = 0;
			context->traceparent = bson_iter_utf8(&iter, &length);
			if (!ValidSampledTraceparent(context->traceparent, length))
			{
				return UdfTraceContext_Invalid;
			}
		}
		else if (strcmp(key, "tracestate") == 0 && context->tracestate == NULL &&
				 BSON_ITER_HOLDS_UTF8(&iter))
		{
			context->tracestate = bson_iter_utf8(&iter, &context->tracestateLength);
			if (!ValidTracestate(context->tracestate, context->tracestateLength))
			{
				return UdfTraceContext_Invalid;
			}
		}
		else
		{
			return UdfTraceContext_Invalid;
		}
	}
	if (!hasVersion || context->traceparent == NULL)
	{
		return UdfTraceContext_Invalid;
	}
	return version == UDF_TRACE_CONTEXT_VERSION ? UdfTraceContext_Valid :
		   UdfTraceContext_UnsupportedVersion;
}


static bool
ValidSampledTraceparent(const char *value, uint32_t length)
{
	if (length != TRACEPARENT_LENGTH || strncmp(value, "00-", 3) != 0 ||
		value[35] != '-' || value[52] != '-')
	{
		return false;
	}
	bool nonzeroTraceId = false;
	bool nonzeroParentId = false;
	for (uint32_t i = 0; i < length; i++)
	{
		if (i == 2 || i == 35 || i == 52)
		{
			continue;
		}
		if (!IsLowerHex(value[i]))
		{
			return false;
		}
		if (i >= 3 && i < 35 && value[i] != '0')
		{
			nonzeroTraceId = true;
		}
		if (i >= 36 && i < 52 && value[i] != '0')
		{
			nonzeroParentId = true;
		}
	}
	return nonzeroTraceId && nonzeroParentId && strchr("13579bdf", value[54]) != NULL;
}


/* W3C Trace Context 1, section 3.3.1.3, with a bounded 512-byte carrier budget. */
static bool
ValidTracestate(const char *value, uint32_t length)
{
	if (length > MAX_TRACESTATE_LENGTH)
	{
		return false;
	}
	const char *keys[MAX_TRACESTATE_MEMBERS];
	uint32_t keyLengths[MAX_TRACESTATE_MEMBERS];
	uint32_t members = 0;
	uint32_t start = 0;
	while (start < length)
	{
		if (members == MAX_TRACESTATE_MEMBERS)
		{
			return false;
		}
		uint32_t end = start;
		while (end < length && value[end] != ',')
		{
			end++;
		}
		uint32_t next = end + 1;
		while (start < end && (value[start] == ' ' || value[start] == '\t'))
		{
			start++;
		}
		while (end > start && (value[end - 1] == ' ' || value[end - 1] == '\t'))
		{
			end--;
		}
		const char *equals = memchr(value + start, '=', end - start);
		if (equals == NULL)
		{
			return false;
		}
		uint32_t keyLength = equals - (value + start);
		uint32_t valueLength = value + end - equals - 1;
		if (!ValidTracestateKey(value + start, keyLength) ||
			valueLength == 0 || valueLength > 256)
		{
			return false;
		}
		for (uint32_t i = 0; i < members; i++)
		{
			if (keyLengths[i] == keyLength &&
				memcmp(keys[i], value + start, keyLength) == 0)
			{
				return false;
			}
		}
		for (uint32_t i = 1; i <= valueLength; i++)
		{
			if (equals[i] < ' ' || equals[i] > '~' || equals[i] == '=')
			{
				return false;
			}
		}
		keys[members] = value + start;
		keyLengths[members++] = keyLength;
		if (next == length)
		{
			return false;
		}
		start = next;
	}
	return true;
}


static bool
ValidTracestateKey(const char *key, uint32_t length)
{
	if (length == 0 || length > 256)
	{
		return false;
	}
	const char *at = memchr(key, '@', length);
	if (at != NULL)
	{
		uint32_t tenantLength = at - key;
		uint32_t systemLength = length - tenantLength - 1;
		if (tenantLength == 0 || tenantLength > 241 ||
			systemLength == 0 || systemLength > 14 ||
			(!IsLowerAlpha(key[0]) && !IsDigit(key[0])) || !IsLowerAlpha(at[1]))
		{
			return false;
		}
	}
	else if (!IsLowerAlpha(key[0]))
	{
		return false;
	}
	for (uint32_t i = 0; i < length; i++)
	{
		if (key + i == at)
		{
			continue;
		}
		if (!IsLowerAlpha(key[i]) && !IsDigit(key[i]) &&
			key[i] != '_' && key[i] != '-' && key[i] != '*' && key[i] != '/')
		{
			return false;
		}
	}
	return true;
}


static bool
IsLowerHex(char value)
{
	return IsDigit(value) || (value >= 'a' && value <= 'f');
}


static bool
IsLowerAlpha(char value)
{
	return value >= 'a' && value <= 'z';
}


static bool
IsDigit(char value)
{
	return value >= '0' && value <= '9';
}


static void
MarkCorruptEnvelope(const bson_iter_t *iter, void *data)
{
	bool *corrupt = data;
	*corrupt = true;
}
