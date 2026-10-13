/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

#ifndef DOCUMENTDB_UDF_TRACE_CONTEXT_H
#define DOCUMENTDB_UDF_TRACE_CONTEXT_H

#include "io/bson_core.h"

#define UDF_TRACE_CONTEXT_VERSION 1
#define UDF_TRACE_CONTEXT_FIELD "$documentdbTrace"

typedef struct UdfTraceIdentifiers
{
	char traceId[33];
	char parentSpanId[17];
} UdfTraceIdentifiers;

bool IsUdfTraceContextField(const char *fieldName);
void ProcessUdfTraceContextAtEntry(pgbson *command, const char *operation);
bool ProcessUdfTraceContextForSpan(pgbson *command, const char *operation,
								   UdfTraceIdentifiers *identifiers);
pgbson * RemoveUdfTraceContext(pgbson *command);

#endif
