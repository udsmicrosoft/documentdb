/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

#ifndef DOCUMENTDB_BACKEND_TELEMETRY_H
#define DOCUMENTDB_BACKEND_TELEMETRY_H

#include "portability/instr_time.h"
#include "utils/udf_trace_context.h"

struct QueryDesc;

typedef enum BackendFindScanKind
{
	BackendFindScan_Unknown,
	BackendFindScan_Sequential,
	BackendFindScan_Index
} BackendFindScanKind;

typedef struct BackendFindSpan
{
	UdfTraceIdentifiers parent;
	char spanId[17];
	int64 startTimeUnixNs;
	instr_time startTimeMonotonic;
	Oid relationId;
	bool executionCaptured;
	bool planUnsupported;
	bool planTruncated;
	BackendFindScanKind scanKind;
	uint64 returnedDocuments;
	int64 examinedDocuments;
	bool examinedDocumentsKnown;
} BackendFindSpan;

Size BackendTelemetryShmemSize(void);
void InitializeBackendTelemetryShmem(void);
BackendFindSpan * BeginBackendFindSpan(const UdfTraceIdentifiers *parent, Oid relationId);
void CaptureBackendFindExecution(BackendFindSpan *span, struct QueryDesc *queryDesc);
void EndBackendFindSpan(const BackendFindSpan *span, int sqlErrorCode);

#endif
