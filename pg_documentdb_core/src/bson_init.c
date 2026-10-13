/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/bson_init.c
 *
 * Initialization of the shared library initialization for bson.
 *-------------------------------------------------------------------------
 */
#include <postgres.h>
#include <miscadmin.h>
#include <utils/guc.h>
#include <bson.h>

#include "bson_init.h"


/* --------------------------------------------------------- */
/* GUCs and default values */
/* --------------------------------------------------------- */

/* GUC controlling whether or not we use the pretty printed version json representation for bson */
/* SystemConfig */
#define DEFAULT_BSON_TEXT_USE_JSON_REPRESENTATION false
bool BsonTextUseJsonRepresentation = DEFAULT_BSON_TEXT_USE_JSON_REPRESENTATION;

/* GUC deciding whether collation is support */
/* FeatureFlag */
/* Added in v0.108, Pending stabilization, enable in v1.6 */
#define DEFAULT_ENABLE_COLLATION false
bool EnableCollation = DEFAULT_ENABLE_COLLATION;

/* FeatureFlag */
/* Added on v0.114, enabled on v0.117, remove after v1.1 */
#define DEFAULT_ENABLE_WRITE_DOCUMENTS_IN_REPATH true
bool EnableWriteDocumentsInRepath = DEFAULT_ENABLE_WRITE_DOCUMENTS_IN_REPATH;

/* FeatureFlag */
/* Added in v0.114, Pending stabilization, enable in v1.6 */
#define DEFAULT_ENABLE_BSON_SELECTIVITY_FROM_BTREE_STATS false
bool EnableBsonSelectivityFromBtreeStats =
	DEFAULT_ENABLE_BSON_SELECTIVITY_FROM_BTREE_STATS;

/* SystemConfig */

/* Whether ANALYZE on bson expression statistics unpacks array values into
 * their elements so that equality on an array path is estimated per element.
 */
#define DEFAULT_BSON_STATS_ENABLE_ARRAY_VALUE_UNPACK false
bool BsonStatsEnableArrayValueUnpack = DEFAULT_BSON_STATS_ENABLE_ARRAY_VALUE_UNPACK;

/*
 * Initializes core configurations pertaining to documentdb core.
 */
void
InitDocumentDBCoreConfigurations(const char *prefix)
{
	DefineCustomBoolVariable(
		psprintf("%s.bsonUseEJson", prefix),
		gettext_noop(
			"Determines whether the bson text is printed as extended Json. Used mainly for test."),
		NULL, &BsonTextUseJsonRepresentation, DEFAULT_BSON_TEXT_USE_JSON_REPRESENTATION,
		PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		psprintf("%s.enableCollation", prefix),
		gettext_noop(
			"Determines whether collation is supported."),
		NULL, &EnableCollation,
		DEFAULT_ENABLE_COLLATION,
		PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		psprintf("%s.enableWriteDocumentsInRepath", prefix),
		gettext_noop(
			"Whether to enable writing documents during bson repath and build."),
		NULL, &EnableWriteDocumentsInRepath,
		DEFAULT_ENABLE_WRITE_DOCUMENTS_IN_REPATH,
		PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		psprintf("%s.enableBsonSelectivityFromBtreeStats", prefix),
		gettext_noop(
			"Whether to enable selectivity calculations based on btree statistics for bson btree operators."),
		NULL, &EnableBsonSelectivityFromBtreeStats,
		DEFAULT_ENABLE_BSON_SELECTIVITY_FROM_BTREE_STATS,
		PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		psprintf("%s.bson_stats_enable_array_value_unpack", prefix),
		gettext_noop(
			"Whether to collect bson expression statistics over the elements of array values."),
		NULL, &BsonStatsEnableArrayValueUnpack,
		DEFAULT_BSON_STATS_ENABLE_ARRAY_VALUE_UNPACK,
		PGC_USERSET, 0, NULL, NULL, NULL);
}
