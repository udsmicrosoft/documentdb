/* -------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/io/bson_analyze.c
 *
 * Implementation of the BSON analyze logic.
 *
 *-------------------------------------------------------------------------
 */


#include <postgres.h>
#include <fmgr.h>
#include <commands/vacuum.h>
#include <io/bson_core.h>
#include <access/detoast.h>
#include <catalog/pg_statistic.h>

#include "planner/selectivity.h"

PG_FUNCTION_INFO_V1(bson_typanalyze);

/* Separate macro for BSON so that we can increase the threshold when it comes to
 * analyzing wide BSON values with arrays. Similar to array_typanalyze in PostgreSQL.
 */
#define BSON_ANALYZE_WIDTH_THRESHOLD 1024

/* Max number of array elements to consider when analyzing BSON arrays */
#define BSON_ANALYZE_MAX_ARRAY_ELEMENTS 128

extern bool BsonStatsEnableArrayValueUnpack;

ShouldEnableBsonStatsArrayValueUnpackFunc
	should_enable_bson_stats_array_value_unpack_hook = NULL;

inline static bool
ShouldEnableBsonStatsArrayValueUnpack(void)
{
	if (should_enable_bson_stats_array_value_unpack_hook != NULL)
	{
		return should_enable_bson_stats_array_value_unpack_hook();
	}

	return BsonStatsEnableArrayValueUnpack;
}


typedef struct InnerAnalyzeData
{
	void *compute_stats_extra_data;
	AnalyzeAttrComputeStatsFunc compute_stats_func;
} InnerAnalyzeData;


static void AnalyzeBsonDataStatsFunc(VacAttrStatsP stats,
									 AnalyzeAttrFetchFunc fetchfunc,
									 int samplerows,
									 double totalrows);


/*
 * Implement type analyze for bson.
 * Right now thunks to the default - but in the future
 * will be extended to support more.
 */
Datum
bson_typanalyze(PG_FUNCTION_ARGS)
{
	VacAttrStats *stats = (VacAttrStats *) PG_GETARG_POINTER(0);

	bool return_value = std_typanalyze(stats);
	if (return_value && ShouldEnableBsonStatsArrayValueUnpack())
	{
		InnerAnalyzeData *inner_data = (InnerAnalyzeData *) palloc(
			sizeof(InnerAnalyzeData));
		inner_data->compute_stats_extra_data = stats->extra_data;
		inner_data->compute_stats_func = stats->compute_stats;
		stats->extra_data = (void *) inner_data;
		stats->compute_stats = AnalyzeBsonDataStatsFunc;
	}

	PG_RETURN_BOOL(return_value);
}


inline static void
AddNullDatum(Datum **datums, bool **nulls, int *currentIndex, int *numAllocated)
{
	if (*currentIndex >= *numAllocated)
	{
		*numAllocated *= 2;
		*datums = (Datum *) repalloc(*datums, sizeof(Datum) * (*numAllocated));
		*nulls = (bool *) repalloc(*nulls, sizeof(bool) * (*numAllocated));
	}

	(*nulls)[*currentIndex] = true;
	(*datums)[*currentIndex] = (Datum) 0;
	(*currentIndex)++;
}


inline static void
AddDatum(Datum **datums, bool **nulls, int *currentIndex, int *numAllocated, Datum value)
{
	if (*currentIndex >= *numAllocated)
	{
		*numAllocated *= 2;
		*datums = (Datum *) repalloc(*datums, sizeof(Datum) * (*numAllocated));
		*nulls = (bool *) repalloc(*nulls, sizeof(bool) * (*numAllocated));
	}

	(*nulls)[*currentIndex] = false;
	(*datums)[*currentIndex] = value;
	(*currentIndex)++;
}


/*
 * The stats were computed over the decomposed array elements, treating each
 * element as a row. Convert them back to fractions of the sampled documents
 * so that a value present in every document's array has a frequency of 1.
 * The projected arrays are already deduplicated, so each value is counted at
 * most once per document and the scaled frequencies stay within [0, 1] per
 * value. The histogram is left as is: it describes the distribution of
 * element values, and the planner weighs it by the fraction not covered by
 * nulls and common values.
 *
 * This follows the frequency heuristic of PostgreSQL's array_typanalyze
 * (compute_array_stats in src/backend/utils/adt/array_typanalyze.c), which
 * counts each element only once per array and stores each element's
 * frequency as the fraction of rows that contain it. Unlike
 * compute_array_stats, which divides by the non-null rows, this divides by
 * all sampled rows since nulls are kept in stanullfrac here.
 */
static void
ScaleElementStatsToDocuments(VacAttrStatsP stats, int numElements, int samplerows,
							 double totalrows)
{
	if (numElements <= samplerows || samplerows <= 0)
	{
		return;
	}

	double factor = (double) numElements / samplerows;

	stats->stanullfrac = Min(1.0, stats->stanullfrac * factor);

	/* A negative n_distinct is a fraction of the element rows. Store it as an
	 * absolute count over the estimated total elements instead, since there
	 * can be more distinct values than documents. */
	if (stats->stadistinct < 0)
	{
		double totalElementsEstimate = Max(totalrows, samplerows) * factor;
		stats->stadistinct = -stats->stadistinct * totalElementsEstimate;
	}

	for (int slot = 0; slot < STATISTIC_NUM_SLOTS; slot++)
	{
		if (stats->stakind[slot] != STATISTIC_KIND_MCV)
		{
			continue;
		}

		for (int i = 0; i < stats->numnumbers[slot]; i++)
		{
			stats->stanumbers[slot][i] = Min(1.0, stats->stanumbers[slot][i] * factor);
		}
	}
}


static void
AnalyzeBsonDataStatsFunc(VacAttrStatsP stats,
						 AnalyzeAttrFetchFunc fetchfunc,
						 int samplerows,
						 double totalrows)
{
	InnerAnalyzeData *inner_data = (InnerAnalyzeData *) stats->extra_data;
	stats->extra_data = inner_data->compute_stats_extra_data;
	stats->compute_stats = inner_data->compute_stats_func;
	if (stats->tupDesc == NULL && stats->rowstride == 1 &&
		stats->exprvals != NULL)
	{
		/* Extended expression stats - similar to array stats, pre-compute and pass down the normalized version
		 * to std compute_scalar_stats. We start with the default assumption that it's all based on samplerows.
		 */
		Datum *exprDatums = (Datum *) palloc(sizeof(Datum) * samplerows);
		bool *exprnulls = (bool *) palloc(sizeof(bool) * samplerows);
		int numAllocated = samplerows;
		int rowsEstimate = 0;
		for (int i = 0; i < samplerows; i++)
		{
			AnalyzeDelayPointCompat();
			if (stats->exprnulls[i])
			{
				AddNullDatum(&exprDatums, &exprnulls, &rowsEstimate, &numAllocated);
				continue;
			}

			Datum value = stats->exprvals[i];

			if (toast_raw_datum_size(value) > BSON_ANALYZE_WIDTH_THRESHOLD)
			{
				/* Too big - let std_typanalyze deal with this */
				AddDatum(&exprDatums, &exprnulls, &rowsEstimate, &numAllocated, value);
				continue;
			}

			pgbson *bsonValue = DatumGetPgBsonPacked(value);
			pgbsonelement singleElement;
			if (TryGetSinglePgbsonElementFromPgbson(bsonValue, &singleElement) &&
				singleElement.pathLength == 0 &&
				singleElement.bsonValue.value_type == BSON_TYPE_ARRAY)
			{
				/* It's a single value array, decompose the array. The projected
				 * array holds distinct elements, so each counts once per document. */
				bson_iter_t arrayIter;
				BsonValueInitIterator(&singleElement.bsonValue, &arrayIter);
				int numArrayElements = 0;
				while (bson_iter_next(&arrayIter))
				{
					numArrayElements++;
					if (numArrayElements > BSON_ANALYZE_MAX_ARRAY_ELEMENTS)
					{
						break;
					}

					const bson_value_t *arrayValue = bson_iter_value(&arrayIter);
					pgbson *res = BsonValueToDocumentPgbson(arrayValue);
					AddDatum(&exprDatums, &exprnulls, &rowsEstimate, &numAllocated,
							 PointerGetDatum(res));
				}

				if (numArrayElements == 0)
				{
					AddDatum(&exprDatums, &exprnulls, &rowsEstimate, &numAllocated,
							 value);
				}
			}
			else
			{
				/* Not an array - pass down as-is to scalar array stats */
				AddDatum(&exprDatums, &exprnulls, &rowsEstimate, &numAllocated, value);
			}
		}

		Datum *oldDatums = stats->exprvals;
		bool *oldNulls = stats->exprnulls;

		stats->exprvals = exprDatums;
		stats->exprnulls = exprnulls;
		double totalRowsEstimate = Max(totalrows, rowsEstimate);
		stats->compute_stats(stats, fetchfunc, rowsEstimate, totalRowsEstimate);
		ScaleElementStatsToDocuments(stats, rowsEstimate, samplerows, totalrows);
		stats->exprvals = oldDatums;
		stats->exprnulls = oldNulls;

		pfree(exprDatums);
		pfree(exprnulls);
	}
	else
	{
		/* Base rel stats */
		stats->compute_stats(stats, fetchfunc, samplerows, totalrows);
	}
}
