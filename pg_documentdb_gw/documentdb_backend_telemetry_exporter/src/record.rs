/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Immutable completed records, validated before any OTLP conversion.

use std::time::{Duration, SystemTime};

use opentelemetry::{
    trace::{SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState},
    InstrumentationScope, KeyValue,
};
use opentelemetry_sdk::trace::{SpanData, SpanEvents, SpanLinks};
use serde::{Deserialize, Serialize};

use crate::{evidence::PlanSummary, identity, Error, MAX_RECORD_BYTES, SCHEMA_VERSION};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum Operation {
    Find,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Origin {
    ActualExecution,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Outcome {
    Ok,
    Error,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ErrorClass {
    Cancelled,
    Timeout,
    InvalidInput,
    PermissionDenied,
    ResourceExhausted,
    Other,
}

impl ErrorClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
            Self::InvalidInput => "invalid_input",
            Self::PermissionDenied => "permission_denied",
            Self::ResourceExhausted => "resource_exhausted",
            Self::Other => "other",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Work {
    returned_documents: Option<u64>,
    examined_documents: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Record {
    schema_version: u32,
    producer_instance_id: String,
    epoch: String,
    sequence: u64,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    trace_flags: u8,
    operation: Operation,
    origin: Origin,
    start_time_unix_ns: u64,
    end_time_unix_ns: u64,
    duration_ns: u64,
    outcome: Outcome,
    error_class: Option<ErrorClass>,
    collection_id: Option<String>,
    query_shape_id: Option<String>,
    query_shape_version: Option<u32>,
    plan: Option<PlanSummary>,
    work: Option<Work>,
}

/// A validated completed Find record, not a newly started SDK span.
#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
pub struct NativeSpan(Record);

impl NativeSpan {
    /// Decodes one bounded v1 record and validates its identities and clock anchor.
    ///
    /// # Errors
    /// Rejects unknown fields/versions, invalid identities, unsupported sampling,
    /// inconsistent outcomes, unbounded attributes, and invalid producer timestamps.
    pub fn from_json(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(Error::Invalid("native record byte limit"));
        }
        let record: Record = serde_json::from_slice(bytes)
            .map_err(|_error| Error::Invalid("native record schema"))?;
        record.validate()?;
        if serde_json::to_vec(&record)
            .map_err(|_error| Error::Invalid("native record schema"))?
            .len()
            > MAX_RECORD_BYTES
        {
            return Err(Error::Invalid("native record byte limit"));
        }
        Ok(Self(record))
    }

    /// Identifies the `PostgreSQL` instance that produced this record.
    #[must_use]
    pub fn producer_instance_id(&self) -> &str {
        &self.0.producer_instance_id
    }

    /// Identifies the producer's buffer allocation epoch.
    #[must_use]
    pub fn epoch(&self) -> &str {
        &self.0.epoch
    }

    /// Returns the immutable, epoch-local completed-record sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.0.sequence
    }
}

impl Record {
    fn validate(&self) -> Result<(), Error> {
        if self.schema_version != SCHEMA_VERSION
            || self.sequence == 0
            || self.sequence > i64::MAX.unsigned_abs()
        {
            return Err(Error::Invalid("native schema version or sequence"));
        }
        for identity in [&self.producer_instance_id, &self.epoch, &self.trace_id] {
            identity::validate_hex(identity, 32)?;
        }
        for identity in [&self.span_id, &self.parent_span_id] {
            identity::validate_hex(identity, 16)?;
        }
        if self.span_id == self.parent_span_id || self.trace_flags != 1 {
            return Err(Error::Invalid(
                "distinct child identity and sampled v1 flags",
            ));
        }
        if self.start_time_unix_ns == 0
            || self.start_time_unix_ns.checked_add(self.duration_ns) != Some(self.end_time_unix_ns)
            || self.end_time_unix_ns > i64::MAX.unsigned_abs()
        {
            return Err(Error::Clock);
        }
        match (self.outcome, self.error_class) {
            (Outcome::Ok, None)
            | (
                Outcome::Error,
                Some(
                    ErrorClass::Timeout
                    | ErrorClass::InvalidInput
                    | ErrorClass::PermissionDenied
                    | ErrorClass::ResourceExhausted
                    | ErrorClass::Other,
                ),
            )
            | (Outcome::Cancelled, Some(ErrorClass::Cancelled)) => {}
            _ => return Err(Error::Invalid("native outcome and error classification")),
        }
        for id in [&self.collection_id, &self.query_shape_id]
            .into_iter()
            .flatten()
        {
            identity::validate_hex(id, 32)?;
        }
        if !matches!(
            (&self.query_shape_id, self.query_shape_version),
            (None, None) | (Some(_), Some(1))
        ) || (self.query_shape_id.is_some() && self.collection_id.is_none())
        {
            return Err(Error::Invalid("versioned native query shape"));
        }
        if let Some(plan) = &self.plan {
            plan.validate()?;
        }
        if let Some(work) = &self.work {
            for count in [work.returned_documents, work.examined_documents]
                .into_iter()
                .flatten()
            {
                i64::try_from(count).map_err(|_error| Error::Invalid("measured work count"))?;
            }
        }
        Ok(())
    }
}

impl TryFrom<&NativeSpan> for SpanData {
    type Error = Error;

    fn try_from(value: &NativeSpan) -> Result<Self, Self::Error> {
        let record = &value.0;
        let mut attributes = vec![
            KeyValue::new("db.operation.name", "Find"),
            KeyValue::new(
                "documentdb.telemetry.schema_version",
                i64::from(SCHEMA_VERSION),
            ),
            KeyValue::new("documentdb.telemetry.origin", "actual_execution"),
            KeyValue::new("documentdb.telemetry.epoch", record.epoch.clone()),
            KeyValue::new(
                "documentdb.telemetry.sequence",
                i64::try_from(record.sequence)
                    .map_err(|_error| Error::Invalid("native sequence"))?,
            ),
            KeyValue::new(
                "documentdb.command.duration_ns",
                i64::try_from(record.duration_ns).map_err(|_error| Error::Clock)?,
            ),
        ];
        append_optional_attributes(record, &mut attributes)?;
        Ok(Self {
            span_context: SpanContext::new(
                TraceId::from_hex(&record.trace_id).map_err(|_error| Error::Invalid("trace ID"))?,
                SpanId::from_hex(&record.span_id).map_err(|_error| Error::Invalid("span ID"))?,
                TraceFlags::SAMPLED,
                false,
                TraceState::default(),
            ),
            parent_span_id: SpanId::from_hex(&record.parent_span_id)
                .map_err(|_error| Error::Invalid("parent span ID"))?,
            parent_span_is_remote: true,
            span_kind: SpanKind::Internal,
            name: "documentdb.command".into(),
            start_time: timestamp(record.start_time_unix_ns)?,
            end_time: timestamp(record.end_time_unix_ns)?,
            attributes,
            dropped_attributes_count: 0,
            events: SpanEvents::default(),
            links: SpanLinks::default(),
            status: match record.outcome {
                Outcome::Ok => Status::Ok,
                Outcome::Error | Outcome::Cancelled => Status::error(""),
            },
            instrumentation_scope: InstrumentationScope::builder("documentdb.backend")
                .with_version(env!("CARGO_PKG_VERSION"))
                .build(),
        })
    }
}

fn timestamp(value: u64) -> Result<SystemTime, Error> {
    SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_nanos(value))
        .ok_or(Error::Clock)
}

fn append_optional_attributes(
    record: &Record,
    attributes: &mut Vec<KeyValue>,
) -> Result<(), Error> {
    let outcome = match record.outcome {
        Outcome::Ok => "ok",
        Outcome::Error => "error",
        Outcome::Cancelled => "cancelled",
    };
    attributes.push(KeyValue::new("documentdb.command.outcome", outcome));
    if let Some(class) = record.error_class {
        attributes.push(KeyValue::new("error.type", class.as_str()));
    }
    for (name, value) in [
        ("documentdb.collection.id", record.collection_id.as_ref()),
        ("documentdb.query.shape_id", record.query_shape_id.as_ref()),
    ] {
        if let Some(value) = value {
            attributes.push(KeyValue::new(name, value.clone()));
        }
    }
    if let Some(version) = record.query_shape_version {
        attributes.push(KeyValue::new(
            "documentdb.query.shape_version",
            i64::from(version),
        ));
    }
    if let Some(plan) = &record.plan {
        plan.append_attributes(attributes);
    }
    if let Some(work) = &record.work {
        for (name, value) in [
            (
                "documentdb.command.returned_documents",
                work.returned_documents,
            ),
            (
                "documentdb.command.examined_documents",
                work.examined_documents,
            ),
        ] {
            if let Some(value) = value {
                attributes.push(KeyValue::new(
                    name,
                    i64::try_from(value).map_err(|_error| Error::Invalid("measured work count"))?,
                ));
            }
        }
    }
    Ok(())
}
