/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Adapts only command BSON. Filters, documents, and continuation values are not carriers.

use std::borrow::Cow;

use bson::{rawdoc, RawDocument, RawDocumentBuf};

use crate::{
    error::{DocumentDBError, ErrorCode, Result},
    protocol::MAX_BSON_OBJECT_USIZE,
    requests::RequestType,
    telemetry::{
        consts::{labels, metric_names},
        trace_context::{self, ActiveTraceContext},
    },
};

const TRACE_FIELD: &str = "$documentdbTrace";
const MAX_ENVELOPE_BYTES: usize = 1024;

pub(super) const fn supports_operation(operation: RequestType) -> bool {
    matches!(
        operation,
        RequestType::Aggregate
            | RequestType::Count
            | RequestType::Delete
            | RequestType::Distinct
            | RequestType::Find
            | RequestType::FindAndModify
            | RequestType::GetMore
            | RequestType::Insert
            | RequestType::Update
    )
}

/// Preserves the unsupported-field contract for caller-supplied reserved metadata.
///
/// # Errors
///
/// Returns the command's existing unknown-field error, including for case aliases.
pub(super) fn validate_command(document: &RawDocument, operation: RequestType) -> Result<()> {
    for element in document {
        let (key, _) = element?;
        if key.eq_ignore_ascii_case(TRACE_FIELD) {
            let write_operation = match operation {
                RequestType::Insert => Some("insert"),
                RequestType::Update => Some("update"),
                RequestType::Delete => Some("delete"),
                _ => None,
            };
            return Err(match write_operation {
                Some(operation) => DocumentDBError::documentdb_error(
                    ErrorCode::UnknownBsonField,
                    format!(
                        "The BSON field '{operation}.{key}' is not recognized as a valid field."
                    ),
                ),
                _ => DocumentDBError::bad_value(format!("{key} is an unknown field")),
            });
        }
    }
    Ok(())
}

/// Reads context inside the execution span and borrows BSON when no carrier is needed.
pub(super) fn outgoing_command(document: &RawDocument, enabled: bool) -> Cow<'_, RawDocument> {
    if !enabled {
        return Cow::Borrowed(document);
    }
    let Some(context) = trace_context::current_context() else {
        return Cow::Borrowed(document);
    };
    append_context(document, &context, MAX_BSON_OBJECT_USIZE)
}

fn append_context<'a>(
    document: &'a RawDocument,
    context: &ActiveTraceContext,
    max_command_bytes: usize,
) -> Cow<'a, RawDocument> {
    let mut envelope = rawdoc! { "version": 1, "traceparent": context.traceparent() };
    if let Some(state) = context.tracestate() {
        envelope.append("tracestate", state);
    }
    if envelope.as_bytes().len() > MAX_ENVELOPE_BYTES {
        record_skipped("metadata_limit");
        return Cow::Borrowed(document);
    }
    let extra_bytes = 1 + TRACE_FIELD.len() + 1 + envelope.as_bytes().len();
    let Some(length) = document.as_bytes().len().checked_add(extra_bytes) else {
        record_skipped("command_limit");
        return Cow::Borrowed(document);
    };
    if length > max_command_bytes || i32::try_from(length).is_err() {
        record_skipped("command_limit");
        return Cow::Borrowed(document);
    }

    // Reserve the bounded final size to avoid copying the command again during append.
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(document.as_bytes());
    let Ok(mut outgoing) = RawDocumentBuf::from_bytes(bytes) else {
        record_skipped("invalid_command");
        return Cow::Borrowed(document);
    };
    outgoing.append(TRACE_FIELD, envelope);
    Cow::Owned(outgoing)
}

fn record_skipped(reason: &'static str) {
    metrics::counter!(
        metric_names::UDF_TRACE_CONTEXT_SKIPPED,
        labels::TRACE_CONTEXT_SKIP_REASON => reason
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, ptr};

    use bson::{doc, rawdoc, Bson, RawDocumentBuf};

    use crate::{
        error::ErrorCode,
        postgres::command_trace_context::{
            append_context, outgoing_command, supports_operation, validate_command, TRACE_FIELD,
        },
        protocol::MAX_BSON_OBJECT_USIZE,
        requests::RequestType,
        telemetry::trace_context::ActiveTraceContext,
    };

    fn context() -> ActiveTraceContext {
        ActiveTraceContext::new(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_owned(),
            Some("vendor=value".to_owned()),
        )
        .expect("valid synthetic context")
    }

    #[test]
    fn disabled_and_unavailable_context_borrow_original() {
        let command = rawdoc! { "count": "events", "comment": "unchanged" };
        for enabled in [false, true] {
            assert!(matches!(outgoing_command(&command, enabled),
                Cow::Borrowed(original) if ptr::eq(original, command.as_ref())));
        }
    }

    #[test]
    fn appending_preserves_raw_fields_and_duplicate_behavior() {
        let mut command = rawdoc! { "count": "events", "comment": "first", "query": {"_id": 1} };
        command.append("comment", 42_i64);
        command.append("query", rawdoc! { "_id": 2 });
        let original = command.as_bytes().to_vec();

        for _ in 0..2 {
            let outgoing = append_context(&command, &context(), usize::MAX);
            assert!(matches!(outgoing, Cow::Owned(_)));
            assert_eq!(command.as_bytes(), original);
            assert_eq!(
                &outgoing.as_bytes()[4..original.len() - 1],
                &original[4..original.len() - 1]
            );
            assert_eq!(outgoing.into_iter().next().unwrap().unwrap().0, "count");
            let metadata = outgoing.get_document(TRACE_FIELD).unwrap();
            assert_eq!(metadata.get_i32("version").unwrap(), 1);
            assert_eq!(
                metadata.get_str("traceparent").unwrap(),
                context().traceparent()
            );
            assert_eq!(metadata.get_str("tracestate").unwrap(), "vendor=value");
        }
    }

    #[test]
    fn preserves_comment_types_and_absence() {
        let values: Vec<Bson> = [
            r#"{"$numberDouble":"1.5"}"#,
            r#""application""#,
            r#""""#,
            r#"{"traceparent":"uninterpreted","text":{"a":1}}"#,
            r#"[1,"two",null]"#,
            r#"{"$binary":{"base64":"AQID","subType":"80"}}"#,
            r#"{"$undefined":true}"#,
            r#"{"$oid":"507f1f77bcf86cd799439011"}"#,
            "true",
            r#"{"$date":{"$numberLong":"1"}}"#,
            "null",
            r#"{"$regularExpression":{"pattern":"a","options":"i"}}"#,
            r#"{"$dbPointer":{"$ref":"events","$id":{"$oid":"507f1f77bcf86cd799439011"}}}"#,
            r#"{"$code":"return 1"}"#,
            r#"{"$symbol":"symbol"}"#,
            r#"{"$code":"return x","$scope":{"x":1}}"#,
            r#"{"$numberInt":"42"}"#,
            r#"{"$timestamp":{"t":1,"i":2}}"#,
            r#"{"$numberLong":"42"}"#,
            r#"{"$numberDecimal":"1.5"}"#,
            r#"{"$minKey":1}"#,
            r#"{"$maxKey":1}"#,
        ]
        .into_iter()
        .map(|json| serde_json::from_str(json).unwrap())
        .collect();
        for value in values {
            let command =
                RawDocumentBuf::from_document(&doc! { "count": "events", "comment": value })
                    .unwrap();
            let outgoing = append_context(&command, &context(), usize::MAX);
            assert_eq!(
                &outgoing.as_bytes()[4..command.as_bytes().len() - 1],
                &command.as_bytes()[4..command.as_bytes().len() - 1]
            );
        }
        let command = rawdoc! { "count": "events" };
        let outgoing = append_context(&command, &context(), usize::MAX);
        assert!(outgoing.get("comment").unwrap().is_none());
    }

    #[test]
    fn rejects_every_reserved_occurrence_and_case_alias() {
        for operation in [
            RequestType::Aggregate,
            RequestType::Count,
            RequestType::Delete,
            RequestType::Distinct,
            RequestType::Find,
            RequestType::FindAndModify,
            RequestType::GetMore,
            RequestType::Insert,
            RequestType::Update,
        ] {
            for field in [TRACE_FIELD, "$DOCUMENTDBTRACE", "$documentdbtrace"] {
                let mut command = rawdoc! { "count": "events", "comment": "" };
                command.append("comment", 1);
                command.append(field, rawdoc! { "version": 1 });
                command.append(field, "second");
                let error = validate_command(&command, operation).unwrap_err();
                assert_eq!(
                    error.error_code(),
                    if matches!(
                        operation,
                        RequestType::Insert | RequestType::Update | RequestType::Delete
                    ) {
                        ErrorCode::UnknownBsonField
                    } else {
                        ErrorCode::BadValue
                    }
                );
            }
        }
        validate_command(
            &rawdoc! { "count": "events", "comment": { TRACE_FIELD: "application data" } },
            RequestType::Count,
        )
        .unwrap();
    }

    #[test]
    fn optional_metadata_never_breaks_command_size_boundary() {
        let command = rawdoc! { "count": "events" };
        let length = append_context(&command, &context(), usize::MAX)
            .as_bytes()
            .len();
        assert!(matches!(
            append_context(&command, &context(), length),
            Cow::Owned(_)
        ));
        assert!(matches!(
            append_context(&command, &context(), length - 1),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn preserves_valid_operations_at_the_actual_sixteen_mib_limit() {
        let empty = rawdoc! { "count": "events", "comment": "" };
        let with_metadata = append_context(&empty, &context(), MAX_BSON_OBJECT_USIZE)
            .as_bytes()
            .len();
        let command = rawdoc! { "count": "events", "comment": "x".repeat(MAX_BSON_OBJECT_USIZE - with_metadata) };
        let outgoing = append_context(&command, &context(), MAX_BSON_OBJECT_USIZE);
        assert!(matches!(outgoing, Cow::Owned(_)));
        assert_eq!(outgoing.as_bytes().len(), MAX_BSON_OBJECT_USIZE);

        let full = rawdoc! { "count": "events", "comment": "x".repeat(MAX_BSON_OBJECT_USIZE - empty.as_bytes().len()) };
        assert_eq!(full.as_bytes().len(), MAX_BSON_OBJECT_USIZE);
        assert!(
            matches!(append_context(&full, &context(), MAX_BSON_OBJECT_USIZE), Cow::Borrowed(original) if ptr::eq(original, full.as_ref()))
        );
    }

    #[test]
    fn supports_data_commands_but_not_administrative_or_explain_commands() {
        for operation in [
            RequestType::Aggregate,
            RequestType::Count,
            RequestType::Delete,
            RequestType::Distinct,
            RequestType::Find,
            RequestType::FindAndModify,
            RequestType::GetMore,
            RequestType::Insert,
            RequestType::Update,
        ] {
            assert!(supports_operation(operation));
        }
        for operation in [
            RequestType::Explain,
            RequestType::Create,
            RequestType::CreateUser,
            RequestType::CreateIndexes,
            RequestType::DropDatabase,
            RequestType::KillCursors,
        ] {
            assert!(!supports_operation(operation));
        }
    }

    #[test]
    fn batch_adaptation_preserves_documents_and_write_options() {
        for command in [
            rawdoc! { "insert": "events", "documents": [{ "_id": 1 }, { "_id": 2 }], "ordered": false },
            rawdoc! { "update": "events", "updates": [{ "q": {}, "u": { "$set": { "value": 1 } }, "multi": true }] },
            rawdoc! { "delete": "events", "deletes": [{ "q": {}, "limit": 0 }] },
        ] {
            let original = command.as_bytes().to_vec();
            let outgoing = append_context(&command, &context(), MAX_BSON_OBJECT_USIZE);
            assert!(matches!(outgoing, Cow::Owned(_)));
            assert_eq!(command.as_bytes(), original);
            assert_eq!(
                &outgoing.as_bytes()[4..original.len() - 1],
                &original[4..original.len() - 1]
            );
        }
    }
}
