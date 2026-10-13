/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Protocol, bounds, provenance, and privacy fixtures without a running database.

use std::error::Error as StdError;

use opentelemetry_sdk::trace::SpanData;
use serde_json::{json, Value};

use crate::{Error, NativeSpan, MAX_RECORD_BYTES};

type TestResult = Result<(), Box<dyn StdError>>;
const RECORD: &[u8] = include_bytes!("../tests/fixtures/native-find-v1.json");

#[test]
fn native_record_preserves_identity_time_and_missing_work() -> TestResult {
    let record = NativeSpan::from_json(RECORD)?;
    let span = SpanData::try_from(&record)?;
    assert_eq!(
        span.span_context.trace_id().to_string(),
        "11112222333344445555666677778888"
    );
    assert_eq!(span.span_context.span_id().to_string(), "1122334455667788");
    assert_eq!(span.parent_span_id.to_string(), "8877665544332211");
    assert_eq!(
        span.end_time.duration_since(span.start_time)?.as_nanos(),
        2_345_678
    );
    assert_eq!(
        span.start_time
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_nanos(),
        1_790_872_000_123_456_789
    );
    assert!(!span
        .attributes
        .iter()
        .any(|attribute| attribute.key.as_str().ends_with("_documents")));
    assert!(span.events.events.is_empty());
    assert!(span.links.links.is_empty());
    Ok(())
}

#[test]
fn native_record_rejects_invalid_envelopes_without_echoing_data() -> TestResult {
    for (field, value) in [
        ("schema_version", json!(2)),
        ("sequence", json!(0)),
        ("trace_id", json!("00000000000000000000000000000000")),
        ("trace_id", json!("AAAABBBBCCCCDDDDEEEEFFFF11112222")),
        ("span_id", json!("8877665544332211")),
        ("trace_flags", json!(0)),
        ("trace_flags", json!(3)),
        ("operation", json!("GetMore")),
        ("origin", json!("diagnostic_explain")),
        ("duration_ns", json!(2_345_679)),
        ("end_time_unix_ns", json!(1)),
        ("outcome", json!("error")),
        ("error_class", json!("permission_denied")),
        ("query_shape_id", json!("11112222333344445555666677778888")),
        ("statement", json!("sensitive-document-literal")),
        ("work", json!({"examined_documents": u64::MAX})),
    ] {
        let mut value_record: Value = serde_json::from_slice(RECORD)?;
        value_record[field] = value;
        let result = NativeSpan::from_json(&serde_json::to_vec(&value_record)?);
        let error = result.err().ok_or("invalid native fixture was accepted")?;
        assert!(!error.to_string().contains("sensitive-document-literal"));
    }
    assert_eq!(
        NativeSpan::from_json(&vec![b' '; MAX_RECORD_BYTES + 1]).err(),
        Some(Error::Invalid("native record byte limit"))
    );
    let duplicate = std::str::from_utf8(RECORD)?.replacen(
        "\"schema_version\": 1",
        "\"schema_version\": 1, \"schema_version\": 1",
        1,
    );
    assert_eq!(
        NativeSpan::from_json(duplicate.as_bytes()).err(),
        Some(Error::Invalid("native record schema"))
    );
    Ok(())
}

#[test]
fn native_outcomes_and_incomplete_plans_remain_explicit() -> TestResult {
    let mut value: Value = serde_json::from_slice(RECORD)?;
    value["outcome"] = json!("cancelled");
    value["error_class"] = json!("cancelled");
    value["plan"] = json!({
        "scan_kind": "unknown", "used_index_name_ids": [], "fingerprint": null,
        "complete": false, "unsupported_nodes": true, "truncated": false
    });
    value["work"] = json!({"returned_documents": 0, "examined_documents": null});
    let span = SpanData::try_from(&NativeSpan::from_json(&serde_json::to_vec(&value)?)?)?;
    assert!(
        matches!(span.status, opentelemetry::trace::Status::Error { description } if description.is_empty())
    );
    assert!(span
        .attributes
        .iter()
        .any(|attribute| attribute.key.as_str() == "error.type"));
    value["plan"]["scan_kind"] = json!("sequential");
    assert_eq!(
        NativeSpan::from_json(&serde_json::to_vec(&value)?).err(),
        Some(Error::Invalid("plan coverage"))
    );
    value["plan"]["scan_kind"] = json!("unknown");
    value["error_class"] = json!("timeout");
    assert_eq!(
        NativeSpan::from_json(&serde_json::to_vec(&value)?).err(),
        Some(Error::Invalid("native outcome and error classification"))
    );
    Ok(())
}

#[test]
fn native_scan_counts_do_not_invent_logical_index_identity() -> TestResult {
    let mut value: Value = serde_json::from_slice(RECORD)?;
    value["plan"] = json!({
        "scan_kind": "index", "used_index_name_ids": [],
        "index_names_complete": false, "fingerprint": null,
        "complete": true, "unsupported_nodes": false, "truncated": false
    });
    value["work"] = json!({"returned_documents": 1, "examined_documents": 7});
    let span = SpanData::try_from(&NativeSpan::from_json(&serde_json::to_vec(&value)?)?)?;
    for (name, expected) in [
        (
            "documentdb.command.returned_documents",
            opentelemetry::Value::I64(1),
        ),
        (
            "documentdb.command.examined_documents",
            opentelemetry::Value::I64(7),
        ),
        ("documentdb.plan.complete", opentelemetry::Value::Bool(true)),
        (
            "documentdb.plan.index_names_complete",
            opentelemetry::Value::Bool(false),
        ),
    ] {
        assert_eq!(
            span.attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == name)
                .map(|attribute| &attribute.value),
            Some(&expected)
        );
    }
    assert!(!span.attributes.iter().any(|attribute| matches!(
        attribute.key.as_str(),
        "documentdb.collection.id" | "documentdb.query.shape_id"
    )));
    value["plan"]["index_names_complete"] = json!(true);
    assert_eq!(
        NativeSpan::from_json(&serde_json::to_vec(&value)?).err(),
        Some(Error::Invalid("plan coverage"))
    );
    value["plan"]["index_names_complete"] = json!(false);
    value["plan"]["fingerprint"] = json!("11112222333344445555666677778888");
    assert_eq!(
        NativeSpan::from_json(&serde_json::to_vec(&value)?).err(),
        Some(Error::Invalid("plan coverage"))
    );
    Ok(())
}
