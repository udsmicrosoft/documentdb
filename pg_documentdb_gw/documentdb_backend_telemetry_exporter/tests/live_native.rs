/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Compatibility smoke check for a batch emitted by the real native producer.

use std::{env, error::Error, fs::File, io::Read, time::SystemTime};

use documentdb_backend_telemetry_exporter::NativeSpan;
use opentelemetry_sdk::trace::SpanData;
use serde_json::Value;

#[test]
#[ignore = "requires a batch captured from the owned native demo backend"]
fn native_backend_records_preserve_producer_identity_and_time() -> Result<(), Box<dyn Error>> {
    let mut bytes = Vec::new();
    File::open(env::var("DOCUMENTDB_TELEMETRY_TEST_NATIVE_BATCH")?)?
        .take(256 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    assert!(bytes.len() <= 256 * 1024);
    let batch: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(batch["schema_version"], 1);
    assert_eq!(batch["epoch_reset"], false);
    let records = batch["records"]
        .as_array()
        .ok_or("missing native records")?;
    assert!((1..=128).contains(&records.len()));
    for value in records {
        let record = NativeSpan::from_json(&serde_json::to_vec(value)?)?;
        assert_eq!(record.producer_instance_id(), batch["producer_instance_id"]);
        assert_eq!(record.epoch(), batch["epoch"]);
        let span = SpanData::try_from(&record)?;
        assert_eq!(
            span.span_context.trace_id().to_string(),
            value["trace_id"].as_str().ok_or("missing trace ID")?
        );
        assert_eq!(
            span.span_context.span_id().to_string(),
            value["span_id"].as_str().ok_or("missing child ID")?
        );
        assert_eq!(
            span.parent_span_id.to_string(),
            value["parent_span_id"]
                .as_str()
                .ok_or("missing parent ID")?
        );
        assert_eq!(
            span.start_time
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_nanos(),
            u128::from(
                value["start_time_unix_ns"]
                    .as_u64()
                    .ok_or("missing start")?
            )
        );
        assert_eq!(
            span.end_time
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_nanos(),
            u128::from(value["end_time_unix_ns"].as_u64().ok_or("missing end")?)
        );
        assert_eq!(span.name, "documentdb.command");
        assert!(span.span_context.is_sampled());
    }
    Ok(())
}
