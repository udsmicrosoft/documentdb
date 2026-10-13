/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Direct bounded export preserves producer identity, without an SDK sampler or queue.

use std::fmt;

use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    trace::{SpanData, SpanExporter},
    Resource,
};
use tokio::time;

use crate::{
    identity, Error, NativeSpan, MAX_BATCH_BYTES, MAX_BATCH_RECORDS, OPERATION_TIMEOUT,
    SCHEMA_VERSION,
};

/// Explicit deployment scope; no automatic resource or environment discovery.
#[derive(Debug)]
pub struct ExportScope {
    namespace: String,
    cluster: String,
    producer_instance_id: String,
}

impl ExportScope {
    /// Validates the configured scope and the backend producer identity.
    ///
    /// # Errors
    /// Rejects malformed or overlong scope labels and invalid producer identities.
    pub fn new(
        namespace: String,
        cluster: String,
        producer_instance_id: String,
    ) -> Result<Self, Error> {
        identity::validate_label(&namespace)?;
        identity::validate_label(&cluster)?;
        identity::validate_hex(&producer_instance_id, 32)?;
        Ok(Self {
            namespace,
            cluster,
            producer_instance_id,
        })
    }
}

/// Owns one exporter and allows only one in-flight batch, without retries or a queue.
pub struct OtlpSink {
    exporter: opentelemetry_otlp::SpanExporter,
    scope: ExportScope,
}

impl fmt::Debug for OtlpSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OtlpSink")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl OtlpSink {
    /// Creates a gRPC exporter for a fixed producer and explicit deployment scope.
    ///
    /// # Errors
    /// Reports exporter setup failures without exposing the endpoint or credentials.
    pub fn new(endpoint: &str, scope: ExportScope) -> Result<Self, Error> {
        let mut exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .with_timeout(OPERATION_TIMEOUT)
            .build()
            .map_err(|_error| Error::ExportFailed)?;
        exporter.set_resource(
            &Resource::builder_empty()
                .with_attributes([
                    KeyValue::new("service.name", "documentdb-backend"),
                    KeyValue::new("service.namespace", scope.namespace.clone()),
                    KeyValue::new("documentdb.cluster", scope.cluster.clone()),
                    KeyValue::new("service.instance.id", scope.producer_instance_id.clone()),
                    KeyValue::new(
                        "documentdb.telemetry.schema_version",
                        i64::from(SCHEMA_VERSION),
                    ),
                ])
                .build(),
        );
        Ok(Self { exporter, scope })
    }

    /// Exports immutable records once, preserving their original identities and times.
    ///
    /// # Errors
    /// Rejects oversized, mixed-producer, mixed-epoch, or out-of-order batches.
    /// Reports failed or timed-out delivery; the caller owns any bounded retry.
    #[expect(
        clippy::needless_pass_by_ref_mut,
        reason = "Exclusive access enforces single-flight export without a queue or lock."
    )]
    pub async fn export(&mut self, records: &[NativeSpan]) -> Result<(), Error> {
        if records.is_empty() || records.len() > MAX_BATCH_RECORDS {
            return Err(Error::Invalid("export batch count"));
        }
        let bytes =
            serde_json::to_vec(records).map_err(|_error| Error::Invalid("export batch schema"))?;
        if bytes.len() > MAX_BATCH_BYTES {
            return Err(Error::Invalid("export batch byte limit"));
        }
        let mut epoch = None;
        let mut sequence = 0;
        for record in records {
            if record.producer_instance_id() != self.scope.producer_instance_id
                || epoch.is_some_and(|value| value != record.epoch())
                || record.sequence() <= sequence
            {
                return Err(Error::Invalid("export producer, epoch, or sequence"));
            }
            epoch = Some(record.epoch());
            sequence = record.sequence();
        }
        let spans = records
            .iter()
            .map(SpanData::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        time::timeout(OPERATION_TIMEOUT, self.exporter.export(spans))
            .await
            .map_err(|_error| Error::ExportTimeout)?
            .map_err(|_error| Error::ExportFailed)
    }
}
