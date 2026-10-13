/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Real native SQL records reach OTLP unchanged, including an ambiguous-delivery retry.

use std::{
    env,
    error::Error,
    fs,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use documentdb_backend_telemetry_exporter::{Error as TelemetryError, NativeCollector};
use futures::stream;
use opentelemetry_proto::tonic::{
    collector::trace::v1::{
        trace_service_server::{TraceService, TraceServiceServer},
        ExportTraceServiceRequest, ExportTraceServiceResponse,
    },
    common::v1::any_value::Value as OtlpValue,
    trace::v1::Span,
};
use serde_json::Value;
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    time,
};
use tonic::{transport::Server, Request, Response, Status};

#[derive(Debug)]
struct Receiver {
    received: mpsc::Sender<ExportTraceServiceRequest>,
    reject_once: AtomicBool,
}

#[tonic::async_trait]
impl TraceService for Receiver {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        self.received
            .try_send(request.into_inner())
            .map_err(|_error| Status::resource_exhausted("fixture full"))?;
        if self.reject_once.swap(false, Ordering::SeqCst) {
            return Err(Status::unavailable("fixture delivery acknowledgement lost"));
        }
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[tokio::test]
#[ignore = "requires an owned native backend and its captured batch"]
async fn native_sql_to_otlp_retries_without_changing_or_losing_records(
) -> Result<(), Box<dyn Error>> {
    let batch: Value = serde_json::from_slice(&fs::read(env::var(
        "DOCUMENTDB_TELEMETRY_TEST_NATIVE_BATCH",
    )?)?)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let incoming = stream::unfold(listener, |listener| async {
        let accepted = listener.accept().await.map(|(stream, _)| stream);
        Some((accepted, listener))
    });
    let (sender, mut received) = mpsc::channel(4);
    let (shutdown, stopped) = oneshot::channel();
    let server = tokio::spawn(
        Server::builder().serve_with_incoming_shutdown(
            TraceServiceServer::new(Receiver {
                received: sender,
                reject_once: AtomicBool::new(true),
            })
            .max_decoding_message_size(256 * 1024),
            incoming,
            async {
                let _ = stopped.await;
            },
        ),
    );
    let mut collector = NativeCollector::connect(
        &env::var("DOCUMENTDB_TELEMETRY_TEST_PG_URI")?,
        &endpoint,
        "local".to_owned(),
        "native-demo".to_owned(),
    )
    .await?;
    assert!(matches!(
        collector.export_next().await,
        Err(TelemetryError::ExportFailed | TelemetryError::ExportTimeout)
    ));
    let first = time::timeout(Duration::from_secs(3), received.recv())
        .await?
        .ok_or("missing first attempt")?;
    let expected = batch["records"]
        .as_array()
        .ok_or("missing source records")?;
    assert!(!expected.is_empty());
    let report = collector.export_next().await?;
    assert_eq!(report.exported_records(), expected.len());
    let acknowledgement: Value = serde_json::from_slice(&report.to_json()?)?;
    assert_eq!(acknowledgement["origin"], "native_collection");
    assert_eq!(
        acknowledgement["producer_instance_id"],
        batch["producer_instance_id"]
    );
    assert_eq!(acknowledgement["epoch"], batch["epoch"]);
    assert_eq!(
        acknowledgement["after_sequence"],
        batch["next_cursor"]["after_sequence"]
    );
    let retry = time::timeout(Duration::from_secs(3), received.recv())
        .await?
        .ok_or("missing retry")?;
    assert_eq!(first, retry);
    verify_native_export(&retry, &batch)?;
    assert_eq!(collector.export_next().await?.exported_records(), 0);
    assert!(matches!(
        received.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    shutdown.send(()).map_err(|()| "receiver stopped")?;
    time::timeout(Duration::from_secs(3), server).await???;
    Ok(())
}

fn verify_native_export(
    request: &ExportTraceServiceRequest,
    batch: &Value,
) -> Result<(), Box<dyn Error>> {
    let expected = batch["records"]
        .as_array()
        .ok_or("missing source records")?;
    let resource = request.resource_spans.first().ok_or("missing resource")?;
    let attributes = &resource
        .resource
        .as_ref()
        .ok_or("missing attributes")?
        .attributes;
    for (key, expected) in [
        ("service.name", "documentdb-backend"),
        ("service.namespace", "local"),
        ("documentdb.cluster", "native-demo"),
        (
            "service.instance.id",
            batch["producer_instance_id"]
                .as_str()
                .ok_or("missing producer")?,
        ),
    ] {
        assert_eq!(
            attributes
                .iter()
                .find(|attribute| attribute.key == key)
                .and_then(|attribute| attribute.value.as_ref())
                .and_then(|value| value.value.as_ref()),
            Some(&OtlpValue::StringValue(expected.to_owned()))
        );
    }
    let scope = resource.scope_spans.first().ok_or("missing scope")?;
    assert_eq!(
        scope.scope.as_ref().ok_or("missing library")?.name,
        "documentdb.backend"
    );
    let spans = &scope.spans;
    assert_eq!(spans.len(), expected.len());
    for (actual, original) in spans.iter().zip(expected) {
        assert_eq!(
            hex::encode(&actual.trace_id),
            original["trace_id"].as_str().ok_or("missing trace ID")?
        );
        assert_eq!(
            hex::encode(&actual.span_id),
            original["span_id"].as_str().ok_or("missing child ID")?
        );
        assert_eq!(
            hex::encode(&actual.parent_span_id),
            original["parent_span_id"]
                .as_str()
                .ok_or("missing parent ID")?
        );
        assert_eq!(
            actual.start_time_unix_nano,
            original["start_time_unix_ns"]
                .as_u64()
                .ok_or("missing start")?
        );
        assert_eq!(
            actual.end_time_unix_nano,
            original["end_time_unix_ns"].as_u64().ok_or("missing end")?
        );
        assert_eq!(actual.name, "documentdb.command");
        assert_eq!(actual.flags & 0xff, 1);
        assert_eq!(original["outcome"], "ok");
        assert_eq!(actual.status.as_ref().ok_or("missing status")?.code, 1);
        verify_plan_and_work(actual, original)?;
    }
    Ok(())
}

fn verify_plan_and_work(span: &Span, record: &Value) -> Result<(), Box<dyn Error>> {
    for (name, value) in [
        ("documentdb.plan.scan_kind", &record["plan"]["scan_kind"]),
        ("documentdb.plan.complete", &record["plan"]["complete"]),
        (
            "documentdb.plan.index_names_complete",
            &record["plan"]["index_names_complete"],
        ),
        (
            "documentdb.plan.unsupported_nodes",
            &record["plan"]["unsupported_nodes"],
        ),
        ("documentdb.plan.truncated", &record["plan"]["truncated"]),
        (
            "documentdb.plan.fingerprint",
            &record["plan"]["fingerprint"],
        ),
        (
            "documentdb.command.returned_documents",
            &record["work"]["returned_documents"],
        ),
        (
            "documentdb.command.examined_documents",
            &record["work"]["examined_documents"],
        ),
        ("documentdb.collection.id", &record["collection_id"]),
        ("documentdb.query.shape_id", &record["query_shape_id"]),
    ] {
        let expected = match value {
            Value::Null => None,
            Value::String(value) => Some(OtlpValue::StringValue(value.clone())),
            Value::Bool(value) => Some(OtlpValue::BoolValue(*value)),
            Value::Number(value) => {
                Some(OtlpValue::IntValue(value.as_i64().ok_or("invalid count")?))
            }
            _ => return Err("unexpected native attribute type".into()),
        };
        assert_eq!(
            span.attributes
                .iter()
                .find(|attribute| attribute.key == name)
                .and_then(|attribute| attribute.value.as_ref())
                .and_then(|value| value.value.as_ref()),
            expected.as_ref(),
            "{name}"
        );
    }
    Ok(())
}
