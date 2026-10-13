/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! A real loopback OTLP receiver verifies that export never regenerates producer context.

use std::{error::Error, time::Duration};

use documentdb_backend_telemetry_exporter::{
    Error as TelemetryError, ExportScope, NativeSpan, OtlpSink,
};
use futures::stream;
use opentelemetry_proto::tonic::collector::trace::v1::{
    trace_service_server::{TraceService, TraceServiceServer},
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use serde_json::{json, Value};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    time,
};
use tonic::{transport::Server, Request, Response, Status};

type TestResult = Result<(), Box<dyn Error>>;
const RECORD: &[u8] = include_bytes!("fixtures/native-find-v1.json");

#[derive(Debug)]
struct Receiver(mpsc::Sender<ExportTraceServiceRequest>);

#[tonic::async_trait]
impl TraceService for Receiver {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        self.0
            .try_send(request.into_inner())
            .map_err(|_error| Status::resource_exhausted("fixture full"))?;
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

fn scope() -> Result<ExportScope, TelemetryError> {
    ExportScope::new(
        "local".to_owned(),
        "fixture".to_owned(),
        "0123456789abcdef0123456789abcdef".to_owned(),
    )
}

#[tokio::test]
async fn preserves_supplied_ids_nanoseconds_sampling_and_status_on_the_wire() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let incoming = stream::unfold(listener, |listener| async {
        let accepted = listener.accept().await.map(|(stream, _)| stream);
        Some((accepted, listener))
    });
    let (sender, mut received) = mpsc::channel(1);
    let (shutdown, stopped) = oneshot::channel();
    let server = tokio::spawn(Server::builder().serve_with_incoming_shutdown(
        TraceServiceServer::new(Receiver(sender)).max_decoding_message_size(256 * 1024),
        incoming,
        async {
            let _ = stopped.await;
        },
    ));
    let mut sink = OtlpSink::new(&endpoint, scope()?)?;
    let record = NativeSpan::from_json(RECORD)?;
    sink.export(std::slice::from_ref(&record)).await?;
    let request = time::timeout(Duration::from_secs(3), received.recv())
        .await?
        .ok_or("missing OTLP request")?;
    let resources = request
        .resource_spans
        .first()
        .ok_or("missing resource spans")?;
    let scope_spans = resources.scope_spans.first().ok_or("missing scope spans")?;
    let span = scope_spans.spans.first().ok_or("missing span")?;
    assert_eq!(
        hex::encode(&span.trace_id),
        "11112222333344445555666677778888"
    );
    assert_eq!(hex::encode(&span.span_id), "1122334455667788");
    assert_eq!(hex::encode(&span.parent_span_id), "8877665544332211");
    assert_eq!(span.start_time_unix_nano, 1_790_872_000_123_456_789);
    assert_eq!(span.end_time_unix_nano, 1_790_872_000_125_802_467);
    assert_eq!(span.flags & 0xff, 1);
    assert_eq!(span.name, "documentdb.command");
    assert_eq!(span.status.as_ref().ok_or("missing status")?.code, 1);
    assert_eq!(
        scope_spans.scope.as_ref().ok_or("missing scope")?.name,
        "documentdb.backend"
    );
    let attributes = &resources
        .resource
        .as_ref()
        .ok_or("missing resource")?
        .attributes;
    assert!(attributes
        .iter()
        .any(|attribute| attribute.key == "service.instance.id"));
    assert!(!attributes
        .iter()
        .any(|attribute| attribute.key.starts_with("k8s.")));
    sink.export(std::slice::from_ref(&record)).await?;
    let retry = time::timeout(Duration::from_secs(3), received.recv())
        .await?
        .ok_or("missing duplicate fixture export")?;
    assert_eq!(
        request, retry,
        "retrying must not mutate timestamps or attributes"
    );
    let mut failed: Value = serde_json::from_slice(RECORD)?;
    failed["outcome"] = json!("error");
    failed["error_class"] = json!("timeout");
    sink.export(&[NativeSpan::from_json(&serde_json::to_vec(&failed)?)?])
        .await?;
    let failure = time::timeout(Duration::from_secs(3), received.recv())
        .await?
        .ok_or("missing failure export")?;
    let status = failure
        .resource_spans
        .first()
        .and_then(|resource| resource.scope_spans.first())
        .and_then(|scope| scope.spans.first())
        .and_then(|span| span.status.as_ref())
        .ok_or("missing failure status")?;
    assert_eq!(status.code, 2);
    assert!(status.message.is_empty());
    shutdown.send(()).map_err(|()| "receiver already stopped")?;
    time::timeout(Duration::from_secs(3), server).await???;
    Ok(())
}

#[tokio::test]
async fn outage_and_invalid_batches_do_not_create_an_export_queue() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut sink = OtlpSink::new(&format!("http://{}", listener.local_addr()?), scope()?)?;
    let record = NativeSpan::from_json(RECORD)?;
    assert_eq!(
        sink.export(&[]).await,
        Err(TelemetryError::Invalid("export batch count"))
    );
    assert_eq!(
        sink.export(&vec![record.clone(); 129]).await,
        Err(TelemetryError::Invalid("export batch count"))
    );
    assert_eq!(
        sink.export(&[record.clone(), record.clone()]).await,
        Err(TelemetryError::Invalid(
            "export producer, epoch, or sequence"
        ))
    );
    let mut foreign: Value = serde_json::from_slice(RECORD)?;
    foreign["producer_instance_id"] = json!("11111111111111111111111111111111");
    assert_eq!(
        sink.export(&[NativeSpan::from_json(&serde_json::to_vec(&foreign)?)?])
            .await,
        Err(TelemetryError::Invalid(
            "export producer, epoch, or sequence"
        ))
    );
    let start = time::Instant::now();
    let result = sink.export(&[record]).await;
    assert!(matches!(
        result,
        Err(TelemetryError::ExportTimeout | TelemetryError::ExportFailed)
    ));
    assert!(start.elapsed() < Duration::from_secs(4));
    Ok(())
}
