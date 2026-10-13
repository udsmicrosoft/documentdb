/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Disposable-stack acceptance of server-only command context logs, without backend spans.

use std::{
    collections::{HashMap, HashSet},
    env,
    error::Error,
    fs,
    future::IntoFuture,
    sync::Arc,
    time::Duration,
};

use bson::{doc, Bson, Document};
use documentdb_gateway_core::{
    auth::AuthenticationManager,
    configuration::{PgConfiguration, SetupConfiguration},
    postgres::{DocumentDBDataClient, ScopedTransaction},
    run_gateway, run_legacy_gateway,
    service::{DefaultRequestRouter, TlsProvider},
    startup,
    telemetry::{trace_context, TelemetrySettings},
};
use documentdb_tests::test_setup::{clients, config, postgres};
use mongodb::Database;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider};
use tokio::{net::TcpStream, time};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::{GatewayTracer, OTEL_ACTIVE_TRACE_CONTEXT_HOOK};

type TestResult<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
type EntryExpectation = (&'static str, bool, usize);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a disposable OSS stack with server context logging and a readable server log"]
async fn gateway_to_real_udf_entrypoints() -> TestResult {
    let log_path = env::var("UDF_TRACE_TEST_PG_LOG")?;
    let sampled = env::var("UDF_TRACE_TEST_SAMPLED")
        .unwrap_or_else(|_| "true".to_owned())
        .parse()?;
    let enabled = env::var("UDF_TRACE_TEST_ENABLED")
        .unwrap_or_else(|_| "true".to_owned())
        .parse()?;
    let backend_enabled: bool = env::var("UDF_TRACE_TEST_BACKEND_ENABLED")
        .unwrap_or_else(|_| "true".to_owned())
        .parse()?;
    let runtime = env::var("UDF_TRACE_TEST_RUNTIME").unwrap_or_else(|_| "v2".to_owned());
    if !matches!(runtime.as_str(), "v1" | "v2") {
        return Err("UDF_TRACE_TEST_RUNTIME must be v1 or v2".into());
    }
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_sampler(if sampled {
            Sampler::AlwaysOn
        } else {
            Sampler::AlwaysOff
        })
        .with_simple_exporter(exporter.clone())
        .build();
    tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(tracing_opentelemetry::OpenTelemetryLayer::new(
            GatewayTracer(provider.tracer("udf-carrier-test")),
        ))
        .try_init()?;
    assert!(trace_context::install_active_trace_context_hook(
        &OTEL_ACTIVE_TRACE_CONTEXT_HOOK
    ));

    postgres::create_user(clients::TEST_USERNAME, clients::TEST_PASSWORD).await?;
    let mut setup = config::setup_configuration();
    setup.set_telemetry_settings(TelemetrySettings::new(false).with_udf_trace_context(enabled));
    let pool = postgres::get_pool_manager();
    let dynamic =
        PgConfiguration::new(&setup, Arc::clone(&pool), vec!["documentdb.".to_owned()]).await?;
    let tls = TlsProvider::new(SetupConfiguration::certificate_options(&setup), None, None).await?;
    let service = startup::get_service_context(
        Box::new(setup),
        dynamic,
        pool,
        tls,
        AuthenticationManager::new(),
    );
    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let gateway = tokio::spawn(async move {
        if runtime == "v1" {
            run_legacy_gateway::<DocumentDBDataClient, _>(
                service,
                None,
                DefaultRequestRouter {},
                token,
            )
            .await
        } else {
            run_gateway::<DocumentDBDataClient, _>(service, None, DefaultRequestRouter {}, token)
                .await
        }
    });

    let result = async {
        time::timeout(Duration::from_secs(10), async {
            loop {
                if TcpStream::connect("127.0.0.1:10260").await.is_ok() {
                    break;
                }
                if gateway.is_finished() {
                    return Err("gateway exited before becoming ready");
                }
                time::sleep(Duration::from_millis(20)).await;
            }
            Ok(())
        })
        .await??;
        let log_offset = usize::try_from(fs::metadata(&log_path)?.len())?;
        let expected = time::timeout(
            Duration::from_mins(1),
            exercise_commands(enabled && sampled && backend_enabled),
        )
        .await??;
        provider.force_flush()?;
        validate_c_entry_contexts(&log_path, log_offset, &expected, &exporter)
    }
    .await;

    shutdown.cancel();
    time::timeout(Duration::from_secs(10), gateway).await???;
    provider.shutdown()?;
    result
}

fn validate_c_entry_contexts(
    log_path: &str,
    log_offset: usize,
    expected: &[EntryExpectation],
    exporter: &InMemorySpanExporter,
) -> TestResult {
    let log = fs::read_to_string(log_path)?;
    let received: Vec<_> = log
        .get(log_offset..)
        .ok_or("test log was truncated")?
        .lines()
        .filter_map(|line| {
            line.split_once("LOG:  udf_trace_context operation=")
                .map(|(_, record)| record)
        })
        .collect();
    assert_eq!(
        received.len(),
        expected
            .iter()
            .filter(|(_, has_context, _)| *has_context)
            .map(|(_, _, attempts)| attempts)
            .sum::<usize>(),
        "every sampled command attempt must log once, with no unsampled or disabled logs"
    );
    let contexts: HashMap<_, _> = exporter
        .get_finished_spans()?
        .into_iter()
        .filter(|span| span.name == "postgres.execute")
        .map(|span| -> TestResult<(String, usize)> {
            let parent = format!(
                "00-{}-{}-01",
                span.span_context.trace_id(),
                span.span_context.span_id()
            );
            let retries = span
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == "retry.count")
                .map(|attribute| attribute.value.as_str().parse())
                .transpose()?
                .unwrap_or_default();
            Ok((parent, retries))
        })
        .collect::<TestResult<_>>()?;
    let mut parents = HashSet::new();
    let mut received = received.iter();
    for &(operation, has_context, attempts) in expected {
        if !has_context {
            continue;
        }
        let expected_prefix = format!("{operation} traceparent=");
        let mut request_parent = None;
        for attempt in 0..attempts {
            let record = received.next().ok_or("missing C-entry attempt")?;
            let parent = record
                .strip_prefix(&expected_prefix)
                .ok_or_else(|| format!("unexpected C-entry record: {record}"))?;
            if has_context {
                let retries = contexts
                    .get(parent)
                    .ok_or("C did not receive an exported postgres.execute parent")?;
                assert_eq!(
                    *retries,
                    attempts - 1,
                    "retry evidence must come from the gateway"
                );
                if attempt == 0 {
                    assert!(
                        parents.insert(parent),
                        "new requests must not reuse an earlier execution parent"
                    );
                    request_parent = Some(parent);
                } else {
                    assert_eq!(
                        request_parent,
                        Some(parent),
                        "retries share their execution span"
                    );
                }
            } else {
                assert_eq!(parent, "none");
            }
        }
    }
    Ok(())
}

async fn exercise_commands(has_context: bool) -> TestResult<Vec<EntryExpectation>> {
    let client = clients::get_client()?;
    let db = clients::setup_db(&client, "udf_trace_acceptance").await?;
    let mut expected = Vec::new();
    let comments = [
        None,
        Some(Bson::String("application".to_owned())),
        Some(Bson::String(String::new())),
        Some(Bson::Null),
        Some(Bson::Array(vec![
            Bson::Int32(1),
            Bson::String("two".to_owned()),
        ])),
        Some(Bson::Document(
            doc! { "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "text": "unchanged" },
        )),
    ];
    for (id, comment) in comments.into_iter().enumerate() {
        let id = i32::try_from(id)?;
        let mut insert = doc! { "insert": "events", "documents": [{ "_id": id }] };
        let mut count = doc! { "count": "events", "query": { "_id": id } };
        if let Some(comment) = comment {
            insert.insert("comment", comment.clone());
            count.insert("comment", comment);
        }
        assert_count(&db.run_command(insert).await?, 1);
        expected.push(("insert", has_context, 1));
        assert_count(&db.run_command(count).await?, 1);
        expected.push(("count", has_context, 1));
    }

    db.collection::<Document>("events")
        .insert_one(doc! { "_id": 99 })
        .await?;
    expected.push(("insert", has_context, 1));
    assert_count(
        &db.run_command(doc! { "insert": "events", "documents": [{ "_id": 100 }, { "_id": 101 }] })
            .await?,
        2,
    );
    expected.push(("insert", has_context, 1));

    db.collection::<Document>("events")
        .insert_many([doc! { "_id": 102 }, doc! { "_id": 103 }])
        .await?;
    expected.push(("insert", has_context, 1));

    let mut session = client.start_session().await?;
    session.start_transaction().await?;
    assert_count(
        &db.run_command(doc! { "insert": "events", "documents": [{ "_id": 200 }] })
            .session(&mut session)
            .await?,
        1,
    );
    expected.push(("insert", has_context, 1));
    assert_count(
        &db.run_command(doc! { "count": "events", "query": { "_id": 200 } })
            .session(&mut session)
            .await?,
        1,
    );
    expected.push(("count", has_context, 1));
    session.abort_transaction().await?;
    drop(session);

    exercise_data_commands(&db, has_context, &mut expected).await?;
    let error = db
        .run_command(doc! { "find": "events", "filter": 1 })
        .await
        .err()
        .ok_or("malformed find unexpectedly succeeded")?;
    assert!(error.to_string().contains("filter"));
    expected.push(("find", has_context, 1));
    for _ in 0..4 {
        assert_count(
            &db.run_command(doc! { "count": "events", "query": { "_id": 200 } })
                .await?,
            0,
        );
        expected.push(("count", has_context, 1));
    }
    reject_client_metadata(&db, has_context, &mut expected).await?;
    exercise_retry(&db).await?;
    expected.push(("find", has_context, 2));
    let persisted = db
        .collection::<Document>("events")
        .find_one(doc! { "_id": 0 })
        .await?
        .ok_or("inserted document missing")?;
    expected.push(("find", has_context, 1));
    assert_eq!(persisted, doc! { "_id": 0 });
    db.drop().await?;
    client.shutdown().await;
    Ok(expected)
}

fn cursor_id(response: &Document, batch: &str, expected_count: usize) -> TestResult<i64> {
    assert!(!response.to_string().contains("$documentdbTrace"));
    let cursor = response.get_document("cursor")?;
    assert_eq!(cursor.get_array(batch)?.len(), expected_count);
    Ok(cursor.get_i64("id")?)
}

async fn exercise_data_commands(
    db: &Database,
    has_context: bool,
    expected: &mut Vec<EntryExpectation>,
) -> TestResult {
    let distinct = db
        .run_command(doc! { "distinct": "events", "key": "_id", "query": { "_id": 0 } })
        .await?;
    assert_eq!(distinct.get_array("values")?, &[Bson::Int32(0)]);
    expected.push(("distinct", has_context, 1));
    for (command, operation) in [
        (
            doc! { "update": "events", "updates": [{ "q": { "_id": 99 }, "u": { "$set": { "active": true } } }] },
            "update",
        ),
        (
            doc! { "delete": "events", "deletes": [{ "q": { "_id": 101 }, "limit": 1 }] },
            "delete",
        ),
    ] {
        assert_count(&db.run_command(command).await?, 1);
        expected.push((operation, has_context, 1));
    }
    let modified = db
        .run_command(doc! { "findAndModify": "events", "query": { "_id": 99 }, "update": { "$set": { "seen": true } }, "new": true })
        .await?;
    assert!(modified.get_document("value")?.get_bool("seen")?);
    expected.push(("findAndModify", has_context, 1));

    for (command, operation) in [
        (
            doc! { "find": "events", "filter": { "_id": { "$lt": 3 } }, "sort": { "_id": 1 }, "batchSize": 1 },
            "find",
        ),
        (
            doc! { "aggregate": "events", "pipeline": [{ "$match": { "_id": { "$lt": 3 } } }, { "$sort": { "_id": 1 } }], "cursor": { "batchSize": 1 } },
            "aggregate",
        ),
    ] {
        let first = db.run_command(command).await?;
        let id = cursor_id(&first, "firstBatch", 1)?;
        assert_ne!(id, 0);
        expected.push((operation, has_context, 1));
        let next = db
            .run_command(doc! { "getMore": id, "collection": "events", "batchSize": 10 })
            .await?;
        assert_eq!(cursor_id(&next, "nextBatch", 2)?, 0);
        expected.push(("getMore", has_context, 1));
    }

    db.run_command(doc! { "create": "untraced" }).await?;
    db.collection::<Document>("untraced").drop().await?;
    db.run_command(doc! { "explain": { "find": "events", "filter": { "_id": 0 } }, "verbosity": "queryPlanner" }).await?;
    Ok(())
}

async fn reject_client_metadata(
    db: &Database,
    has_context: bool,
    expected: &mut Vec<EntryExpectation>,
) -> TestResult {
    for field in ["$documentdbTrace", "$DOCUMENTDBTRACE", "$documentdbtrace"] {
        for mut command in [
            doc! { "count": "events" },
            doc! { "insert": "events", "documents": [{ "_id": 300 }] },
            doc! { "find": "events" },
            doc! { "aggregate": "events", "pipeline": [], "cursor": {} },
            doc! { "distinct": "events", "key": "_id" },
            doc! { "update": "events", "updates": [{ "q": {}, "u": { "$set": { "injected": true } } }] },
            doc! { "delete": "events", "deletes": [{ "q": {}, "limit": 1 }] },
            doc! { "findAndModify": "events", "query": {}, "remove": true },
        ] {
            command.insert(field, doc! { "version": 1, "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" });
            let error = db
                .run_command(command)
                .await
                .err()
                .ok_or("caller-supplied metadata was accepted")?;
            assert!(error.to_string().contains(field));
        }
        let first = db
            .run_command(doc! { "find": "events", "batchSize": 1 })
            .await?;
        let id = cursor_id(&first, "firstBatch", 1)?;
        expected.push(("find", has_context, 1));
        let mut get_more = doc! { "getMore": id, "collection": "events" };
        get_more.insert(field, doc! { "version": 1 });
        let error = db
            .run_command(get_more)
            .await
            .err()
            .ok_or("getMore accepted client metadata")?;
        assert!(error.to_string().contains(field));
        db.run_command(doc! { "killCursors": "events", "cursors": [id] })
            .await?;

        let mut target = doc! { "find": "events" };
        target.insert(field, doc! { "version": 1 });
        let error = db
            .run_command(doc! { "explain": target })
            .await
            .err()
            .ok_or("explain accepted client metadata")?;
        assert!(error.to_string().contains(field));
    }
    Ok(())
}

async fn exercise_retry(db: &Database) -> TestResult {
    let pool = postgres::get_pool_manager();
    let lock = Arc::new(pool.authentication_connection().await?);
    let admin = pool.authentication_connection().await?;
    let rows = lock.query(
        "SELECT collection_id, pg_backend_pid() FROM documentdb_api_catalog.collections WHERE database_name = 'udf_trace_acceptance' AND collection_name = 'events'",
        &[], &[],
    ).await?;
    let row = rows.first().ok_or("retry fixture collection missing")?;
    let collection_id: i64 = row.get(0);
    let lock_pid: i32 = row.get(1);
    let mut transaction = ScopedTransaction::start_if_necessary(&lock).await?;
    lock.batch_execute(&format!(
        "LOCK TABLE documentdb_data.documents_{collection_id} IN ACCESS EXCLUSIVE MODE"
    ))
    .await?;

    let interrupt = async {
        let result: TestResult = async {
            let pid = time::timeout(Duration::from_secs(10), async {
                loop {
                    let rows = admin.query(
                        &format!("SELECT pid FROM pg_stat_activity WHERE {lock_pid} = ANY(pg_blocking_pids(pid)) AND query LIKE '%find_cursor_first_page%'"),
                        &[], &[],
                    ).await?;
                    if let [row] = rows.as_slice() {
                        return Ok::<i32, Box<dyn Error + Send + Sync>>(row.get(0));
                    }
                    time::sleep(Duration::from_millis(10)).await;
                }
            }).await??;
            let rows = admin.query(&format!("SELECT pg_terminate_backend({pid})"), &[], &[]).await?;
            assert!(rows.first().ok_or("termination result missing")?.get::<_, bool>(0));
            Ok(())
        }.await;
        transaction.commit().await?;
        result
    };
    let (found, interrupted) = tokio::join!(
        db.run_command(doc! { "find": "events", "filter": { "_id": 0 }, "singleBatch": true })
            .into_future(),
        interrupt
    );
    interrupted?;
    assert_eq!(cursor_id(&found?, "firstBatch", 1)?, 0);
    Ok(())
}

fn assert_count(response: &Document, expected: i64) {
    let actual = match response.get("n") {
        Some(Bson::Int32(value)) => Some(i64::from(*value)),
        Some(Bson::Int64(value)) => Some(*value),
        _ => None,
    };
    assert_eq!(actual, Some(expected));
}
