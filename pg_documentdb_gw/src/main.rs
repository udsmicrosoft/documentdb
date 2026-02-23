/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/main.rs
 *
 *-------------------------------------------------------------------------
 */

use std::{env, path::PathBuf, sync::Arc};

use documentdb_gateway::{
    configuration::{DocumentDBSetupConfiguration, PgConfiguration, SetupConfiguration},
    postgres::{
        create_query_catalog, DocumentDBDataClient, AUTHENTICATION_MAX_CONNECTIONS,
        SYSTEM_REQUESTS_MAX_CONNECTIONS,
    },
    run_gateway,
    service::TlsProvider,
    shutdown_controller::SHUTDOWN_CONTROLLER,
    startup::{create_postgres_object, get_service_context, get_system_connection_pool},
    telemetry::{OtelTelemetryProvider, TelemetryConfig, TelemetryManager, TelemetryProvider},
};
use opentelemetry::KeyValue;

use tokio::signal;

fn main() {
    // Takes the configuration file as an argument
    let cfg_file = if let Some(arg1) = env::args().nth(1) {
        PathBuf::from(arg1)
    } else {
        // Defaults to the source directory for local runs
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("SetupConfiguration.json")
    };

    // Load configuration
    let setup_configuration =
        DocumentDBSetupConfiguration::new(&cfg_file).expect("Failed to load configuration.");

    // Create Tokio runtime with configured worker threads
    let async_runtime_worker_threads = setup_configuration.async_runtime_worker_threads();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(async_runtime_worker_threads)
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");

    // Run the async main logic (telemetry init must happen inside runtime)
    runtime.block_on(start_gateway(setup_configuration));
}

async fn start_gateway(setup_configuration: DocumentDBSetupConfiguration) {
    // Initialize telemetry inside the async runtime (OTLP exporter requires it)
    let telemetry_config = TelemetryConfig::new(setup_configuration.telemetry_options());
    let attributes = vec![
        KeyValue::new("service.name", telemetry_config.service_name()),
        KeyValue::new("service.version", telemetry_config.service_version()),
    ];

    let (telemetry_manager, telemetry_initialized) = if telemetry_config.any_signal_enabled() {
        match TelemetryManager::init_telemetry(telemetry_config, attributes) {
            Ok(manager) => (Some(manager), true),
            Err(e) => {
                eprintln!("Failed to initialize OpenTelemetry: {e}");
                (None, false)
            }
        }
    } else {
        (None, false)
    };

    tracing::info!("Starting server with configuration: {setup_configuration:?}");

    let shutdown_token = SHUTDOWN_CONTROLLER.token();

    tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        tracing::info!("Ctrl+C received. Shutting down Rust gateway.");
        SHUTDOWN_CONTROLLER.shutdown();
    });

    let tls_provider = TlsProvider::new(
        SetupConfiguration::certificate_options(&setup_configuration),
        None,
        None,
    )
    .await
    .expect("Failed to create TLS provider.");

    tracing::info!("TLS provider initialized successfully.");

    let query_catalog = create_query_catalog();

    let system_requests_pool = Arc::new(
        get_system_connection_pool(
            &setup_configuration,
            &query_catalog,
            "SystemRequests",
            SYSTEM_REQUESTS_MAX_CONNECTIONS,
        )
        .await,
    );
    tracing::info!("System requests pool initialized");

    let dynamic_configuration = create_postgres_object(
        || async {
            PgConfiguration::new(
                &query_catalog,
                &setup_configuration,
                &system_requests_pool,
                vec!["documentdb.".to_string()],
            )
            .await
        },
        &setup_configuration,
    )
    .await;

    let authentication_pool = get_system_connection_pool(
        &setup_configuration,
        &query_catalog,
        "PreAuthRequests",
        AUTHENTICATION_MAX_CONNECTIONS,
    )
    .await;
    tracing::info!("Authentication pool initialized");

    let service_context = get_service_context(
        Box::new(setup_configuration),
        dynamic_configuration,
        query_catalog,
        system_requests_pool,
        authentication_pool,
        tls_provider,
    );

    let telemetry: Option<Box<dyn TelemetryProvider>> = if telemetry_initialized {
        Some(Box::new(OtelTelemetryProvider::new()))
    } else {
        None
    };
    run_gateway::<DocumentDBDataClient>(service_context, telemetry, shutdown_token)
        .await
        .unwrap();

    if let Some(manager) = telemetry_manager {
        if let Err(err) = manager.shutdown() {
            eprintln!("Failed to shutdown telemetry manager: {err}");
        }
    }
}
