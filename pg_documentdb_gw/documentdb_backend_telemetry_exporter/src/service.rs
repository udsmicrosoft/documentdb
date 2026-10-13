/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Long-running local collection with bounded recovery and sanitized status.

use std::{
    env,
    io::{self, Write},
    net::IpAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use documentdb_backend_telemetry_exporter::{
    Error, NativeCheckpoint, NativeCollectionReport, NativeCollector, NativeResumeReport,
    MAX_CONFIG_BYTES,
};
use serde::Serialize;
use tokio::{signal, time};
use tokio_postgres::{
    config::{Host, SslMode},
    Config,
};

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const STATUS_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Serialize)]
struct Status {
    origin: &'static str,
    state: &'static str,
    error: Option<String>,
    reconnects: u64,
    delivery_failures: u64,
    gap_events: u64,
    source_changes: u64,
    exported_records: u64,
    unconfirmed_records: usize,
    last_export_unix_ms: Option<u64>,
    last_resume: Option<NativeResumeReport>,
    last_collection: Option<NativeCollectionReport>,
}

impl Status {
    const fn new() -> Self {
        Self {
            origin: "native_exporter_status",
            state: "starting",
            error: None,
            reconnects: 0,
            delivery_failures: 0,
            gap_events: 0,
            source_changes: 0,
            exported_records: 0,
            unconfirmed_records: 0,
            last_export_unix_ms: None,
            last_resume: None,
            last_collection: None,
        }
    }

    fn write(&self) -> Result<(), Error> {
        let bytes =
            serde_json::to_vec(self).map_err(|_error| Error::Invalid("native exporter status"))?;
        let mut stdout = io::stdout().lock();
        stdout.write_all(&bytes).map_err(|_error| Error::Io)?;
        stdout.write_all(b"\n").map_err(|_error| Error::Io)
    }
}

struct Service {
    collector: Option<NativeCollector>,
    checkpoint: Option<NativeCheckpoint>,
    status: Status,
    backoff: Duration,
}

impl Service {
    const fn new() -> Self {
        Self {
            collector: None,
            checkpoint: None,
            status: Status::new(),
            backoff: POLL_INTERVAL,
        }
    }

    async fn step(
        &mut self,
        uri: &str,
        endpoint: &str,
        namespace: &str,
        cluster: &str,
    ) -> Result<bool, Error> {
        let rebound = if self.collector.is_none() {
            let (collector, report) = NativeCollector::resume(
                uri,
                endpoint,
                namespace.to_owned(),
                cluster.to_owned(),
                self.checkpoint.as_ref(),
            )
            .await?;
            self.status.gap_events += u64::from(report.history_gap());
            self.status.source_changes += u64::from(report.source_changed());
            self.status.last_resume = Some(report);
            self.checkpoint = Some(collector.checkpoint());
            self.collector = Some(collector);
            true
        } else {
            false
        };
        let collector = self
            .collector
            .as_mut()
            .ok_or(Error::Invalid("native service source"))?;
        let report = collector.export_next().await?;
        self.checkpoint = Some(collector.checkpoint());
        if report.exported_records() > 0 {
            self.status.exported_records += u64::try_from(report.exported_records())
                .map_err(|_error| Error::Invalid("native exported record count"))?;
            self.status.last_export_unix_ms = Some(
                u64::try_from(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_err(|_error| Error::Clock)?
                        .as_millis(),
                )
                .map_err(|_error| Error::Clock)?,
            );
        }
        self.status.last_collection = Some(report);
        self.status.state = "running";
        self.status.error = None;
        self.status.unconfirmed_records = 0;
        self.backoff = POLL_INTERVAL;
        Ok(rebound)
    }

    fn failed(&mut self, error: Error) -> Duration {
        self.status.error = Some(error.to_string());
        if matches!(error, Error::ExportFailed | Error::ExportTimeout) {
            self.status.state = "retrying_export";
            self.status.delivery_failures += 1;
            self.status.unconfirmed_records = self
                .collector
                .as_ref()
                .map_or(0, NativeCollector::pending_records);
        } else {
            self.status.state = "waiting_for_source";
            self.status.reconnects += 1;
            self.collector = None;
        }
        let delay = self.backoff;
        self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
        delay
    }
}

fn validate_local_uri(uri: &str) -> Result<(), Error> {
    if uri.len() > MAX_CONFIG_BYTES {
        return Err(Error::Invalid("native connection configuration byte limit"));
    }
    let config: Config = uri
        .parse()
        .map_err(|_error| Error::Invalid("native sidecar connection configuration"))?;
    let local_host = config.get_hosts().len() == 1
        && config.get_hosts().iter().all(|host| {
            matches!(host, Host::Tcp(host) if host == "localhost"
            || host.parse::<IpAddr>().is_ok_and(|address| address.is_loopback()))
        });
    if !local_host
        || config
            .get_hostaddrs()
            .iter()
            .any(|address| !address.is_loopback())
        || config.get_ssl_mode() != SslMode::Disable
        || config.get_user().is_none_or(str::is_empty)
        || config.get_dbname().is_none_or(str::is_empty)
    {
        return Err(Error::Invalid(
            "native sidecar requires explicit local database, reader, and sslmode=disable",
        ));
    }
    Ok(())
}

/// Collects local native spans until shutdown, reporting source and delivery recovery.
///
/// # Errors
/// Rejects invalid startup configuration and reports signal or status-output failures.
pub async fn run(namespace: String, cluster: String) -> Result<(), Error> {
    let uri = env::var("DOCUMENTDB_TELEMETRY_PG_URI")
        .map_err(|_error| Error::Invalid("DOCUMENTDB_TELEMETRY_PG_URI is required"))?;
    let endpoint = env::var("DOCUMENTDB_TELEMETRY_OTLP_ENDPOINT")
        .map_err(|_error| Error::Invalid("DOCUMENTDB_TELEMETRY_OTLP_ENDPOINT is required"))?;
    validate_local_uri(&uri)?;

    #[cfg(unix)]
    let mut terminate =
        signal::unix::signal(signal::unix::SignalKind::terminate()).map_err(|_error| Error::Io)?;
    let shutdown = async move {
        #[cfg(unix)]
        {
            tokio::select! {
                result = signal::ctrl_c() => result.map_err(|_error| Error::Io),
                _ = terminate.recv() => Ok(()),
            }
        }
        #[cfg(not(unix))]
        signal::ctrl_c().await.map_err(|_error| Error::Io)
    };
    tokio::pin!(shutdown);
    let mut service = Service::new();
    service.status.write()?;
    let mut last_status = time::Instant::now();
    loop {
        let previous_state = service.status.state;
        let result = tokio::select! {
            result = service.step(&uri, &endpoint, &namespace, &cluster) => result,
            result = &mut shutdown => {
                result?;
                break;
            }
        };
        let (delay, force_status) = match result {
            Ok(rebound) => (POLL_INTERVAL, rebound),
            Err(error) => (service.failed(error), true),
        };
        if force_status
            || previous_state != service.status.state
            || last_status.elapsed() >= STATUS_INTERVAL
        {
            service.status.write()?;
            last_status = time::Instant::now();
        }
        tokio::select! {
            () = time::sleep(delay) => (),
            result = &mut shutdown => {
                result?;
                break;
            }
        }
    }
    service.status.state = "stopped";
    service.status.unconfirmed_records = service
        .collector
        .as_ref()
        .map_or(0, NativeCollector::pending_records);
    service.status.write()
}

#[cfg(test)]
mod tests {
    use documentdb_backend_telemetry_exporter::{Error, MAX_CONFIG_BYTES};

    use super::{validate_local_uri, Service, MAX_BACKOFF};

    #[test]
    fn sidecar_rejects_remote_implicit_and_redirected_sources() -> Result<(), Error> {
        for uri in [
            "host=database user=reader dbname=postgres sslmode=disable",
            "host=127.0.0.1 dbname=postgres sslmode=disable",
            "host=127.0.0.1 user='' dbname=postgres sslmode=disable",
            "host=127.0.0.1 user=reader sslmode=disable",
            "host=127.0.0.1 user=reader dbname='' sslmode=disable",
            "host=127.0.0.1 user=reader dbname=postgres",
            "host=127.0.0.1 hostaddr=192.0.2.1 user=reader dbname=postgres sslmode=disable",
        ] {
            assert_eq!(
                validate_local_uri(uri),
                Err(Error::Invalid(
                    "native sidecar requires explicit local database, reader, and sslmode=disable"
                ))
            );
        }
        assert_eq!(
            validate_local_uri(&" ".repeat(MAX_CONFIG_BYTES + 1)),
            Err(Error::Invalid("native connection configuration byte limit"))
        );
        validate_local_uri("host=127.0.0.1 user=reader dbname=postgres sslmode=disable")?;
        Ok(())
    }

    #[test]
    fn repeated_failures_have_bounded_backoff_and_explicit_status() {
        let mut service = Service::new();
        for _ in 0..20 {
            assert!(service.failed(Error::ExportTimeout) <= MAX_BACKOFF);
        }
        assert_eq!(service.status.state, "retrying_export");
        assert_eq!(service.status.delivery_failures, 20);
        service.failed(Error::NativeSourceChanged);
        assert_eq!(service.status.state, "waiting_for_source");
        assert_eq!(service.status.reconnects, 1);
    }
}
