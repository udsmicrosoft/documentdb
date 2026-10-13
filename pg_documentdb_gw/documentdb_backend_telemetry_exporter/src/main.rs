/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Native-export entrypoint; credentials never enter its output.

#![forbid(unsafe_code)]

use std::{
    env,
    ffi::OsStr,
    io::{self, Write},
    process::ExitCode,
    time::Duration,
};

use documentdb_backend_telemetry_exporter::{Error, NativeCollector};
use tokio::{
    signal,
    time::{self, MissedTickBehavior},
};

mod service;

enum Mode {
    Export,
    Serve,
}

fn parse_command() -> Result<(Mode, String, String), Error> {
    let mut arguments = env::args_os().skip(1);
    let mode = match arguments.next().as_deref() {
        Some(value) if value == OsStr::new("export-native") => Mode::Export,
        Some(value) if value == OsStr::new("serve-native") => Mode::Serve,
        _ => {
            return Err(Error::Invalid(
                "usage: {export-native|serve-native} NAMESPACE CLUSTER",
            ))
        }
    };
    let namespace = arguments
        .next()
        .ok_or(Error::Invalid("deployment namespace required"))?
        .into_string()
        .map_err(|_argument| Error::Invalid("deployment namespace"))?;
    let cluster = arguments
        .next()
        .ok_or(Error::Invalid("deployment cluster required"))?
        .into_string()
        .map_err(|_argument| Error::Invalid("deployment cluster"))?;
    if arguments.next().is_some() {
        return Err(Error::Invalid("unexpected argument"));
    }
    Ok((mode, namespace, cluster))
}

fn write_report(report: &[u8]) -> Result<(), Error> {
    let mut output = io::stdout().lock();
    output.write_all(report).map_err(|_error| Error::Io)?;
    output.write_all(b"\n").map_err(|_error| Error::Io)
}

async fn export_native(namespace: String, cluster: String) -> Result<(), Error> {
    let uri = env::var("DOCUMENTDB_TELEMETRY_PG_URI")
        .map_err(|_error| Error::Invalid("DOCUMENTDB_TELEMETRY_PG_URI is required"))?;
    let endpoint = env::var("DOCUMENTDB_TELEMETRY_OTLP_ENDPOINT")
        .map_err(|_error| Error::Invalid("DOCUMENTDB_TELEMETRY_OTLP_ENDPOINT is required"))?;
    let mut collector = NativeCollector::connect(&uri, &endpoint, namespace, cluster).await?;
    let mut interval = time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut failures = 0u8;
    loop {
        interval.tick().await;
        match collector.export_next().await {
            Ok(report) => {
                failures = 0;
                write_report(&report.to_json()?)?;
            }
            Err(error @ (Error::ExportFailed | Error::ExportTimeout)) if failures < 2 => {
                failures += 1;
                eprintln!("documentdb-backend-telemetry: {error}; retrying the same bounded batch");
            }
            Err(error) => return Err(error),
        }
    }
}

async fn run() -> Result<(), Error> {
    let (mode, namespace, cluster) = parse_command()?;
    if matches!(mode, Mode::Serve) {
        return service::run(namespace, cluster).await;
    }
    tokio::select! {
        result = export_native(namespace, cluster) => result,
        result = signal::ctrl_c() => result.map_err(|_error| Error::Io),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("documentdb-backend-telemetry: {error}");
            ExitCode::FAILURE
        }
    }
}
