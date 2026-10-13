/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Native collection commands do not read diagnostic files or echo arguments.

use std::{error::Error, process::Command};

#[test]
fn cli_accepts_only_native_export_and_reports_safe_errors() -> Result<(), Box<dyn Error>> {
    let cases: &[(&[&str], &str)] = &[
        (&[], "usage: {export-native|serve-native} NAMESPACE CLUSTER"),
        (
            &["probe", "private-config", "private-key"],
            "usage: {export-native|serve-native} NAMESPACE CLUSTER",
        ),
        (&["export-native"], "deployment namespace required"),
        (&["export-native", "local"], "deployment cluster required"),
        (
            &["export-native", "local", "fixture", "private-argument"],
            "unexpected argument",
        ),
        (
            &["export-native", "local", "fixture"],
            "DOCUMENTDB_TELEMETRY_PG_URI is required",
        ),
        (
            &["serve-native", "local", "fixture"],
            "DOCUMENTDB_TELEMETRY_PG_URI is required",
        ),
    ];
    for (arguments, message) in cases {
        let output = Command::new(env!("CARGO_BIN_EXE_documentdb_backend_telemetry_exporter"))
            .args(*arguments)
            .env_remove("DOCUMENTDB_TELEMETRY_PG_URI")
            .env_remove("DOCUMENTDB_TELEMETRY_OTLP_ENDPOINT")
            .output()?;
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert_eq!(
            String::from_utf8(output.stderr)?,
            format!("documentdb-backend-telemetry: invalid telemetry input: {message}\n")
        );
    }

    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn sidecar_reports_unavailable_source_and_stops_on_sigterm() -> Result<(), Box<dyn Error>> {
    use std::{net::TcpListener, process::Stdio, time::Duration};

    use serde_json::Value;
    use tokio::{
        io::{AsyncBufReadExt, BufReader},
        process::Command as AsyncCommand,
        time,
    };

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    let mut child = AsyncCommand::new(env!("CARGO_BIN_EXE_documentdb_backend_telemetry_exporter"))
            .args(["serve-native", "local", "native-demo"])
            .env("DOCUMENTDB_TELEMETRY_PG_URI",
                format!("host=127.0.0.1 port={port} user=reader password=not-for-output dbname=postgres sslmode=disable"))
            .env("DOCUMENTDB_TELEMETRY_OTLP_ENDPOINT", "http://127.0.0.1:4317")
            .stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true).spawn()?;
    let stdout = child.stdout.take().ok_or("missing stdout")?;
    let mut lines = BufReader::new(stdout).lines();
    for state in ["starting", "waiting_for_source"] {
        let line = time::timeout(Duration::from_secs(5), lines.next_line())
            .await??
            .ok_or("missing status")?;
        let status: Value = serde_json::from_str(&line)?;
        assert_eq!(status["state"], state);
        assert!(!line.contains("not-for-output"));
    }
    let pid = child.id().ok_or("sidecar exited")?;
    let signal = AsyncCommand::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .await?;
    assert_eq!(signal.code(), Some(0));
    let status = time::timeout(Duration::from_secs(5), child.wait()).await??;
    assert_eq!(status.code(), Some(0));
    let stopped: Value =
        serde_json::from_str(&lines.next_line().await?.ok_or("missing stopped status")?)?;
    assert_eq!(stopped["state"], "stopped");
    Ok(())
}
