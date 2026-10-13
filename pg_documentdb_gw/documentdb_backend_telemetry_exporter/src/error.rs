/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Safe error categories never retain driver errors, statements, or credentials.

use std::{error, fmt};

/// A bounded, non-sensitive error classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// A configured value or record violates the named protocol invariant.
    Invalid(&'static str),
    /// A stream could not be read or written.
    Io,
    /// The server denied a telemetry read.
    PermissionDenied,
    /// OTLP delivery exceeded its deadline.
    ExportTimeout,
    /// OTLP construction or delivery failed.
    ExportFailed,
    /// A native SQL operation failed.
    NativeFailed,
    /// A native SQL operation exceeded its deadline.
    NativeTimeout,
    /// The deployed API, recording configuration, or storage is unavailable.
    NativeUnavailable,
    /// The native producer or epoch changed and requires rediscovery.
    NativeSourceChanged,
    /// The requested native history has been overwritten.
    NativeGap,
    /// A native record's timestamps are inconsistent or cannot be represented.
    Clock,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(invariant) => write!(f, "invalid telemetry input: {invariant}"),
            Self::Io => f.write_str("telemetry input/output failed"),
            Self::PermissionDenied => f.write_str("telemetry read was denied"),
            Self::ExportTimeout => f.write_str("OTLP export timed out"),
            Self::ExportFailed => f.write_str("OTLP export failed"),
            Self::NativeFailed => f.write_str("native telemetry SQL operation failed"),
            Self::NativeTimeout => f.write_str("native telemetry SQL operation timed out"),
            Self::NativeUnavailable => {
                f.write_str("native telemetry API or recording is unavailable")
            }
            Self::NativeSourceChanged => {
                f.write_str("native telemetry source changed; restart collection")
            }
            Self::NativeGap => {
                f.write_str("native telemetry history was overwritten; collection stopped")
            }
            Self::Clock => f.write_str("invalid telemetry timestamp or clock reversal"),
        }
    }
}

impl error::Error for Error {}
