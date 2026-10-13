/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Bounded, out-of-process export of completed native backend telemetry.

#![forbid(unsafe_code)]

mod error;
mod evidence;
mod identity;
mod native;
mod otlp;
mod record;
#[cfg(test)]
mod tests;

#[doc(inline)]
pub use crate::{
    error::Error,
    native::{NativeCheckpoint, NativeCollectionReport, NativeCollector, NativeResumeReport},
    otlp::{ExportScope, OtlpSink},
    record::NativeSpan,
};

/// Independently versioned native backend span protocol.
pub const SCHEMA_VERSION: u32 = 1;
/// Maximum serialized native record size, including its envelope.
pub const MAX_RECORD_BYTES: usize = 2 * 1024;
/// Maximum number of native records in one read or export.
pub const MAX_BATCH_RECORDS: usize = 128;
/// Maximum serialized native batch size.
pub const MAX_BATCH_BYTES: usize = 256 * 1024;
/// Maximum native connection configuration or capability response size.
pub const MAX_CONFIG_BYTES: usize = 16 * 1024;
/// Per-operation deadline for native telemetry SQL reads and OTLP export.
pub const OPERATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
