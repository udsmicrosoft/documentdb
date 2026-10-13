/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 * SPDX-License-Identifier: MIT
 *
 * src/telemetry/config.rs
 *
 * Shared telemetry configuration types and helpers.
 * Follows SetupConfiguration pattern: accessor methods resolve JSON > env > default.
 *
 *-------------------------------------------------------------------------
 */

/// Provider-neutral telemetry capabilities resolved by the active adapter.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TelemetrySettings {
    request_metrics_enabled: bool,
    udf_trace_context_enabled: bool,
}

impl TelemetrySettings {
    #[must_use]
    pub const fn new(request_metrics_enabled: bool) -> Self {
        Self {
            request_metrics_enabled,
            udf_trace_context_enabled: false,
        }
    }

    #[must_use]
    pub const fn request_metrics_enabled(self) -> bool {
        self.request_metrics_enabled
    }

    /// Selects opt-in command-BSON trace propagation independently of request metrics.
    #[must_use]
    pub const fn with_udf_trace_context(mut self, enabled: bool) -> Self {
        self.udf_trace_context_enabled = enabled;
        self
    }

    /// Returns whether the adapter selected command-BSON propagation.
    #[must_use]
    pub const fn udf_trace_context_enabled(self) -> bool {
        self.udf_trace_context_enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_expose_adapter_resolved_capabilities() {
        let settings = TelemetrySettings::new(true);
        assert!(settings.request_metrics_enabled());
    }
}
