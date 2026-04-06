/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/telemetry/telemetry_manager.rs
 *
 *-------------------------------------------------------------------------
 */

use crate::{
    error::{DocumentDBError, Result},
    telemetry::{config::TelemetryConfig, metrics::create_metrics_provider},
};
use opentelemetry::{global, KeyValue};
use opentelemetry_sdk::{metrics::SdkMeterProvider, Resource};

/// Manages OpenTelemetry providers for telemetry signals.
///
/// Currently supports metrics. Tracing and logging will be added in follow-up PRs.
#[derive(Debug)]
pub struct TelemetryManager {
    meter_provider: Option<SdkMeterProvider>,
}

impl TelemetryManager {
    pub fn init_telemetry(config: TelemetryConfig, attributes: Vec<KeyValue>) -> Result<Self> {
        let resource = Resource::builder()
            .with_attributes(attributes)
            .build();

        let meter_provider = create_metrics_provider(config.metrics(), resource)?;

        if let Some(ref provider) = meter_provider {
            global::set_meter_provider(provider.clone());
        }

        Ok(Self { meter_provider })
    }

    pub fn shutdown(self) -> Result<()> {
        if let Some(meter_provider) = self.meter_provider {
            if let Err(e) = meter_provider.shutdown() {
                return Err(DocumentDBError::internal_error(format!(
                    "failed to shutdown meter provider: {e}"
                )));
            }
        }

        Ok(())
    }
}
