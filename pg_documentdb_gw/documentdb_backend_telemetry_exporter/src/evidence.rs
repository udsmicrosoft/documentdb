/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Native plan summaries preserve explicit scan and index-name coverage.

use opentelemetry::{Array, KeyValue, Value};
use serde::{Deserialize, Serialize};

use crate::{identity, Error};

const MAX_PLAN_INDEXES: usize = 8;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ScanKind {
    Index,
    Sequential,
    Mixed,
    #[default]
    Unknown,
}

impl ScanKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Index => "index",
            Self::Sequential => "sequential",
            Self::Mixed => "mixed",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent serialized flags distinguish plan-walk and index-name coverage."
)]
pub struct PlanSummary {
    scan_kind: ScanKind,
    used_index_name_ids: Vec<String>,
    #[serde(default = "complete_index_names")]
    index_names_complete: bool,
    fingerprint: Option<String>,
    complete: bool,
    unsupported_nodes: bool,
    truncated: bool,
}

impl PlanSummary {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.used_index_name_ids.len() > MAX_PLAN_INDEXES
            || (self.complete
                && (self.unsupported_nodes
                    || self.truncated
                    || self.scan_kind == ScanKind::Unknown))
            || (!self.complete
                && (self.scan_kind != ScanKind::Unknown || self.fingerprint.is_some()))
            || (matches!(self.scan_kind, ScanKind::Index | ScanKind::Mixed)
                && self.index_names_complete
                && self.used_index_name_ids.is_empty())
            || (self.scan_kind == ScanKind::Sequential && !self.used_index_name_ids.is_empty())
            || (!self.index_names_complete && self.fingerprint.is_some())
        {
            return Err(Error::Invalid("plan coverage"));
        }
        for id in &self.used_index_name_ids {
            identity::validate_hex(id, 32)?;
        }
        if let Some(fingerprint) = &self.fingerprint {
            identity::validate_hex(fingerprint, 32)?;
        }
        Ok(())
    }

    pub(crate) fn append_attributes(&self, attributes: &mut Vec<KeyValue>) {
        attributes.extend([
            KeyValue::new("documentdb.plan.scan_kind", self.scan_kind.as_str()),
            KeyValue::new("documentdb.plan.complete", self.complete),
            KeyValue::new(
                "documentdb.plan.index_names_complete",
                self.index_names_complete,
            ),
            KeyValue::new("documentdb.plan.unsupported_nodes", self.unsupported_nodes),
            KeyValue::new("documentdb.plan.truncated", self.truncated),
            KeyValue::new(
                "documentdb.plan.used_index_name_ids",
                Value::Array(Array::String(
                    self.used_index_name_ids
                        .iter()
                        .cloned()
                        .map(Into::into)
                        .collect(),
                )),
            ),
        ]);
        if let Some(fingerprint) = &self.fingerprint {
            attributes.push(KeyValue::new(
                "documentdb.plan.fingerprint",
                fingerprint.clone(),
            ));
        }
    }
}

const fn complete_index_names() -> bool {
    true
}
