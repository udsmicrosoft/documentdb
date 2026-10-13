/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Provider-neutral access to sampled execution context, without choosing a carrier.

use std::{fmt::Debug, sync::OnceLock};

/// Maximum supported W3C trace-state length in bytes.
pub const MAX_TRACESTATE_BYTES: usize = 512;

/// Validated, sampled W3C context for the currently executing span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTraceContext {
    traceparent: String,
    tracestate: Option<String>,
}

impl ActiveTraceContext {
    /// Creates context in the version-00 format, or returns `None` for unsupported context.
    #[must_use]
    pub fn new(traceparent: String, tracestate: Option<String>) -> Option<Self> {
        if !valid_sampled_traceparent(&traceparent)
            || tracestate
                .as_deref()
                .is_some_and(|state| !valid_tracestate(state))
        {
            return None;
        }
        Some(Self {
            traceparent,
            tracestate: tracestate.filter(|state| !state.is_empty()),
        })
    }

    /// Returns the trace and immediate parent identifiers in W3C format.
    #[must_use]
    pub fn traceparent(&self) -> &str {
        &self.traceparent
    }

    /// Returns the optional W3C vendor state.
    #[must_use]
    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }
}

/// Adapter for reading the active span without interpreting client command data.
pub trait ActiveTraceContextHook: Send + Sync + Debug {
    /// Returns only valid, sampled context, or `None` when propagation is unavailable.
    fn current_context(&self) -> Option<ActiveTraceContext>;
}

static ACTIVE_TRACE_CONTEXT_HOOK: OnceLock<&'static dyn ActiveTraceContextHook> = OnceLock::new();

/// Installs the process-wide active-context accessor.
///
/// Returns `false` when an accessor has already been installed.
#[must_use]
pub fn install_active_trace_context_hook(hook: &'static dyn ActiveTraceContextHook) -> bool {
    ACTIVE_TRACE_CONTEXT_HOOK.set(hook).is_ok()
}

pub(crate) fn current_context() -> Option<ActiveTraceContext> {
    ACTIVE_TRACE_CONTEXT_HOOK
        .get()
        .and_then(|hook| hook.current_context())
}

fn valid_sampled_traceparent(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 55
        && bytes.starts_with(b"00-")
        && bytes.get(35) == Some(&b'-')
        && bytes.get(52) == Some(&b'-')
        && bytes.iter().enumerate().all(|(index, byte)| {
            matches!(index, 2 | 35 | 52) || matches!(byte, b'0'..=b'9' | b'a'..=b'f')
        })
        && bytes
            .get(3..35)
            .is_some_and(|id| id.iter().any(|byte| *byte != b'0'))
        && bytes
            .get(36..52)
            .is_some_and(|id| id.iter().any(|byte| *byte != b'0'))
        && bytes.last().is_some_and(|byte| {
            matches!(byte, b'1' | b'3' | b'5' | b'7' | b'9' | b'b' | b'd' | b'f')
        })
}

// W3C Trace Context 1, section 3.3.1.3. The private carrier accepts at most 512 bytes.
fn valid_tracestate(state: &str) -> bool {
    if state.len() > MAX_TRACESTATE_BYTES {
        return false;
    }
    if state.is_empty() {
        return true;
    }
    let mut keys = [""; 32];
    for (index, member) in state.split(',').enumerate() {
        if index >= keys.len() {
            return false;
        }
        let Some((key, value)) = member.trim_matches([' ', '\t']).split_once('=') else {
            return false;
        };
        if !valid_tracestate_key(key)
            || value.is_empty()
            || value.len() > 256
            || !value
                .bytes()
                .all(|byte| matches!(byte, b' '..=b'~') && byte != b'=')
            || keys.contains(&key)
        {
            return false;
        }
        if let Some(slot) = keys.get_mut(index) {
            *slot = key;
        }
    }
    true
}

fn valid_tracestate_key(key: &str) -> bool {
    let valid_chars = |value: &str| {
        value
            .bytes()
            .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'*' | b'/'))
    };
    if let Some((tenant, system)) = key.split_once('@') {
        !tenant.is_empty()
            && tenant.len() <= 241
            && tenant.starts_with(|ch: char| ch.is_ascii_lowercase() || ch.is_ascii_digit())
            && !system.is_empty()
            && system.len() <= 14
            && system.starts_with(|ch: char| ch.is_ascii_lowercase())
            && valid_chars(tenant)
            && valid_chars(system)
    } else {
        !key.is_empty()
            && key.len() <= 256
            && key.starts_with(|ch: char| ch.is_ascii_lowercase())
            && valid_chars(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::telemetry::trace_context::ActiveTraceContext;

    const PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    #[test]
    fn validates_sampled_context_and_optional_state() {
        for state in [
            None,
            Some(""),
            Some("vendor=value"),
            Some("1tenant@vendor=value,other=two"),
        ] {
            let context = ActiveTraceContext::new(PARENT.to_owned(), state.map(str::to_owned))
                .expect("valid context");
            assert_eq!(context.traceparent(), PARENT);
            assert_eq!(
                context.tracestate(),
                state.filter(|state| !state.is_empty())
            );
        }
    }

    #[test]
    fn rejects_invalid_or_unsampled_context() {
        for parent in [
            PARENT.replace("-01", "-00"),
            PARENT.replacen("00-", "01-", 1),
            PARENT.to_uppercase(),
            PARENT.replace(
                "0af7651916cd43dd8448eb211c80319c",
                "00000000000000000000000000000000",
            ),
            PARENT.replace("b7ad6b7169203331", "0000000000000000"),
            format!("{PARENT}-extra"),
            "short".to_owned(),
        ] {
            assert!(ActiveTraceContext::new(parent, None).is_none());
        }
        for state in [
            "invalid".to_owned(),
            "a=1,a=2".to_owned(),
            "A=1".to_owned(),
            "a=1,".to_owned(),
            "a=1=2".to_owned(),
            "a=\n".to_owned(),
            format!("a={}", "x".repeat(257)),
            (0..33)
                .map(|i| format!("a{i}=1"))
                .collect::<Vec<_>>()
                .join(","),
            format!("a={},b={},c=x", "x".repeat(253), "x".repeat(253)),
        ] {
            assert!(ActiveTraceContext::new(PARENT.to_owned(), Some(state)).is_none());
        }
    }
}
