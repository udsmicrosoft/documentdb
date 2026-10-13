/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Validation of producer identities and explicit deployment scope labels.

use crate::Error;

pub fn validate_hex(value: &str, digits: usize) -> Result<(), Error> {
    if value.len() != digits
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || value.bytes().all(|byte| byte == b'0')
    {
        return Err(Error::Invalid("nonzero lowercase hexadecimal identity"));
    }
    Ok(())
}

pub fn validate_label(value: &str) -> Result<(), Error> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
    {
        return Err(Error::Invalid("deployment scope label"));
    }
    Ok(())
}
