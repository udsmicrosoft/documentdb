/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * documentdb_gateway_core/src/protocol/opcode.rs
 *
 *-------------------------------------------------------------------------
 */

/// Wire Protocol `OpCode`s
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum OpCode {
    Invalid = 0,
    #[deprecated(note = "OP_REPLY Deprecated")]
    Reply = 1,
    #[deprecated(note = "OP_UPDATE Deprecated")]
    Update = 2001,
    #[deprecated(note = "OP_INSERT Deprecated")]
    Insert = 2002,
    Reserved = 2003,
    #[deprecated(note = "OP_QUERY Deprecated")]
    Query = 2004,
    #[deprecated(note = "OP_GET_MORE Deprecated")]
    GetMore = 2005,
    #[deprecated(note = "OP_DELETE Deprecated")]
    Delete = 2006,
    #[deprecated(note = "OP_KILL_CURSORS Deprecated")]
    KillCursors = 2007,
    Command = 2010,
    CommandReply = 2011,
    Compressed = 2012,
    Msg = 2013,
}

impl OpCode {
    #[expect(
        deprecated,
        reason = "We still need to support parsing legacy opcodes from the wire, even if they're deprecated."
    )]
    #[must_use]
    pub const fn from_value(code: i32) -> Self {
        match code {
            1 => Self::Reply,
            2001 => Self::Update,
            2002 => Self::Insert,
            2003 => Self::Reserved,
            2004 => Self::Query,
            2005 => Self::GetMore,
            2006 => Self::Delete,
            2007 => Self::KillCursors,
            2010 => Self::Command,
            2011 => Self::CommandReply,
            2012 => Self::Compressed,
            2013 => Self::Msg,
            _ => Self::Invalid,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[expect(deprecated, reason = "legacy opcodes remain part of wire decoding")]
    fn known_values_decode_to_expected_opcodes() {
        let cases = [
            (1, OpCode::Reply),
            (2001, OpCode::Update),
            (2002, OpCode::Insert),
            (2003, OpCode::Reserved),
            (2004, OpCode::Query),
            (2005, OpCode::GetMore),
            (2006, OpCode::Delete),
            (2007, OpCode::KillCursors),
            (2010, OpCode::Command),
            (2011, OpCode::CommandReply),
            (2012, OpCode::Compressed),
            (2013, OpCode::Msg),
        ];

        for (value, expected) in cases {
            assert_eq!(OpCode::from_value(value), expected);
        }
    }

    #[test]
    fn unknown_values_decode_as_invalid() {
        for value in [i32::MIN, -1, 0, 2, 2000, 2008, i32::MAX] {
            assert_eq!(OpCode::from_value(value), OpCode::Invalid);
        }
    }
}
