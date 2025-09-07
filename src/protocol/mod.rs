//! Minimal MongoDB wire protocol scaffolding.
//! Currently defines message header parsing and OP codes used by the server.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageHeader {
    pub message_length: i32,
    pub request_id: i32,
    pub response_to: i32,
    pub op_code: i32,
}

impl MessageHeader {
    pub fn parse(buf: &[u8]) -> Option<(Self, usize)> {
        if buf.len() < 16 {
            return None;
        }
        // Little-endian 32-bit fields
        let message_length = i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let request_id = i32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let response_to = i32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let op_code = i32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);

        Some((
            Self {
                message_length,
                request_id,
                response_to,
                op_code,
            },
            16,
        ))
    }
}

// Relevant op codes
pub const OP_MSG: i32 = 2013;
pub const OP_COMPRESSED: i32 = 2012;
// Legacy ops that may appear during handshake with older drivers
pub const OP_QUERY: i32 = 2004;
pub const OP_REPLY: i32 = 1;

