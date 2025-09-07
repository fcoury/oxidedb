//! Minimal MongoDB wire protocol scaffolding.
//! - Message header parsing
//! - OP_MSG encode/decode (section 0 only)

use bson::Document;

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

#[derive(Debug)]
pub enum OpMsgBody<'a> {
    Section0(&'a [u8], Document),
}

/// Decode OP_MSG section-0 document from the provided body bytes.
/// Returns the parsed Document and the number of bytes consumed (entire body length expected).
pub fn decode_op_msg_section0(body: &[u8]) -> Option<(u32, Document)> {
    if body.len() < 5 {
        return None;
    }
    // flags
    let flags = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
    // section kind
    if body[4] != 0u8 {
        return None; // only support section 0
    }
    // the rest should be a BSON document
    let doc_bytes = &body[5..];
    match bson::Document::from_reader(&mut std::io::Cursor::new(doc_bytes)) {
        Ok(doc) => Some((flags, doc)),
        Err(_) => None,
    }
}

/// Encode an OP_MSG with section 0 containing a single BSON document.
/// Returns a Vec with the full wire message including the message header.
pub fn encode_op_msg(doc: &Document, response_to: i32, request_id: i32) -> Vec<u8> {
    let doc_bytes = bson::to_vec(doc).expect("bson encode");
    let flags: u32 = 0;
    let body_len = 4 /*flags*/ + 1 /*kind*/ + doc_bytes.len();
    let message_length = 16 + body_len as i32;

    let mut out = Vec::with_capacity(message_length as usize);
    out.extend_from_slice(&message_length.to_le_bytes());
    out.extend_from_slice(&request_id.to_le_bytes());
    out.extend_from_slice(&response_to.to_le_bytes());
    out.extend_from_slice(&OP_MSG.to_le_bytes());

    out.extend_from_slice(&flags.to_le_bytes());
    out.push(0u8); // section 0
    out.extend_from_slice(&doc_bytes);
    out
}

/// Decode OP_QUERY body into (flags, fullCollectionName, numberToSkip, numberToReturn, queryDoc).
pub fn decode_op_query(body: &[u8]) -> Option<(u32, String, i32, i32, Document)> {
    if body.len() < 4 {
        return None;
    }
    let flags = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
    // parse cstring starting at offset 4
    let mut i = 4;
    let mut end = i;
    while end < body.len() && body[end] != 0 {
        end += 1;
    }
    if end >= body.len() {
        return None;
    }
    let full_collection_name = match std::str::from_utf8(&body[i..end]) {
        Ok(s) => s.to_string(),
        Err(_) => return None,
    };
    i = end + 1; // skip null terminator
    if i + 8 > body.len() {
        return None;
    }
    let number_to_skip = i32::from_le_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
    i += 4;
    let number_to_return = i32::from_le_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
    i += 4;
    if i >= body.len() {
        return None;
    }
    let query_bytes = &body[i..];
    let doc = match bson::Document::from_reader(&mut std::io::Cursor::new(query_bytes)) {
        Ok(d) => d,
        Err(_) => return None,
    };
    Some((flags, full_collection_name, number_to_skip, number_to_return, doc))
}

/// Encode OP_REPLY with the provided documents.
pub fn encode_op_reply(docs: &[Document], response_to: i32, request_id: i32) -> Vec<u8> {
    let response_flags: u32 = 0;
    let cursor_id: i64 = 0; // no cursor for command replies
    let starting_from: i32 = 0;
    let number_returned: i32 = docs.len() as i32;

    // Serialize docs to a contiguous buffer
    let mut docs_buf = Vec::new();
    for d in docs {
        let b = bson::to_vec(d).expect("bson encode");
        docs_buf.extend_from_slice(&b);
    }

    let body_len = 4 + 8 + 4 + 4 + docs_buf.len();
    let message_length = 16 + body_len as i32;

    let mut out = Vec::with_capacity(message_length as usize);
    out.extend_from_slice(&message_length.to_le_bytes());
    out.extend_from_slice(&request_id.to_le_bytes());
    out.extend_from_slice(&response_to.to_le_bytes());
    out.extend_from_slice(&OP_REPLY.to_le_bytes());

    out.extend_from_slice(&response_flags.to_le_bytes());
    out.extend_from_slice(&cursor_id.to_le_bytes());
    out.extend_from_slice(&starting_from.to_le_bytes());
    out.extend_from_slice(&number_returned.to_le_bytes());
    out.extend_from_slice(&docs_buf);
    out
}
