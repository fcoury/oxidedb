//! Minimal MongoDB wire protocol scaffolding.
//! - Message header parsing
//! - OP_MSG encode/decode (section 0 only)

use bson::Document;
use std::collections::HashMap;

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

/// Decode OP_MSG with support for section 0 (command) and section 1 document sequences.
/// Returns: (flags, command_doc, sequences) where sequences maps the identifier (e.g., "documents")
/// to the list of BSON documents contained in that sequence.
pub fn decode_op_msg(body: &[u8]) -> Option<(u32, Document, HashMap<String, Vec<Document>>)> {
    if body.len() < 5 { return None; }
    let flags = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
    let mut i = 4usize;

    // section 0 must be first and exactly one
    if i >= body.len() || body[i] != 0u8 { return None; }
    i += 1; // skip kind
    if i + 4 > body.len() { return None; }
    let dlen = i32::from_le_bytes([body[i], body[i+1], body[i+2], body[i+3]]) as usize;
    if i + dlen > body.len() || dlen < 5 { return None; }
    let mut cur = std::io::Cursor::new(&body[i..i+dlen]);
    let cmd = bson::Document::from_reader(&mut cur).ok()?;
    i += dlen;

    let mut seqs: HashMap<String, Vec<Document>> = HashMap::new();
    while i < body.len() {
        let kind = body[i];
        i += 1;
        match kind {
            1u8 => {
                if i + 4 > body.len() { return None; }
                let size = i32::from_le_bytes([body[i], body[i+1], body[i+2], body[i+3]]) as usize;
                let section_end = i + size; // size counts from the size field itself
                i += 4; // advance past size field
                if section_end > body.len() { return None; }
                // parse cstring identifier
                let mut j = i;
                while j < section_end && body[j] != 0 { j += 1; }
                if j >= section_end { return None; }
                let ident = std::str::from_utf8(&body[i..j]).ok()?.to_string();
                i = j + 1; // skip null
                let mut docs: Vec<Document> = Vec::new();
                while i < section_end {
                    if i + 4 > section_end { break; }
                    let dlen = i32::from_le_bytes([body[i], body[i+1], body[i+2], body[i+3]]) as usize;
                    if dlen < 5 || i + dlen > section_end { break; }
                    let mut cur = std::io::Cursor::new(&body[i..i+dlen]);
                    if let Ok(d) = bson::Document::from_reader(&mut cur) { docs.push(d); }
                    i += dlen;
                }
                seqs.insert(ident, docs);
                // ensure we are exactly at section_end
                i = section_end;
            }
            // Unknown section kinds are not supported
            _ => {
                // Stop parsing on unknown kinds to avoid desync
                break;
            }
        }
    }
    Some((flags, cmd, seqs))
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

/// Decode OP_REPLY body and return the first document if present.
/// OP_REPLY body layout:
/// - int32: responseFlags
/// - int64: cursorID
/// - int32: startingFrom
/// - int32: numberReturned
/// - BSON documents (numberReturned of them)
pub fn decode_op_reply_first_doc(body: &[u8]) -> Option<Document> {
    if body.len() < 4 + 8 + 4 + 4 {
        return None;
    }
    let mut i = 0usize;
    // responseFlags
    let _flags = u32::from_le_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
    i += 4;
    // cursorID
    let _cursor_id = i64::from_le_bytes([
        body[i], body[i + 1], body[i + 2], body[i + 3], body[i + 4], body[i + 5], body[i + 6], body[i + 7],
    ]);
    i += 8;
    // startingFrom
    let _starting_from = i32::from_le_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
    i += 4;
    // numberReturned
    let number_returned = i32::from_le_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
    i += 4;
    if number_returned <= 0 {
        return None;
    }
    let doc_bytes = &body[i..];
    let mut cur = std::io::Cursor::new(doc_bytes);
    match bson::Document::from_reader(&mut cur) {
        Ok(doc) => Some(doc),
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{doc, Bson, Document as BsonDocument};

    // Build an OP_MSG body with:
    // - flags = 0
    // - section 0: a command document
    // - section 1: a document sequence identified by `ident` containing `docs`
    fn build_op_msg_body_with_sequence(mut cmd: BsonDocument, ident: &str, docs: Vec<BsonDocument>) -> Vec<u8> {
        // Ensure placeholder (e.g., documents: []) exists in section 0 to mimic real drivers
        if !cmd.contains_key(ident) {
            cmd.insert(ident, Bson::Array(Vec::new()));
        }
        let mut body = Vec::new();
        let flags: u32 = 0;
        body.extend_from_slice(&flags.to_le_bytes());
        // section 0
        body.push(0u8);
        let cmd_bytes = bson::to_vec(&cmd).unwrap();
        body.extend_from_slice(&cmd_bytes);
        // section 1
        body.push(1u8);
        // compute section size (includes the 4 size bytes themselves)
        let ident_cstr = {
            let mut v = ident.as_bytes().to_vec();
            v.push(0u8);
            v
        };
        let mut seq_docs_flat: Vec<u8> = Vec::new();
        for d in docs {
            let b = bson::to_vec(&d).unwrap();
            seq_docs_flat.extend_from_slice(&b);
        }
        let section_size = 4 + ident_cstr.len() + seq_docs_flat.len();
        body.extend_from_slice(&(section_size as i32).to_le_bytes());
        body.extend_from_slice(&ident_cstr);
        body.extend_from_slice(&seq_docs_flat);
        body
    }

    #[test]
    fn decodes_op_msg_document_sequence_and_leaves_section0_intact() {
        let cmd = doc!{"insert": "tenants", "ordered": true, "$db": "methodiq", "documents": Bson::Array(Vec::new())};
        let d1 = doc!{"name": "hepquant", "domains": ["hepquant.dev.dxflow.io"]};
        let body = build_op_msg_body_with_sequence(cmd.clone(), "documents", vec![d1.clone()]);
        let (_flags, decoded_cmd, seqs) = decode_op_msg(&body).expect("decode");
        // Section 0 command should be equal to original (still has empty array)
        assert_eq!(decoded_cmd.get_array("documents").unwrap().len(), 0);
        // Sequence should carry the real docs
        let seq_docs = seqs.get("documents").expect("seq present");
        assert_eq!(seq_docs.len(), 1);
        assert_eq!(seq_docs[0].get_str("name").unwrap(), "hepquant");
    }

    #[test]
    fn merge_logic_appends_sequence_into_existing_array() {
        // Simulate server-side merge: append sequence docs into an existing array field
        let mut cmd = doc!{"insert": "tenants", "$db": "methodiq", "documents": Bson::Array(Vec::new())};
        let d1 = doc!{"name": "hepquant"};
        let body = build_op_msg_body_with_sequence(cmd.clone(), "documents", vec![d1.clone()]);
        let (_flags, mut decoded_cmd, seqs) = decode_op_msg(&body).expect("decode");
        // Merge (same as server): if array exists, extend; else insert
        if let Some(list) = seqs.get("documents") {
            let to_append: Vec<Bson> = list.iter().cloned().map(Bson::Document).collect();
            match decoded_cmd.get_mut("documents") {
                Some(Bson::Array(a)) => a.extend(to_append),
                _ => { decoded_cmd.insert("documents", Bson::Array(to_append)); }
            }
        }
        let arr = decoded_cmd.get_array("documents").unwrap();
        assert_eq!(arr.len(), 1);
        match &arr[0] { Bson::Document(d) => assert_eq!(d.get_str("name").unwrap(), "hepquant"), _ => panic!("expected doc") }
    }
}
