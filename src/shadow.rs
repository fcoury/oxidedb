use crate::config::{ShadowCompareOptions, ShadowConfig, ShadowMode};
use crate::protocol::{decode_op_msg_section0, decode_op_query, decode_op_reply_first_doc, encode_op_msg, MessageHeader, OP_MSG, OP_QUERY};
use anyhow::{anyhow, Context, Result};
use bson::{doc, Bson, Document};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{timeout, Duration, Instant};

#[derive(Clone)]
pub struct ShadowSession {
    pub cfg: Arc<ShadowConfig>,
    stream: Arc<Mutex<Option<TcpStream>>>,
}

impl ShadowSession {
    pub fn new(cfg: Arc<ShadowConfig>) -> Self {
        Self { cfg, stream: Arc::new(Mutex::new(None)) }
    }

    async fn ensure_connected(&self) -> Result<()> {
        let mut guard = self.stream.lock().await;
        if guard.is_some() {
            return Ok(());
        }
        let dur = Duration::from_millis(self.cfg.timeout_ms);
        let addr = self.cfg.addr.clone();
        let stream = timeout(dur, TcpStream::connect(&addr))
            .await
            .context("shadow connect timeout")??;
        *guard = Some(stream);
        Ok(())
    }

    /// Forward the original wire message (header + body) to upstream, optionally rewriting db prefix, then read one reply.
    /// Returns (op_code, reply_body_bytes).
    async fn forward_raw(&self, hdr: &MessageHeader, header_bytes: &[u8], body: &[u8]) -> Result<(i32, Vec<u8>)> {
        // Prepare outbound bytes: either rewrite (if db_prefix set) or forward exact bytes.
        let mut out: Vec<u8> = Vec::new();
        if let Some(prefix) = &self.cfg.db_prefix {
            match hdr.op_code {
                OP_MSG => {
                    if let Some((_flags, mut doc)) = decode_op_msg_section0(body) {
                        if let Ok(db) = doc.get_str("$db") {
                            let new_db = format!("{}_{}", prefix, db);
                            doc.insert("$db", new_db);
                        }
                        // Build a new OP_MSG request. For requests, response_to must be 0.
                        let new_wire = encode_op_msg(&doc, 0, hdr.request_id);
                        out.extend_from_slice(&new_wire);
                    } else {
                        // Fallback: forward as-is if decoding fails
                        out.extend_from_slice(header_bytes);
                        out.extend_from_slice(body);
                    }
                }
                OP_QUERY => {
                    // Re-encode OP_QUERY with rewritten fullCollectionName
                    let rewritten = rewrite_op_query_db_prefix(body, prefix);
                    let msg_len = 16 + rewritten.as_ref().map(|v| v.len()).unwrap_or(body.len()) as i32;
                    out.extend_from_slice(&msg_len.to_le_bytes());
                    out.extend_from_slice(&hdr.request_id.to_le_bytes());
                    out.extend_from_slice(&0i32.to_le_bytes()); // response_to=0 for request
                    out.extend_from_slice(&OP_QUERY.to_le_bytes());
                    match rewritten {
                        Some(v) => out.extend_from_slice(&v),
                        None => out.extend_from_slice(body),
                    }
                }
                _ => {
                    // Unknown or unsupported; forward raw
                    out.extend_from_slice(header_bytes);
                    out.extend_from_slice(body);
                }
            }
        } else {
            // Forward bytes unmodified
            out.extend_from_slice(header_bytes);
            out.extend_from_slice(body);
        }

        self.ensure_connected().await?;
        let dur = Duration::from_millis(self.cfg.timeout_ms);

        // Serialize access to the stream per session
        let mut guard = self.stream.lock().await;
        let stream = guard.as_mut().unwrap();

        // Send
        timeout(dur, stream.write_all(&out)).await.context("shadow send timeout")??;
        timeout(dur, stream.flush()).await.context("shadow flush timeout")??;

        // Read reply header
        let mut header_buf = [0u8; 16];
        if let Err(e) = timeout(dur, stream.read_exact(&mut header_buf)).await.context("shadow recv header timeout")? {
            // Connection likely broken; drop it so we reconnect on next try
            *guard = None;
            return Err(e.into());
        }
        let (reply_hdr, _) = MessageHeader::parse(&header_buf).ok_or_else(|| anyhow!("invalid reply header"))?;
        if reply_hdr.message_length < 16 { return Err(anyhow!("invalid reply length")); }
        let body_len = (reply_hdr.message_length as usize).saturating_sub(16);
        let mut buf = vec![0u8; body_len];
        if body_len > 0 {
            if let Err(e) = timeout(dur, stream.read_exact(&mut buf)).await.context("shadow recv body timeout")? {
                *guard = None;
                return Err(e.into());
            }
        }
        Ok((reply_hdr.op_code, buf))
    }

    /// Forward request and return upstream's first doc (from OP_MSG section-0 or OP_REPLY's first doc).
    pub async fn forward_and_read_doc(&self, hdr: &MessageHeader, header_bytes: &[u8], body: &[u8]) -> Result<Option<Document>> {
        let (op, buf) = self.forward_raw(hdr, header_bytes, body).await?;
        match op {
            OP_MSG => decode_op_msg_section0(&buf).map(|(_f, d)| Some(d)).ok_or_else(|| anyhow!("malformed upstream OP_MSG")),
            crate::protocol::OP_REPLY => Ok(decode_op_reply_first_doc(&buf)),
            _ => Ok(None),
        }
    }
}

fn rewrite_op_query_db_prefix(body: &[u8], prefix: &str) -> Option<Vec<u8>> {
    // Body layout: flags (4), cstring fullCollectionName, numberToSkip (4), numberToReturn (4), queryDoc (bson), [optional returnFieldsSelector]
    if body.len() < 4 { return None; }
    let flags = &body[0..4];
    // find cstring
    let mut i = 4usize;
    while i < body.len() && body[i] != 0 { i += 1; }
    if i >= body.len() { return None; }
    let end = i; // points to null byte
    let fqn = std::str::from_utf8(&body[4..end]).ok()?;
    let mut parts = fqn.splitn(2, '.');
    let db = parts.next().unwrap_or("");
    let rest = parts.next().unwrap_or("");
    let new_db = format!("{}_{}", prefix, db);
    let new_fqn = format!("{}.{}", new_db, rest);
    let mut out: Vec<u8> = Vec::with_capacity(body.len() + new_db.len());
    out.extend_from_slice(flags);
    out.extend_from_slice(new_fqn.as_bytes());
    out.push(0u8);
    // Copy remainder
    let mut j = end + 1; // skip null
    out.extend_from_slice(&body[j..]);
    Some(out)
}

pub struct DiffResult {
    pub matched: bool,
    pub summary: String,
    pub details: Option<String>,
}

pub fn compare_docs(ours: &Document, theirs: &Document, opts: &ShadowCompareOptions) -> DiffResult {
    // Apply ignores (top-level only for initial pass)
    let mut ours_n = ours.clone();
    let mut theirs_n = theirs.clone();
    for k in &opts.ignore_fields {
        ours_n.remove(k);
        theirs_n.remove(k);
    }

    let mut diffs: Vec<String> = Vec::new();
    // compare keys union
    let mut keys: Vec<String> = ours_n.keys().map(|s| s.to_string()).collect();
    for k in theirs_n.keys() { if !keys.iter().any(|x| x == k) { keys.push(k.to_string()); } }
    keys.sort(); keys.dedup();
    for k in keys {
        let a = ours_n.get(&k);
        let b = theirs_n.get(&k);
        match (a, b) {
            (None, Some(_)) => diffs.push(format!("missing key in ours: {}", k)),
            (Some(_), None) => diffs.push(format!("extra key in ours: {}", k)),
            (Some(av), Some(bv)) => {
                if !bson_equal(av, bv, opts) {
                    diffs.push(format!("{}. ours={:?} theirs={:?}", k, av, bv));
                }
            }
            _ => {}
        }
    }
    let matched = diffs.is_empty();
    let summary = if matched { "match".to_string() } else { format!("{} diffs", diffs.len()) };
    let details = if matched { None } else { Some(diffs.join("; ")) };
    DiffResult { matched, summary, details }
}

fn bson_equal(a: &Bson, b: &Bson, opts: &ShadowCompareOptions) -> bool {
    match (a, b) {
        (Bson::Document(da), Bson::Document(db)) => compare_docs(da, db, opts).matched,
        (Bson::Array(aa), Bson::Array(ab)) => {
            if aa.len() != ab.len() { return false; }
            for (ea, eb) in aa.iter().zip(ab.iter()) {
                if !bson_equal(ea, eb, opts) { return false; }
            }
            true
        }
        // Numeric equivalence disabled by default per requirements.
        _ => a == b,
    }
}
