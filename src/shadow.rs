use crate::config::{ShadowCompareOptions, ShadowConfig};
use crate::namespace::rewrite_command_doc;
use crate::protocol::{
    MessageHeader, OP_COMPRESSED, OP_MSG, OP_QUERY, OpCompressed, decode_op_compressed_reply,
    decode_op_msg_section0, decode_op_reply_first_doc, encode_op_compressed, encode_op_msg,
};
use crate::scram::ScramAuth;
use anyhow::{Context, Result, anyhow};
use bson::{Bson, Document};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};

#[derive(Clone)]
pub struct ShadowSession {
    pub cfg: Arc<ShadowConfig>,
    stream: Arc<Mutex<Option<TcpStream>>>,
}

impl ShadowSession {
    pub fn new(cfg: Arc<ShadowConfig>) -> Self {
        Self {
            cfg,
            stream: Arc::new(Mutex::new(None)),
        }
    }

    async fn ensure_connected(&self) -> Result<()> {
        let mut guard = self.stream.lock().await;
        if guard.is_some() {
            return Ok(());
        }
        let dur = Duration::from_millis(self.cfg.timeout_ms);
        let addr = self.cfg.addr.clone();
        let mut stream = timeout(dur, TcpStream::connect(&addr))
            .await
            .context("shadow connect timeout")??;

        // Perform SCRAM-SHA-256 authentication if credentials are provided
        if let (Some(username), Some(password)) = (&self.cfg.username, &self.cfg.password) {
            let auth_db = &self.cfg.auth_db;
            tracing::info!(username = %username, auth_db = %auth_db, "performing SCRAM-SHA-256 authentication");
            let mut auth = ScramAuth::new(username.clone(), password.clone(), auth_db.clone());
            auth.authenticate(&mut stream, self.cfg.timeout_ms)
                .await
                .context("SCRAM-SHA-256 authentication failed")?;
            tracing::info!("SCRAM-SHA-256 authentication successful");
        }

        *guard = Some(stream);
        Ok(())
    }

    /// Forward the original wire message (header + body) to upstream, optionally rewriting db prefix, then read one reply.
    /// Returns (op_code, reply_body_bytes).
    async fn forward_raw(
        &self,
        hdr: &MessageHeader,
        header_bytes: &[u8],
        body: &[u8],
    ) -> Result<(i32, Vec<u8>)> {
        // Prepare outbound bytes: either rewrite (if db_prefix set) or forward exact bytes.
        let mut out: Vec<u8> = Vec::new();
        if let Some(prefix) = &self.cfg.db_prefix {
            match hdr.op_code {
                OP_MSG => {
                    // Use comprehensive namespace rewriting
                    if let Some((_flags, doc)) = decode_op_msg_section0(body) {
                        let new_doc = rewrite_command_doc(&doc, prefix);
                        // Build a new OP_MSG request. For requests, response_to must be 0.
                        let new_wire = encode_op_msg(&new_doc, 0, hdr.request_id);
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
                    let msg_len =
                        16 + rewritten.as_ref().map(|v| v.len()).unwrap_or(body.len()) as i32;
                    out.extend_from_slice(&msg_len.to_le_bytes());
                    out.extend_from_slice(&hdr.request_id.to_le_bytes());
                    out.extend_from_slice(&0i32.to_le_bytes()); // response_to=0 for request
                    out.extend_from_slice(&OP_QUERY.to_le_bytes());
                    match rewritten {
                        Some(v) => out.extend_from_slice(&v),
                        None => out.extend_from_slice(body),
                    }
                }
                OP_COMPRESSED => {
                    // Handle OP_COMPRESSED: decompress, rewrite, recompress
                    if let Some(op) = OpCompressed::parse(body) {
                        if let Some(uncompressed) = crate::protocol::decompress_op_compressed(&op) {
                            // Rewrite based on original opcode
                            let rewritten_uncompressed = match op.original_opcode {
                                OP_MSG => {
                                    if let Some((_flags, doc)) =
                                        decode_op_msg_section0(&uncompressed)
                                    {
                                        let new_doc = rewrite_command_doc(&doc, prefix);
                                        let new_body = encode_op_msg(&new_doc, 0, hdr.request_id);
                                        // Extract just the body (skip header)
                                        new_body[16..].to_vec()
                                    } else {
                                        uncompressed.clone()
                                    }
                                }
                                OP_QUERY => rewrite_op_query_db_prefix(&uncompressed, prefix)
                                    .unwrap_or_else(|| uncompressed.clone()),
                                _ => uncompressed.clone(),
                            };

                            // Recompress with same compressor
                            let new_wire = encode_op_compressed(
                                op.original_opcode,
                                &rewritten_uncompressed,
                                op.compressor_id,
                                0,
                                hdr.request_id,
                            );
                            out.extend_from_slice(&new_wire);
                        } else {
                            // Decompression failed, forward raw
                            out.extend_from_slice(header_bytes);
                            out.extend_from_slice(body);
                        }
                    } else {
                        // Parse failed, forward raw
                        out.extend_from_slice(header_bytes);
                        out.extend_from_slice(body);
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
        timeout(dur, stream.write_all(&out))
            .await
            .context("shadow send timeout")??;
        timeout(dur, stream.flush())
            .await
            .context("shadow flush timeout")??;

        // Read reply header
        let mut header_buf = [0u8; 16];
        if let Err(e) = timeout(dur, stream.read_exact(&mut header_buf))
            .await
            .context("shadow recv header timeout")?
        {
            // Connection likely broken; drop it so we reconnect on next try
            *guard = None;
            return Err(e.into());
        }
        let (reply_hdr, _) =
            MessageHeader::parse(&header_buf).ok_or_else(|| anyhow!("invalid reply header"))?;
        if reply_hdr.message_length < 16 {
            return Err(anyhow!("invalid reply length"));
        }
        let body_len = (reply_hdr.message_length as usize).saturating_sub(16);
        let mut buf = vec![0u8; body_len];
        if body_len > 0
            && let Err(e) = timeout(dur, stream.read_exact(&mut buf))
                .await
                .context("shadow recv body timeout")?
        {
            *guard = None;
            return Err(e.into());
        }
        Ok((reply_hdr.op_code, buf))
    }

    /// Forward request and return upstream's first doc (from OP_MSG section-0 or OP_REPLY's first doc).
    /// Now supports OP_COMPRESSED by decompressing and extracting the document.
    pub async fn forward_and_read_doc(
        &self,
        hdr: &MessageHeader,
        header_bytes: &[u8],
        body: &[u8],
    ) -> Result<Option<Document>> {
        let (op, buf) = self.forward_raw(hdr, header_bytes, body).await?;
        match op {
            OP_MSG => decode_op_msg_section0(&buf)
                .map(|(_f, d)| Some(d))
                .ok_or_else(|| anyhow!("malformed upstream OP_MSG")),
            crate::protocol::OP_REPLY => Ok(decode_op_reply_first_doc(&buf)),
            OP_COMPRESSED => Ok(decode_op_compressed_reply(&buf)),
            _ => Ok(None),
        }
    }
}

fn rewrite_op_query_db_prefix(body: &[u8], prefix: &str) -> Option<Vec<u8>> {
    // Body layout: flags (4), cstring fullCollectionName, numberToSkip (4), numberToReturn (4), queryDoc (bson), [optional returnFieldsSelector]
    if body.len() < 4 {
        return None;
    }
    let flags = &body[0..4];
    // find cstring
    let mut i = 4usize;
    while i < body.len() && body[i] != 0 {
        i += 1;
    }
    if i >= body.len() {
        return None;
    }
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
    let j = end + 1; // skip null
    out.extend_from_slice(&body[j..]);
    Some(out)
}

pub struct DiffResult {
    pub matched: bool,
    pub summary: String,
    pub details: Option<String>,
}

pub fn compare_docs(ours: &Document, theirs: &Document, opts: &ShadowCompareOptions) -> DiffResult {
    // Clone and apply path-based ignores
    let mut ours_n = ours.clone();
    let mut theirs_n = theirs.clone();
    let patterns: Vec<Vec<PatternSeg>> = opts
        .ignore_fields
        .iter()
        .map(|p| parse_pattern(p))
        .collect();
    for pat in &patterns {
        apply_ignore_pattern_doc_inplace(&mut ours_n, pat, 0);
        apply_ignore_pattern_doc_inplace(&mut theirs_n, pat, 0);
    }

    // Collect diffs with path context
    let mut diffs: Vec<String> = Vec::new();
    diff_bson(
        &Bson::Document(ours_n),
        &Bson::Document(theirs_n),
        &mut Vec::new(),
        &mut diffs,
        opts,
        0,
    );

    let matched = diffs.is_empty();
    let summary = if matched {
        "match".to_string()
    } else {
        format!("{} diffs", diffs.len())
    };
    // Limit details to avoid log blow-ups
    let max_lines = 12usize;
    if diffs.len() > max_lines {
        diffs.truncate(max_lines);
        diffs.push("... (truncated)".to_string());
    }
    let details = if matched {
        None
    } else {
        Some(diffs.join("; "))
    };
    DiffResult {
        matched,
        summary,
        details,
    }
}

fn bson_equal(a: &Bson, b: &Bson, opts: &ShadowCompareOptions) -> bool {
    match (a, b) {
        (Bson::Document(da), Bson::Document(db)) => compare_docs(da, db, opts).matched,
        (Bson::Array(aa), Bson::Array(ab)) => {
            if aa.len() != ab.len() {
                return false;
            }
            for (ea, eb) in aa.iter().zip(ab.iter()) {
                if !bson_equal(ea, eb, opts) {
                    return false;
                }
            }
            true
        }
        // Optional numeric equivalence
        (Bson::Int32(x), Bson::Int64(y)) => opts.numeric_equivalence && (*x as i64) == *y,
        (Bson::Int64(x), Bson::Int32(y)) => opts.numeric_equivalence && *x == (*y as i64),
        (Bson::Int32(x), Bson::Double(y)) => opts.numeric_equivalence && (*x as f64) == *y,
        (Bson::Int64(x), Bson::Double(y)) => opts.numeric_equivalence && (*x as f64) == *y,
        (Bson::Double(x), Bson::Int32(y)) => opts.numeric_equivalence && *x == (*y as f64),
        (Bson::Double(x), Bson::Int64(y)) => opts.numeric_equivalence && *x == (*y as f64),
        _ => a == b,
    }
}

// -------- Path-based ignore implementation --------

#[derive(Debug, Clone, PartialEq, Eq)]
enum PatternSeg {
    Exact(String),
    Wildcard,
}

fn parse_pattern(p: &str) -> Vec<PatternSeg> {
    p.split('.')
        .map(|seg| {
            if seg == "*" {
                PatternSeg::Wildcard
            } else {
                PatternSeg::Exact(seg.to_string())
            }
        })
        .collect()
}

fn apply_ignore_pattern_doc_inplace(doc: &mut Document, pat: &Vec<PatternSeg>, idx: usize) {
    if idx >= pat.len() {
        return;
    }
    match &pat[idx] {
        PatternSeg::Exact(k) => {
            if idx == pat.len() - 1 {
                doc.remove(k);
            } else if let Some(Bson::Document(sub)) = doc.get_mut(k) {
                apply_ignore_pattern_doc_inplace(sub, pat, idx + 1);
            } else if let Some(Bson::Array(arr)) = doc.get_mut(k) {
                for v in arr.iter_mut() {
                    if let Bson::Document(d) = v {
                        apply_ignore_pattern_doc_inplace(d, pat, idx + 1);
                    }
                }
            }
        }
        PatternSeg::Wildcard => {
            if idx == pat.len() - 1 {
                let keys: Vec<String> = doc.keys().map(|s| s.to_string()).collect();
                for k in keys {
                    doc.remove(&k);
                }
            } else {
                for (_k, v) in doc.iter_mut() {
                    match v {
                        Bson::Document(sub) => apply_ignore_pattern_doc_inplace(sub, pat, idx + 1),
                        Bson::Array(arr) => {
                            for vv in arr.iter_mut() {
                                if let Bson::Document(d) = vv {
                                    apply_ignore_pattern_doc_inplace(d, pat, idx + 1);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

// -------- Diff implementation --------

fn diff_bson(
    a: &Bson,
    b: &Bson,
    path: &mut Vec<String>,
    out: &mut Vec<String>,
    opts: &ShadowCompareOptions,
    _depth: usize,
) {
    match (a, b) {
        (Bson::Document(da), Bson::Document(db)) => {
            // compare keys union
            let mut keys: Vec<String> = da.keys().map(|s| s.to_string()).collect();
            for k in db.keys() {
                if !keys.iter().any(|x| x == k) {
                    keys.push(k.to_string());
                }
            }
            keys.sort();
            keys.dedup();
            for k in keys {
                let av = da.get(&k);
                let bv = db.get(&k);
                path.push(k.clone());
                match (av, bv) {
                    (None, Some(_)) => out.push(format!("{} missing in ours", path_str(path))),
                    (Some(_), None) => out.push(format!("{} extra in ours", path_str(path))),
                    (Some(x), Some(y)) => diff_bson(x, y, path, out, opts, _depth + 1),
                    _ => {}
                }
                path.pop();
            }
        }
        (Bson::Array(aa), Bson::Array(ab)) => {
            if aa.len() != ab.len() {
                out.push(format!(
                    "{} array length {} != {}",
                    path_str(path),
                    aa.len(),
                    ab.len()
                ));
                return;
            }
            for (i, (ea, eb)) in aa.iter().zip(ab.iter()).enumerate() {
                path.push(i.to_string());
                diff_bson(ea, eb, path, out, opts, _depth + 1);
                path.pop();
            }
        }
        _ => {
            if !bson_equal(a, b, opts) {
                out.push(format!(
                    "{} ours={} theirs={}",
                    path_str(path),
                    redact_and_summarize(a, path),
                    redact_and_summarize(b, path)
                ));
            }
        }
    }
}

fn path_str(path: &[String]) -> String {
    if path.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", path.join("/"))
    }
}

fn redact_and_summarize(v: &Bson, path: &[String]) -> String {
    if is_sensitive_path(path) {
        return "<redacted>".to_string();
    }
    let s = format_bson_one_line(v);
    if s.len() > 200 {
        format!("{}...", &s[..200])
    } else {
        s
    }
}

fn is_sensitive_path(path: &[String]) -> bool {
    // Basic heuristic: redact fields containing these substrings
    let lower: Vec<String> = path.iter().map(|s| s.to_lowercase()).collect();
    for seg in &lower {
        if seg.contains("password")
            || seg.contains("credential")
            || seg.contains("secret")
            || seg.contains("token")
            || seg.contains("sasl")
        {
            return true;
        }
    }
    false
}

fn format_bson_one_line(v: &Bson) -> String {
    match v {
        Bson::String(s) => format!("\"{}\"", s),
        _ => format!("{:?}", v),
    }
}
