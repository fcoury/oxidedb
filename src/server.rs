use crate::config::{Config, ShadowConfig};
use crate::error::Result;
use crate::protocol::{decode_op_msg_section0, decode_op_query, encode_op_msg, encode_op_reply, MessageHeader, OP_MSG, OP_QUERY};
use crate::shadow::{compare_docs, ShadowSession};
use crate::store::PgStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use bson::{doc, Document};
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, Instant};
// use rand::Rng; // not used directly

static REQ_ID: AtomicI32 = AtomicI32::new(1);

use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::Mutex;

struct CursorEntry {
    ns: String,
    docs: Vec<Document>,
    pos: usize,
    last_access: Instant,
}

pub struct AppState {
    pub store: Option<PgStore>,
    pub started_at: Instant,
    pub cursors: Mutex<HashMap<i64, CursorEntry>>,
    pub shadow: Option<std::sync::Arc<ShadowConfig>>,
    // Shadow metrics
    pub shadow_attempts: std::sync::atomic::AtomicU64,
    pub shadow_matches: std::sync::atomic::AtomicU64,
    pub shadow_mismatches: std::sync::atomic::AtomicU64,
    pub shadow_timeouts: std::sync::atomic::AtomicU64,
}


pub async fn run(cfg: Config) -> Result<()> {
    let listener = TcpListener::bind(&cfg.listen_addr).await?;
    tracing::info!(listen_addr = %cfg.listen_addr, "oxidedb listening");

    let state = if let Some(url) = cfg.postgres_url.clone() {
        match PgStore::connect(&url).await {
            Ok(pg) => {
                if let Err(e) = pg.bootstrap().await {
                    tracing::error!(error = %format!("{e:?}"), "failed to bootstrap metadata");
                }
                AppState {
                    store: Some(pg),
                    started_at: Instant::now(),
                    cursors: Mutex::new(HashMap::new()),
                    shadow: cfg.shadow.as_ref().map(|s| std::sync::Arc::new(s.clone())),
                    shadow_attempts: std::sync::atomic::AtomicU64::new(0),
                    shadow_matches: std::sync::atomic::AtomicU64::new(0),
                    shadow_mismatches: std::sync::atomic::AtomicU64::new(0),
                    shadow_timeouts: std::sync::atomic::AtomicU64::new(0),
                }
            }
            Err(e) => {
                tracing::error!(error = %format!("{e:?}"), "failed to connect to postgres; continuing without store");
                AppState {
                    store: None,
                    started_at: Instant::now(),
                    cursors: Mutex::new(HashMap::new()),
                    shadow: cfg.shadow.as_ref().map(|s| std::sync::Arc::new(s.clone())),
                    shadow_attempts: std::sync::atomic::AtomicU64::new(0),
                    shadow_matches: std::sync::atomic::AtomicU64::new(0),
                    shadow_mismatches: std::sync::atomic::AtomicU64::new(0),
                    shadow_timeouts: std::sync::atomic::AtomicU64::new(0),
                }
            }
        }
    } else {
        AppState {
            store: None,
            started_at: Instant::now(),
            cursors: Mutex::new(HashMap::new()),
            shadow: cfg.shadow.as_ref().map(|s| std::sync::Arc::new(s.clone())),
            shadow_attempts: std::sync::atomic::AtomicU64::new(0),
            shadow_matches: std::sync::atomic::AtomicU64::new(0),
            shadow_mismatches: std::sync::atomic::AtomicU64::new(0),
            shadow_timeouts: std::sync::atomic::AtomicU64::new(0),
        }
    };
    let state = Arc::new(state);

    // Spawn cursor sweeper (no shutdown in run())
    let ttl = Duration::from_secs(cfg.cursor_timeout_secs.unwrap_or(300));
    let sweep_interval = Duration::from_secs(cfg.cursor_sweep_interval_secs.unwrap_or(30));
    let sweeper_state = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(sweep_interval).await;
            prune_cursors_once(&sweeper_state, ttl).await;
        }
    });

    loop {
        let (socket, addr) = listener.accept().await?;
        tracing::debug!(%addr, "accepted connection");
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(state, socket).await {
                tracing::debug!(error = %format!("{e:?}"), "connection closed with error");
            }
        });
    }
}

/// Spawn the server on the provided listen address and run until `shutdown` is signaled.
/// Returns the shared `AppState`, the bound local address, a shutdown sender, and the task handle.
pub async fn spawn_with_shutdown(mut cfg: Config) -> Result<(
    std::sync::Arc<AppState>,
    std::net::SocketAddr,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<Result<()>>,
)> {
    use tokio::sync::watch;

    // Allow ephemeral port usage in tests (e.g., 127.0.0.1:0)
    let listener = TcpListener::bind(&cfg.listen_addr).await?;
    let local_addr = listener.local_addr()?;

    // Build state (mirrors run())
    let state = if let Some(url) = cfg.postgres_url.clone() {
        match PgStore::connect(&url).await {
            Ok(pg) => {
                if let Err(e) = pg.bootstrap().await {
                    tracing::error!(error = %format!("{e:?}"), "failed to bootstrap metadata");
                }
                AppState {
                    store: Some(pg),
                    started_at: Instant::now(),
                    cursors: Mutex::new(HashMap::new()),
                    shadow: cfg.shadow.as_ref().map(|s| std::sync::Arc::new(s.clone())),
                    shadow_attempts: std::sync::atomic::AtomicU64::new(0),
                    shadow_matches: std::sync::atomic::AtomicU64::new(0),
                    shadow_mismatches: std::sync::atomic::AtomicU64::new(0),
                    shadow_timeouts: std::sync::atomic::AtomicU64::new(0),
                }
            }
            Err(e) => {
                tracing::error!(error = %format!("{e:?}"), "failed to connect to postgres; continuing without store");
                AppState {
                    store: None,
                    started_at: Instant::now(),
                    cursors: Mutex::new(HashMap::new()),
                    shadow: cfg.shadow.as_ref().map(|s| std::sync::Arc::new(s.clone())),
                    shadow_attempts: std::sync::atomic::AtomicU64::new(0),
                    shadow_matches: std::sync::atomic::AtomicU64::new(0),
                    shadow_mismatches: std::sync::atomic::AtomicU64::new(0),
                    shadow_timeouts: std::sync::atomic::AtomicU64::new(0),
                }
            }
        }
    } else {
        AppState {
            store: None,
            started_at: Instant::now(),
            cursors: Mutex::new(HashMap::new()),
            shadow: cfg.shadow.as_ref().map(|s| std::sync::Arc::new(s.clone())),
            shadow_attempts: std::sync::atomic::AtomicU64::new(0),
            shadow_matches: std::sync::atomic::AtomicU64::new(0),
            shadow_mismatches: std::sync::atomic::AtomicU64::new(0),
            shadow_timeouts: std::sync::atomic::AtomicU64::new(0),
        }
    };
    let state = std::sync::Arc::new(state);

    // Sweeper with shutdown
    let ttl = Duration::from_secs(cfg.cursor_timeout_secs.unwrap_or(300));
    let sweep_interval = Duration::from_secs(cfg.cursor_sweep_interval_secs.unwrap_or(30));
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let sweeper_state = state.clone();
    let mut sweeper_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(sweep_interval) => {
                    prune_cursors_once(&sweeper_state, ttl).await;
                }
                _ = sweeper_shutdown.changed() => {
                    if *sweeper_shutdown.borrow() { break; }
                }
            }
        }
    });

    // Accept loop with shutdown
    let state_accept = state.clone();
    let handle = tokio::spawn(async move {
        let listener = listener;
        loop {
            tokio::select! {
                res = listener.accept() => {
                    let (socket, addr) = match res {
                        Ok(v) => v,
                        Err(e) => { return Err(e.into()); }
                    };
                    tracing::debug!(%addr, "accepted connection");
                    let state = state_accept.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(state, socket).await {
                            tracing::debug!(error = %format!("{e:?}"), "connection closed with error");
                        }
                    });
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() { break; }
                }
            }
        }
        Ok(())
    });

    Ok((state, local_addr, shutdown_tx, handle))
}

async fn handle_connection(state: Arc<AppState>, mut socket: TcpStream) -> Result<()> {
    // Per-connection shadow session (lazy connect)
    let shadow_session = state.shadow.as_ref().and_then(|cfg| if cfg.enabled { Some(ShadowSession::new(cfg.clone())) } else { None });
    loop {
        // Read header
        let mut header_buf = [0u8; 16];
        if let Err(e) = socket.read_exact(&mut header_buf).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof || e.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            } else {
                return Err(e.into());
            }
        }
        let (hdr, _) = match MessageHeader::parse(&header_buf) {
            Some(h) => h,
            None => {
                tracing::warn!("invalid message header");
                break;
            }
        };
        if hdr.message_length < 16 {
            tracing::warn!(?hdr, "invalid message length");
            break;
        }
        let body_len = (hdr.message_length as usize).saturating_sub(16);
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            socket.read_exact(&mut body).await?;
        }

        match hdr.op_code {
            OP_MSG => {
                let (reply_doc, cmd_opt) = match decode_op_msg_section0(&body) {
                    Some((_flags, cmd)) => {
                        let db = cmd.get_str("$db").ok().map(|s| s.to_string());
                        (handle_command(&state, db.as_deref(), cmd.clone()).await, Some(cmd))
                    }
                    None => {
                        tracing::warn!("malformed OP_MSG body; sending ok:0");
                        (error_doc(1, "Malformed OP_MSG body"), None)
                    }
                };
                let request_id = REQ_ID.fetch_add(1, Ordering::Relaxed);
                let resp = encode_op_msg(&reply_doc, hdr.request_id, request_id);
                socket.write_all(&resp).await?;
                socket.flush().await?;

                // Shadow forwarding (non-blocking)
                if let Some(sh) = &shadow_session {
                    let cfg = sh.cfg.clone();
                    let header_bytes = header_buf.clone();
                    let body_bytes = body.clone();
                    let ours = reply_doc.clone();
                    let cmd_name = cmd_opt.as_ref().and_then(|d| d.iter().next().map(|(k, _)| k.clone())).unwrap_or_else(|| "".to_string());
                    let sample = rand::random::<f64>() < cfg.sample_rate;
                    if sample {
                        let sh = sh.clone();
                        let state2 = state.clone();
                        tokio::spawn(async move {
                            let start = Instant::now();
                            let res = tokio::time::timeout(Duration::from_millis(cfg.timeout_ms), sh.forward_and_read_doc(&hdr, &header_bytes, &body_bytes)).await;
                            match res {
                                Err(_) => {
                                    state2.shadow_timeouts.fetch_add(1, Ordering::Relaxed);
                                    tracing::debug!(command=%cmd_name, elapsed_ms=%start.elapsed().as_millis(), "shadow timeout");
                                }
                                Ok(Err(e)) => {
                                    tracing::debug!(command=%cmd_name, error=%e, "shadow error");
                                }
                                Ok(Ok(up_doc_opt)) => {
                                    if let Some(up_doc) = up_doc_opt {
                                        state2.shadow_attempts.fetch_add(1, Ordering::Relaxed);
                                        let diff = compare_docs(&ours, &up_doc, &cfg.compare);
                                        if diff.matched {
                                            state2.shadow_matches.fetch_add(1, Ordering::Relaxed);
                                            tracing::debug!(command=%cmd_name, elapsed_ms=%start.elapsed().as_millis(), "shadow match");
                                        } else {
                                            state2.shadow_mismatches.fetch_add(1, Ordering::Relaxed);
                                            tracing::info!(command=%cmd_name, elapsed_ms=%start.elapsed().as_millis(), summary=%diff.summary, details=?diff.details, "shadow mismatch");
                                        }
                                    } else {
                                        tracing::debug!(command=%cmd_name, elapsed_ms=%start.elapsed().as_millis(), "shadow unsupported reply op");
                                    }
                                }
                            }
                        });
                    }
                }
            }
            OP_QUERY => {
                match decode_op_query(&body) {
                    Some((_flags, fqn, _skip, _nret, cmd)) => {
                        let db = parse_db_from_fqn(&fqn);
                        let cmd_name = cmd.iter().next().map(|(k, _)| k.clone()).unwrap_or_else(|| "".to_string());
                        let reply_doc = handle_command(&state, db.as_deref(), cmd).await;
                        let request_id = REQ_ID.fetch_add(1, Ordering::Relaxed);
                        let resp = encode_op_reply(&[reply_doc.clone()], hdr.request_id, request_id);
                        socket.write_all(&resp).await?;
                        socket.flush().await?;

                        // Shadow forwarding (non-blocking)
                        if let Some(sh) = &shadow_session {
                            let cfg = sh.cfg.clone();
                            let header_bytes = header_buf.clone();
                            let body_bytes = body.clone();
                            let ours = reply_doc.clone();
                            let sample = rand::random::<f64>() < cfg.sample_rate;
                            if sample {
                                let sh = sh.clone();
                                let state2 = state.clone();
                                tokio::spawn(async move {
                                    let start = Instant::now();
                                    let res = tokio::time::timeout(Duration::from_millis(cfg.timeout_ms), sh.forward_and_read_doc(&hdr, &header_bytes, &body_bytes)).await;
                                    match res {
                                        Err(_) => {
                                            state2.shadow_timeouts.fetch_add(1, Ordering::Relaxed);
                                            tracing::debug!(command=%cmd_name, elapsed_ms=%start.elapsed().as_millis(), "shadow timeout");
                                        }
                                        Ok(Err(e)) => {
                                            tracing::debug!(command=%cmd_name, error=%e, "shadow error");
                                        }
                                        Ok(Ok(up_doc_opt)) => {
                                            if let Some(up_doc) = up_doc_opt {
                                                state2.shadow_attempts.fetch_add(1, Ordering::Relaxed);
                                                let diff = compare_docs(&ours, &up_doc, &cfg.compare);
                                                if diff.matched {
                                                    state2.shadow_matches.fetch_add(1, Ordering::Relaxed);
                                                    tracing::debug!(command=%cmd_name, elapsed_ms=%start.elapsed().as_millis(), "shadow match");
                                                } else {
                                                    state2.shadow_mismatches.fetch_add(1, Ordering::Relaxed);
                                                    tracing::info!(command=%cmd_name, elapsed_ms=%start.elapsed().as_millis(), summary=%diff.summary, details=?diff.details, "shadow mismatch");
                                                }
                                            } else {
                                                tracing::debug!(command=%cmd_name, elapsed_ms=%start.elapsed().as_millis(), "shadow unsupported reply op");
                                            }
                                        }
                                    }
                                });
                            }
                        }
                    }
                    None => {
                        tracing::warn!("malformed OP_QUERY body; ignoring");
                    }
                }
            }
            _ => {
                tracing::warn!(op_code = hdr.op_code, "unsupported op code");
            }
        }
    }

    Ok(())
}

async fn handle_command(state: &AppState, db: Option<&str>, mut cmd: Document) -> Document {
    // command name is the first key in the doc
    let cmd_name = cmd.iter().next().map(|(k, _)| k.as_str()).unwrap_or("");
    match cmd_name {
        "hello" | "ismaster" | "isMaster" => hello_reply(),
        "ping" => doc! { "ok": 1.0 },
        "buildInfo" | "buildinfo" => build_info_reply(),
        "listDatabases" => list_databases_reply(state, &cmd).await,
        "listCollections" => list_collections_reply(state, db).await,
        "serverStatus" => server_status_reply(state).await,
        "create" => create_collection_reply(state, db, &cmd).await,
        "drop" => drop_collection_reply(state, db, &cmd).await,
        "dropDatabase" => drop_database_reply(state, db).await,
        "insert" => insert_reply(state, db, &mut cmd).await,
        "update" => update_reply(state, db, &cmd).await,
        "delete" => delete_reply(state, db, &cmd).await,
        "aggregate" => aggregate_reply(state, db, &cmd).await,
        "find" => find_reply(state, db, &cmd).await,
        "getMore" => get_more_reply(state, &cmd).await,
        "createIndexes" => create_indexes_reply(state, db, &cmd).await,
        "dropIndexes" => drop_indexes_reply(state, db, &cmd).await,
        "killCursors" => kill_cursors_reply(state, &cmd).await,
        _ => {
            tracing::debug!(cmd = ?cmd, "unrecognized command; replying ok:0");
            error_doc(59, format!("Command '{}' not implemented", cmd_name))
        }
    }
}

fn hello_reply() -> Document {
    doc! {
        "ismaster": true,
        "isWritablePrimary": true,
        "helloOk": true,
        "minWireVersion": 0i32,
        // Advertise a minimally compatible wire version for modern drivers
        // (MongoDB 4.0+ expects >= 7)
        "maxWireVersion": 7i32,
        "maxBsonObjectSize": 16_777_216i32, // 16MB
        "maxMessageSizeBytes": 48_000_000i32,
        "maxWriteBatchSize": 100_000i32,
        "logicalSessionTimeoutMinutes": 30i32,
        "ok": 1.0
    }
}

fn error_doc(code: i32, msg: impl Into<String>) -> Document {
    doc! { "ok": 0.0, "errmsg": msg.into(), "code": code }
}

fn build_info_reply() -> Document {
    doc! {
        "version": env!("CARGO_PKG_VERSION"),
        "gitVersion": "",
        "sysInfo": "oxidedb",
        "loaderFlags": "",
        "compilerFlags": "",
        "allocator": "system",
        "javascriptEngine": "none",
        "bits": 64i32,
        "debug": false,
        "maxBsonObjectSize": 16_777_216i32,
        "ok": 1.0
    }
}

async fn list_databases_reply(state: &AppState, cmd: &Document) -> Document {
    // If Postgres is connected, read from metadata; otherwise, return empty list
    let names = if let Some(ref pg) = state.store {
        match pg.list_databases().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %format!("{e:?}"), "list_databases failed; returning empty");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    let name_only = cmd.get_bool("nameOnly").unwrap_or(false);
    let mut dbs = Vec::with_capacity(names.len());
    for n in names {
        if name_only {
            dbs.push(doc! { "name": n });
        } else {
            dbs.push(doc! { "name": n, "sizeOnDisk": 0i64, "empty": true });
        }
    }
    let mut reply = doc! { "databases": dbs, "ok": 1.0 };
    if !name_only { reply.insert("totalSize", 0i64); }
    reply
}

async fn list_collections_reply(state: &AppState, db: Option<&str>) -> Document {
    let dbname = db.unwrap_or("");
    let names = if let Some(ref pg) = state.store {
        match pg.list_collections(dbname).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %format!("{e:?}"), "list_collections failed; returning empty");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    let mut first_batch = Vec::with_capacity(names.len());
    for n in names {
        first_batch.push(doc! {
            "name": n,
            "type": "collection",
            "options": doc!{},
            "info": doc!{"readOnly": false},
        });
    }
    let ns = format!("{}.$cmd.listCollections", dbname);
    doc! {
        "cursor": {"id": 0i64, "ns": ns, "firstBatch": first_batch},
        "ok": 1.0
    }
}

fn parse_db_from_fqn(fqn: &str) -> Option<String> {
    // format: "<db>.$cmd" or "<db>.<coll>"
    fqn.split('.').next().map(|s| s.to_string())
}

async fn server_status_reply(state: &AppState) -> Document {
    let uptime = state.started_at.elapsed().as_secs_f64();
    doc! {
        "version": env!("CARGO_PKG_VERSION"),
        "process": "oxidedb",
        "uptime": uptime,
        "ok": 1.0
    }
}

async fn create_collection_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db { Some(d) => d, None => return error_doc(59, "Missing $db") };
    let coll = match cmd.get_str("create") { Ok(c) => c, Err(_) => return error_doc(9, "Invalid create") };
    if let Some(ref pg) = state.store {
        match pg.ensure_collection(dbname, coll).await {
            Ok(_) => doc!{ "ok": 1.0 },
            Err(e) => error_doc(59, format!("create failed: {}", e)),
        }
    } else {
        error_doc(13, "No storage configured")
    }
}

async fn drop_collection_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db { Some(d) => d, None => return error_doc(59, "Missing $db") };
    let coll = match cmd.get_str("drop") { Ok(c) => c, Err(_) => return error_doc(9, "Invalid drop") };
    if let Some(ref pg) = state.store {
        match pg.drop_collection(dbname, coll).await {
            Ok(_) => doc!{ "nIndexesWas": 0i32, "ns": format!("{}.{}", dbname, coll), "ok": 1.0 },
            Err(e) => error_doc(59, format!("drop failed: {}", e)),
        }
    } else {
        error_doc(13, "No storage configured")
    }
}

async fn drop_database_reply(state: &AppState, db: Option<&str>) -> Document {
    let dbname = match db { Some(d) => d, None => return error_doc(59, "Missing $db") };
    if let Some(ref pg) = state.store {
        match pg.drop_database(dbname).await {
            Ok(_) => doc!{ "dropped": dbname, "ok": 1.0 },
            Err(e) => error_doc(59, format!("dropDatabase failed: {}", e)),
        }
    } else {
        error_doc(13, "No storage configured")
    }
}

async fn insert_reply(state: &AppState, db: Option<&str>, cmd: &mut Document) -> Document {
    let dbname = match db { Some(d) => d, None => return error_doc(59, "Missing $db") };
    let coll: String = match cmd.get_str("insert") { Ok(c) => c.to_string(), Err(_) => return error_doc(9, "Invalid insert") };
    let docs_bson: Vec<bson::Bson> = match cmd.get_array("documents") { Ok(a) => a.clone(), Err(_) => return error_doc(9, "Missing documents") };
    if let Some(ref pg) = state.store {
        let mut inserted = 0u32;
        let mut write_errors: Vec<Document> = Vec::new();
        for (i, b) in docs_bson.iter().enumerate() {
            if let bson::Bson::Document(d0) = b {
                let mut d = d0.clone();
                ensure_id(&mut d);
                match id_bytes(d.get("_id")) {
                    Some(idb) => {
                        let json = match serde_json::to_value(&d) { Ok(v) => v, Err(e) => { write_errors.push(doc!{"index": i as i32, "code": 2, "errmsg": e.to_string()}); continue; } };
                        let bson_bytes = match bson::to_vec(&d) { Ok(v) => v, Err(e) => { write_errors.push(doc!{"index": i as i32, "code": 2, "errmsg": e.to_string()}); continue; } };
                        match pg.insert_one(dbname, &coll, &idb, &bson_bytes, &json).await {
                            Ok(n) => {
                                if n == 1 { inserted += 1; } else {
                                    write_errors.push(doc!{"index": i as i32, "code": 11000i32, "errmsg": "duplicate key"});
                                }
                            }
                            Err(e) => write_errors.push(doc!{"index": i as i32, "code": 59i32, "errmsg": e.to_string()}),
                        }
                    }
                    None => write_errors.push(doc!{"index": i as i32, "code": 2i32, "errmsg": "unsupported _id type"}),
                }
            } else {
                write_errors.push(doc!{"index": i as i32, "code": 2i32, "errmsg": "document must be object"});
            }
        }
        let mut reply = doc!{ "n": inserted as i32, "ok": 1.0 };
        if !write_errors.is_empty() { reply.insert("writeErrors", write_errors); }
        reply
    } else {
        error_doc(13, "No storage configured")
    }
}

async fn update_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db { Some(d) => d, None => return error_doc(59, "Missing $db") };
    let coll = match cmd.get_str("update") { Ok(c) => c, Err(_) => return error_doc(9, "Invalid update") };
    let updates = match cmd.get_array("updates") { Ok(a) => a, Err(_) => return error_doc(9, "Missing updates") };
    if updates.is_empty() { return error_doc(9, "Empty updates"); }
    if state.store.is_none() { return error_doc(13, "No storage configured"); }
    let pg = state.store.as_ref().unwrap();

    let mut matched_total = 0i32;
    let mut modified_total = 0i32;

    for upd_b in updates {
        let spec = match upd_b { bson::Bson::Document(d) => d, _ => return error_doc(9, "Invalid update spec") };
        let filter = match spec.get_document("q") { Ok(d) => d.clone(), Err(_) => return error_doc(9, "Missing q") };
        let udoc = match spec.get_document("u") { Ok(d) => d.clone(), Err(_) => return error_doc(9, "Missing u") };
        let multi = spec.get_bool("multi").unwrap_or(false);
        let upsert = spec.get_bool("upsert").unwrap_or(false);
        if upsert { return error_doc(20, "upsert not supported"); }
        let set_doc = udoc.get_document("$set").ok().cloned();
        let unset_doc = udoc.get_document("$unset").ok().cloned();
        let inc_doc = udoc.get_document("$inc").ok().cloned();
        let rename_doc = udoc.get_document("$rename").ok().cloned();
        let push_doc = udoc.get_document("$push").ok().cloned();
        let pull_doc = udoc.get_document("$pull").ok().cloned();
        if set_doc.is_none() && unset_doc.is_none() && inc_doc.is_none() && rename_doc.is_none() && push_doc.is_none() && pull_doc.is_none() {
            return error_doc(9, "Only $set/$unset/$inc/$rename/$push/$pull supported");
        }

        // Validate array index segments (disallow negative indexes)
        if let Some(ref sets) = set_doc {
            for (k, _v) in sets.iter() {
                if path_has_negative_index(k) { return error_doc(2, "Negative array indexes not supported"); }
            }
        }
        if let Some(ref unsets) = unset_doc {
            for (k, _v) in unsets.iter() {
                if path_has_negative_index(k) { return error_doc(2, "Negative array indexes not supported"); }
            }
        }
        if let Some(ref incs) = inc_doc {
            for (k, v) in incs.iter() {
                if path_has_negative_index(k) { return error_doc(2, "Negative array indexes not supported"); }
                if !matches!(v, bson::Bson::Int32(_)|bson::Bson::Int64(_)|bson::Bson::Double(_)) { return error_doc(2, "$inc requires numeric value"); }
            }
        }
        if let Some(ref ren) = rename_doc {
            let mut pairs: Vec<(String, String)> = Vec::new();
            for (from, to_b) in ren.iter() {
                if path_has_negative_index(from) { return error_doc(2, "Negative array indexes not supported"); }
                let to = match to_b { bson::Bson::String(s) => s, _ => return error_doc(2, "$rename target must be string path") };
                if path_has_negative_index(to) { return error_doc(2, "Negative array indexes not supported"); }
                pairs.push((from.to_string(), to.to_string()));
            }
            if let Some(err) = validate_rename_pairs(&pairs) { return err; }
        }
        if let Some(ref pushes) = push_doc {
            for (k, _v) in pushes.iter() {
                if path_has_negative_index(k) { return error_doc(2, "Negative array indexes not supported"); }
            }
        }
        if let Some(ref pulls) = pull_doc {
            for (k, _v) in pulls.iter() {
                if path_has_negative_index(k) { return error_doc(2, "Negative array indexes not supported"); }
            }
        }

        if multi {
            // Fetch docs by filter and update each
            let docs = match pg.find_docs(dbname, coll, Some(&filter), None, None, 10_000).await {
                Ok(v) => v,
                Err(e) => return error_doc(59, format!("find failed: {}", e)),
            };
            if docs.is_empty() { continue; }
            for mut d in docs {
                let idb = match d.get("_id").and_then(|b| id_bytes_bson(b)) { Some(v) => v, None => continue };
                // remember original
                let orig = d.clone();
                // $set
                if let Some(ref sets) = set_doc {
                    for (k, v) in sets.iter() { set_path_nested(&mut d, k, v.clone()); }
                }
                // $unset
                if let Some(ref unsets) = unset_doc {
                    for (k, _v) in unsets.iter() { unset_path_nested(&mut d, k); }
                }
                // $inc
                if let Some(ref incs) = inc_doc { for (k, v) in incs.iter() { if !apply_inc(&mut d, k, v.clone()) { continue; } } }
                // $rename
                if let Some(ref ren) = rename_doc { for (from, to_b) in ren.iter() { if let bson::Bson::String(to) = to_b { apply_rename(&mut d, from, to); } } }
                // $push
                if let Some(ref pushes) = push_doc { for (k, v) in pushes.iter() { if !apply_push(&mut d, k, v.clone()) { return error_doc(2, "$push on non-array"); } } }
                // $pull
                if let Some(ref pulls) = pull_doc { for (k, v) in pulls.iter() { apply_pull(&mut d, k, v.clone()); } }
                if let Err(e) = pg.update_doc_by_id(dbname, coll, &idb, &d).await {
                    tracing::warn!("update_doc_by_id failed: {}", e);
                } else {
                    matched_total += 1;
                    if d != orig { modified_total += 1; }
                }
            }
        } else {
            // Single
            let found = match pg.find_one_for_update(dbname, coll, &filter).await {
                Ok(v) => v,
                Err(e) => return error_doc(59, format!("find failed: {}", e)),
            };
            if let Some((idb, mut doc0)) = found {
                let orig = doc0.clone();
                if let Some(ref sets) = set_doc { for (k, v) in sets.iter() { set_path_nested(&mut doc0, k, v.clone()); } }
                if let Some(ref unsets) = unset_doc { for (k, _v) in unsets.iter() { unset_path_nested(&mut doc0, k); } }
                if let Some(ref incs) = inc_doc { for (k, v) in incs.iter() { if !apply_inc(&mut doc0, k, v.clone()) { return error_doc(2, "$inc requires numeric field"); } } }
                if let Some(ref ren) = rename_doc { for (from, to_b) in ren.iter() { if let bson::Bson::String(to) = to_b { apply_rename(&mut doc0, from, to); } } }
                if let Some(ref pushes) = push_doc { for (k, v) in pushes.iter() { if !apply_push(&mut doc0, k, v.clone()) { return error_doc(2, "$push on non-array"); } } }
                if let Some(ref pulls) = pull_doc { for (k, v) in pulls.iter() { apply_pull(&mut doc0, k, v.clone()); } }
                match pg.update_doc_by_id(dbname, coll, &idb, &doc0).await {
                    Ok(n) => {
                        matched_total += 1;
                        if doc0 != orig { modified_total += 1; }
                    }
                    Err(e) => return error_doc(59, format!("update failed: {}", e)),
                }
            }
        }
    }

    doc!{"n": matched_total, "nModified": modified_total, "ok": 1.0}
}

fn set_path_nested(doc: &mut Document, path: &str, value: bson::Bson) {
    // Implement via a generic BSON walker that supports arrays and documents
    let mut root = bson::Bson::Document(doc.clone());
    let segments: Vec<&str> = path.split('.').collect();
    set_path_bson(&mut root, &segments, value);
    if let bson::Bson::Document(updated) = root { *doc = updated; }
}

fn seg_is_index(seg: &str) -> Option<usize> { seg.parse::<usize>().ok() }

fn set_path_bson(cur: &mut bson::Bson, segs: &[&str], value: bson::Bson) {
    if segs.is_empty() { *cur = value; return; }
    let seg = segs[0];
    let next_is_index = segs.get(1).and_then(|s| seg_is_index(s)).is_some();
    match seg_is_index(seg) {
        Some(idx) => {
            // Ensure array
            if !matches!(cur, bson::Bson::Array(_)) { *cur = bson::Bson::Array(Vec::new()); }
            if let bson::Bson::Array(arr) = cur {
                let desired_len = idx + 1;
                if arr.len() < desired_len {
                    let pad = desired_len - arr.len();
                    arr.extend(std::iter::repeat(bson::Bson::Null).take(pad));
                }
                if segs.len() == 1 { arr[idx] = value; }
                else {
                    // Choose default element type for deeper path
                    if matches!(arr[idx], bson::Bson::Null) {
                        arr[idx] = if next_is_index { bson::Bson::Array(Vec::new()) } else { bson::Bson::Document(Document::new()) };
                    }
                    set_path_bson(&mut arr[idx], &segs[1..], value);
                }
            }
        }
        None => {
            // Document key
            if !matches!(cur, bson::Bson::Document(_)) { *cur = bson::Bson::Document(Document::new()); }
            if let bson::Bson::Document(d) = cur {
                if segs.len() == 1 { d.insert(seg.to_string(), value); }
                else {
                    let entry = d.entry(seg.to_string()).or_insert_with(|| if next_is_index { bson::Bson::Array(Vec::new()) } else { bson::Bson::Document(Document::new()) });
                    set_path_bson(entry, &segs[1..], value);
                }
            }
        }
    }
}

fn unset_path_nested(doc: &mut Document, path: &str) {
    let mut root = bson::Bson::Document(doc.clone());
    let segments: Vec<&str> = path.split('.').collect();
    unset_path_bson(&mut root, &segments);
    if let bson::Bson::Document(updated) = root { *doc = updated; }
}

fn path_has_negative_index(path: &str) -> bool {
    for seg in path.split('.') {
        if let Ok(n) = seg.parse::<i32>() { if n < 0 { return true; } }
    }
    false
}

fn is_ancestor_path(a: &str, b: &str) -> bool {
    b.starts_with(a) && (b.len() == a.len() || b.as_bytes().get(a.len()) == Some(&b'.'))
}

fn validate_rename_pairs(pairs: &[(String, String)]) -> Option<Document> {
    // Disallow renaming a field to itself or ancestor/descendant paths; disallow target duplicates
    // Collect targets to detect duplicates
    use std::collections::HashSet;
    let mut targets: HashSet<&str> = HashSet::new();
    for (from, to) in pairs {
        if from == to { return Some(error_doc(2, "$rename from and to cannot be the same")); }
        if is_ancestor_path(from, to) || is_ancestor_path(to, from) {
            return Some(error_doc(2, "$rename cannot target ancestor/descendant path"));
        }
        if !targets.insert(to.as_str()) { return Some(error_doc(2, "$rename duplicate target path")); }
    }
    None
}

fn parse_path<'a>(path: &'a str) -> Vec<&'a str> { path.split('.').collect() }

fn get_path_bson_value(doc: &Document, path: &str) -> Option<bson::Bson> {
    let mut cur = bson::Bson::Document(doc.clone());
    for seg in parse_path(path) {
        match (&cur, seg.parse::<usize>().ok()) {
            (bson::Bson::Document(d), None) => {
                cur = d.get(seg)?.clone();
            }
            (bson::Bson::Array(arr), Some(idx)) => {
                cur = arr.get(idx)?.clone();
            }
            _ => return None,
        }
    }
    Some(cur)
}

fn apply_rename(doc: &mut Document, from: &str, to: &str) {
    if let Some(val) = get_path_bson_value(doc, from) {
        unset_path_nested(doc, from);
        set_path_nested(doc, to, val);
    }
}

fn number_as_f64(b: &bson::Bson) -> Option<f64> {
    match b { bson::Bson::Int32(n) => Some(*n as f64), bson::Bson::Int64(n) => Some(*n as f64), bson::Bson::Double(n) => Some(*n), _ => None }
}

fn apply_inc(doc: &mut Document, path: &str, delta: bson::Bson) -> bool {
    // Determine delta type
    let delta_num = match &delta {
        bson::Bson::Int32(n) => (Some(*n as i64), None),
        bson::Bson::Int64(n) => (Some(*n), None),
        bson::Bson::Double(f) => (None, Some(*f)),
        _ => return false,
    };
    let existing = get_path_bson_value(doc, path);
    if let Some(curv) = existing {
        match curv {
            bson::Bson::Int32(ci) => {
                if let Some(di) = delta_num.0 { // int
                    let sum = (ci as i64) + di;
                    if sum >= i32::MIN as i64 && sum <= i32::MAX as i64 {
                        set_path_nested(doc, path, bson::Bson::Int32(sum as i32));
                    } else {
                        set_path_nested(doc, path, bson::Bson::Int64(sum));
                    }
                    true
                } else if let Some(df) = delta_num.1 { // double
                    let sum = (ci as f64) + df;
                    set_path_nested(doc, path, bson::Bson::Double(sum));
                    true
                } else { false }
            }
            bson::Bson::Int64(cl) => {
                if let Some(di) = delta_num.0 { set_path_nested(doc, path, bson::Bson::Int64(cl + di)); true }
                else if let Some(df) = delta_num.1 { set_path_nested(doc, path, bson::Bson::Double((cl as f64) + df)); true }
                else { false }
            }
            bson::Bson::Double(cd) => {
                let add = if let Some(di) = delta_num.0 { di as f64 } else { delta_num.1.unwrap() };
                set_path_nested(doc, path, bson::Bson::Double(cd + add)); true
            }
            _ => false,
        }
    } else {
        // Missing: set to delta as-is, preferring numeric type given
        set_path_nested(doc, path, delta);
        true
    }
}

fn apply_push(doc: &mut Document, path: &str, val: bson::Bson) -> bool {
    // Support:
    // - { $push: { a: v }}
    // - { $push: { a: { $each: [v1, v2], $position: i?, $slice: n? } } }
    // Fetch current value
    let mut root = bson::Bson::Document(doc.clone());
    let segs: Vec<&str> = path.split('.').collect();
    // Navigate to parent array
    let (parent_path, last) = if segs.is_empty() { return false; } else { ( &segs[..segs.len()-1], segs[segs.len()-1]) };
    // Get parent
    let mut parent = &mut root;
    for seg in parent_path {
        set_path_bson(parent, &[seg], bson::Bson::Document(Document::new()));
        if let bson::Bson::Document(d) = parent { parent = d.get_mut(*seg).unwrap(); } else { return false; }
    }
    // Ensure array at last
    if let bson::Bson::Document(d) = parent {
        let entry = d.entry(last.to_string()).or_insert_with(|| bson::Bson::Array(Vec::new()));
        match entry {
            bson::Bson::Array(arr) => {
                if let bson::Bson::Document(spec) = val {
                    // $each required if using modifiers
                    let each = match spec.get_array("$each") { Ok(a) => a.clone(), Err(_) => return false };
                    let pos = spec.get_i32("$position").ok().unwrap_or(i32::MAX);
                    let slice = spec.get_i32("$slice").ok();
                    // Insert at position (clamped)
                    let mut insert_at = if pos < 0 { 0usize } else { pos as usize };
                    if insert_at > arr.len() { insert_at = arr.len(); }
                    arr.splice(insert_at..insert_at, each.into_iter());
                    // Apply slice trimming if provided
                    if let Some(n) = slice {
                        if n >= 0 {
                            let n = n as usize;
                            if arr.len() > n { arr.truncate(n); }
                        } else {
                            let n = (-n) as usize;
                            if arr.len() > n {
                                let remove = arr.len() - n;
                                arr.drain(0..remove);
                            }
                        }
                    }
                } else {
                    arr.push(val);
                }
            }
            _ => return false,
        }
    }
    if let bson::Bson::Document(updated) = root { *doc = updated; true } else { false }
}

fn apply_pull(doc: &mut Document, path: &str, criterion: bson::Bson) {
    let mut root = bson::Bson::Document(doc.clone());
    let segs: Vec<&str> = path.split('.').collect();
    let (parent_path, last) = if segs.is_empty() { return; } else { ( &segs[..segs.len()-1], segs[segs.len()-1]) };
    // Navigate to parent
    let mut parent = &mut root;
    for seg in parent_path {
        match parent {
            bson::Bson::Document(d) => {
                if !d.contains_key(*seg) { return; }
                parent = d.get_mut(*seg).unwrap();
            }
            _ => return,
        }
    }
    if let bson::Bson::Document(d) = parent {
        if let Some(bson::Bson::Array(arr)) = d.get_mut(last) {
            match criterion {
                bson::Bson::Array(ref vals) => {
                    arr.retain(|e| !vals.contains(e));
                }
                bson::Bson::Document(ref doc_crit) => {
                    arr.retain(|e| !matches_predicate(e, doc_crit));
                }
                ref v => {
                    arr.retain(|e| e != v);
                }
            }
        }
    }
    if let bson::Bson::Document(updated) = root { *doc = updated; }
}

fn matches_predicate(elem: &bson::Bson, crit: &Document) -> bool {
    // Support simple operators: $gt,$gte,$lt,$lte,$eq,$in
    for (op, val) in crit.iter() {
        match op.as_str() {
            "$gt" => if !cmp(elem, val, |a,b| a > b) { return false; },
            "$gte" => if !cmp(elem, val, |a,b| a >= b) { return false; },
            "$lt" => if !cmp(elem, val, |a,b| a < b) { return false; },
            "$lte" => if !cmp(elem, val, |a,b| a <= b) { return false; },
            "$eq" => if !cmp(elem, val, |a,b| a == b) { return false; },
            "$in" => {
                if let bson::Bson::Array(arr) = val {
                    let mut ok = false;
                    for v in arr {
                        if cmp(elem, v, |a,b| a == b) { ok = true; break; }
                    }
                    if !ok { return false; }
                } else { return false; }
            }
            _ => {}
        }
    }
    true
}

fn cmp<F: Fn(f64,f64)->bool>(a: &bson::Bson, b: &bson::Bson, f: F) -> bool {
    match (number_as_f64(a), number_as_f64(b)) {
        (Some(x), Some(y)) => f(x,y),
        _ => a == b,
    }
}

fn eval_expr(doc: &Document, v: &bson::Bson) -> Option<bson::Bson> {
    match v {
        bson::Bson::String(s) if s.starts_with('$') => {
            let path = &s[1..];
            get_path_bson_value(doc, path)
        }
        bson::Bson::Document(d) => {
            if let Some(arrs) = d.get_array("$concatArrays").ok() {
                let mut out: Vec<bson::Bson> = Vec::new();
                for op in arrs {
                    let ev = eval_expr(doc, op)?;
                    match ev {
                        bson::Bson::Array(mut a) => { out.append(&mut a); }
                        _ => return None,
                    }
                }
                return Some(bson::Bson::Array(out));
            }
            if let Some(arg) = d.get("$toString") {
                // Support either scalar or single-element array
                let val = if let bson::Bson::Array(arr) = arg { if arr.len() == 1 { eval_expr(doc, &arr[0]) } else { None } } else { eval_expr(doc, arg) };
                let val = val.unwrap_or(bson::Bson::Null);
                return Some(bson::Bson::String(bson_to_string(&val)));
            }
            if let Some(arg) = d.get("$toInt") {
                let val = eval_expr(doc, arg).unwrap_or(bson::Bson::Null);
                return to_int(&val);
            }
            if let Some(arg) = d.get("$toDouble") {
                let val = eval_expr(doc, arg).unwrap_or(bson::Bson::Null);
                return to_double(&val);
            }
            if let Some(arr) = d.get_array("$ifNull").ok() {
                if arr.len() == 2 {
                    let first = eval_expr(doc, &arr[0]);
                    match first {
                        Some(bson::Bson::Null) | None => {
                            let second = eval_expr(doc, &arr[1]).unwrap_or(bson::Bson::Null);
                            return Some(second);
                        }
                        Some(v) => { return Some(v); }
                    }
                }
            }
            if let Some(arr) = d.get_array("$add").ok() {
                let mut sum = 0.0f64;
                for op in arr {
                    let ev = eval_expr(doc, op)?;
                    let n = number_as_f64(&ev)?;
                    sum += n;
                }
                Some(bson::Bson::Double(sum))
            } else if let Some(arr) = d.get_array("$subtract").ok() {
                if arr.len() != 2 { return None; }
                let a = number_as_f64(&eval_expr(doc, &arr[0])?)?;
                let b = number_as_f64(&eval_expr(doc, &arr[1])?)?;
                Some(bson::Bson::Double(a - b))
            } else if let Some(arr) = d.get_array("$multiply").ok() {
                let mut prod = 1.0f64;
                for op in arr { prod *= number_as_f64(&eval_expr(doc, op)?)?; }
                Some(bson::Bson::Double(prod))
            } else if let Some(arr) = d.get_array("$divide").ok() {
                if arr.len() != 2 { return None; }
                let a = number_as_f64(&eval_expr(doc, &arr[0])?)?;
                let b = number_as_f64(&eval_expr(doc, &arr[1])?)?;
                Some(bson::Bson::Double(a / b))
            } else if let Some(arr) = d.get_array("$concat").ok() {
                let mut out = String::new();
                for op in arr {
                    let ev = eval_expr(doc, op).unwrap_or(bson::Bson::Null);
                    out.push_str(&bson_to_string(&ev));
                }
                Some(bson::Bson::String(out))
            } else {
                None
            }
        }
        _ => Some(v.clone()),
    }
}

fn bson_to_string(b: &bson::Bson) -> String {
    match b {
        bson::Bson::String(s) => s.clone(),
        bson::Bson::Int32(n) => n.to_string(),
        bson::Bson::Int64(n) => n.to_string(),
        bson::Bson::Double(f) => {
            let s = format!("{}", f);
            s
        }
        bson::Bson::Boolean(v) => if *v { "true".into() } else { "false".into() },
        bson::Bson::Null => String::new(),
        other => format!("{:?}", other),
    }
}

fn to_int(b: &bson::Bson) -> Option<bson::Bson> {
    match b {
        bson::Bson::Int32(n) => Some(bson::Bson::Int32(*n)),
        bson::Bson::Int64(n) => {
            if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 { Some(bson::Bson::Int32(*n as i32)) } else { Some(bson::Bson::Int64(*n)) }
        }
        bson::Bson::Double(f) => Some(bson::Bson::Int32(*f as i32)),
        bson::Bson::String(s) => s.parse::<i64>().ok().map(|n| {
            if n >= i32::MIN as i64 && n <= i32::MAX as i64 { bson::Bson::Int32(n as i32) } else { bson::Bson::Int64(n) }
        }),
        bson::Bson::Boolean(b) => Some(bson::Bson::Int32(if *b { 1 } else { 0 })),
        _ => None,
    }
}

fn to_double(b: &bson::Bson) -> Option<bson::Bson> {
    match b {
        bson::Bson::Int32(n) => Some(bson::Bson::Double(*n as f64)),
        bson::Bson::Int64(n) => Some(bson::Bson::Double(*n as f64)),
        bson::Bson::Double(f) => Some(bson::Bson::Double(*f)),
        bson::Bson::String(s) => s.parse::<f64>().ok().map(bson::Bson::Double),
        bson::Bson::Boolean(b) => Some(bson::Bson::Double(if *b { 1.0 } else { 0.0 })),
        _ => None,
    }
}

fn apply_project_with_expr(input: &Document, proj: &Document) -> Document {
    // If spec contains computed fields or includes, build output accordingly.
    let mut include_mode = false;
    let mut include_id = true;
    for (k, v) in proj.iter() {
        if k == "_id" {
            match v { bson::Bson::Int32(n) if *n == 0 => include_id = false, bson::Bson::Boolean(b) if !*b => include_id = false, _ => {} }
        } else if matches!(v, bson::Bson::Int32(n) if *n != 0) || matches!(v, bson::Bson::Boolean(true)) {
            include_mode = true;
        }
    }

    let mut out = Document::new();
    if include_mode && include_id {
        if let Some(idv) = input.get("_id").cloned() { out.insert("_id", idv); }
    }

    for (k, v) in proj.iter() {
        if k == "_id" { continue; }
        match v {
            bson::Bson::Int32(n) => {
                if *n != 0 {
                    if let Some(val) = get_path_bson_value(input, k) { set_path_nested(&mut out, k, val); }
                }
            }
            bson::Bson::Boolean(b) => {
                if *b { if let Some(val) = get_path_bson_value(input, k) { set_path_nested(&mut out, k, val); } }
            }
            other => {
                // computed expression
                let val = eval_expr(input, other).unwrap_or(bson::Bson::Null);
                out.insert(k.to_string(), val);
            }
        }
    }

    out
}

fn unset_path_bson(cur: &mut bson::Bson, segs: &[&str]) {
    if segs.is_empty() { return; }
    let seg = segs[0];
    let is_last = segs.len() == 1;
    match seg_is_index(seg) {
        Some(idx) => {
            if let bson::Bson::Array(arr) = cur {
                if idx < arr.len() {
                    if is_last { arr[idx] = bson::Bson::Null; }
                    else { unset_path_bson(&mut arr[idx], &segs[1..]); }
                }
            }
        }
        None => {
            if let bson::Bson::Document(d) = cur {
                if is_last { d.remove(seg); }
                else if let Some(child) = d.get_mut(seg) { unset_path_bson(child, &segs[1..]); }
            }
        }
    }
}

async fn delete_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db { Some(d) => d, None => return error_doc(59, "Missing $db") };
    let coll = match cmd.get_str("delete") { Ok(c) => c, Err(_) => return error_doc(9, "Invalid delete") };
    let deletes = match cmd.get_array("deletes") { Ok(a) => a, Err(_) => return error_doc(9, "Missing deletes") };
    if deletes.is_empty() { return error_doc(9, "Empty deletes"); }
    let first = match &deletes[0] { bson::Bson::Document(d) => d, _ => return error_doc(9, "Invalid delete spec") };
    let filter = match first.get_document("q") { Ok(d) => d.clone(), Err(_) => return error_doc(9, "Missing q") };
    let limit = first.get_i32("limit").unwrap_or(1);
    if state.store.is_none() { return error_doc(13, "No storage configured"); }
    let pg = state.store.as_ref().unwrap();
    if limit == 0 {
        match pg.delete_many_by_filter(dbname, coll, &filter).await {
            Ok(n) => doc!{"n": (n as i32), "ok": 1.0},
            Err(e) => error_doc(59, format!("delete failed: {}", e)),
        }
    } else if limit == 1 {
        match pg.delete_one_by_filter(dbname, coll, &filter).await {
            Ok(n) => doc!{"n": (n as i32), "ok": 1.0},
            Err(e) => error_doc(59, format!("delete failed: {}", e)),
        }
    } else {
        error_doc(2, "Only limit 0 (many) or 1 supported")
    }
}

async fn aggregate_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db { Some(d) => d, None => return error_doc(59, "Missing $db") };
    let coll = match cmd.get_str("aggregate") { Ok(c) => c, Err(_) => return error_doc(9, "Invalid aggregate") };
    let pipeline = match cmd.get_array("pipeline") { Ok(a) => a, Err(_) => return error_doc(9, "Missing pipeline") };
    let cursor_spec = cmd.get_document("cursor").unwrap_or(&doc!{}).clone();
    let batch_size = cursor_spec.get_i32("batchSize").unwrap_or(101) as i64;
    // Minimal pipeline support: $match, $sort, $project, $limit, $skip, $addFields/$set in order
    let mut filter: Option<bson::Document> = None;
    let mut sort: Option<bson::Document> = None;
    let mut project: Option<bson::Document> = None;
    let mut limit: Option<i64> = None;
    let mut skip: Option<i64> = None;
    let mut add_fields: Option<bson::Document> = None;
    let mut count_field: Option<String> = None;
    let mut computed_project = false;
    for st in pipeline {
        if let bson::Bson::Document(stage) = st {
            if let Ok(m) = stage.get_document("$match") { filter = Some(m.clone()); continue; }
            if let Ok(s) = stage.get_document("$sort") { sort = Some(s.clone()); continue; }
            if let Ok(p) = stage.get_document("$project") {
                computed_project = p.iter().any(|(_k, v)| !matches!(v, bson::Bson::Int32(n) if *n == 0 || *n == 1) && !matches!(v, bson::Bson::Boolean(b) if *b || !*b));
                project = Some(p.clone());
                continue;
            }
            if let Some(l) = stage.get_i64("$limit").ok().or(stage.get_i32("$limit").ok().map(|v| v as i64)) { limit = Some(l); continue; }
            if let Some(sk) = stage.get_i64("$skip").ok().or(stage.get_i32("$skip").ok().map(|v| v as i64)) { skip = Some(sk); continue; }
            if let Ok(af) = stage.get_document("$addFields").or(stage.get_document("$set")) { add_fields = Some(af.clone()); continue; }
            if let Ok(cf) = stage.get_str("$count") { count_field = Some(cf.to_string()); continue; }
            // Unknown $stage: error
            if stage.keys().any(|k| k.starts_with('$')) {
                return error_doc(9, format!("Unsupported pipeline stage: {}", stage.keys().next().unwrap()));
            }
        }
    }
    if state.store.is_none() { return error_doc(13, "No storage configured"); }
    let pg = state.store.as_ref().unwrap();
    // Handle $count early: compute count from filter and adjust with skip/limit
    if let Some(field) = count_field {
        let total = match pg.count_docs(dbname, coll, filter.as_ref()).await { Ok(n) => n, Err(e) => return error_doc(59, format!("aggregate failed: {}", e)) };
        let mut n = total;
        if let Some(sk) = skip { n = (n - sk).max(0); }
        if let Some(l) = limit { n = n.min(l); }
        let d = doc!{ field: n };
        return doc!{ "cursor": {"id": 0i64, "ns": format!("{}.{}", dbname, coll), "firstBatch": [d] }, "ok": 1.0 };
    }

    let fetch_limit = skip.unwrap_or(0) + limit.unwrap_or(batch_size * 10);
    // Disable projection pushdown if project has computed fields
    let pushdown_project = if computed_project { None } else { project.as_ref() };
    let mut docs = match pg.find_docs(dbname, coll, filter.as_ref(), sort.as_ref(), pushdown_project, fetch_limit).await {
        Ok(v) => v,
        Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
    };
    // Apply skip trimming if requested
    if let Some(sk) = skip { if sk as usize <= docs.len() { docs.drain(0..(sk as usize)); } else { docs.clear(); } }
    // Apply addFields/$set
    if let Some(af) = add_fields.as_ref() {
        for d in docs.iter_mut() {
            for (k, v) in af.iter() {
                let val = eval_expr(d, v).unwrap_or(bson::Bson::Null);
                set_path_nested(d, k, val);
            }
        }
    }

    // Apply project expressions if present and disable pushdown was used
    if let Some(ref proj) = project.as_ref() {
        if computed_project {
            for d in docs.iter_mut() {
                let out = apply_project_with_expr(d, proj);
                *d = out;
            }
        }
    }
    let (first_batch, remainder): (Vec<_>, Vec<_>) = docs.into_iter().enumerate().partition(|(i, _)| (*i as i64) < batch_size);
    let first_docs: Vec<Document> = first_batch.into_iter().map(|(_, d)| d).collect();
    let rest_docs: Vec<Document> = remainder.into_iter().map(|(_, d)| d).collect();
    let ns = format!("{}.{}", dbname, coll);
    let mut cursor_doc = doc!{ "ns": ns.clone(), "firstBatch": first_docs };
    let cursor_id = if rest_docs.is_empty() { 0i64 } else { new_cursor(state, ns, rest_docs).await };
    cursor_doc.insert("id", cursor_id);
    doc! { "cursor": cursor_doc, "ok": 1.0 }
}

async fn find_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db { Some(d) => d, None => return error_doc(59, "Missing $db") };
    let coll = match cmd.get_str("find") { Ok(c) => c, Err(_) => return error_doc(9, "Invalid find") };
    let limit = cmd.get_i64("limit").unwrap_or(0);
    let batch_size = cmd.get_i64("batchSize").unwrap_or(0);
    let first_batch_limit = if limit > 0 { limit } else if batch_size > 0 { batch_size } else { 101 };
    let filter = cmd.get_document("filter").ok();

    if let Some(ref pg) = state.store {
        let sort = cmd.get_document("sort").ok();
        let projection = cmd.get_document("projection").ok();
        let docs: Vec<Document> = if let Some(f) = filter {
            if let Some(idv) = f.get("_id") {
                if let Some(idb) = id_bytes_bson(idv) {
                    match pg.find_by_id_docs(dbname, coll, &idb, first_batch_limit * 10).await {
                        Ok(v) => v,
                        Err(e) => { tracing::warn!("find_by_id failed: {}", e); Vec::new() }
                    }
                } else {
                    match pg.find_docs(dbname, coll, Some(f), sort, projection, first_batch_limit * 10).await {
                        Ok(v) => v,
                        Err(e) => { tracing::warn!("find_docs failed: {}", e); Vec::new() }
                    }
                }
            } else {
                match pg.find_docs(dbname, coll, Some(f), sort, projection, first_batch_limit * 10).await {
                    Ok(v) => v,
                    Err(e) => { tracing::warn!("find_docs failed: {}", e); Vec::new() }
                }
            }
        } else {
            match pg.find_docs(dbname, coll, None, sort, projection, first_batch_limit * 10).await {
                Ok(v) => v,
                Err(e) => { tracing::warn!("find_docs failed: {}", e); Vec::new() }
            }
        };

        let mut first_batch: Vec<Document> = Vec::new();
        let mut remainder: Vec<Document> = Vec::new();
        for (idx, d) in docs.into_iter().enumerate() {
            if (idx as i64) < first_batch_limit { first_batch.push(d); } else { remainder.push(d); }
        }
        let ns = format!("{}.{}", dbname, coll);
        let mut cursor_doc = doc!{ "ns": ns.clone(), "firstBatch": first_batch };
        let cursor_id = if !remainder.is_empty() { new_cursor(state, ns, remainder).await } else { 0i64 };
        cursor_doc.insert("id", cursor_id);
        doc! { "cursor": cursor_doc, "ok": 1.0 }
    } else {
        error_doc(13, "No storage configured")
    }
}

fn ensure_id(doc: &mut Document) {
    if !doc.contains_key("_id") {
        doc.insert("_id", bson::Bson::ObjectId(bson::oid::ObjectId::new()));
    }
}

fn id_bytes(opt: Option<&bson::Bson>) -> Option<Vec<u8>> {
    match opt? {
        bson::Bson::ObjectId(oid) => Some(oid.bytes().to_vec()),
        bson::Bson::String(s) => Some(s.as_bytes().to_vec()),
        _ => None,
    }
}

fn id_bytes_bson(b: &bson::Bson) -> Option<Vec<u8>> {
    match b {
        bson::Bson::ObjectId(oid) => Some(oid.bytes().to_vec()),
        bson::Bson::String(s) => Some(s.as_bytes().to_vec()),
        _ => None,
    }
}

// removed: is_simple_equality_filter; find_with_top_level_filter handles equality and ops now

// (second duplicate removed)

static CURSOR_SEQ: AtomicI32 = AtomicI32::new(1000);

async fn new_cursor(state: &AppState, ns: String, docs: Vec<Document>) -> i64 {
    let id = CURSOR_SEQ.fetch_add(1, Ordering::Relaxed) as i64;
    let entry = CursorEntry { ns, docs, pos: 0, last_access: Instant::now() };
    let mut map = state.cursors.lock().await;
    map.insert(id, entry);
    id
}

async fn get_more_reply(state: &AppState, cmd: &Document) -> Document {
    let cursor_id = match cmd.get_i64("getMore") { Ok(v) => v, Err(_) => return error_doc(9, "Invalid getMore") };
    let batch_size = cmd.get_i32("batchSize").unwrap_or(101) as usize;
    let mut map = state.cursors.lock().await;
    if let Some(entry) = map.get_mut(&cursor_id) {
        let ns = entry.ns.clone();
        let start = entry.pos;
        let end = (start + batch_size).min(entry.docs.len());
        let mut next_batch = Vec::with_capacity(end - start);
        for d in &entry.docs[start..end] { next_batch.push(d.clone()); }
        entry.pos = end;
        entry.last_access = Instant::now();
        let id = if entry.pos >= entry.docs.len() { map.remove(&cursor_id); 0i64 } else { cursor_id };
        return doc! { "cursor": {"id": id, "ns": ns, "nextBatch": next_batch}, "ok": 1.0 };
    }
    // Unknown cursor id
    let ns = match cmd.get_str("collection") { Ok(c) => c.to_string(), Err(_) => String::new() };
    let empty: Vec<Document> = Vec::new();
    doc! { "cursor": {"id": 0i64, "ns": ns, "nextBatch": empty }, "ok": 1.0 }
}

async fn prune_cursors_once(state: &AppState, ttl: Duration) {
    let now = Instant::now();
    let mut map = state.cursors.lock().await;
    let before = map.len();
    map.retain(|_, e| now.duration_since(e.last_access) <= ttl);
    let after = map.len();
    if before != after {
        tracing::debug!(removed = before - after, remaining = after, "pruned idle cursors");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn prune_removes_idle_cursors() {
        let state = AppState {
            store: None,
            started_at: Instant::now(),
            cursors: Mutex::new(HashMap::new()),
            shadow: None,
            shadow_attempts: std::sync::atomic::AtomicU64::new(0),
            shadow_matches: std::sync::atomic::AtomicU64::new(0),
            shadow_mismatches: std::sync::atomic::AtomicU64::new(0),
            shadow_timeouts: std::sync::atomic::AtomicU64::new(0),
        };
        {
            let mut map = state.cursors.lock().await;
            map.insert(1, CursorEntry { ns: "db.coll".into(), docs: vec![], pos: 0, last_access: Instant::now() - Duration::from_secs(100) });
        }
        prune_cursors_once(&state, Duration::from_secs(10)).await;
        let map = state.cursors.lock().await;
        assert!(map.is_empty());
    }
}

async fn kill_cursors_reply(state: &AppState, cmd: &Document) -> Document {
    let cursors = match cmd.get_array("cursors") { Ok(a) => a, Err(_) => return error_doc(9, "Missing cursors") };
    let mut killed: Vec<i64> = Vec::new();
    let mut not_found: Vec<i64> = Vec::new();
    let mut map = state.cursors.lock().await;
    for b in cursors {
        let id_opt = match b {
            bson::Bson::Int64(v) => Some(*v),
            bson::Bson::Int32(v) => Some(*v as i64),
            _ => None,
        };
        if let Some(id) = id_opt {
            if map.remove(&id).is_some() { killed.push(id); } else { not_found.push(id); }
        }
    }
    doc!{
        "cursorsKilled": killed,
        "cursorsNotFound": not_found,
        "cursorsAlive": Vec::<i32>::new(),
        "cursorsUnknown": Vec::<i32>::new(),
        "ok": 1.0
    }
}

async fn create_indexes_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db { Some(d) => d, None => return error_doc(59, "Missing $db") };
    let coll = match cmd.get_str("createIndexes") { Ok(c) => c, Err(_) => return error_doc(9, "Invalid createIndexes") };
    let indexes = match cmd.get_array("indexes") { Ok(a) => a, Err(_) => return error_doc(9, "Missing indexes") };
    if state.store.is_none() { return error_doc(13, "No storage configured"); }
    let pg = state.store.as_ref().unwrap();
    let mut created = 0i32;
    for spec_b in indexes {
        let spec_doc = match spec_b { bson::Bson::Document(d) => d, _ => continue };
        let name = match spec_doc.get_str("name") { Ok(n) => n, Err(_) => continue };
        let key = match spec_doc.get_document("key") { Ok(k) => k, Err(_) => continue };
        let spec_json = match bson::to_bson(spec_doc) {
            Ok(b) => match serde_json::to_value(&b) { Ok(v) => v, Err(_) => serde_json::json!({}) },
            Err(_) => serde_json::json!({})
        };
        if key.len() == 1 {
            let (field, order_b) = key.iter().next().unwrap();
            let order = match order_b { bson::Bson::Int32(n) => *n, bson::Bson::Int64(n) => *n as i32, _ => 1 };
            if let Err(e) = pg.create_index_single_field(dbname, coll, name, field, order, &spec_json).await {
                tracing::warn!("create_index(single) failed: {}", e);
            } else { created += 1; }
        } else {
            let mut fields: Vec<(String, i32)> = Vec::with_capacity(key.len());
            for (k, v) in key.iter() {
                let ord = match v { bson::Bson::Int32(n) => *n, bson::Bson::Int64(n) => *n as i32, _ => 1 };
                fields.push((k.to_string(), ord));
            }
            if let Err(e) = pg.create_index_compound(dbname, coll, name, &fields, &spec_json).await {
                tracing::warn!("create_index(compound) failed: {}", e);
            } else { created += 1; }
        }
    }
    doc! { "createdIndexes": created, "ok": 1.0 }
}

async fn drop_indexes_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db { Some(d) => d, None => return error_doc(59, "Missing $db") };
    let coll = match cmd.get_str("dropIndexes") { Ok(c) => c, Err(_) => return error_doc(9, "Invalid dropIndexes") };
    if state.store.is_none() { return error_doc(13, "No storage configured"); }
    let pg = state.store.as_ref().unwrap();
    if let Ok(name) = cmd.get_str("index") {
        if name == "*" {
            // drop all metadata-managed indexes for the collection
            // fetch names
            // For simplicity, try to drop names known in metadata; ignore failure
            let names = pg.list_index_names(dbname, coll).await.unwrap_or_default();
            let mut dropped: Vec<String> = Vec::new();
            for n in names { if pg.drop_index(dbname, coll, &n).await.unwrap_or(false) { dropped.push(n); } }
            return doc! { "nIndexesWas": (dropped.len() as i32), "msg": "dropped metadata indexes", "ok": 1.0 };
        } else {
            let ok = pg.drop_index(dbname, coll, name).await.unwrap_or(false);
            return doc! { "nIndexesWas": (if ok { 1 } else { 0 }), "ok": 1.0 };
        }
    }
    error_doc(9, "Missing index name")
}
