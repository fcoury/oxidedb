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
        if upsert { return error_doc(9, "upsert not supported"); }
        let set_doc = udoc.get_document("$set").ok().cloned();
        let unset_doc = udoc.get_document("$unset").ok().cloned();
        if set_doc.is_none() && unset_doc.is_none() { return error_doc(9, "Only $set/$unset supported"); }

        if multi {
            // Fetch docs by filter and update each
            let docs = match pg.find_docs(dbname, coll, Some(&filter), None, None, 10_000).await {
                Ok(v) => v,
                Err(e) => return error_doc(59, format!("find failed: {}", e)),
            };
            if docs.is_empty() { continue; }
            for mut d in docs {
                let idb = match d.get("_id").and_then(|b| id_bytes_bson(b)) { Some(v) => v, None => continue };
                // apply nested $set
                if let Some(ref sets) = set_doc {
                    for (k, v) in sets.iter() { set_path_nested(&mut d, k, v.clone()); }
                }
                if let Some(ref unsets) = unset_doc {
                    for (k, _v) in unsets.iter() { unset_path_nested(&mut d, k); }
                }
                if let Err(e) = pg.update_doc_by_id(dbname, coll, &idb, &d).await {
                    tracing::warn!("update_doc_by_id failed: {}", e);
                } else {
                    matched_total += 1;
                    modified_total += 1;
                }
            }
        } else {
            // Single
            let found = match pg.find_one_for_update(dbname, coll, &filter).await {
                Ok(v) => v,
                Err(e) => return error_doc(59, format!("find failed: {}", e)),
            };
            if let Some((idb, mut doc0)) = found {
                if let Some(ref sets) = set_doc { for (k, v) in sets.iter() { set_path_nested(&mut doc0, k, v.clone()); } }
                if let Some(ref unsets) = unset_doc { for (k, _v) in unsets.iter() { unset_path_nested(&mut doc0, k); } }
                match pg.update_doc_by_id(dbname, coll, &idb, &doc0).await {
                    Ok(n) => {
                        matched_total += 1;
                        if n > 0 { modified_total += 1; }
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
        error_doc(9, "Only limit 0 (many) or 1 supported")
    }
}

async fn aggregate_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db { Some(d) => d, None => return error_doc(59, "Missing $db") };
    let coll = match cmd.get_str("aggregate") { Ok(c) => c, Err(_) => return error_doc(9, "Invalid aggregate") };
    let pipeline = match cmd.get_array("pipeline") { Ok(a) => a, Err(_) => return error_doc(9, "Missing pipeline") };
    let cursor_spec = cmd.get_document("cursor").unwrap_or(&doc!{}).clone();
    let batch_size = cursor_spec.get_i32("batchSize").unwrap_or(101) as i64;
    // Minimal pipeline support: $match, $sort, $project, $limit in order
    let mut filter: Option<bson::Document> = None;
    let mut sort: Option<bson::Document> = None;
    let mut project: Option<bson::Document> = None;
    let mut limit: Option<i64> = None;
    for st in pipeline {
        if let bson::Bson::Document(stage) = st {
            if let Ok(m) = stage.get_document("$match") { filter = Some(m.clone()); continue; }
            if let Ok(s) = stage.get_document("$sort") { sort = Some(s.clone()); continue; }
            if let Ok(p) = stage.get_document("$project") { project = Some(p.clone()); continue; }
            if let Some(l) = stage.get_i64("$limit").ok().or(stage.get_i32("$limit").ok().map(|v| v as i64)) { limit = Some(l); continue; }
        }
    }
    if state.store.is_none() { return error_doc(13, "No storage configured"); }
    let pg = state.store.as_ref().unwrap();
    let docs = match pg.find_docs(dbname, coll, filter.as_ref(), sort.as_ref(), project.as_ref(), limit.unwrap_or(batch_size * 10)).await {
        Ok(v) => v,
        Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
    };
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
