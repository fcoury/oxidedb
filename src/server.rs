use crate::config::{Config, ShadowConfig};
use crate::error::Result;
use crate::protocol::{
    MessageHeader, OP_MSG, OP_QUERY, decode_op_query, encode_op_msg, encode_op_reply,
};
use crate::session::{
    ERROR_ILLEGAL_OPERATION, ERROR_NO_SUCH_TRANSACTION, ERROR_TRANSACTION_EXPIRED, SessionManager,
};
use crate::shadow::{ShadowSession, compare_docs};
use crate::store::PgStore;
use bson::{Document, doc};
use rand::seq::SliceRandom;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

/// Specification for $bucket aggregation stage
#[derive(Clone, Debug)]
struct BucketSpec {
    group_by: String,
    boundaries: Vec<bson::Bson>,
    default: Option<bson::Bson>,
    output: Option<Document>,
}

static REQ_ID: AtomicI32 = AtomicI32::new(1);

use std::collections::HashMap;
use std::sync::Arc;
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
    cursors: Mutex<HashMap<i64, CursorEntry>>,
    pub shadow: Option<std::sync::Arc<ShadowConfig>>,
    // Shadow metrics
    pub shadow_attempts: std::sync::atomic::AtomicU64,
    pub shadow_matches: std::sync::atomic::AtomicU64,
    pub shadow_mismatches: std::sync::atomic::AtomicU64,
    pub shadow_timeouts: std::sync::atomic::AtomicU64,
    // Session management
    pub session_manager: std::sync::Arc<SessionManager>,
    // Production metrics
    pub request_count: AtomicU64,
    pub request_duration_ms: AtomicU64,
    pub query_count: AtomicU64,
    pub insert_count: AtomicU64,
    pub update_count: AtomicU64,
    pub delete_count: AtomicU64,
    pub error_count: AtomicU64,
    pub active_connections: AtomicU32,
}

impl AppState {
    /// Record a request with its duration
    pub fn record_request(&self, duration: Duration) {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.request_duration_ms
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    /// Record a query operation
    pub fn record_query(&self) {
        self.query_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an insert operation
    pub fn record_insert(&self) {
        self.insert_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an update operation
    pub fn record_update(&self) {
        self.update_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a delete operation
    pub fn record_delete(&self) {
        self.delete_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error
    pub fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment active connection count
    pub fn increment_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active connection count
    pub fn decrement_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get uptime in seconds
    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// Get average request duration in milliseconds
    pub fn avg_request_duration_ms(&self) -> f64 {
        let count = self.request_count.load(Ordering::Relaxed);
        let total_ms = self.request_duration_ms.load(Ordering::Relaxed);
        if count == 0 {
            0.0
        } else {
            total_ms as f64 / count as f64
        }
    }
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
                    session_manager: std::sync::Arc::new(SessionManager::new()),
                    request_count: AtomicU64::new(0),
                    request_duration_ms: AtomicU64::new(0),
                    query_count: AtomicU64::new(0),
                    insert_count: AtomicU64::new(0),
                    update_count: AtomicU64::new(0),
                    delete_count: AtomicU64::new(0),
                    error_count: AtomicU64::new(0),
                    active_connections: AtomicU32::new(0),
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
                    session_manager: std::sync::Arc::new(SessionManager::new()),
                    request_count: AtomicU64::new(0),
                    request_duration_ms: AtomicU64::new(0),
                    query_count: AtomicU64::new(0),
                    insert_count: AtomicU64::new(0),
                    update_count: AtomicU64::new(0),
                    delete_count: AtomicU64::new(0),
                    error_count: AtomicU64::new(0),
                    active_connections: AtomicU32::new(0),
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
            session_manager: std::sync::Arc::new(SessionManager::new()),
            request_count: AtomicU64::new(0),
            request_duration_ms: AtomicU64::new(0),
            query_count: AtomicU64::new(0),
            insert_count: AtomicU64::new(0),
            update_count: AtomicU64::new(0),
            delete_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            active_connections: AtomicU32::new(0),
        }
    };
    let state = Arc::new(state);

    // Create shutdown channel
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);
    let shutdown_tx = Arc::new(shutdown_tx);

    // Spawn signal handler for graceful shutdown
    let shutdown_tx_signal = shutdown_tx.clone();
    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to create SIGTERM handler");
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .expect("failed to create SIGINT handler");

        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!("received SIGTERM, initiating graceful shutdown");
            }
            _ = sigint.recv() => {
                tracing::info!("received SIGINT, initiating graceful shutdown");
            }
        }

        let _ = shutdown_tx_signal.send(());
    });

    // Spawn cursor sweeper with shutdown support
    let ttl = Duration::from_secs(cfg.cursor_timeout_secs.unwrap_or(300));
    let sweep_interval = Duration::from_secs(cfg.cursor_sweep_interval_secs.unwrap_or(30));
    let sweeper_state = state.clone();
    let mut sweeper_shutdown = shutdown_tx.subscribe();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(sweep_interval) => {
                    prune_cursors_once(&sweeper_state, ttl).await;
                }
                _ = sweeper_shutdown.recv() => {
                    tracing::debug!("cursor sweeper shutting down");
                    break;
                }
            }
        }
    });

    // Spawn session cleanup task with shutdown support
    let session_cleanup_state = state.clone();
    let mut session_cleanup_shutdown = shutdown_tx.subscribe();
    tokio::spawn(async move {
        let cleanup_interval = Duration::from_secs(60); // Check every minute
        loop {
            tokio::select! {
                _ = tokio::time::sleep(cleanup_interval) => {
                    let removed = session_cleanup_state
                        .session_manager
                        .cleanup_expired_sessions()
                        .await;
                    if removed > 0 {
                        tracing::debug!(removed_sessions = removed, "cleaned up expired sessions");
                    }
                }
                _ = session_cleanup_shutdown.recv() => {
                    tracing::debug!("session cleanup task shutting down");
                    break;
                }
            }
        }
    });

    // Accept loop with shutdown support
    let mut connection_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    loop {
        tokio::select! {
            res = listener.accept() => {
                let (socket, addr) = match res {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!(error = %format!("{e:?}"), "failed to accept connection");
                        continue;
                    }
                };
                tracing::debug!(%addr, "accepted connection");
                state.increment_connections();
                let state = state.clone();
                let mut shutdown_rx_conn = shutdown_tx.subscribe();
                let handle = tokio::spawn(async move {
                    tokio::select! {
                        res = handle_connection(state.clone(), socket) => {
                            if let Err(e) = res {
                                tracing::debug!(error = %format!("{e:?}"), "connection closed with error");
                            }
                        }
                        _ = shutdown_rx_conn.recv() => {
                            tracing::debug!("connection handler received shutdown signal");
                        }
                    }
                    state.decrement_connections();
                });
                connection_handles.push(handle);
            }
            _ = shutdown_rx.recv() => {
                tracing::info!("shutting down server, waiting for connections to complete");
                break;
            }
        }
    }

    // Wait for all connection handlers to complete
    for handle in connection_handles {
        let _ = handle.await;
    }

    tracing::info!("server shutdown complete");
    Ok(())
}

/// Spawn the server on the provided listen address and run until `shutdown` is signaled.
/// Returns the shared `AppState`, the bound local address, a shutdown sender, and the task handle.
pub async fn spawn_with_shutdown(
    cfg: Config,
) -> Result<(
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
                    session_manager: std::sync::Arc::new(SessionManager::new()),
                    request_count: AtomicU64::new(0),
                    request_duration_ms: AtomicU64::new(0),
                    query_count: AtomicU64::new(0),
                    insert_count: AtomicU64::new(0),
                    update_count: AtomicU64::new(0),
                    delete_count: AtomicU64::new(0),
                    error_count: AtomicU64::new(0),
                    active_connections: AtomicU32::new(0),
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
                    session_manager: std::sync::Arc::new(SessionManager::new()),
                    request_count: AtomicU64::new(0),
                    request_duration_ms: AtomicU64::new(0),
                    query_count: AtomicU64::new(0),
                    insert_count: AtomicU64::new(0),
                    update_count: AtomicU64::new(0),
                    delete_count: AtomicU64::new(0),
                    error_count: AtomicU64::new(0),
                    active_connections: AtomicU32::new(0),
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
            session_manager: std::sync::Arc::new(SessionManager::new()),
            request_count: AtomicU64::new(0),
            request_duration_ms: AtomicU64::new(0),
            query_count: AtomicU64::new(0),
            insert_count: AtomicU64::new(0),
            update_count: AtomicU64::new(0),
            delete_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            active_connections: AtomicU32::new(0),
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

    // Session cleanup task with shutdown
    let session_cleanup_state = state.clone();
    let mut session_cleanup_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        let cleanup_interval = Duration::from_secs(60); // Check every minute
        loop {
            tokio::select! {
                _ = tokio::time::sleep(cleanup_interval) => {
                    let removed = session_cleanup_state.session_manager.cleanup_expired_sessions().await;
                    if removed > 0 {
                        tracing::debug!(removed_sessions = removed, "cleaned up expired sessions");
                    }
                }
                _ = session_cleanup_shutdown.changed() => {
                    if *session_cleanup_shutdown.borrow() { break; }
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
    let shadow_session = state.shadow.as_ref().and_then(|cfg| {
        if cfg.enabled {
            Some(ShadowSession::new(cfg.clone()))
        } else {
            None
        }
    });
    loop {
        // Read header
        let mut header_buf = [0u8; 16];
        if let Err(e) = socket.read_exact(&mut header_buf).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof
                || e.kind() == std::io::ErrorKind::UnexpectedEof
            {
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
                let (reply_doc, cmd_opt) = match crate::protocol::decode_op_msg(&body) {
                    Some((_flags, mut cmd, seqs)) => {
                        // Merge any section-1 sequences into the command doc. If the command
                        // already has an array placeholder (e.g., documents: []), append to it.
                        for (name, docs) in seqs.into_iter() {
                            let seq_arr: Vec<bson::Bson> =
                                docs.into_iter().map(|d| bson::Bson::Document(d)).collect();
                            match cmd.get_mut(&name) {
                                Some(existing) => {
                                    if let bson::Bson::Array(a) = existing {
                                        a.extend(seq_arr);
                                    } else {
                                        cmd.insert(name.clone(), bson::Bson::Array(seq_arr));
                                    }
                                }
                                None => {
                                    cmd.insert(name.clone(), bson::Bson::Array(seq_arr));
                                }
                            }
                        }
                        let db = cmd.get_str("$db").ok().map(|s| s.to_string());
                        let cmd_name = cmd
                            .iter()
                            .next()
                            .map(|(k, _)| k.clone())
                            .unwrap_or_else(|| "".to_string());
                        tracing::debug!(command=%cmd_name, db=%db.as_deref().unwrap_or(""), cmd=?cmd, "received OP_MSG");
                        (
                            handle_command(&state, db.as_deref(), cmd.clone()).await,
                            Some(cmd),
                        )
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
                    let cmd_name = cmd_opt
                        .as_ref()
                        .and_then(|d| d.iter().next().map(|(k, _)| k.clone()))
                        .unwrap_or_else(|| "".to_string());
                    let db = cmd_opt
                        .as_ref()
                        .and_then(|d| d.get_str("$db").ok())
                        .unwrap_or("");
                    let sample = if cfg.deterministic_sampling {
                        crate::config::should_sample_deterministically(
                            hdr.request_id,
                            db,
                            cfg.sample_rate,
                        )
                    } else {
                        rand::random::<f64>() < cfg.sample_rate
                    };
                    if sample {
                        let sh = sh.clone();
                        let state2 = state.clone();
                        tokio::spawn(async move {
                            let start = Instant::now();
                            let res = tokio::time::timeout(
                                Duration::from_millis(cfg.timeout_ms),
                                sh.forward_and_read_doc(&hdr, &header_bytes, &body_bytes),
                            )
                            .await;
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
                                            state2
                                                .shadow_mismatches
                                                .fetch_add(1, Ordering::Relaxed);
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
                        let cmd_name = cmd
                            .iter()
                            .next()
                            .map(|(k, _)| k.clone())
                            .unwrap_or_else(|| "".to_string());
                        tracing::debug!(command=%cmd_name, db=%db.as_deref().unwrap_or(""), cmd=?cmd, "received OP_QUERY");
                        let reply_doc = handle_command(&state, db.as_deref(), cmd).await;
                        let request_id = REQ_ID.fetch_add(1, Ordering::Relaxed);
                        let resp =
                            encode_op_reply(&[reply_doc.clone()], hdr.request_id, request_id);
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
                                    let res = tokio::time::timeout(
                                        Duration::from_millis(cfg.timeout_ms),
                                        sh.forward_and_read_doc(&hdr, &header_bytes, &body_bytes),
                                    )
                                    .await;
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
                                                state2
                                                    .shadow_attempts
                                                    .fetch_add(1, Ordering::Relaxed);
                                                let diff =
                                                    compare_docs(&ours, &up_doc, &cfg.compare);
                                                if diff.matched {
                                                    state2
                                                        .shadow_matches
                                                        .fetch_add(1, Ordering::Relaxed);
                                                    tracing::debug!(command=%cmd_name, elapsed_ms=%start.elapsed().as_millis(), "shadow match");
                                                } else {
                                                    state2
                                                        .shadow_mismatches
                                                        .fetch_add(1, Ordering::Relaxed);
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
        "findAndModify" | "findandmodify" => find_and_modify_reply(state, db, &cmd).await,
        "aggregate" => aggregate_reply(state, db, &cmd).await,
        "find" => find_reply(state, db, &cmd).await,
        "getMore" => get_more_reply(state, &cmd).await,
        "createIndexes" => create_indexes_reply(state, db, &cmd).await,
        "dropIndexes" => drop_indexes_reply(state, db, &cmd).await,
        "killCursors" => kill_cursors_reply(state, &cmd).await,
        "oxidedbShadowMetrics" => shadow_metrics_reply(state).await,
        "oxidedbMetrics" => {
            let metrics_text = metrics_reply(state).await;
            doc! { "metrics": metrics_text, "ok": 1.0 }
        }
        "startTransaction" => start_transaction_reply(state, db, &cmd).await,
        "commitTransaction" => commit_transaction_reply(state, db, &cmd).await,
        "abortTransaction" => abort_transaction_reply(state, db, &cmd).await,
        "endSessions" => end_sessions_reply(state, &cmd).await,
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
        // Advertise compatibility for MongoDB 4.2+ clients
        // Node.js driver v6 requires >= 8
        "maxWireVersion": 8i32,
        "maxBsonObjectSize": 16_777_216i32, // 16MB
        "maxMessageSizeBytes": 48_000_000i32,
        "maxWriteBatchSize": 100_000i32,
        "logicalSessionTimeoutMinutes": 30i32,
        "ok": 1.0
    }
}

#[cfg(test)]
mod hello_tests {
    use super::hello_reply;

    #[test]
    fn advertises_wire_version_8() {
        let d = hello_reply();
        assert_eq!(d.get_i32("maxWireVersion").unwrap(), 8);
        assert!(d.get_bool("helloOk").unwrap_or(false));
        assert!(d.get_bool("isWritablePrimary").unwrap_or(false));
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
    if !name_only {
        reply.insert("totalSize", 0i64);
    }
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

async fn shadow_metrics_reply(state: &AppState) -> Document {
    let attempts = state
        .shadow_attempts
        .load(std::sync::atomic::Ordering::Relaxed);
    let matches = state
        .shadow_matches
        .load(std::sync::atomic::Ordering::Relaxed);
    let mismatches = state
        .shadow_mismatches
        .load(std::sync::atomic::Ordering::Relaxed);
    let timeouts = state
        .shadow_timeouts
        .load(std::sync::atomic::Ordering::Relaxed);

    doc! {
        "shadow": {
            "attempts": attempts as i64,
            "matches": matches as i64,
            "mismatches": mismatches as i64,
            "timeouts": timeouts as i64,
        },
        "ok": 1.0
    }
}

/// Generate Prometheus-formatted metrics
async fn metrics_reply(state: &AppState) -> String {
    let requests = state.request_count.load(Ordering::Relaxed);
    let duration_ms = state.request_duration_ms.load(Ordering::Relaxed);
    let queries = state.query_count.load(Ordering::Relaxed);
    let inserts = state.insert_count.load(Ordering::Relaxed);
    let updates = state.update_count.load(Ordering::Relaxed);
    let deletes = state.delete_count.load(Ordering::Relaxed);
    let errors = state.error_count.load(Ordering::Relaxed);
    let active_conn = state.active_connections.load(Ordering::Relaxed);
    let uptime = state.uptime_seconds();

    // Shadow metrics
    let shadow_attempts = state.shadow_attempts.load(Ordering::Relaxed);
    let shadow_matches = state.shadow_matches.load(Ordering::Relaxed);
    let shadow_mismatches = state.shadow_mismatches.load(Ordering::Relaxed);
    let shadow_timeouts = state.shadow_timeouts.load(Ordering::Relaxed);

    format!(
        "# HELP oxidedb_requests_total Total number of requests\n\
         # TYPE oxidedb_requests_total counter\n\
         oxidedb_requests_total {}\n\n\
         # HELP oxidedb_request_duration_seconds Total request duration in seconds\n\
         # TYPE oxidedb_request_duration_seconds counter\n\
         oxidedb_request_duration_seconds {}\n\n\
         # HELP oxidedb_queries_total Total number of queries\n\
         # TYPE oxidedb_queries_total counter\n\
         oxidedb_queries_total {}\n\n\
         # HELP oxidedb_inserts_total Total number of inserts\n\
         # TYPE oxidedb_inserts_total counter\n\
         oxidedb_inserts_total {}\n\n\
         # HELP oxidedb_updates_total Total number of updates\n\
         # TYPE oxidedb_updates_total counter\n\
         oxidedb_updates_total {}\n\n\
         # HELP oxidedb_deletes_total Total number of deletes\n\
         # TYPE oxidedb_deletes_total counter\n\
         oxidedb_deletes_total {}\n\n\
         # HELP oxidedb_errors_total Total number of errors\n\
         # TYPE oxidedb_errors_total counter\n\
         oxidedb_errors_total {}\n\n\
         # HELP oxidedb_active_connections Current number of active connections\n\
         # TYPE oxidedb_active_connections gauge\n\
         oxidedb_active_connections {}\n\n\
         # HELP oxidedb_uptime_seconds Server uptime in seconds\n\
         # TYPE oxidedb_uptime_seconds gauge\n\
         oxidedb_uptime_seconds {}\n\n\
         # HELP oxidedb_shadow_attempts_total Total shadow comparison attempts\n\
         # TYPE oxidedb_shadow_attempts_total counter\n\
         oxidedb_shadow_attempts_total {}\n\n\
         # HELP oxidedb_shadow_matches_total Total shadow matches\n\
         # TYPE oxidedb_shadow_matches_total counter\n\
         oxidedb_shadow_matches_total {}\n\n\
         # HELP oxidedb_shadow_mismatches_total Total shadow mismatches\n\
         # TYPE oxidedb_shadow_mismatches_total counter\n\
         oxidedb_shadow_mismatches_total {}\n\n\
         # HELP oxidedb_shadow_timeouts_total Total shadow timeouts\n\
         # TYPE oxidedb_shadow_timeouts_total counter\n\
         oxidedb_shadow_timeouts_total {}\n",
        requests,
        duration_ms as f64 / 1000.0,
        queries,
        inserts,
        updates,
        deletes,
        errors,
        active_conn,
        uptime,
        shadow_attempts,
        shadow_matches,
        shadow_mismatches,
        shadow_timeouts,
    )
}

async fn create_collection_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db {
        Some(d) => d,
        None => return error_doc(59, "Missing $db"),
    };
    let coll = match cmd.get_str("create") {
        Ok(c) => c,
        Err(_) => return error_doc(9, "Invalid create"),
    };
    if let Some(ref pg) = state.store {
        match pg.ensure_collection(dbname, coll).await {
            Ok(_) => doc! { "ok": 1.0 },
            Err(e) => error_doc(59, format!("create failed: {}", e)),
        }
    } else {
        error_doc(13, "No storage configured")
    }
}

async fn drop_collection_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db {
        Some(d) => d,
        None => return error_doc(59, "Missing $db"),
    };
    let coll = match cmd.get_str("drop") {
        Ok(c) => c,
        Err(_) => return error_doc(9, "Invalid drop"),
    };
    if let Some(ref pg) = state.store {
        match pg.drop_collection(dbname, coll).await {
            Ok(_) => doc! { "nIndexesWas": 0i32, "ns": format!("{}.{}", dbname, coll), "ok": 1.0 },
            Err(e) => error_doc(59, format!("drop failed: {}", e)),
        }
    } else {
        error_doc(13, "No storage configured")
    }
}

async fn drop_database_reply(state: &AppState, db: Option<&str>) -> Document {
    let dbname = match db {
        Some(d) => d,
        None => return error_doc(59, "Missing $db"),
    };
    if let Some(ref pg) = state.store {
        match pg.drop_database(dbname).await {
            Ok(_) => doc! { "dropped": dbname, "ok": 1.0 },
            Err(e) => error_doc(59, format!("dropDatabase failed: {}", e)),
        }
    } else {
        error_doc(13, "No storage configured")
    }
}

async fn insert_reply(state: &AppState, db: Option<&str>, cmd: &mut Document) -> Document {
    let dbname = match db {
        Some(d) => d,
        None => return error_doc(59, "Missing $db"),
    };
    let coll: String = match cmd.get_str("insert") {
        Ok(c) => c.to_string(),
        Err(_) => return error_doc(9, "Invalid insert"),
    };
    let docs_bson: Vec<bson::Bson> = match cmd.get_array("documents") {
        Ok(a) => a.clone(),
        Err(_) => {
            tracing::warn!(collection=%coll, cmd=?cmd, "insert missing 'documents' array");
            return error_doc(9, "Missing documents");
        }
    };
    if let Some(ref pg) = state.store {
        // Check if we're in a transaction
        let in_transaction = if let Some(lsid) = extract_lsid(cmd) {
            if let Some(autocommit) = extract_autocommit(cmd) {
                if !autocommit {
                    if let Some(session_arc) = state.session_manager.get_session(lsid).await {
                        let session = session_arc.lock().await;
                        session.in_transaction
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        let mut inserted = 0u32;
        let mut write_errors: Vec<Document> = Vec::new();

        if in_transaction {
            // In transaction - get session and perform all inserts with transaction client
            if let Some(lsid) = extract_lsid(cmd) {
                if let Some(session_arc) = state.session_manager.get_session(lsid).await {
                    let session = session_arc.lock().await;
                    if let Some(ref client) = session.postgres_client {
                        for (i, b) in docs_bson.iter().enumerate() {
                            if let bson::Bson::Document(d0) = b {
                                let mut d = d0.clone();
                                ensure_id(&mut d);
                                match id_bytes(d.get("_id")) {
                                    Some(idb) => {
                                        let json = match serde_json::to_value(&d) {
                                            Ok(v) => v,
                                            Err(e) => {
                                                write_errors.push(
                                                    doc! {"index": i as i32, "code": 2, "errmsg": e.to_string()},
                                                );
                                                continue;
                                            }
                                        };
                                        let bson_bytes = match bson::to_vec(&d) {
                                            Ok(v) => v,
                                            Err(e) => {
                                                write_errors.push(
                                                    doc! {"index": i as i32, "code": 2, "errmsg": e.to_string()},
                                                );
                                                continue;
                                            }
                                        };

                                        match pg.insert_one_with_client(
                                            client,
                                            dbname,
                                            &coll,
                                            &idb,
                                            &bson_bytes,
                                            &json,
                                        ).await {
                                            Ok(n) => {
                                                if n == 1 {
                                                    inserted += 1;
                                                } else {
                                                    write_errors.push(doc!{"index": i as i32, "code": 11000i32, "errmsg": "duplicate key"});
                                                }
                                            }
                                            Err(e) => write_errors.push(
                                                doc! {"index": i as i32, "code": 59i32, "errmsg": e.to_string()},
                                            ),
                                        }
                                    }
                                    None => write_errors.push(
                                        doc! {"index": i as i32, "code": 2i32, "errmsg": "unsupported _id type"},
                                    ),
                                }
                            } else {
                                write_errors.push(
                                    doc! {"index": i as i32, "code": 2i32, "errmsg": "document must be object"},
                                );
                            }
                        }
                    }
                }
            }
        } else {
            // Not in transaction - use pool
            for (i, b) in docs_bson.iter().enumerate() {
                if let bson::Bson::Document(d0) = b {
                    let mut d = d0.clone();
                    ensure_id(&mut d);
                    match id_bytes(d.get("_id")) {
                        Some(idb) => {
                            let json = match serde_json::to_value(&d) {
                                Ok(v) => v,
                                Err(e) => {
                                    write_errors.push(
                                        doc! {"index": i as i32, "code": 2, "errmsg": e.to_string()},
                                    );
                                    continue;
                                }
                            };
                            let bson_bytes = match bson::to_vec(&d) {
                                Ok(v) => v,
                                Err(e) => {
                                    write_errors.push(
                                        doc! {"index": i as i32, "code": 2, "errmsg": e.to_string()},
                                    );
                                    continue;
                                }
                            };

                            match pg.insert_one(dbname, &coll, &idb, &bson_bytes, &json).await {
                                Ok(n) => {
                                    if n == 1 {
                                        inserted += 1;
                                    } else {
                                        write_errors.push(doc!{"index": i as i32, "code": 11000i32, "errmsg": "duplicate key"});
                                    }
                                }
                                Err(e) => write_errors.push(
                                    doc! {"index": i as i32, "code": 59i32, "errmsg": e.to_string()},
                                ),
                            }
                        }
                        None => write_errors.push(
                            doc! {"index": i as i32, "code": 2i32, "errmsg": "unsupported _id type"},
                        ),
                    }
                } else {
                    write_errors.push(
                        doc! {"index": i as i32, "code": 2i32, "errmsg": "document must be object"},
                    );
                }
            }
        }

        let mut reply = doc! { "n": inserted as i32, "ok": 1.0 };
        if !write_errors.is_empty() {
            reply.insert("writeErrors", write_errors);
        }
        reply
    } else {
        error_doc(13, "No storage configured")
    }
}

async fn update_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db {
        Some(d) => d,
        None => return error_doc(59, "Missing $db"),
    };
    let coll = match cmd.get_str("update") {
        Ok(c) => c,
        Err(_) => return error_doc(9, "Invalid update"),
    };
    let updates = match cmd.get_array("updates") {
        Ok(a) => a,
        Err(_) => {
            tracing::warn!(collection=%coll, cmd=?cmd, "update missing 'updates' array");
            return error_doc(9, "Missing updates");
        }
    };
    if updates.is_empty() {
        return error_doc(9, "Empty updates");
    }
    if state.store.is_none() {
        return error_doc(13, "No storage configured");
    }
    let pg = state.store.as_ref().unwrap();

    let mut matched_total = 0i32;
    let mut modified_total = 0i32;

    let mut upserted_entries: Vec<Document> = Vec::new();
    for (spec_index, upd_b) in updates.iter().enumerate() {
        let spec = match upd_b {
            bson::Bson::Document(d) => d,
            _ => return error_doc(9, "Invalid update spec"),
        };
        let filter = match spec.get_document("q") {
            Ok(d) => d.clone(),
            Err(_) => return error_doc(9, "Missing q"),
        };
        let udoc = match spec.get_document("u") {
            Ok(d) => d.clone(),
            Err(_) => return error_doc(9, "Missing u"),
        };
        let multi = spec.get_bool("multi").unwrap_or(false);
        let upsert = spec.get_bool("upsert").unwrap_or(false);
        let set_doc = udoc.get_document("$set").ok().cloned();
        let unset_doc = udoc.get_document("$unset").ok().cloned();
        let inc_doc = udoc.get_document("$inc").ok().cloned();
        let rename_doc = udoc.get_document("$rename").ok().cloned();
        let push_doc = udoc.get_document("$push").ok().cloned();
        let pull_doc = udoc.get_document("$pull").ok().cloned();
        if set_doc.is_none()
            && unset_doc.is_none()
            && inc_doc.is_none()
            && rename_doc.is_none()
            && push_doc.is_none()
            && pull_doc.is_none()
        {
            return error_doc(9, "Only $set/$unset/$inc/$rename/$push/$pull supported");
        }

        // Validate array index segments (disallow negative indexes)
        if let Some(ref sets) = set_doc {
            for (k, _v) in sets.iter() {
                if path_has_negative_index(k) {
                    return error_doc(2, "Negative array indexes not supported");
                }
            }
        }
        if let Some(ref unsets) = unset_doc {
            for (k, _v) in unsets.iter() {
                if path_has_negative_index(k) {
                    return error_doc(2, "Negative array indexes not supported");
                }
            }
        }
        if let Some(ref incs) = inc_doc {
            for (k, v) in incs.iter() {
                if path_has_negative_index(k) {
                    return error_doc(2, "Negative array indexes not supported");
                }
                if !matches!(
                    v,
                    bson::Bson::Int32(_) | bson::Bson::Int64(_) | bson::Bson::Double(_)
                ) {
                    return error_doc(2, "$inc requires numeric value");
                }
            }
        }
        if let Some(ref ren) = rename_doc {
            let mut pairs: Vec<(String, String)> = Vec::new();
            for (from, to_b) in ren.iter() {
                if path_has_negative_index(from) {
                    return error_doc(2, "Negative array indexes not supported");
                }
                let to = match to_b {
                    bson::Bson::String(s) => s,
                    _ => return error_doc(2, "$rename target must be string path"),
                };
                if path_has_negative_index(to) {
                    return error_doc(2, "Negative array indexes not supported");
                }
                pairs.push((from.to_string(), to.to_string()));
            }
            if let Some(err) = validate_rename_pairs(&pairs) {
                return err;
            }
        }
        if let Some(ref pushes) = push_doc {
            for (k, _v) in pushes.iter() {
                if path_has_negative_index(k) {
                    return error_doc(2, "Negative array indexes not supported");
                }
            }
        }
        if let Some(ref pulls) = pull_doc {
            for (k, _v) in pulls.iter() {
                if path_has_negative_index(k) {
                    return error_doc(2, "Negative array indexes not supported");
                }
            }
        }

        if multi {
            // Fetch docs by filter and update each
            let docs = match pg
                .find_docs(dbname, coll, Some(&filter), None, None, 10_000)
                .await
            {
                Ok(v) => v,
                Err(e) => return error_doc(59, format!("find failed: {}", e)),
            };
            if docs.is_empty() {
                if upsert {
                    // Upsert: insert one synthesized document
                    if let Err(e) = pg.ensure_collection(dbname, coll).await {
                        return error_doc(59, format!("ensure_collection failed: {}", e));
                    }
                    let mut new_doc = bson::Document::new();
                    for (k, v) in filter.iter() {
                        match v {
                            bson::Bson::Document(d) => {
                                if let Some(eqv) = d.get("$eq") {
                                    new_doc.insert(k.clone(), eqv.clone());
                                }
                            }
                            other => {
                                new_doc.insert(k.clone(), other.clone());
                            }
                        }
                    }
                    if let Some(ref sets) = set_doc {
                        for (k, v) in sets.iter() {
                            set_path_nested(&mut new_doc, k, v.clone());
                        }
                    }
                    if let Some(ref unsets) = unset_doc {
                        for (k, _v) in unsets.iter() {
                            unset_path_nested(&mut new_doc, k);
                        }
                    }
                    if let Some(ref incs) = inc_doc {
                        for (k, v) in incs.iter() {
                            if !apply_inc(&mut new_doc, k, v.clone()) {
                                return error_doc(2, "$inc requires numeric field");
                            }
                        }
                    }
                    if let Some(ref ren) = rename_doc {
                        for (from, to_b) in ren.iter() {
                            if let bson::Bson::String(to) = to_b {
                                apply_rename(&mut new_doc, from, to);
                            }
                        }
                    }
                    if let Some(ref pushes) = push_doc {
                        for (k, v) in pushes.iter() {
                            if !apply_push(&mut new_doc, k, v.clone()) {
                                return error_doc(2, "$push on non-array");
                            }
                        }
                    }
                    if let Some(ref pulls) = pull_doc {
                        for (k, v) in pulls.iter() {
                            apply_pull(&mut new_doc, k, v.clone());
                        }
                    }
                    ensure_id(&mut new_doc);
                    let idb = match new_doc.get("_id").and_then(|b| id_bytes_bson(b)) {
                        Some(v) => v,
                        None => return error_doc(2, "unsupported _id type"),
                    };
                    let json = match serde_json::to_value(&new_doc) {
                        Ok(v) => v,
                        Err(e) => return error_doc(2, e.to_string()),
                    };
                    let bson_bytes = match bson::to_vec(&new_doc) {
                        Ok(v) => v,
                        Err(e) => return error_doc(2, e.to_string()),
                    };
                    match pg.insert_one(dbname, coll, &idb, &bson_bytes, &json).await {
                        Ok(n) => {
                            if n == 1 {
                                matched_total += 1;
                                upserted_entries.push(doc!{"index": (spec_index as i32), "_id": new_doc.get("_id").cloned().unwrap_or(bson::Bson::Null)});
                            }
                        }
                        Err(e) => return error_doc(59, format!("insert failed: {}", e)),
                    }
                }
                continue;
            }
            for mut d in docs {
                let idb = match d.get("_id").and_then(|b| id_bytes_bson(b)) {
                    Some(v) => v,
                    None => continue,
                };
                // remember original
                let orig = d.clone();
                // $set
                if let Some(ref sets) = set_doc {
                    for (k, v) in sets.iter() {
                        set_path_nested(&mut d, k, v.clone());
                    }
                }
                // $unset
                if let Some(ref unsets) = unset_doc {
                    for (k, _v) in unsets.iter() {
                        unset_path_nested(&mut d, k);
                    }
                }
                // $inc
                if let Some(ref incs) = inc_doc {
                    for (k, v) in incs.iter() {
                        if !apply_inc(&mut d, k, v.clone()) {
                            continue;
                        }
                    }
                }
                // $rename
                if let Some(ref ren) = rename_doc {
                    for (from, to_b) in ren.iter() {
                        if let bson::Bson::String(to) = to_b {
                            apply_rename(&mut d, from, to);
                        }
                    }
                }
                // $push
                if let Some(ref pushes) = push_doc {
                    for (k, v) in pushes.iter() {
                        if !apply_push(&mut d, k, v.clone()) {
                            return error_doc(2, "$push on non-array");
                        }
                    }
                }
                // $pull
                if let Some(ref pulls) = pull_doc {
                    for (k, v) in pulls.iter() {
                        apply_pull(&mut d, k, v.clone());
                    }
                }
                if let Err(e) = pg.update_doc_by_id(dbname, coll, &idb, &d).await {
                    tracing::warn!("update_doc_by_id failed: {}", e);
                } else {
                    matched_total += 1;
                    if d != orig {
                        modified_total += 1;
                    }
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
                if let Some(ref sets) = set_doc {
                    for (k, v) in sets.iter() {
                        set_path_nested(&mut doc0, k, v.clone());
                    }
                }
                if let Some(ref unsets) = unset_doc {
                    for (k, _v) in unsets.iter() {
                        unset_path_nested(&mut doc0, k);
                    }
                }
                if let Some(ref incs) = inc_doc {
                    for (k, v) in incs.iter() {
                        if !apply_inc(&mut doc0, k, v.clone()) {
                            return error_doc(2, "$inc requires numeric field");
                        }
                    }
                }
                if let Some(ref ren) = rename_doc {
                    for (from, to_b) in ren.iter() {
                        if let bson::Bson::String(to) = to_b {
                            apply_rename(&mut doc0, from, to);
                        }
                    }
                }
                if let Some(ref pushes) = push_doc {
                    for (k, v) in pushes.iter() {
                        if !apply_push(&mut doc0, k, v.clone()) {
                            return error_doc(2, "$push on non-array");
                        }
                    }
                }
                if let Some(ref pulls) = pull_doc {
                    for (k, v) in pulls.iter() {
                        apply_pull(&mut doc0, k, v.clone());
                    }
                }
                match pg.update_doc_by_id(dbname, coll, &idb, &doc0).await {
                    Ok(_n) => {
                        matched_total += 1;
                        if doc0 != orig {
                            modified_total += 1;
                        }
                    }
                    Err(e) => return error_doc(59, format!("update failed: {}", e)),
                }
            } else if upsert {
                if let Err(e) = pg.ensure_collection(dbname, coll).await {
                    return error_doc(59, format!("ensure_collection failed: {}", e));
                }
                let mut new_doc = bson::Document::new();
                for (k, v) in filter.iter() {
                    match v {
                        bson::Bson::Document(d) => {
                            if let Some(eqv) = d.get("$eq") {
                                new_doc.insert(k.clone(), eqv.clone());
                            }
                        }
                        other => {
                            new_doc.insert(k.clone(), other.clone());
                        }
                    }
                }
                if let Some(ref sets) = set_doc {
                    for (k, v) in sets.iter() {
                        set_path_nested(&mut new_doc, k, v.clone());
                    }
                }
                if let Some(ref unsets) = unset_doc {
                    for (k, _v) in unsets.iter() {
                        unset_path_nested(&mut new_doc, k);
                    }
                }
                if let Some(ref incs) = inc_doc {
                    for (k, v) in incs.iter() {
                        if !apply_inc(&mut new_doc, k, v.clone()) {
                            return error_doc(2, "$inc requires numeric field");
                        }
                    }
                }
                if let Some(ref ren) = rename_doc {
                    for (from, to_b) in ren.iter() {
                        if let bson::Bson::String(to) = to_b {
                            apply_rename(&mut new_doc, from, to);
                        }
                    }
                }
                if let Some(ref pushes) = push_doc {
                    for (k, v) in pushes.iter() {
                        if !apply_push(&mut new_doc, k, v.clone()) {
                            return error_doc(2, "$push on non-array");
                        }
                    }
                }
                if let Some(ref pulls) = pull_doc {
                    for (k, v) in pulls.iter() {
                        apply_pull(&mut new_doc, k, v.clone());
                    }
                }
                ensure_id(&mut new_doc);
                let idb = match new_doc.get("_id").and_then(|b| id_bytes_bson(b)) {
                    Some(v) => v,
                    None => return error_doc(2, "unsupported _id type"),
                };
                let json = match serde_json::to_value(&new_doc) {
                    Ok(v) => v,
                    Err(e) => return error_doc(2, e.to_string()),
                };
                let bson_bytes = match bson::to_vec(&new_doc) {
                    Ok(v) => v,
                    Err(e) => return error_doc(2, e.to_string()),
                };
                match pg.insert_one(dbname, coll, &idb, &bson_bytes, &json).await {
                    Ok(n) => {
                        if n == 1 {
                            matched_total += 1; // emulate n=1 for upsert
                            upserted_entries.push(doc!{"index": (spec_index as i32), "_id": new_doc.get("_id").cloned().unwrap_or(bson::Bson::Null)});
                        }
                    }
                    Err(e) => return error_doc(59, format!("insert failed: {}", e)),
                }
            }
        }
    }
    let mut reply = doc! {"n": matched_total, "nModified": modified_total, "ok": 1.0};
    if !upserted_entries.is_empty() {
        reply.insert("upserted", upserted_entries);
    }
    reply
}

async fn find_and_modify_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db {
        Some(d) => d,
        None => return error_doc(59, "Missing $db"),
    };
    let coll = match cmd.get_str("findAndModify") {
        Ok(c) => c,
        Err(_) => return error_doc(9, "Invalid findAndModify"),
    };
    if state.store.is_none() {
        return error_doc(13, "No storage configured");
    }
    let pg = state.store.as_ref().unwrap();

    let filter = cmd
        .get_document("query")
        .ok()
        .cloned()
        .unwrap_or_else(bson::Document::new);
    let sort = cmd.get_document("sort").ok().cloned();
    let new_return = cmd.get_bool("new").unwrap_or(false);
    let remove = cmd.get_bool("remove").unwrap_or(false);
    let update_doc = cmd.get_document("update").ok().cloned();
    let upsert = cmd.get_bool("upsert").unwrap_or(false);
    let proj = cmd
        .get_document("fields")
        .ok()
        .cloned()
        .or_else(|| cmd.get_document("projection").ok().cloned());

    if remove && update_doc.is_some() {
        return error_doc(9, "Cannot specify both remove and update");
    }
    if !remove && update_doc.is_none() {
        return error_doc(9, "Missing update or remove");
    }

    // Ensure collection exists if we may insert (upsert)
    if upsert {
        if let Err(e) = pg.ensure_collection(dbname, coll).await {
            return error_doc(59, format!("ensure_collection failed: {}", e));
        }
    }

    // Transactional path using pooled connection
    let mut client = match pg.get_client().await {
        Ok(c) => c,
        Err(e) => return error_doc(59, format!("tx client failed: {}", e)),
    };
    let tx = match client.transaction().await {
        Ok(t) => t,
        Err(e) => return error_doc(59, format!("tx begin failed: {}", e)),
    };
    let found = match pg
        .find_one_for_update_sorted_tx(&tx, dbname, coll, &filter, sort.as_ref())
        .await
    {
        Ok(v) => v,
        Err(e) => {
            let _ = tx.rollback().await;
            return error_doc(59, format!("find failed: {}", e));
        }
    };
    if let Some((idb, mut current)) = found {
        // Found document; apply remove or update
        let before = current.clone();
        if remove {
            match pg.delete_by_id_tx(&tx, dbname, coll, &idb).await {
                Ok(n) => {
                    if let Err(e) = tx.commit().await {
                        return error_doc(59, format!("tx commit failed: {}", e));
                    }
                    let mut value = before;
                    if let Some(ref p) = proj {
                        value = apply_project_with_expr(&value, p);
                    }
                    return doc! { "lastErrorObject": { "n": (n as i32), "updatedExisting": false }, "value": value, "ok": 1.0 };
                }
                Err(e) => {
                    let _ = tx.rollback().await;
                    return error_doc(59, format!("delete failed: {}", e));
                }
            }
        } else {
            // Apply update operators to current
            let udoc = update_doc.unwrap();
            let set_doc = udoc.get_document("$set").ok().cloned();
            let unset_doc = udoc.get_document("$unset").ok().cloned();
            let inc_doc = udoc.get_document("$inc").ok().cloned();
            let rename_doc = udoc.get_document("$rename").ok().cloned();
            let push_doc = udoc.get_document("$push").ok().cloned();
            let pull_doc = udoc.get_document("$pull").ok().cloned();
            if set_doc.is_none()
                && unset_doc.is_none()
                && inc_doc.is_none()
                && rename_doc.is_none()
                && push_doc.is_none()
                && pull_doc.is_none()
            {
                let _ = tx.rollback().await;
                return error_doc(9, "Only $set/$unset/$inc/$rename/$push/$pull supported");
            }
            if let Some(ref sets) = set_doc {
                for (k, v) in sets.iter() {
                    set_path_nested(&mut current, k, v.clone());
                }
            }
            if let Some(ref unsets) = unset_doc {
                for (k, _v) in unsets.iter() {
                    unset_path_nested(&mut current, k);
                }
            }
            if let Some(ref incs) = inc_doc {
                for (k, v) in incs.iter() {
                    if !apply_inc(&mut current, k, v.clone()) {
                        let _ = tx.rollback().await;
                        return error_doc(2, "$inc requires numeric field");
                    }
                }
            }
            if let Some(ref ren) = rename_doc {
                let mut pairs: Vec<(String, String)> = Vec::new();
                for (from, to_b) in ren.iter() {
                    let to = match to_b {
                        bson::Bson::String(s) => s.clone(),
                        _ => {
                            let _ = tx.rollback().await;
                            return error_doc(2, "$rename target must be string path");
                        }
                    };
                    pairs.push((from.to_string(), to));
                }
                if let Some(err) = validate_rename_pairs(&pairs) {
                    let _ = tx.rollback().await;
                    return err;
                }
                for (from, to) in pairs {
                    apply_rename(&mut current, &from, &to);
                }
            }
            if let Some(ref pushes) = push_doc {
                for (k, v) in pushes.iter() {
                    if !apply_push(&mut current, k, v.clone()) {
                        let _ = tx.rollback().await;
                        return error_doc(2, "$push on non-array");
                    }
                }
            }
            if let Some(ref pulls) = pull_doc {
                for (k, v) in pulls.iter() {
                    apply_pull(&mut current, k, v.clone());
                }
            }

            match pg
                .update_doc_by_id_tx(&tx, dbname, coll, &idb, &current)
                .await
            {
                Ok(_n) => {
                    if let Err(e) = tx.commit().await {
                        return error_doc(59, format!("tx commit failed: {}", e));
                    }
                    let value = if new_return { current } else { before };
                    let value = if let Some(ref p) = proj {
                        apply_project_with_expr(&value, p)
                    } else {
                        value
                    };
                    return doc! { "lastErrorObject": { "n": 1i32, "updatedExisting": true }, "value": value, "ok": 1.0 };
                }
                Err(e) => {
                    let _ = tx.rollback().await;
                    return error_doc(59, format!("update failed: {}", e));
                }
            }
        }
    } else {
        // Not found
        if !upsert {
            let value = bson::Bson::Null;
            if new_return { /* still null */ }
            return doc! { "lastErrorObject": { "n": 0i32, "updatedExisting": false }, "value": value, "ok": 1.0 };
        }
        // Upsert: build new doc from filter equality and apply operators
        let mut new_doc = bson::Document::new();
        // Extract simple equality constraints from filter into new_doc
        for (k, v) in filter.iter() {
            match v {
                bson::Bson::Document(d) => {
                    if let Some(eqv) = d.get("$eq") {
                        new_doc.insert(k.clone(), eqv.clone());
                    }
                }
                other => {
                    new_doc.insert(k.clone(), other.clone());
                }
            }
        }
        if let Some(ref udoc) = update_doc {
            let set_doc = udoc.get_document("$set").ok().cloned();
            let unset_doc = udoc.get_document("$unset").ok().cloned();
            let inc_doc = udoc.get_document("$inc").ok().cloned();
            let rename_doc = udoc.get_document("$rename").ok().cloned();
            let push_doc = udoc.get_document("$push").ok().cloned();
            let pull_doc = udoc.get_document("$pull").ok().cloned();
            if let Some(ref sets) = set_doc {
                for (k, v) in sets.iter() {
                    set_path_nested(&mut new_doc, k, v.clone());
                }
            }
            if let Some(ref unsets) = unset_doc {
                for (k, _v) in unsets.iter() {
                    unset_path_nested(&mut new_doc, k);
                }
            }
            if let Some(ref incs) = inc_doc {
                for (k, v) in incs.iter() {
                    if !apply_inc(&mut new_doc, k, v.clone()) {
                        return error_doc(2, "$inc requires numeric field");
                    }
                }
            }
            if let Some(ref ren) = rename_doc {
                let mut pairs: Vec<(String, String)> = Vec::new();
                for (from, to_b) in ren.iter() {
                    if let bson::Bson::String(s) = to_b {
                        pairs.push((from.to_string(), s.clone()));
                    }
                }
                if let Some(err) = validate_rename_pairs(&pairs) {
                    return err;
                }
                for (from, to) in pairs {
                    apply_rename(&mut new_doc, &from, &to);
                }
            }
            if let Some(ref pushes) = push_doc {
                for (k, v) in pushes.iter() {
                    if !apply_push(&mut new_doc, k, v.clone()) {
                        return error_doc(2, "$push on non-array");
                    }
                }
            }
            if let Some(ref pulls) = pull_doc {
                for (k, v) in pulls.iter() {
                    apply_pull(&mut new_doc, k, v.clone());
                }
            }
        }
        ensure_id(&mut new_doc);
        let idb = match new_doc.get("_id").and_then(|b| id_bytes_bson(b)) {
            Some(v) => v,
            None => return error_doc(2, "unsupported _id type"),
        };
        let bson_bytes = match bson::to_vec(&new_doc) {
            Ok(v) => v,
            Err(e) => return error_doc(2, e.to_string()),
        };
        let json = match serde_json::to_value(&new_doc) {
            Ok(v) => v,
            Err(e) => return error_doc(2, e.to_string()),
        };
        // Insert in its own transaction
        let mut client2 = match pg.get_client().await {
            Ok(c) => c,
            Err(e) => return error_doc(59, format!("tx client failed: {}", e)),
        };
        let tx2 = match client2.transaction().await {
            Ok(t) => t,
            Err(e) => return error_doc(59, format!("tx begin failed: {}", e)),
        };
        match pg
            .insert_one_tx(&tx2, dbname, coll, &idb, &bson_bytes, &json)
            .await
        {
            Ok(n) => {
                if let Err(e) = tx2.commit().await {
                    return error_doc(59, format!("tx commit failed: {}", e));
                }
                let value = if new_return {
                    let mut v = new_doc.clone();
                    if let Some(ref p) = proj {
                        v = apply_project_with_expr(&v, p);
                    }
                    bson::Bson::Document(v)
                } else {
                    bson::Bson::Null
                };
                let mut leo = doc! { "n": (n as i32), "updatedExisting": false };
                if n == 1 {
                    if let Some(idv) = new_doc.get("_id").cloned() {
                        leo.insert("upserted", idv);
                    }
                }
                return doc! { "lastErrorObject": leo, "value": value, "ok": 1.0 };
            }
            Err(e) => {
                let _ = tx2.rollback().await;
                return error_doc(59, format!("insert failed: {}", e));
            }
        }
    }
}

fn set_path_nested(doc: &mut Document, path: &str, value: bson::Bson) {
    // Implement via a generic BSON walker that supports arrays and documents
    let mut root = bson::Bson::Document(doc.clone());
    let segments: Vec<&str> = path.split('.').collect();
    set_path_bson(&mut root, &segments, value);
    if let bson::Bson::Document(updated) = root {
        *doc = updated;
    }
}

fn seg_is_index(seg: &str) -> Option<usize> {
    seg.parse::<usize>().ok()
}

fn set_path_bson(cur: &mut bson::Bson, segs: &[&str], value: bson::Bson) {
    if segs.is_empty() {
        *cur = value;
        return;
    }
    let seg = segs[0];
    let next_is_index = segs.get(1).and_then(|s| seg_is_index(s)).is_some();
    match seg_is_index(seg) {
        Some(idx) => {
            // Ensure array
            if !matches!(cur, bson::Bson::Array(_)) {
                *cur = bson::Bson::Array(Vec::new());
            }
            if let bson::Bson::Array(arr) = cur {
                let desired_len = idx + 1;
                if arr.len() < desired_len {
                    let pad = desired_len - arr.len();
                    arr.extend(std::iter::repeat(bson::Bson::Null).take(pad));
                }
                if segs.len() == 1 {
                    arr[idx] = value;
                } else {
                    // Choose default element type for deeper path
                    if matches!(arr[idx], bson::Bson::Null) {
                        arr[idx] = if next_is_index {
                            bson::Bson::Array(Vec::new())
                        } else {
                            bson::Bson::Document(Document::new())
                        };
                    }
                    set_path_bson(&mut arr[idx], &segs[1..], value);
                }
            }
        }
        None => {
            // Document key
            if !matches!(cur, bson::Bson::Document(_)) {
                *cur = bson::Bson::Document(Document::new());
            }
            if let bson::Bson::Document(d) = cur {
                if segs.len() == 1 {
                    d.insert(seg.to_string(), value);
                } else {
                    let entry = d.entry(seg.to_string()).or_insert_with(|| {
                        if next_is_index {
                            bson::Bson::Array(Vec::new())
                        } else {
                            bson::Bson::Document(Document::new())
                        }
                    });
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
    if let bson::Bson::Document(updated) = root {
        *doc = updated;
    }
}

fn unset_path_bson(cur: &mut bson::Bson, segs: &[&str]) {
    if segs.is_empty() {
        return;
    }
    let seg = segs[0];
    match seg.parse::<usize>().ok() {
        Some(idx) => {
            if let bson::Bson::Array(arr) = cur {
                if idx < arr.len() {
                    if segs.len() == 1 {
                        arr[idx] = bson::Bson::Null;
                    } else {
                        unset_path_bson(&mut arr[idx], &segs[1..]);
                    }
                }
            }
        }
        None => {
            if let bson::Bson::Document(d) = cur {
                if segs.len() == 1 {
                    d.remove(seg);
                } else if let Some(child) = d.get_mut(seg) {
                    unset_path_bson(child, &segs[1..]);
                }
            }
        }
    }
}

fn path_has_negative_index(path: &str) -> bool {
    for seg in path.split('.') {
        if let Ok(n) = seg.parse::<i32>() {
            if n < 0 {
                return true;
            }
        }
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
        if from == to {
            return Some(error_doc(2, "$rename from and to cannot be the same"));
        }
        if is_ancestor_path(from, to) || is_ancestor_path(to, from) {
            return Some(error_doc(
                2,
                "$rename cannot target ancestor/descendant path",
            ));
        }
        if !targets.insert(to.as_str()) {
            return Some(error_doc(2, "$rename duplicate target path"));
        }
    }
    None
}

fn parse_path<'a>(path: &'a str) -> Vec<&'a str> {
    path.split('.').collect()
}

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
    match b {
        bson::Bson::Int32(n) => Some(*n as f64),
        bson::Bson::Int64(n) => Some(*n as f64),
        bson::Bson::Double(n) => Some(*n),
        _ => None,
    }
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
                if let Some(di) = delta_num.0 {
                    // int
                    let sum = (ci as i64) + di;
                    if sum >= i32::MIN as i64 && sum <= i32::MAX as i64 {
                        set_path_nested(doc, path, bson::Bson::Int32(sum as i32));
                    } else {
                        set_path_nested(doc, path, bson::Bson::Int64(sum));
                    }
                    true
                } else if let Some(df) = delta_num.1 {
                    // double
                    let sum = (ci as f64) + df;
                    set_path_nested(doc, path, bson::Bson::Double(sum));
                    true
                } else {
                    false
                }
            }
            bson::Bson::Int64(cl) => {
                if let Some(di) = delta_num.0 {
                    set_path_nested(doc, path, bson::Bson::Int64(cl + di));
                    true
                } else if let Some(df) = delta_num.1 {
                    set_path_nested(doc, path, bson::Bson::Double((cl as f64) + df));
                    true
                } else {
                    false
                }
            }
            bson::Bson::Double(cd) => {
                let add = if let Some(di) = delta_num.0 {
                    di as f64
                } else {
                    delta_num.1.unwrap()
                };
                set_path_nested(doc, path, bson::Bson::Double(cd + add));
                true
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
    let (parent_path, last) = if segs.is_empty() {
        return false;
    } else {
        (&segs[..segs.len() - 1], segs[segs.len() - 1])
    };
    // Get parent
    let mut parent = &mut root;
    for seg in parent_path {
        set_path_bson(parent, &[seg], bson::Bson::Document(Document::new()));
        if let bson::Bson::Document(d) = parent {
            parent = d.get_mut(*seg).unwrap();
        } else {
            return false;
        }
    }
    // Ensure array at last
    if let bson::Bson::Document(d) = parent {
        let entry = d
            .entry(last.to_string())
            .or_insert_with(|| bson::Bson::Array(Vec::new()));
        match entry {
            bson::Bson::Array(arr) => {
                if let bson::Bson::Document(spec) = val {
                    // $each required if using modifiers
                    let each = match spec.get_array("$each") {
                        Ok(a) => a.clone(),
                        Err(_) => return false,
                    };
                    let pos = spec.get_i32("$position").ok().unwrap_or(i32::MAX);
                    let slice = spec.get_i32("$slice").ok();
                    // Insert at position (clamped)
                    let mut insert_at = if pos < 0 { 0usize } else { pos as usize };
                    if insert_at > arr.len() {
                        insert_at = arr.len();
                    }
                    arr.splice(insert_at..insert_at, each.into_iter());
                    // Apply slice trimming if provided
                    if let Some(n) = slice {
                        if n >= 0 {
                            let n = n as usize;
                            if arr.len() > n {
                                arr.truncate(n);
                            }
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
    if let bson::Bson::Document(updated) = root {
        *doc = updated;
        true
    } else {
        false
    }
}

fn apply_pull(doc: &mut Document, path: &str, criterion: bson::Bson) {
    let mut root = bson::Bson::Document(doc.clone());
    let segs: Vec<&str> = path.split('.').collect();
    let (parent_path, last) = if segs.is_empty() {
        return;
    } else {
        (&segs[..segs.len() - 1], segs[segs.len() - 1])
    };
    // Navigate to parent
    let mut parent = &mut root;
    for seg in parent_path {
        match parent {
            bson::Bson::Document(d) => {
                if !d.contains_key(*seg) {
                    return;
                }
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
    if let bson::Bson::Document(updated) = root {
        *doc = updated;
    }
}

fn matches_predicate(elem: &bson::Bson, crit: &Document) -> bool {
    // Support simple operators: $gt,$gte,$lt,$lte,$eq,$in
    for (op, val) in crit.iter() {
        match op.as_str() {
            "$gt" => {
                if !cmp(elem, val, |a, b| a > b) {
                    return false;
                }
            }
            "$gte" => {
                if !cmp(elem, val, |a, b| a >= b) {
                    return false;
                }
            }
            "$lt" => {
                if !cmp(elem, val, |a, b| a < b) {
                    return false;
                }
            }
            "$lte" => {
                if !cmp(elem, val, |a, b| a <= b) {
                    return false;
                }
            }
            "$eq" => {
                if !cmp(elem, val, |a, b| a == b) {
                    return false;
                }
            }
            "$in" => {
                if let bson::Bson::Array(arr) = val {
                    let mut ok = false;
                    for v in arr {
                        if cmp(elem, v, |a, b| a == b) {
                            ok = true;
                            break;
                        }
                    }
                    if !ok {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            _ => {}
        }
    }
    true
}

fn cmp<F: Fn(f64, f64) -> bool>(a: &bson::Bson, b: &bson::Bson, f: F) -> bool {
    match (number_as_f64(a), number_as_f64(b)) {
        (Some(x), Some(y)) => f(x, y),
        _ => a == b,
    }
}

fn cmp_multi(a: &Document, b: &Document, spec: &Document) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (k, v) in spec.iter() {
        let dir = match v {
            bson::Bson::Int32(n) => *n,
            bson::Bson::Int64(n) => *n as i32,
            bson::Bson::Double(f) => {
                if *f < 0.0 {
                    -1
                } else {
                    1
                }
            }
            _ => 1,
        };
        let av = get_path_bson_value(a, k);
        let bv = get_path_bson_value(b, k);
        let ord = cmp_bson_opt(&av, &bv);
        if ord != Ordering::Equal {
            return if dir < 0 { ord.reverse() } else { ord };
        }
    }
    Ordering::Equal
}

fn cmp_bson_opt(a: &Option<bson::Bson>, b: &Option<bson::Bson>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(av), Some(bv)) => cmp_bson(av, bv),
    }
}

fn cmp_bson(a: &bson::Bson, b: &bson::Bson) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    // Numeric compare if both numeric
    if let (Some(x), Some(y)) = (number_as_f64(a), number_as_f64(b)) {
        return if x < y {
            Ordering::Less
        } else if x > y {
            Ordering::Greater
        } else {
            Ordering::Equal
        };
    }
    match (a, b) {
        (bson::Bson::String(sa), bson::Bson::String(sb)) => sa.cmp(sb),
        (bson::Bson::Boolean(ba), bson::Bson::Boolean(bb)) => ba.cmp(bb),
        (bson::Bson::Null, bson::Bson::Null) => Ordering::Equal,
        (bson::Bson::Null, _) => Ordering::Less,
        (_, bson::Bson::Null) => Ordering::Greater,
        _ => format!("{:?}", a).cmp(&format!("{:?}", b)),
    }
}

fn key_repr(v: &bson::Bson) -> String {
    format!("{:?}", v)
}

fn apply_group(docs: &Vec<Document>, spec: &Document) -> Vec<Document> {
    use std::collections::HashMap;
    #[derive(Clone)]
    enum Acc {
        SumF64(f64),
        SumI64(i64),
        Avg(f64, i64),
        Min(Option<bson::Bson>),
        Max(Option<bson::Bson>),
        First(Option<bson::Bson>),
        Last(Option<bson::Bson>),
        Push(Vec<bson::Bson>),
    }
    #[derive(Clone)]
    enum AccSpec {
        Sum(bson::Bson),
        Avg(bson::Bson),
        Min(bson::Bson),
        Max(bson::Bson),
        First(bson::Bson),
        Last(bson::Bson),
        Push(bson::Bson),
    }
    let id_expr = spec.get("_id").cloned().unwrap_or(bson::Bson::Null);
    let mut acc_specs: Vec<(String, AccSpec)> = Vec::new();
    for (k, v) in spec.iter() {
        if k == "_id" {
            continue;
        }
        if let bson::Bson::Document(d) = v {
            if let Some(x) = d.get("$sum") {
                acc_specs.push((k.clone(), AccSpec::Sum(x.clone())));
                continue;
            }
            if let Some(x) = d.get("$avg") {
                acc_specs.push((k.clone(), AccSpec::Avg(x.clone())));
                continue;
            }
            if let Some(x) = d.get("$min") {
                acc_specs.push((k.clone(), AccSpec::Min(x.clone())));
                continue;
            }
            if let Some(x) = d.get("$max") {
                acc_specs.push((k.clone(), AccSpec::Max(x.clone())));
                continue;
            }
            if let Some(x) = d.get("$first") {
                acc_specs.push((k.clone(), AccSpec::First(x.clone())));
                continue;
            }
            if let Some(x) = d.get("$last") {
                acc_specs.push((k.clone(), AccSpec::Last(x.clone())));
                continue;
            }
            if let Some(x) = d.get("$push") {
                acc_specs.push((k.clone(), AccSpec::Push(x.clone())));
                continue;
            }
        }
    }
    let mut map: HashMap<String, (bson::Bson, HashMap<String, Acc>)> = HashMap::new();
    for d in docs.iter() {
        let key_val = eval_expr(d, &id_expr).unwrap_or(bson::Bson::Null);
        let key = key_repr(&key_val);
        let entry = map.entry(key).or_insert_with(|| {
            let mut hm = HashMap::new();
            for (name, spec) in acc_specs.iter() {
                let st = match spec {
                    // If $sum has a constant integer expression, accumulate as integer so small results come back as Int32
                    AccSpec::Sum(expr) => match expr {
                        bson::Bson::Int32(_) | bson::Bson::Int64(_) => Acc::SumI64(0),
                        _ => Acc::SumF64(0.0),
                    },
                    AccSpec::Avg(_) => Acc::Avg(0.0, 0),
                    AccSpec::Min(_) => Acc::Min(None),
                    AccSpec::Max(_) => Acc::Max(None),
                    AccSpec::First(_) => Acc::First(None),
                    AccSpec::Last(_) => Acc::Last(None),
                    AccSpec::Push(_) => Acc::Push(Vec::new()),
                };
                hm.insert(name.clone(), st);
            }
            (key_val.clone(), hm)
        });
        for (name, spec) in acc_specs.iter() {
            let st = entry.1.get_mut(name).unwrap();
            match (st, spec) {
                (Acc::SumF64(s), AccSpec::Sum(expr)) => {
                    let v = eval_expr(d, expr).unwrap_or(bson::Bson::Null);
                    let n = number_as_f64(&v).unwrap_or(0.0);
                    *s += n;
                }
                (Acc::SumI64(total), AccSpec::Sum(expr)) => {
                    let v = eval_expr(d, expr).unwrap_or(bson::Bson::Null);
                    match v {
                        bson::Bson::Int32(n) => {
                            *total += n as i64;
                        }
                        bson::Bson::Int64(n) => {
                            *total += n;
                        }
                        // If expression unexpectedly resolves to non-integer, add its floor value
                        bson::Bson::Double(f) => {
                            *total += f.trunc() as i64;
                        }
                        _ => {}
                    }
                }
                (Acc::Avg(s, c), AccSpec::Avg(expr)) => {
                    let v = eval_expr(d, expr).unwrap_or(bson::Bson::Null);
                    if let Some(n) = number_as_f64(&v) {
                        *s += n;
                        *c += 1;
                    }
                }
                (Acc::Min(cur), AccSpec::Min(expr)) => {
                    let v = eval_expr(d, expr).unwrap_or(bson::Bson::Null);
                    *cur = match cur.take() {
                        None => Some(v),
                        Some(old) => Some(if cmp_bson(&v, &old) == std::cmp::Ordering::Less {
                            v
                        } else {
                            old
                        }),
                    };
                }
                (Acc::Max(cur), AccSpec::Max(expr)) => {
                    let v = eval_expr(d, expr).unwrap_or(bson::Bson::Null);
                    *cur = match cur.take() {
                        None => Some(v),
                        Some(old) => Some(if cmp_bson(&v, &old) == std::cmp::Ordering::Greater {
                            v
                        } else {
                            old
                        }),
                    };
                }
                (Acc::First(cur), AccSpec::First(expr)) => {
                    if cur.is_none() {
                        *cur = Some(eval_expr(d, expr).unwrap_or(bson::Bson::Null));
                    }
                }
                (Acc::Last(cur), AccSpec::Last(expr)) => {
                    *cur = Some(eval_expr(d, expr).unwrap_or(bson::Bson::Null));
                }
                (Acc::Push(arr), AccSpec::Push(expr)) => {
                    arr.push(eval_expr(d, expr).unwrap_or(bson::Bson::Null));
                }
                _ => {}
            }
        }
    }
    // build result docs
    let mut out: Vec<Document> = Vec::with_capacity(map.len());
    for (_k, (idv, accs)) in map.into_iter() {
        let mut doc = Document::new();
        doc.insert("_id", idv);
        for (name, st) in accs.into_iter() {
            match st {
                Acc::SumF64(s) => {
                    doc.insert(name, bson::Bson::Double(s));
                }
                Acc::SumI64(n) => {
                    if n <= i32::MAX as i64 && n >= i32::MIN as i64 {
                        doc.insert(name, bson::Bson::Int32(n as i32));
                    } else {
                        doc.insert(name, bson::Bson::Int64(n));
                    }
                }
                Acc::Avg(s, c) => {
                    let v = if c > 0 { s / (c as f64) } else { 0.0 };
                    doc.insert(name, bson::Bson::Double(v));
                }
                Acc::Min(v) => {
                    doc.insert(name, v.unwrap_or(bson::Bson::Null));
                }
                Acc::Max(v) => {
                    doc.insert(name, v.unwrap_or(bson::Bson::Null));
                }
                Acc::First(v) => {
                    doc.insert(name, v.unwrap_or(bson::Bson::Null));
                }
                Acc::Last(v) => {
                    doc.insert(name, v.unwrap_or(bson::Bson::Null));
                }
                Acc::Push(arr) => {
                    doc.insert(name, bson::Bson::Array(arr));
                }
            }
        }
        out.push(doc);
    }
    out
}

/// Apply $bucket aggregation stage
fn apply_bucket(docs: &Vec<Document>, spec: &BucketSpec) -> Vec<Document> {
    use std::collections::HashMap;

    // Validate boundaries are sorted and same type
    if spec.boundaries.len() < 2 {
        return Vec::new();
    }

    // Check boundaries are sorted
    for i in 1..spec.boundaries.len() {
        if cmp_bson(&spec.boundaries[i], &spec.boundaries[i - 1]) != std::cmp::Ordering::Greater {
            return Vec::new();
        }
    }

    // Parse output accumulators
    #[derive(Clone)]
    enum Acc {
        SumF64(f64),
        SumI64(i64),
        Avg(f64, i64),
        Min(Option<bson::Bson>),
        Max(Option<bson::Bson>),
        Count(i64),
    }

    #[derive(Clone)]
    enum AccSpec {
        Sum(bson::Bson),
        Avg(bson::Bson),
        Min(bson::Bson),
        Max(bson::Bson),
        Count,
    }

    let mut acc_specs: Vec<(String, AccSpec)> = Vec::new();
    if let Some(output) = &spec.output {
        for (k, v) in output.iter() {
            if let bson::Bson::Document(d) = v {
                if let Some(x) = d.get("$sum") {
                    acc_specs.push((k.clone(), AccSpec::Sum(x.clone())));
                    continue;
                }
                if let Some(x) = d.get("$avg") {
                    acc_specs.push((k.clone(), AccSpec::Avg(x.clone())));
                    continue;
                }
                if let Some(x) = d.get("$min") {
                    acc_specs.push((k.clone(), AccSpec::Min(x.clone())));
                    continue;
                }
                if let Some(x) = d.get("$max") {
                    acc_specs.push((k.clone(), AccSpec::Max(x.clone())));
                    continue;
                }
                if d.contains_key("$count") {
                    acc_specs.push((k.clone(), AccSpec::Count));
                    continue;
                }
            }
        }
    }

    // Group documents into buckets
    let mut buckets: HashMap<usize, (Option<bson::Bson>, HashMap<String, Acc>, i64)> =
        HashMap::new();
    let mut default_bucket: Option<(Option<bson::Bson>, HashMap<String, Acc>, i64)> = None;

    for d in docs.iter() {
        let value = eval_expr(d, &bson::Bson::String(spec.group_by.clone()));

        // Find which bucket this document belongs to
        let mut bucket_idx: Option<usize> = None;
        if let Some(ref val) = value {
            for i in 0..spec.boundaries.len() - 1 {
                let lower = &spec.boundaries[i];
                let upper = &spec.boundaries[i + 1];
                // Check if value >= lower and value < upper
                if cmp_bson(val, lower) != std::cmp::Ordering::Less
                    && cmp_bson(val, upper) == std::cmp::Ordering::Less
                {
                    bucket_idx = Some(i);
                    break;
                }
            }
        }

        // Initialize accumulator state if needed
        let init_acc = || {
            let mut hm = HashMap::new();
            for (name, acc_spec) in acc_specs.iter() {
                let st = match acc_spec {
                    AccSpec::Sum(expr) => match expr {
                        bson::Bson::Int32(_) | bson::Bson::Int64(_) => Acc::SumI64(0),
                        _ => Acc::SumF64(0.0),
                    },
                    AccSpec::Avg(_) => Acc::Avg(0.0, 0),
                    AccSpec::Min(_) => Acc::Min(None),
                    AccSpec::Max(_) => Acc::Max(None),
                    AccSpec::Count => Acc::Count(0),
                };
                hm.insert(name.clone(), st);
            }
            hm
        };

        // Get or create bucket entry
        let (accs, doc_count) = if let Some(idx) = bucket_idx {
            let entry = buckets.entry(idx).or_insert_with(|| {
                let lower_boundary = spec.boundaries[idx].clone();
                (Some(lower_boundary), init_acc(), 0i64)
            });
            (&mut entry.1, &mut entry.2)
        } else if let Some(ref default_val) = spec.default {
            if default_bucket.is_none() {
                default_bucket = Some((Some(default_val.clone()), init_acc(), 0i64));
            }
            let bucket = default_bucket.as_mut().unwrap();
            (&mut bucket.1, &mut bucket.2)
        } else {
            // No default bucket and value outside boundaries - skip document
            continue;
        };

        // Increment document count for this bucket
        *doc_count += 1;

        // Accumulate values
        for (name, acc_spec) in acc_specs.iter() {
            let st = accs.get_mut(name).unwrap();
            match (st, acc_spec) {
                (Acc::SumF64(s), AccSpec::Sum(expr)) => {
                    let v = eval_expr(d, expr).unwrap_or(bson::Bson::Null);
                    let n = number_as_f64(&v).unwrap_or(0.0);
                    *s += n;
                }
                (Acc::SumI64(total), AccSpec::Sum(expr)) => {
                    let v = eval_expr(d, expr).unwrap_or(bson::Bson::Null);
                    match v {
                        bson::Bson::Int32(n) => {
                            *total += n as i64;
                        }
                        bson::Bson::Int64(n) => {
                            *total += n;
                        }
                        bson::Bson::Double(f) => {
                            *total += f.trunc() as i64;
                        }
                        _ => {}
                    }
                }
                (Acc::Avg(s, c), AccSpec::Avg(expr)) => {
                    let v = eval_expr(d, expr).unwrap_or(bson::Bson::Null);
                    if let Some(n) = number_as_f64(&v) {
                        *s += n;
                        *c += 1;
                    }
                }
                (Acc::Min(cur), AccSpec::Min(expr)) => {
                    let v = eval_expr(d, expr).unwrap_or(bson::Bson::Null);
                    *cur = match cur.take() {
                        None => Some(v),
                        Some(old) => Some(if cmp_bson(&v, &old) == std::cmp::Ordering::Less {
                            v
                        } else {
                            old
                        }),
                    };
                }
                (Acc::Max(cur), AccSpec::Max(expr)) => {
                    let v = eval_expr(d, expr).unwrap_or(bson::Bson::Null);
                    *cur = match cur.take() {
                        None => Some(v),
                        Some(old) => Some(if cmp_bson(&v, &old) == std::cmp::Ordering::Greater {
                            v
                        } else {
                            old
                        }),
                    };
                }
                (Acc::Count(c), AccSpec::Count) => {
                    *c += 1;
                }
                _ => {}
            }
        }
    }

    // Build result documents
    let mut out: Vec<Document> = Vec::new();

    // Add regular buckets
    for i in 0..spec.boundaries.len() - 1 {
        if let Some((id_val, accs, doc_count)) = buckets.remove(&i) {
            let mut doc = Document::new();
            doc.insert("_id", id_val.unwrap_or(bson::Bson::Null));

            // Process accumulators
            for (name, st) in accs.iter() {
                match st {
                    Acc::SumF64(s) => {
                        // If the sum is a whole number and fits in i32, return as Int32
                        let epsilon = 1e-10;
                        if (*s - s.round()).abs() < epsilon
                            && *s <= i32::MAX as f64
                            && *s >= i32::MIN as f64
                        {
                            doc.insert(name.clone(), bson::Bson::Int32(*s as i32));
                        } else {
                            doc.insert(name.clone(), bson::Bson::Double(*s));
                        }
                    }
                    Acc::SumI64(n) => {
                        if *n <= i32::MAX as i64 && *n >= i32::MIN as i64 {
                            doc.insert(name.clone(), bson::Bson::Int32(*n as i32));
                        } else {
                            doc.insert(name.clone(), bson::Bson::Int64(*n));
                        }
                    }
                    Acc::Avg(s, c) => {
                        let v = if *c > 0 { *s / (*c as f64) } else { 0.0 };
                        doc.insert(name.clone(), bson::Bson::Double(v));
                    }
                    Acc::Min(v) => {
                        doc.insert(name.clone(), v.clone().unwrap_or(bson::Bson::Null));
                    }
                    Acc::Max(v) => {
                        doc.insert(name.clone(), v.clone().unwrap_or(bson::Bson::Null));
                    }
                    Acc::Count(c) => {
                        doc.insert(name.clone(), bson::Bson::Int64(*c));
                    }
                }
            }

            // Always add count field (use actual document count)
            if !doc.contains_key("count") {
                if doc_count <= i32::MAX as i64 && doc_count >= i32::MIN as i64 {
                    doc.insert("count", bson::Bson::Int32(doc_count as i32));
                } else {
                    doc.insert("count", bson::Bson::Int64(doc_count));
                }
            }

            out.push(doc);
        }
    }

    // Add default bucket if present
    if let Some((id_val, accs, doc_count)) = default_bucket {
        let mut doc = Document::new();
        doc.insert("_id", id_val.unwrap_or(bson::Bson::Null));

        for (name, st) in accs.iter() {
            match st {
                Acc::SumF64(s) => {
                    // If the sum is a whole number and fits in i32, return as Int32
                    let epsilon = 1e-10;
                    if (*s - s.round()).abs() < epsilon
                        && *s <= i32::MAX as f64
                        && *s >= i32::MIN as f64
                    {
                        doc.insert(name.clone(), bson::Bson::Int32(*s as i32));
                    } else {
                        doc.insert(name.clone(), bson::Bson::Double(*s));
                    }
                }
                Acc::SumI64(n) => {
                    if *n <= i32::MAX as i64 && *n >= i32::MIN as i64 {
                        doc.insert(name.clone(), bson::Bson::Int32(*n as i32));
                    } else {
                        doc.insert(name.clone(), bson::Bson::Int64(*n));
                    }
                }
                Acc::Avg(s, c) => {
                    let v = if *c > 0 { *s / (*c as f64) } else { 0.0 };
                    doc.insert(name.clone(), bson::Bson::Double(v));
                }
                Acc::Min(v) => {
                    doc.insert(name.clone(), v.clone().unwrap_or(bson::Bson::Null));
                }
                Acc::Max(v) => {
                    doc.insert(name.clone(), v.clone().unwrap_or(bson::Bson::Null));
                }
                Acc::Count(c) => {
                    doc.insert(name.clone(), bson::Bson::Int64(*c));
                }
            }
        }

        if !doc.contains_key("count") {
            if doc_count <= i32::MAX as i64 && doc_count >= i32::MIN as i64 {
                doc.insert("count", bson::Bson::Int32(doc_count as i32));
            } else {
                doc.insert("count", bson::Bson::Int64(doc_count));
            }
        }

        out.push(doc);
    }

    out
}

use crate::error::Error;
async fn apply_lookup(
    db: &str,
    pg: &crate::store::PgStore,
    mut docs: Vec<Document>,
    spec: &Document,
) -> crate::error::Result<Vec<Document>> {
    use std::collections::HashMap;
    let from = spec
        .get_str("from")
        .map_err(|_| Error::Msg("lookup.from missing".into()))?;
    let local_field = spec
        .get_str("localField")
        .map_err(|_| Error::Msg("lookup.localField missing".into()))?;
    let foreign_field = spec
        .get_str("foreignField")
        .map_err(|_| Error::Msg("lookup.foreignField missing".into()))?;
    let as_field = spec
        .get_str("as")
        .map_err(|_| Error::Msg("lookup.as missing".into()))?;
    // fetch foreign docs (naively up to a large number)
    let foreign = pg
        .find_docs(db, from, None, None, None, 100_000)
        .await
        .map_err(|e| crate::error::Error::Msg(e.to_string()))?;
    let mut map: HashMap<String, Vec<Document>> = HashMap::new();
    for fd in foreign.into_iter() {
        if let Some(val) = get_path_bson_value(&fd, foreign_field) {
            map.entry(key_repr(&val)).or_default().push(fd);
        }
    }
    for d in docs.iter_mut() {
        let key = get_path_bson_value(d, local_field).map(|v| key_repr(&v));
        let arr = key
            .and_then(|k| map.get(&k))
            .cloned()
            .unwrap_or_else(|| Vec::new());
        set_path_nested(
            d,
            as_field,
            bson::Bson::Array(arr.into_iter().map(|m| bson::Bson::Document(m)).collect()),
        );
    }
    Ok(docs)
}

/// Execute a sub-pipeline for $facet stage
/// Supports: $match, $sort, $limit, $skip, $project, $addFields/$set, $unwind, $group
fn execute_sub_pipeline(
    docs: Vec<Document>,
    pipeline: &[bson::Bson],
) -> std::result::Result<Vec<Document>, String> {
    let mut result = docs;

    for stage in pipeline {
        if let bson::Bson::Document(stage_doc) = stage {
            // $match
            if let Ok(filter) = stage_doc.get_document("$match") {
                result.retain(|d| document_matches_filter(d, filter));
                continue;
            }

            // $sort
            if let Ok(sort_spec) = stage_doc.get_document("$sort") {
                result.sort_by(|a, b| cmp_multi(a, b, sort_spec));
                continue;
            }

            // $limit
            if let Some(limit) = stage_doc
                .get_i64("$limit")
                .ok()
                .or(stage_doc.get_i32("$limit").ok().map(|v| v as i64))
            {
                if limit >= 0 && (limit as usize) < result.len() {
                    result.truncate(limit as usize);
                }
                continue;
            }

            // $skip
            if let Some(skip) = stage_doc
                .get_i64("$skip")
                .ok()
                .or(stage_doc.get_i32("$skip").ok().map(|v| v as i64))
            {
                if skip > 0 {
                    let skip_count = (skip as usize).min(result.len());
                    result.drain(0..skip_count);
                }
                continue;
            }

            // $project
            if let Ok(proj) = stage_doc.get_document("$project") {
                result = result
                    .into_iter()
                    .map(|d| apply_project_with_expr(&d, proj))
                    .collect();
                continue;
            }

            // $addFields / $set
            if let Ok(af) = stage_doc
                .get_document("$addFields")
                .or(stage_doc.get_document("$set"))
            {
                for d in result.iter_mut() {
                    for (k, v) in af.iter() {
                        let val = eval_expr(d, v).unwrap_or(bson::Bson::Null);
                        set_path_nested(d, k, val);
                    }
                }
                continue;
            }

            // $unwind
            if let Ok(u) = stage_doc.get_str("$unwind") {
                let path = u.trim_start_matches('$').to_string();
                let mut out: Vec<Document> = Vec::new();
                for d in result.into_iter() {
                    match get_path_bson_value(&d, &path) {
                        Some(bson::Bson::Array(arr)) => {
                            for item in arr {
                                let mut nd = d.clone();
                                set_path_nested(&mut nd, &path, item);
                                out.push(nd);
                            }
                        }
                        _ => {}
                    }
                }
                result = out;
                continue;
            }

            if let Ok(ud) = stage_doc.get_document("$unwind") {
                if let Ok(pth) = ud.get_str("path") {
                    let path = pth.trim_start_matches('$').to_string();
                    let preserve = ud.get_bool("preserveNullAndEmptyArrays").unwrap_or(false);
                    let include = ud.get_str("includeArrayIndex").ok().map(|s| s.to_string());
                    let mut out: Vec<Document> = Vec::new();
                    for d in result.into_iter() {
                        match get_path_bson_value(&d, &path) {
                            Some(bson::Bson::Array(arr)) => {
                                if arr.is_empty() {
                                    if preserve {
                                        let mut nd = d.clone();
                                        set_path_nested(&mut nd, &path, bson::Bson::Null);
                                        if let Some(ref fpath) = include {
                                            set_path_nested(&mut nd, fpath, bson::Bson::Null);
                                        }
                                        out.push(nd);
                                    }
                                    continue;
                                }
                                for (i, item) in arr.into_iter().enumerate() {
                                    let mut nd = d.clone();
                                    set_path_nested(&mut nd, &path, item);
                                    if let Some(ref fpath) = include {
                                        set_path_nested(
                                            &mut nd,
                                            fpath,
                                            bson::Bson::Int32(i as i32),
                                        );
                                    }
                                    out.push(nd);
                                }
                            }
                            None | Some(bson::Bson::Null) => {
                                if preserve {
                                    let mut nd = d.clone();
                                    set_path_nested(&mut nd, &path, bson::Bson::Null);
                                    if let Some(ref fpath) = include {
                                        set_path_nested(&mut nd, fpath, bson::Bson::Null);
                                    }
                                    out.push(nd);
                                }
                            }
                            _ => {}
                        }
                    }
                    result = out;
                    continue;
                }
            }

            // $group
            if let Ok(group_spec) = stage_doc.get_document("$group") {
                result = apply_group(&result, group_spec);
                continue;
            }

            // Unknown stage in sub-pipeline
            if stage_doc.keys().any(|k| k.starts_with('$')) {
                let stage_name = stage_doc.keys().next().unwrap().clone();
                return Err(format!("Unsupported sub-pipeline stage: {}", stage_name));
            }
        }
    }

    Ok(result)
}

/// Check if a document matches a filter (for $match in sub-pipelines)
fn document_matches_filter(doc: &Document, filter: &Document) -> bool {
    for (key, value) in filter.iter() {
        if !field_matches(doc, key, value) {
            return false;
        }
    }
    true
}

fn field_matches(doc: &Document, field: &str, expected: &bson::Bson) -> bool {
    let actual = get_path_bson_value(doc, field);

    match expected {
        bson::Bson::Document(op_doc) => {
            // Handle operators like $eq, $gt, $gte, $lt, $lte, $ne, $in, $nin, etc.
            for (op, op_val) in op_doc.iter() {
                match op.as_str() {
                    "$eq" => {
                        if actual.as_ref() != Some(op_val) {
                            return false;
                        }
                    }
                    "$ne" => {
                        if actual.as_ref() == Some(op_val) {
                            return false;
                        }
                    }
                    "$gt" => {
                        if let (Some(a), Some(b)) = (
                            actual.as_ref().and_then(number_as_f64),
                            number_as_f64(op_val),
                        ) {
                            if a <= b {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    "$gte" => {
                        if let (Some(a), Some(b)) = (
                            actual.as_ref().and_then(number_as_f64),
                            number_as_f64(op_val),
                        ) {
                            if a < b {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    "$lt" => {
                        if let (Some(a), Some(b)) = (
                            actual.as_ref().and_then(number_as_f64),
                            number_as_f64(op_val),
                        ) {
                            if a >= b {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    "$lte" => {
                        if let (Some(a), Some(b)) = (
                            actual.as_ref().and_then(number_as_f64),
                            number_as_f64(op_val),
                        ) {
                            if a > b {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    "$in" => {
                        if let bson::Bson::Array(arr) = op_val {
                            if let Some(ref a) = actual {
                                if !arr.contains(a) {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    "$nin" => {
                        if let bson::Bson::Array(arr) = op_val {
                            if let Some(ref a) = actual {
                                if arr.contains(a) {
                                    return false;
                                }
                            }
                        } else {
                            return false;
                        }
                    }
                    "$exists" => {
                        let exists = actual.is_some();
                        let want_exists = op_val.as_bool().unwrap_or(true);
                        if exists != want_exists {
                            return false;
                        }
                    }
                    _ => {
                        // Unknown operator - treat as not matching
                        return false;
                    }
                }
            }
            true
        }
        _ => {
            // Direct equality comparison
            actual.as_ref() == Some(expected)
        }
    }
}

fn eval_expr(doc: &Document, v: &bson::Bson) -> Option<bson::Bson> {
    match v {
        bson::Bson::String(s) if s.starts_with('$') => {
            let path = &s[1..];
            get_path_bson_value(doc, path)
        }
        bson::Bson::Document(d) => {
            // Object literal: keys that do not start with '$' are treated as nested fields
            if d.keys().any(|k| !k.starts_with('$')) {
                let mut out = Document::new();
                for (k, vv) in d.iter() {
                    if k.starts_with('$') {
                        // If any operator key appears alone, it will be handled below; otherwise skip unknown here
                        continue;
                    }
                    let ev = eval_expr(doc, vv).unwrap_or(bson::Bson::Null);
                    set_path_nested(&mut out, k, ev);
                }
                if !out.is_empty() {
                    return Some(bson::Bson::Document(out));
                }
            }
            if let Some(arrs) = d.get_array("$concatArrays").ok() {
                let mut out: Vec<bson::Bson> = Vec::new();
                for op in arrs {
                    let ev = eval_expr(doc, op)?;
                    match ev {
                        bson::Bson::Array(mut a) => {
                            out.append(&mut a);
                        }
                        _ => return None,
                    }
                }
                return Some(bson::Bson::Array(out));
            }
            if let Some(arg) = d.get("$toString") {
                // Support either scalar or single-element array
                let val = if let bson::Bson::Array(arr) = arg {
                    if arr.len() == 1 {
                        eval_expr(doc, &arr[0])
                    } else {
                        None
                    }
                } else {
                    eval_expr(doc, arg)
                };
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
            if let Some(arg) = d.get("$toBool") {
                let val = eval_expr(doc, arg).unwrap_or(bson::Bson::Null);
                return Some(to_bool(&val));
            }
            if let Some(arr) = d.get_array("$ifNull").ok() {
                if arr.len() == 2 {
                    let first = eval_expr(doc, &arr[0]);
                    match first {
                        Some(bson::Bson::Null) | None => {
                            let second = eval_expr(doc, &arr[1]).unwrap_or(bson::Bson::Null);
                            return Some(second);
                        }
                        Some(v) => {
                            return Some(v);
                        }
                    }
                }
            }
            if let Some(arr) = d.get_array("$add").ok() {
                // Evaluate operands first
                let mut vals: Vec<bson::Bson> = Vec::with_capacity(arr.len());
                for op in arr {
                    vals.push(eval_expr(doc, op).unwrap_or(bson::Bson::Null));
                }
                // If all integer types, return integer with appropriate width
                if vals
                    .iter()
                    .all(|v| matches!(v, bson::Bson::Int32(_) | bson::Bson::Int64(_)))
                {
                    let mut acc: i128 = 0;
                    for v in vals {
                        match v {
                            bson::Bson::Int32(n) => acc += n as i128,
                            bson::Bson::Int64(n) => acc += n as i128,
                            _ => {}
                        }
                    }
                    if acc >= i32::MIN as i128 && acc <= i32::MAX as i128 {
                        return Some(bson::Bson::Int32(acc as i32));
                    }
                    if acc >= i64::MIN as i128 && acc <= i64::MAX as i128 {
                        return Some(bson::Bson::Int64(acc as i64));
                    }
                    return Some(bson::Bson::Double(acc as f64));
                }
                // Otherwise, compute as double
                let mut sum = 0.0f64;
                for v in vals {
                    let n = number_as_f64(&v)?;
                    sum += n;
                }
                Some(bson::Bson::Double(sum))
            } else if let Some(arr) = d.get_array("$subtract").ok() {
                if arr.len() != 2 {
                    return None;
                }
                let a = number_as_f64(&eval_expr(doc, &arr[0])?)?;
                let b = number_as_f64(&eval_expr(doc, &arr[1])?)?;
                Some(bson::Bson::Double(a - b))
            } else if let Some(arr) = d.get_array("$multiply").ok() {
                let mut prod = 1.0f64;
                for op in arr {
                    prod *= number_as_f64(&eval_expr(doc, op)?)?;
                }
                Some(bson::Bson::Double(prod))
            } else if let Some(arr) = d.get_array("$divide").ok() {
                if arr.len() != 2 {
                    return None;
                }
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
        bson::Bson::Boolean(v) => {
            if *v {
                "true".into()
            } else {
                "false".into()
            }
        }
        bson::Bson::Null => String::new(),
        other => format!("{:?}", other),
    }
}

fn to_int(b: &bson::Bson) -> Option<bson::Bson> {
    match b {
        bson::Bson::Int32(n) => Some(bson::Bson::Int32(*n)),
        bson::Bson::Int64(n) => {
            if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                Some(bson::Bson::Int32(*n as i32))
            } else {
                Some(bson::Bson::Int64(*n))
            }
        }
        bson::Bson::Double(f) => Some(bson::Bson::Int32(*f as i32)),
        bson::Bson::String(s) => s.parse::<i64>().ok().map(|n| {
            if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                bson::Bson::Int32(n as i32)
            } else {
                bson::Bson::Int64(n)
            }
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

fn to_bool(b: &bson::Bson) -> bson::Bson {
    let truthy = match b {
        bson::Bson::Boolean(v) => *v,
        bson::Bson::Null => false,
        bson::Bson::String(s) => !s.is_empty(),
        bson::Bson::Int32(n) => *n != 0,
        bson::Bson::Int64(n) => *n != 0,
        bson::Bson::Double(f) => *f != 0.0,
        bson::Bson::Array(a) => !a.is_empty(),
        bson::Bson::Document(d) => !d.is_empty(),
        _ => true,
    };
    bson::Bson::Boolean(truthy)
}

fn apply_project_with_expr(input: &Document, proj: &Document) -> Document {
    use std::collections::HashSet;
    // Classify fields: includes (==1/true), excludes (==0/false), computed (other values)
    let mut include_id = true;
    let mut includes: HashSet<&str> = HashSet::new();
    let mut excludes: HashSet<&str> = HashSet::new();
    let mut computed: Vec<(&str, &bson::Bson)> = Vec::new();
    for (k, v) in proj.iter() {
        if k == "_id" {
            match v {
                bson::Bson::Int32(n) if *n == 0 => include_id = false,
                bson::Bson::Boolean(b) if !*b => include_id = false,
                _ => {}
            }
            continue;
        }
        match v {
            bson::Bson::Int32(n) => {
                if *n != 0 {
                    includes.insert(k);
                } else {
                    excludes.insert(k);
                }
            }
            bson::Bson::Boolean(b) => {
                if *b {
                    includes.insert(k);
                } else {
                    excludes.insert(k);
                }
            }
            other => {
                computed.push((k.as_str(), other));
            }
        }
    }

    // Inclusion mode when there are any includes or computed fields.
    let include_mode = !includes.is_empty() || !computed.is_empty();

    if include_mode {
        let mut out = Document::new();
        if include_id {
            if let Some(idv) = input.get("_id").cloned() {
                out.insert("_id", idv);
            }
        }
        for k in includes {
            if let Some(val) = get_path_bson_value(input, k) {
                set_path_nested(&mut out, k, val);
            }
        }
        for (k, expr) in computed {
            let val = eval_expr(input, expr).unwrap_or(bson::Bson::Null);
            out.insert(k.to_string(), val);
        }
        return out;
    }

    // Exclusion mode (only excludes and maybe _id suppression)
    let mut out = input.clone();
    for k in excludes {
        unset_path_nested(&mut out, k);
    }
    if !include_id {
        out.remove("_id");
    }
    out
}

// (removed duplicate unset_path_bson; single definition lives above)

async fn delete_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db {
        Some(d) => d,
        None => return error_doc(59, "Missing $db"),
    };
    let coll = match cmd.get_str("delete") {
        Ok(c) => c,
        Err(_) => return error_doc(9, "Invalid delete"),
    };
    let deletes = match cmd.get_array("deletes") {
        Ok(a) => a,
        Err(_) => {
            tracing::warn!(collection=%coll, cmd=?cmd, "delete missing 'deletes' array");
            return error_doc(9, "Missing deletes");
        }
    };
    if deletes.is_empty() {
        return error_doc(9, "Empty deletes");
    }
    let first = match &deletes[0] {
        bson::Bson::Document(d) => d,
        _ => return error_doc(9, "Invalid delete spec"),
    };
    let filter = match first.get_document("q") {
        Ok(d) => d.clone(),
        Err(_) => return error_doc(9, "Missing q"),
    };
    let limit = first.get_i32("limit").unwrap_or(1);
    if state.store.is_none() {
        return error_doc(13, "No storage configured");
    }
    let pg = state.store.as_ref().unwrap();
    if limit == 0 {
        match pg.delete_many_by_filter(dbname, coll, &filter).await {
            Ok(n) => doc! {"n": (n as i32), "ok": 1.0},
            Err(e) => error_doc(59, format!("delete failed: {}", e)),
        }
    } else if limit == 1 {
        match pg.delete_one_by_filter(dbname, coll, &filter).await {
            Ok(n) => doc! {"n": (n as i32), "ok": 1.0},
            Err(e) => error_doc(59, format!("delete failed: {}", e)),
        }
    } else {
        error_doc(2, "Only limit 0 (many) or 1 supported")
    }
}

async fn aggregate_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db {
        Some(d) => d,
        None => return error_doc(59, "Missing $db"),
    };
    let coll = match cmd.get_str("aggregate") {
        Ok(c) => c,
        Err(_) => return error_doc(9, "Invalid aggregate"),
    };
    let pipeline = match cmd.get_array("pipeline") {
        Ok(a) => a,
        Err(_) => {
            tracing::warn!(collection=%coll, cmd=?cmd, "aggregate missing 'pipeline' array");
            return error_doc(9, "Missing pipeline");
        }
    };
    let cursor_spec = cmd.get_document("cursor").unwrap_or(&doc! {}).clone();
    let batch_size = cursor_spec.get_i32("batchSize").unwrap_or(101) as i64;

    if state.store.is_none() {
        return error_doc(13, "No storage configured");
    }
    let pg = state.store.as_ref().unwrap();

    // First pass: scan for special stages that affect execution ($count, $facet, $out, $merge)
    // These need to be handled specially and may short-circuit normal execution
    let mut count_field: Option<String> = None;
    let mut facet_pipelines: Option<Vec<(String, Vec<bson::Bson>)>> = None;
    let mut out_coll: Option<String> = None;
    let mut merge_spec: Option<Document> = None;
    let mut stage_count = 0;
    for st in pipeline {
        stage_count += 1;
        if let bson::Bson::Document(stage) = st {
            if let Ok(cf) = stage.get_str("$count") {
                count_field = Some(cf.to_string());
                continue;
            }
            // $facet: document with sub-pipelines (must be last stage)
            if let Ok(facet_doc) = stage.get_document("$facet") {
                if stage_count < pipeline.len() {
                    return error_doc(9, "$facet must be the last stage in the pipeline");
                }
                // Parse facet sub-pipelines
                let mut parsed_facets: Vec<(String, Vec<bson::Bson>)> = Vec::new();
                for (facet_name, facet_pipeline_bson) in facet_doc.iter() {
                    if let bson::Bson::Array(arr) = facet_pipeline_bson {
                        parsed_facets.push((facet_name.clone(), arr.clone()));
                    } else {
                        return error_doc(
                            9,
                            format!(
                                "$facet field '{}' must be an array of pipeline stages",
                                facet_name
                            ),
                        );
                    }
                }
                if parsed_facets.is_empty() {
                    return error_doc(9, "$facet must have at least one sub-pipeline");
                }
                facet_pipelines = Some(parsed_facets);
                continue;
            }
            // $out: can be string or document with "coll" field
            if let Some(out_val) = stage.get("$out") {
                if stage_count < pipeline.len() {
                    return error_doc(9, "$out must be the last stage in the pipeline");
                }
                if let bson::Bson::String(s) = out_val {
                    out_coll = Some(s.clone());
                } else if let bson::Bson::Document(d) = out_val {
                    if let Ok(coll) = d.get_str("coll") {
                        out_coll = Some(coll.to_string());
                    } else {
                        return error_doc(9, "$out document must have a 'coll' field");
                    }
                } else {
                    return error_doc(9, "$out must be a string or document");
                }
                continue;
            }
            // $merge: document with required "into" field
            if let Ok(merge_doc) = stage.get_document("$merge") {
                if stage_count < pipeline.len() {
                    return error_doc(9, "$merge must be the last stage in the pipeline");
                }
                merge_spec = Some(merge_doc.clone());
                continue;
            }
        }
    }

    // Handle $count early: compute count from filter and adjust with skip/limit
    // Note: $count is a terminal stage that returns a single document with the count
    if let Some(field) = count_field {
        // Parse the pipeline to extract filter, skip, and limit for count calculation
        let mut filter: Option<bson::Document> = None;
        let mut skip: Option<i64> = None;
        let mut limit: Option<i64> = None;
        for st in pipeline {
            if let bson::Bson::Document(stage) = st {
                if let Ok(m) = stage.get_document("$match") {
                    filter = Some(m.clone());
                }
                if let Some(sk) = stage
                    .get_i64("$skip")
                    .ok()
                    .or(stage.get_i32("$skip").ok().map(|v| v as i64))
                {
                    skip = Some(sk);
                }
                if let Some(l) = stage
                    .get_i64("$limit")
                    .ok()
                    .or(stage.get_i32("$limit").ok().map(|v| v as i64))
                {
                    limit = Some(l);
                }
            }
        }
        let total = match pg.count_docs(dbname, coll, filter.as_ref()).await {
            Ok(n) => n,
            Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
        };
        let mut n = total;
        if let Some(sk) = skip {
            n = (n - sk).max(0);
        }
        if let Some(l) = limit {
            n = n.min(l);
        }
        // Match MongoDB's typical small-int behavior when it fits in 32-bit.
        let d = if n <= i32::MAX as i64 {
            doc! { field: (n as i32) }
        } else {
            doc! { field: n }
        };
        return doc! { "cursor": {"id": 0i64, "ns": format!("{}.{}", dbname, coll), "firstBatch": [d] }, "ok": 1.0 };
    }

    // Execute pipeline stages in order, handling $unionWith correctly
    // $unionWith concatenates union collection docs, and subsequent stages apply to the combined set
    let mut docs: Vec<Document> = Vec::new();
    let mut main_coll_fetched = false;

    for st in pipeline {
        if let bson::Bson::Document(stage) = st {
            // $match: filter current docs (or fetch main collection if not yet fetched)
            if let Ok(filter) = stage.get_document("$match") {
                if !main_coll_fetched {
                    // First $match before any $unionWith: fetch from main collection with filter
                    docs = match pg
                        .find_docs(dbname, coll, Some(filter), None, None, batch_size * 10)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                    };
                    main_coll_fetched = true;
                } else {
                    // $match after $unionWith: filter the combined docs
                    docs.retain(|d| document_matches_filter(d, filter));
                }
                continue;
            }

            // $unionWith: fetch from union collection and concatenate
            if stage.contains_key("$unionWith") {
                // Fetch main collection if not yet fetched (no filter)
                if !main_coll_fetched {
                    docs = match pg
                        .find_docs(dbname, coll, None, None, None, batch_size * 10)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                    };
                    main_coll_fetched = true;
                }

                // Parse $unionWith specification
                let (union_coll, union_pipeline): (String, Vec<bson::Bson>) = if let Ok(union_doc) =
                    stage.get_document("$unionWith")
                {
                    // Document format: { $unionWith: { coll: "name", pipeline: [...] } }
                    let coll_name = match union_doc.get_str("coll") {
                        Ok(c) => c.to_string(),
                        Err(_) => {
                            return error_doc(9, "$unionWith document must have a 'coll' field");
                        }
                    };
                    let pipeline = union_doc
                        .get_array("pipeline")
                        .ok()
                        .cloned()
                        .unwrap_or_default();
                    (coll_name, pipeline)
                } else if let Ok(coll_name) = stage.get_str("$unionWith") {
                    // String format: { $unionWith: "collName" }
                    (coll_name.to_string(), Vec::new())
                } else {
                    return error_doc(9, "$unionWith must be a string or document");
                };

                // Fetch all documents from the union collection (no filter)
                let union_docs = match pg
                    .find_docs(dbname, &union_coll, None, None, None, 100_000)
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        // MongoDB returns empty results if collection doesn't exist
                        tracing::debug!(error = %e, collection=%union_coll, "unionWith collection not found, returning empty");
                        Vec::new()
                    }
                };

                // If a pipeline is specified, execute it on the union collection documents
                let union_docs = if !union_pipeline.is_empty() {
                    match execute_sub_pipeline(union_docs, &union_pipeline) {
                        Ok(v) => v,
                        Err(err_msg) => {
                            return error_doc(9, format!("$unionWith pipeline error: {}", err_msg));
                        }
                    }
                } else {
                    union_docs
                };

                // Concatenate union results to current docs (current docs first, then union docs)
                docs.extend(union_docs);
                continue;
            }

            // $sort: sort current docs
            if let Ok(sort_spec) = stage.get_document("$sort") {
                // Fetch main collection if not yet fetched
                if !main_coll_fetched {
                    docs = match pg
                        .find_docs(dbname, coll, None, Some(sort_spec), None, batch_size * 10)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                    };
                    main_coll_fetched = true;
                } else {
                    // Sort the current docs in memory
                    docs.sort_by(|a, b| cmp_multi(a, b, sort_spec));
                }
                continue;
            }

            // $limit: limit current docs
            if let Some(limit_val) = stage
                .get_i64("$limit")
                .ok()
                .or(stage.get_i32("$limit").ok().map(|v| v as i64))
            {
                // Fetch main collection if not yet fetched
                if !main_coll_fetched {
                    docs = match pg
                        .find_docs(dbname, coll, None, None, None, limit_val)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                    };
                    main_coll_fetched = true;
                } else if limit_val >= 0 && (limit_val as usize) < docs.len() {
                    docs.truncate(limit_val as usize);
                }
                continue;
            }

            // $skip: skip current docs
            if let Some(skip_val) = stage
                .get_i64("$skip")
                .ok()
                .or(stage.get_i32("$skip").ok().map(|v| v as i64))
            {
                // Fetch main collection if not yet fetched
                if !main_coll_fetched {
                    docs = match pg
                        .find_docs(dbname, coll, None, None, None, skip_val + batch_size * 10)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                    };
                    main_coll_fetched = true;
                }
                if skip_val > 0 {
                    let skip_count = (skip_val as usize).min(docs.len());
                    docs.drain(0..skip_count);
                }
                continue;
            }

            // $project: project current docs
            if let Ok(proj) = stage.get_document("$project") {
                // Check if this is a computed projection (has field refs like "$v" or exprs like {"$add": [...]})
                let has_computed = proj.iter().any(|(_k, v)| {
                    !matches!(v, bson::Bson::Int32(n) if *n == 0 || *n == 1)
                        && !matches!(v, bson::Bson::Boolean(b) if *b || !*b)
                });
                // Fetch main collection if not yet fetched
                if !main_coll_fetched {
                    if has_computed {
                        // Can't push down computed projections - fetch full docs and apply in memory
                        docs = match pg
                            .find_docs(dbname, coll, None, None, None, batch_size * 10)
                            .await
                        {
                            Ok(v) => v,
                            Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                        };
                        main_coll_fetched = true;
                        // Apply computed projection in memory
                        for d in docs.iter_mut() {
                            *d = apply_project_with_expr(d, proj);
                        }
                    } else {
                        // Simple projection - can try SQL pushdown
                        docs = match pg
                            .find_docs(dbname, coll, None, None, Some(proj), batch_size * 10)
                            .await
                        {
                            Ok(v) => v,
                            Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                        };
                        main_coll_fetched = true;
                    }
                } else {
                    // Apply projection in memory to existing docs
                    for d in docs.iter_mut() {
                        *d = apply_project_with_expr(d, proj);
                    }
                }
                continue;
            }

            // $addFields / $set: add fields to current docs
            if let Ok(af) = stage
                .get_document("$addFields")
                .or(stage.get_document("$set"))
            {
                // Fetch main collection if not yet fetched
                if !main_coll_fetched {
                    docs = match pg
                        .find_docs(dbname, coll, None, None, None, batch_size * 10)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                    };
                    main_coll_fetched = true;
                }
                for d in docs.iter_mut() {
                    for (k, v) in af.iter() {
                        let val = eval_expr(d, v).unwrap_or(bson::Bson::Null);
                        set_path_nested(d, k, val);
                    }
                }
                continue;
            }

            // $unwind: unwind arrays in current docs
            if let Ok(u) = stage.get_str("$unwind") {
                let path = u.trim_start_matches('$').to_string();
                // Fetch main collection if not yet fetched
                if !main_coll_fetched {
                    docs = match pg
                        .find_docs(dbname, coll, None, None, None, batch_size * 10)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                    };
                    main_coll_fetched = true;
                }
                let mut out: Vec<Document> = Vec::new();
                for d in docs.into_iter() {
                    match get_path_bson_value(&d, &path) {
                        Some(bson::Bson::Array(arr)) => {
                            for item in arr {
                                let mut nd = d.clone();
                                set_path_nested(&mut nd, &path, item);
                                out.push(nd);
                            }
                        }
                        _ => {}
                    }
                }
                docs = out;
                continue;
            }
            if let Ok(ud) = stage.get_document("$unwind") {
                if let Ok(pth) = ud.get_str("path") {
                    let path = pth.trim_start_matches('$').to_string();
                    let preserve = ud.get_bool("preserveNullAndEmptyArrays").unwrap_or(false);
                    let include = ud.get_str("includeArrayIndex").ok().map(|s| s.to_string());
                    // Fetch main collection if not yet fetched
                    if !main_coll_fetched {
                        docs = match pg
                            .find_docs(dbname, coll, None, None, None, batch_size * 10)
                            .await
                        {
                            Ok(v) => v,
                            Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                        };
                        main_coll_fetched = true;
                    }
                    let mut out: Vec<Document> = Vec::new();
                    for d in docs.into_iter() {
                        match get_path_bson_value(&d, &path) {
                            Some(bson::Bson::Array(arr)) => {
                                if arr.is_empty() {
                                    if preserve {
                                        let mut nd = d.clone();
                                        set_path_nested(&mut nd, &path, bson::Bson::Null);
                                        if let Some(ref fpath) = include {
                                            set_path_nested(&mut nd, fpath, bson::Bson::Null);
                                        }
                                        out.push(nd);
                                    }
                                    continue;
                                }
                                for (i, item) in arr.into_iter().enumerate() {
                                    let mut nd = d.clone();
                                    set_path_nested(&mut nd, &path, item);
                                    if let Some(ref fpath) = include {
                                        set_path_nested(
                                            &mut nd,
                                            fpath,
                                            bson::Bson::Int32(i as i32),
                                        );
                                    }
                                    out.push(nd);
                                }
                            }
                            None | Some(bson::Bson::Null) => {
                                if preserve {
                                    let mut nd = d.clone();
                                    set_path_nested(&mut nd, &path, bson::Bson::Null);
                                    if let Some(ref fpath) = include {
                                        set_path_nested(&mut nd, fpath, bson::Bson::Null);
                                    }
                                    out.push(nd);
                                }
                            }
                            _ => {}
                        }
                    }
                    docs = out;
                    continue;
                }
            }

            // $group: group current docs
            if let Ok(gspec) = stage.get_document("$group") {
                // Fetch main collection if not yet fetched
                if !main_coll_fetched {
                    docs = match pg
                        .find_docs(dbname, coll, None, None, None, batch_size * 10)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                    };
                    main_coll_fetched = true;
                }
                docs = apply_group(&docs, gspec);
                continue;
            }

            // $bucket: bucket current docs
            if let Ok(b) = stage.get_document("$bucket") {
                // Parse $bucket specification
                let group_by = match b.get("groupBy") {
                    Some(bson::Bson::String(s)) => s.clone(),
                    _ => {
                        return error_doc(9, "$bucket requires groupBy expression");
                    }
                };

                let boundaries = match b.get_array("boundaries") {
                    Ok(arr) => arr.clone(),
                    Err(_) => {
                        return error_doc(9, "$bucket requires boundaries array");
                    }
                };

                if boundaries.len() < 2 {
                    return error_doc(9, "$bucket boundaries must have at least 2 elements");
                }

                // Check boundaries are sorted
                for i in 1..boundaries.len() {
                    if cmp_bson(&boundaries[i], &boundaries[i - 1]) != std::cmp::Ordering::Greater {
                        return error_doc(
                            9,
                            "$bucket boundaries must be sorted in ascending order",
                        );
                    }
                }

                let default = b.get("default").cloned();
                let output = b.get_document("output").ok().cloned();

                let bspec = BucketSpec {
                    group_by,
                    boundaries,
                    default,
                    output,
                };

                // Fetch main collection if not yet fetched
                if !main_coll_fetched {
                    docs = match pg
                        .find_docs(dbname, coll, None, None, None, batch_size * 10)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                    };
                    main_coll_fetched = true;
                }
                docs = apply_bucket(&docs, &bspec);
                continue;
            }

            // $lookup: lookup from another collection
            if let Ok(lspec) = stage.get_document("$lookup") {
                // Fetch main collection if not yet fetched
                if !main_coll_fetched {
                    docs = match pg
                        .find_docs(dbname, coll, None, None, None, batch_size * 10)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                    };
                    main_coll_fetched = true;
                }
                docs = apply_lookup(dbname, pg, docs, lspec)
                    .await
                    .unwrap_or_else(|_| Vec::new());
                continue;
            }

            // $sample: random sample of current docs
            if let Ok(sample_doc) = stage.get_document("$sample") {
                let sample_size = if let Ok(size) = sample_doc.get_i64("size") {
                    size
                } else if let Ok(size) = sample_doc.get_i32("size") {
                    size as i64
                } else {
                    return error_doc(9, "$sample must have a numeric 'size' field");
                };

                // Fetch main collection if not yet fetched
                if !main_coll_fetched {
                    docs = match pg
                        .find_docs(dbname, coll, None, None, None, batch_size * 10)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                    };
                    main_coll_fetched = true;
                }

                if sample_size <= 0 {
                    docs.clear();
                } else {
                    let n = sample_size as usize;
                    docs.shuffle(&mut rand::thread_rng());
                    if n < docs.len() {
                        docs.truncate(n);
                    }
                }
                continue;
            }

            // $facet: handled separately after the main loop
            if stage.contains_key("$facet") {
                // Fetch main collection if not yet fetched (needed for facet sub-pipelines)
                if !main_coll_fetched {
                    docs = match pg
                        .find_docs(dbname, coll, None, None, None, batch_size * 10)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
                    };
                    main_coll_fetched = true;
                }
                // $facet is processed after the main loop, so just continue here
                continue;
            }

            // Unknown $stage: error
            if stage.keys().any(|k| k.starts_with('$')) {
                return error_doc(
                    9,
                    format!(
                        "Unsupported pipeline stage: {}",
                        stage.keys().next().unwrap()
                    ),
                );
            }
        }
    }

    // If no stages were processed that fetched the main collection, fetch it now
    if !main_coll_fetched {
        docs = match pg
            .find_docs(dbname, coll, None, None, None, batch_size * 10)
            .await
        {
            Ok(v) => v,
            Err(e) => return error_doc(59, format!("aggregate failed: {}", e)),
        };
    }

    // Handle $facet: execute sub-pipelines and return single document with results
    if let Some(facets) = facet_pipelines {
        let mut facet_result = Document::new();

        for (facet_name, sub_pipeline) in facets {
            match execute_sub_pipeline(docs.clone(), &sub_pipeline) {
                Ok(facet_docs) => {
                    let facet_array: Vec<bson::Bson> = facet_docs
                        .into_iter()
                        .map(|d| bson::Bson::Document(d))
                        .collect();
                    facet_result.insert(facet_name, bson::Bson::Array(facet_array));
                }
                Err(err_msg) => {
                    return error_doc(9, format!("$facet sub-pipeline error: {}", err_msg));
                }
            }
        }

        // $facet always returns a single document with all facet results
        return doc! {
            "cursor": {
                "id": 0i64,
                "ns": format!("{}.{}", dbname, coll),
                "firstBatch": [facet_result]
            },
            "ok": 1.0
        };
    }

    // Handle $out: write results to a new collection
    if let Some(target_coll) = out_coll {
        return handle_out_stage(state, dbname, &target_coll, docs).await;
    }

    // Handle $merge: upsert results into existing collection
    if let Some(merge_doc) = merge_spec {
        return handle_merge_stage(state, dbname, &merge_doc, docs).await;
    }

    let (first_batch, remainder): (Vec<_>, Vec<_>) = docs
        .into_iter()
        .enumerate()
        .partition(|(i, _)| (*i as i64) < batch_size);
    let first_docs: Vec<Document> = first_batch.into_iter().map(|(_, d)| d).collect();
    let rest_docs: Vec<Document> = remainder.into_iter().map(|(_, d)| d).collect();
    let ns = format!("{}.{}", dbname, coll);
    let mut cursor_doc = doc! { "ns": ns.clone(), "firstBatch": first_docs };
    let cursor_id = if rest_docs.is_empty() {
        0i64
    } else {
        new_cursor(state, ns, rest_docs).await
    };
    cursor_doc.insert("id", cursor_id);
    doc! { "cursor": cursor_doc, "ok": 1.0 }
}

async fn handle_out_stage(
    state: &AppState,
    dbname: &str,
    target_coll: &str,
    docs: Vec<Document>,
) -> Document {
    if state.store.is_none() {
        return error_doc(13, "No storage configured");
    }
    let pg = state.store.as_ref().unwrap();

    // Ensure the target collection exists
    if let Err(e) = pg.ensure_collection(dbname, target_coll).await {
        return error_doc(59, format!("Failed to create target collection: {}", e));
    }

    // Delete all existing documents in the target collection
    match pg
        .delete_many_by_filter(dbname, target_coll, &doc! {})
        .await
    {
        Ok(_) => {}
        Err(e) => {
            return error_doc(59, format!("Failed to clear target collection: {}", e));
        }
    }

    // Insert all documents from the aggregation result
    let mut inserted_count = 0i32;
    for doc in docs {
        let mut d = doc;
        ensure_id(&mut d);
        let idb = match d.get("_id").and_then(|b| id_bytes_bson(b)) {
            Some(v) => v,
            None => {
                continue;
            }
        };
        let json = match serde_json::to_value(&d) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let bson_bytes = match bson::to_vec(&d) {
            Ok(v) => v,
            Err(_) => continue,
        };
        match pg
            .insert_one(dbname, target_coll, &idb, &bson_bytes, &json)
            .await
        {
            Ok(n) => {
                if n == 1 {
                    inserted_count += 1;
                }
            }
            Err(_) => {
                // Continue on error for individual documents
            }
        }
    }

    doc! { "ok": 1.0, "nInserted": inserted_count }
}

async fn handle_merge_stage(
    state: &AppState,
    dbname: &str,
    merge_spec: &Document,
    docs: Vec<Document>,
) -> Document {
    if state.store.is_none() {
        return error_doc(13, "No storage configured");
    }
    let pg = state.store.as_ref().unwrap();

    // Get target collection name from "into" field
    let target_coll = match merge_spec.get("into") {
        Some(bson::Bson::String(s)) => s.clone(),
        Some(bson::Bson::Document(d)) => {
            // Support { into: { coll: "name" } } format
            match d.get_str("coll") {
                Ok(s) => s.to_string(),
                Err(_) => return error_doc(9, "$merge 'into' must specify a collection"),
            }
        }
        _ => return error_doc(9, "$merge 'into' must be a string or document"),
    };

    // Get merge options with defaults
    let on_field = merge_spec.get_str("on").unwrap_or("_id");
    let when_matched = merge_spec.get_str("whenMatched").unwrap_or("merge");
    let when_not_matched = merge_spec.get_str("whenNotMatched").unwrap_or("insert");

    // Ensure the target collection exists
    if let Err(e) = pg.ensure_collection(dbname, &target_coll).await {
        return error_doc(59, format!("Failed to ensure target collection: {}", e));
    }

    let mut matched_count = 0i32;
    let mut modified_count = 0i32;
    let mut inserted_count = 0i32;

    for mut doc in docs {
        // Ensure the document has an _id
        ensure_id(&mut doc);

        // Build filter based on the "on" field
        let filter = if on_field == "_id" {
            doc.get("_id")
                .map(|id| doc! { "_id": id.clone() })
                .unwrap_or_else(|| doc! {})
        } else {
            // For other fields, build a filter from the document
            let mut filter_doc = doc! {};
            if let Some(val) = get_path_bson_value(&doc, on_field) {
                filter_doc.insert(on_field, val);
            }
            filter_doc
        };

        // Check if document exists in target
        let existing = match pg.find_one_for_update(dbname, &target_coll, &filter).await {
            Ok(v) => v,
            Err(_) => None,
        };

        if let Some((idb, mut existing_doc)) = existing {
            // Document exists - handle whenMatched
            matched_count += 1;

            match when_matched {
                "merge" => {
                    // Merge fields from source into existing document
                    for (k, v) in doc.iter() {
                        if k != "_id" {
                            set_path_nested(&mut existing_doc, k, v.clone());
                        }
                    }
                    if let Err(_) = pg
                        .update_doc_by_id(dbname, &target_coll, &idb, &existing_doc)
                        .await
                    {
                        // Continue on error
                    } else {
                        modified_count += 1;
                    }
                }
                "replace" => {
                    // Replace entire document but keep _id
                    let id_val = existing_doc.get("_id").cloned();
                    doc.insert("_id", id_val.unwrap_or(bson::Bson::Null));
                    if let Err(_) = pg.update_doc_by_id(dbname, &target_coll, &idb, &doc).await {
                        // Continue on error
                    } else {
                        modified_count += 1;
                    }
                }
                "keepExisting" => {
                    // Do nothing, keep existing document
                }
                "fail" => {
                    return error_doc(59, "Document already exists in target collection");
                }
                _ => {
                    // Default to merge
                    for (k, v) in doc.iter() {
                        if k != "_id" {
                            set_path_nested(&mut existing_doc, k, v.clone());
                        }
                    }
                    if let Err(_) = pg
                        .update_doc_by_id(dbname, &target_coll, &idb, &existing_doc)
                        .await
                    {
                        // Continue on error
                    } else {
                        modified_count += 1;
                    }
                }
            }
        } else {
            // Document doesn't exist - handle whenNotMatched
            match when_not_matched {
                "insert" => {
                    let idb = match doc.get("_id").and_then(|b| id_bytes_bson(b)) {
                        Some(v) => v,
                        None => continue,
                    };
                    let json = match serde_json::to_value(&doc) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    let bson_bytes = match bson::to_vec(&doc) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    match pg
                        .insert_one(dbname, &target_coll, &idb, &bson_bytes, &json)
                        .await
                    {
                        Ok(n) => {
                            if n == 1 {
                                inserted_count += 1;
                            }
                        }
                        Err(_) => {
                            // Continue on error
                        }
                    }
                }
                "discard" => {
                    // Do nothing, discard the document
                }
                _ => {
                    // Default to insert
                    let idb = match doc.get("_id").and_then(|b| id_bytes_bson(b)) {
                        Some(v) => v,
                        None => continue,
                    };
                    let json = match serde_json::to_value(&doc) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    let bson_bytes = match bson::to_vec(&doc) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    match pg
                        .insert_one(dbname, &target_coll, &idb, &bson_bytes, &json)
                        .await
                    {
                        Ok(n) => {
                            if n == 1 {
                                inserted_count += 1;
                            }
                        }
                        Err(_) => {
                            // Continue on error
                        }
                    }
                }
            }
        }
    }

    doc! {
        "ok": 1.0,
        "nMatched": matched_count,
        "nModified": modified_count,
        "nInserted": inserted_count,
    }
}

async fn find_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db {
        Some(d) => d,
        None => return error_doc(59, "Missing $db"),
    };
    let coll = match cmd.get_str("find") {
        Ok(c) => c,
        Err(_) => return error_doc(9, "Invalid find"),
    };
    let limit = cmd
        .get_i64("limit")
        .ok()
        .or(cmd.get_i32("limit").ok().map(|v| v as i64))
        .unwrap_or(0);
    let batch_size = cmd
        .get_i64("batchSize")
        .ok()
        .or(cmd.get_i32("batchSize").ok().map(|v| v as i64))
        .unwrap_or(0);
    let first_batch_limit = if limit > 0 {
        limit
    } else if batch_size > 0 {
        batch_size
    } else {
        101
    };
    let filter = cmd
        .get_document("filter")
        .ok()
        .or(cmd.get_document("query").ok());
    let sort = cmd.get_document("sort").ok();
    let projection = cmd.get_document("projection").ok();

    if let Some(ref pg) = state.store {
        // Check if we're in a transaction
        let in_transaction = if let Some(lsid) = extract_lsid(cmd) {
            if let Some(autocommit) = extract_autocommit(cmd) {
                if !autocommit {
                    if let Some(session_arc) = state.session_manager.get_session(lsid).await {
                        let session = session_arc.lock().await;
                        session.in_transaction
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        let docs: Vec<Document> = if in_transaction {
            // In transaction - get session and perform find with transaction client
            if let Some(lsid) = extract_lsid(cmd) {
                if let Some(session_arc) = state.session_manager.get_session(lsid).await {
                    let session = session_arc.lock().await;
                    if let Some(ref client) = session.postgres_client {
                        if let Some(f) = filter {
                            if let Some(idv) = f.get("_id") {
                                if let Some(idb) = id_bytes_bson(idv) {
                                    match pg
                                        .find_by_id_docs_with_client(
                                            client,
                                            dbname,
                                            coll,
                                            &idb,
                                            first_batch_limit * 10,
                                        )
                                        .await
                                    {
                                        Ok(v) => v,
                                        Err(e) => {
                                            tracing::warn!("find_by_id_with_client failed: {}", e);
                                            Vec::new()
                                        }
                                    }
                                } else {
                                    match pg
                                        .find_docs_with_client(
                                            client,
                                            dbname,
                                            coll,
                                            Some(f),
                                            sort,
                                            projection,
                                            first_batch_limit * 10,
                                        )
                                        .await
                                    {
                                        Ok(v) => v,
                                        Err(e) => {
                                            tracing::warn!("find_docs_with_client failed: {}", e);
                                            Vec::new()
                                        }
                                    }
                                }
                            } else {
                                match pg
                                    .find_docs_with_client(
                                        client,
                                        dbname,
                                        coll,
                                        Some(f),
                                        sort,
                                        projection,
                                        first_batch_limit * 10,
                                    )
                                    .await
                                {
                                    Ok(v) => v,
                                    Err(e) => {
                                        tracing::warn!("find_docs_with_client failed: {}", e);
                                        Vec::new()
                                    }
                                }
                            }
                        } else {
                            match pg
                                .find_docs_with_client(
                                    client,
                                    dbname,
                                    coll,
                                    None,
                                    sort,
                                    projection,
                                    first_batch_limit * 10,
                                )
                                .await
                            {
                                Ok(v) => v,
                                Err(e) => {
                                    tracing::warn!("find_docs_with_client failed: {}", e);
                                    Vec::new()
                                }
                            }
                        }
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        } else {
            // Not in transaction - use pool
            if let Some(f) = filter {
                if let Some(idv) = f.get("_id") {
                    if let Some(idb) = id_bytes_bson(idv) {
                        match pg
                            .find_by_id_docs(dbname, coll, &idb, first_batch_limit * 10)
                            .await
                        {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!("find_by_id failed: {}", e);
                                Vec::new()
                            }
                        }
                    } else {
                        match pg
                            .find_docs(
                                dbname,
                                coll,
                                Some(f),
                                sort,
                                projection,
                                first_batch_limit * 10,
                            )
                            .await
                        {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!("find_docs failed: {}", e);
                                Vec::new()
                            }
                        }
                    }
                } else {
                    match pg
                        .find_docs(
                            dbname,
                            coll,
                            Some(f),
                            sort,
                            projection,
                            first_batch_limit * 10,
                        )
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("find_docs failed: {}", e);
                            Vec::new()
                        }
                    }
                }
            } else {
                match pg
                    .find_docs(dbname, coll, None, sort, projection, first_batch_limit * 10)
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!("find_docs failed: {}", e);
                        Vec::new()
                    }
                }
            }
        };

        let mut first_batch: Vec<Document> = Vec::new();
        let mut remainder: Vec<Document> = Vec::new();
        for (idx, d) in docs.into_iter().enumerate() {
            if (idx as i64) < first_batch_limit {
                first_batch.push(d);
            } else {
                remainder.push(d);
            }
        }
        let ns = format!("{}.{}", dbname, coll);
        let mut cursor_doc = doc! { "ns": ns.clone(), "firstBatch": first_batch };
        let cursor_id = if !remainder.is_empty() {
            new_cursor(state, ns, remainder).await
        } else {
            0i64
        };
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
    let entry = CursorEntry {
        ns,
        docs,
        pos: 0,
        last_access: Instant::now(),
    };
    let mut map = state.cursors.lock().await;
    map.insert(id, entry);
    id
}

async fn get_more_reply(state: &AppState, cmd: &Document) -> Document {
    let cursor_id = match cmd.get_i64("getMore") {
        Ok(v) => v,
        Err(_) => return error_doc(9, "Invalid getMore"),
    };
    let batch_size = cmd.get_i32("batchSize").unwrap_or(101) as usize;
    let mut map = state.cursors.lock().await;
    if let Some(entry) = map.get_mut(&cursor_id) {
        let ns = entry.ns.clone();
        let start = entry.pos;
        let end = (start + batch_size).min(entry.docs.len());
        let mut next_batch = Vec::with_capacity(end - start);
        for d in &entry.docs[start..end] {
            next_batch.push(d.clone());
        }
        entry.pos = end;
        entry.last_access = Instant::now();
        let id = if entry.pos >= entry.docs.len() {
            map.remove(&cursor_id);
            0i64
        } else {
            cursor_id
        };
        return doc! { "cursor": {"id": id, "ns": ns, "nextBatch": next_batch}, "ok": 1.0 };
    }
    // Unknown cursor id
    let ns = match cmd.get_str("collection") {
        Ok(c) => c.to_string(),
        Err(_) => String::new(),
    };
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
        tracing::debug!(
            removed = before - after,
            remaining = after,
            "pruned idle cursors"
        );
    }
}

// Helper to extract UUID from lsid document
fn extract_lsid(cmd: &Document) -> Option<Uuid> {
    cmd.get_document("lsid").ok().and_then(|lsid_doc| {
        lsid_doc.get("id").and_then(|bson_val| {
            if let bson::Bson::Binary(binary) = bson_val {
                // UUID is stored as a Binary with subtype 4
                if binary.subtype == bson::spec::BinarySubtype::Uuid {
                    Uuid::from_slice(binary.bytes.as_slice()).ok()
                } else {
                    None
                }
            } else {
                None
            }
        })
    })
}

// Helper to extract txnNumber from command
fn extract_txn_number(cmd: &Document) -> Option<i64> {
    cmd.get_i64("txnNumber")
        .ok()
        .or_else(|| cmd.get_i32("txnNumber").ok().map(|v| v as i64))
}

// Helper to extract autocommit from command
fn extract_autocommit(cmd: &Document) -> Option<bool> {
    cmd.get_bool("autocommit").ok()
}

async fn start_transaction_reply(state: &AppState, _db: Option<&str>, cmd: &Document) -> Document {
    let lsid = match extract_lsid(cmd) {
        Some(id) => id,
        None => return error_doc(ERROR_ILLEGAL_OPERATION, "Missing or invalid lsid"),
    };

    let txn_number = match extract_txn_number(cmd) {
        Some(n) => n,
        None => return error_doc(ERROR_ILLEGAL_OPERATION, "Missing txnNumber"),
    };

    let session_arc = state.session_manager.get_or_create_session(lsid).await;
    let mut session = session_arc.lock().await;

    // Validate txnNumber is monotonically increasing
    if let Err(code) = session.validate_txn_number(txn_number) {
        return error_doc(code, "Invalid txnNumber");
    }

    // Check if already in transaction
    if session.in_transaction {
        return error_doc(ERROR_ILLEGAL_OPERATION, "Transaction already in progress");
    }

    // Get the pool from store
    let pg = match state.store.as_ref() {
        Some(pg) => pg,
        None => return error_doc(13, "No storage configured"),
    };

    // Start the transaction
    if let Err(e) = session.start_transaction(pg.pool()).await {
        return error_doc(ERROR_NO_SUCH_TRANSACTION, e);
    }

    // Update session state
    session.autocommit = extract_autocommit(cmd).unwrap_or(true);

    doc! { "ok": 1.0 }
}

async fn commit_transaction_reply(state: &AppState, _db: Option<&str>, cmd: &Document) -> Document {
    let lsid = match extract_lsid(cmd) {
        Some(id) => id,
        None => return error_doc(ERROR_ILLEGAL_OPERATION, "Missing or invalid lsid"),
    };

    let txn_number = match extract_txn_number(cmd) {
        Some(n) => n,
        None => return error_doc(ERROR_ILLEGAL_OPERATION, "Missing txnNumber"),
    };

    let session_arc = match state.session_manager.get_session(lsid).await {
        Some(s) => s,
        None => return error_doc(ERROR_NO_SUCH_TRANSACTION, "Session not found"),
    };

    let mut session = session_arc.lock().await;

    // Check if in transaction
    if !session.in_transaction {
        return error_doc(ERROR_NO_SUCH_TRANSACTION, "No transaction in progress");
    }

    // Check for transaction expiry
    if session.is_transaction_expired(Duration::from_secs(60)) {
        let _ = session.abort_transaction().await;
        return error_doc(ERROR_TRANSACTION_EXPIRED, "Transaction has expired");
    }

    // Validate txnNumber
    if txn_number < session.txn_number {
        // This is a retry - check for cached result
        if let Some(result) = session.get_retryable_write(txn_number).cloned() {
            return result.to_document();
        }
    }

    // Commit the transaction
    if let Err(e) = session.commit_transaction().await {
        return error_doc(ERROR_NO_SUCH_TRANSACTION, e);
    }

    doc! { "ok": 1.0 }
}

async fn abort_transaction_reply(state: &AppState, _db: Option<&str>, cmd: &Document) -> Document {
    let lsid = match extract_lsid(cmd) {
        Some(id) => id,
        None => return error_doc(ERROR_ILLEGAL_OPERATION, "Missing or invalid lsid"),
    };

    let session_arc = match state.session_manager.get_session(lsid).await {
        Some(s) => s,
        None => return error_doc(ERROR_NO_SUCH_TRANSACTION, "Session not found"),
    };

    let mut session = session_arc.lock().await;

    // Check if in transaction
    if !session.in_transaction {
        return error_doc(ERROR_NO_SUCH_TRANSACTION, "No transaction in progress");
    }

    // Abort the transaction
    if let Err(e) = session.abort_transaction().await {
        return error_doc(ERROR_NO_SUCH_TRANSACTION, e);
    }

    doc! { "ok": 1.0 }
}

async fn end_sessions_reply(state: &AppState, cmd: &Document) -> Document {
    let ids = match cmd.get_array("endSessions") {
        Ok(arr) => arr,
        Err(_) => return error_doc(ERROR_ILLEGAL_OPERATION, "Missing endSessions array"),
    };

    for id_bson in ids {
        if let Some(lsid_doc) = id_bson.as_document() {
            if let Some(bson_val) = lsid_doc.get("id") {
                if let bson::Bson::Binary(binary) = bson_val {
                    if binary.subtype == bson::spec::BinarySubtype::Uuid {
                        if let Ok(uuid) = Uuid::from_slice(binary.bytes.as_slice()) {
                            state.session_manager.end_session(uuid).await;
                        }
                    }
                }
            }
        }
    }

    doc! { "ok": 1.0 }
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
            session_manager: std::sync::Arc::new(SessionManager::new()),
            request_count: AtomicU64::new(0),
            request_duration_ms: AtomicU64::new(0),
            query_count: AtomicU64::new(0),
            insert_count: AtomicU64::new(0),
            update_count: AtomicU64::new(0),
            delete_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            active_connections: AtomicU32::new(0),
        };
        {
            let mut map = state.cursors.lock().await;
            map.insert(
                1,
                CursorEntry {
                    ns: "db.coll".into(),
                    docs: vec![],
                    pos: 0,
                    last_access: Instant::now() - Duration::from_secs(100),
                },
            );
        }
        prune_cursors_once(&state, Duration::from_secs(10)).await;
        let map = state.cursors.lock().await;
        assert!(map.is_empty());
    }
}

async fn kill_cursors_reply(state: &AppState, cmd: &Document) -> Document {
    let cursors = match cmd.get_array("cursors") {
        Ok(a) => a,
        Err(_) => return error_doc(9, "Missing cursors"),
    };
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
            if map.remove(&id).is_some() {
                killed.push(id);
            } else {
                not_found.push(id);
            }
        }
    }
    doc! {
        "cursorsKilled": killed,
        "cursorsNotFound": not_found,
        "cursorsAlive": Vec::<i32>::new(),
        "cursorsUnknown": Vec::<i32>::new(),
        "ok": 1.0
    }
}

async fn create_indexes_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db {
        Some(d) => d,
        None => return error_doc(59, "Missing $db"),
    };
    let coll = match cmd.get_str("createIndexes") {
        Ok(c) => c,
        Err(_) => return error_doc(9, "Invalid createIndexes"),
    };
    let indexes = match cmd.get_array("indexes") {
        Ok(a) => a,
        Err(_) => return error_doc(9, "Missing indexes"),
    };
    if state.store.is_none() {
        return error_doc(13, "No storage configured");
    }
    let pg = state.store.as_ref().unwrap();
    // Ensure collection exists so index DDL succeeds
    if let Err(e) = pg.ensure_collection(dbname, coll).await {
        tracing::warn!("ensure_collection before createIndexes failed: {}", e);
    }
    let mut created = 0i32;
    for spec_b in indexes {
        let spec_doc = match spec_b {
            bson::Bson::Document(d) => d,
            _ => continue,
        };
        let name = match spec_doc.get_str("name") {
            Ok(n) => n,
            Err(_) => continue,
        };
        let key = match spec_doc.get_document("key") {
            Ok(k) => k,
            Err(_) => continue,
        };
        let spec_json = match bson::to_bson(spec_doc) {
            Ok(b) => match serde_json::to_value(&b) {
                Ok(v) => v,
                Err(_) => serde_json::json!({}),
            },
            Err(_) => serde_json::json!({}),
        };
        if key.len() == 1 {
            let (field, order_b) = key.iter().next().unwrap();
            // Check if this is a 2dsphere index
            if let bson::Bson::String(s) = order_b {
                if s == "2dsphere" {
                    if let Err(e) = pg
                        .create_index_2dsphere(dbname, coll, name, field, &spec_json)
                        .await
                    {
                        tracing::warn!("create_index(2dsphere) failed: {}", e);
                    } else {
                        created += 1;
                    }
                } else {
                    tracing::warn!("Unknown index type: {}", s);
                }
            } else {
                let order = match order_b {
                    bson::Bson::Int32(n) => *n,
                    bson::Bson::Int64(n) => *n as i32,
                    _ => 1,
                };
                if let Err(e) = pg
                    .create_index_single_field(dbname, coll, name, field, order, &spec_json)
                    .await
                {
                    tracing::warn!("create_index(single) failed: {}", e);
                } else {
                    created += 1;
                }
            }
        } else {
            let mut fields: Vec<(String, i32)> = Vec::with_capacity(key.len());
            for (k, v) in key.iter() {
                let ord = match v {
                    bson::Bson::Int32(n) => *n,
                    bson::Bson::Int64(n) => *n as i32,
                    _ => 1,
                };
                fields.push((k.to_string(), ord));
            }
            if let Err(e) = pg
                .create_index_compound(dbname, coll, name, &fields, &spec_json)
                .await
            {
                tracing::warn!("create_index(compound) failed: {}", e);
            } else {
                created += 1;
            }
        }
    }
    doc! { "createdIndexes": created, "ok": 1.0 }
}

async fn drop_indexes_reply(state: &AppState, db: Option<&str>, cmd: &Document) -> Document {
    let dbname = match db {
        Some(d) => d,
        None => return error_doc(59, "Missing $db"),
    };
    let coll = match cmd.get_str("dropIndexes") {
        Ok(c) => c,
        Err(_) => return error_doc(9, "Invalid dropIndexes"),
    };
    if state.store.is_none() {
        return error_doc(13, "No storage configured");
    }
    let pg = state.store.as_ref().unwrap();
    if let Ok(name) = cmd.get_str("index") {
        if name == "*" {
            // drop all metadata-managed indexes for the collection
            // fetch names
            // For simplicity, try to drop names known in metadata; ignore failure
            let names = pg.list_index_names(dbname, coll).await.unwrap_or_default();
            let mut dropped: Vec<String> = Vec::new();
            for n in names {
                if pg.drop_index(dbname, coll, &n).await.unwrap_or(false) {
                    dropped.push(n);
                }
            }
            return doc! { "nIndexesWas": (dropped.len() as i32), "msg": "dropped metadata indexes", "ok": 1.0 };
        } else {
            let ok = pg.drop_index(dbname, coll, name).await.unwrap_or(false);
            return doc! { "nIndexesWas": (if ok { 1 } else { 0 }), "ok": 1.0 };
        }
    }
    error_doc(9, "Missing index name")
}
