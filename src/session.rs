use bson::{Document, doc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use uuid::Uuid;

/// MongoDB error codes for transactions
pub const ERROR_UNAUTHORIZED: i32 = 13;
pub const ERROR_ILLEGAL_OPERATION: i32 = 20;
pub const ERROR_NO_SUCH_TRANSACTION: i32 = 251;
pub const ERROR_TRANSACTION_EXPIRED: i32 = 211;

/// Result of a write operation for retryable writes
#[derive(Clone, Debug)]
pub struct WriteResult {
    pub n: i32,
    pub n_modified: Option<i32>,
    pub upserted: Option<Document>,
    pub write_errors: Option<Vec<Document>>,
}

impl WriteResult {
    pub fn to_document(&self) -> Document {
        let mut doc = doc! { "n": self.n };
        if let Some(n_modified) = self.n_modified {
            doc.insert("nModified", n_modified);
        }
        if let Some(ref upserted) = self.upserted {
            doc.insert("upserted", upserted.clone());
        }
        if let Some(ref errors) = self.write_errors {
            doc.insert("writeErrors", errors.clone());
        }
        doc.insert("ok", 1.0);
        doc
    }
}

/// Represents a MongoDB logical session
pub struct Session {
    pub lsid: Uuid,
    pub txn_number: i64,
    pub autocommit: bool,
    pub in_transaction: bool,
    pub postgres_client: Option<deadpool_postgres::Object>,
    pub last_write_time: Instant,
    pub retryable_writes: HashMap<i64, WriteResult>,
    pub transaction_start_time: Option<Instant>,
}

impl Session {
    pub fn new(lsid: Uuid) -> Self {
        Self {
            lsid,
            txn_number: 0,
            autocommit: true,
            in_transaction: false,
            postgres_client: None,
            last_write_time: Instant::now(),
            retryable_writes: HashMap::new(),
            transaction_start_time: None,
        }
    }

    /// Update the last write time (called on any activity)
    pub fn touch(&mut self) {
        self.last_write_time = Instant::now();
    }

    /// Check if the session has expired
    pub fn is_expired(&self, timeout: Duration) -> bool {
        self.last_write_time.elapsed() > timeout
    }

    /// Check if the transaction has expired
    pub fn is_transaction_expired(&self, timeout: Duration) -> bool {
        self.transaction_start_time
            .map(|start| start.elapsed() > timeout)
            .unwrap_or(false)
    }

    /// Store a retryable write result
    pub fn store_retryable_write(&mut self, txn_number: i64, result: WriteResult) {
        self.retryable_writes.insert(txn_number, result);
        // Keep only the last 100 entries to prevent memory growth
        if self.retryable_writes.len() > 100 {
            let oldest = *self.retryable_writes.keys().min().unwrap_or(&0);
            self.retryable_writes.remove(&oldest);
        }
    }

    /// Get a cached retryable write result
    pub fn get_retryable_write(&self, txn_number: i64) -> Option<&WriteResult> {
        self.retryable_writes.get(&txn_number)
    }

    /// Validate that txn_number is monotonically increasing
    pub fn validate_txn_number(&mut self, txn_number: i64) -> Result<(), i32> {
        if txn_number < self.txn_number {
            // Went backwards - this is an error
            return Err(ERROR_ILLEGAL_OPERATION);
        }
        if txn_number == self.txn_number {
            // Same txnNumber - this is a retry, ok if we have a cached result
            return Ok(());
        }
        if txn_number > self.txn_number + 1 {
            // Gap in txnNumber sequence - error
            return Err(ERROR_ILLEGAL_OPERATION);
        }
        // txn_number == self.txn_number + 1, advance the txn_number
        self.txn_number = txn_number;
        Ok(())
    }

    /// Start a transaction
    pub async fn start_transaction(
        &mut self,
        pool: &deadpool_postgres::Pool,
    ) -> Result<(), String> {
        if self.in_transaction {
            return Err("Transaction already in progress".to_string());
        }

        // Get a client from the pool for this transaction
        let client = pool.get().await.map_err(|e| e.to_string())?;

        // Begin a PostgreSQL transaction on this client
        client
            .batch_execute("BEGIN")
            .await
            .map_err(|e| e.to_string())?;

        self.postgres_client = Some(client);
        self.in_transaction = true;
        self.transaction_start_time = Some(Instant::now());
        self.touch();
        Ok(())
    }

    /// Commit the current transaction
    pub async fn commit_transaction(&mut self) -> Result<(), String> {
        if !self.in_transaction {
            return Err("No transaction in progress".to_string());
        }

        // Commit the PostgreSQL transaction
        if let Some(ref mut client) = self.postgres_client {
            client
                .batch_execute("COMMIT")
                .await
                .map_err(|e| e.to_string())?;
        }

        self.in_transaction = false;
        self.postgres_client = None;
        self.transaction_start_time = None;
        self.touch();
        Ok(())
    }

    /// Abort (rollback) the current transaction
    pub async fn abort_transaction(&mut self) -> Result<(), String> {
        if !self.in_transaction {
            return Err("No transaction in progress".to_string());
        }

        // Explicitly rollback the transaction before dropping the client
        // This is necessary because the client is from a connection pool,
        // and simply dropping it would return the connection to the pool
        // with the transaction still open
        if let Some(ref mut client) = self.postgres_client {
            client
                .batch_execute("ROLLBACK")
                .await
                .map_err(|e| e.to_string())?;
        }

        self.in_transaction = false;
        self.postgres_client = None;
        self.transaction_start_time = None;
        self.touch();
        Ok(())
    }

    /// Get the current transaction client if in a transaction
    pub fn get_transaction_client(&self) -> Option<&deadpool_postgres::Object> {
        if self.in_transaction {
            self.postgres_client.as_ref()
        } else {
            None
        }
    }
}

/// Manages all active sessions
pub struct SessionManager {
    sessions: Mutex<HashMap<Uuid, Arc<Mutex<Session>>>>,
    timeout: Duration,
    transaction_timeout: Duration,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            timeout: Duration::from_secs(30 * 60), // 30 minutes default
            transaction_timeout: Duration::from_secs(60), // 1 minute transaction timeout
        }
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            timeout,
            transaction_timeout: Duration::from_secs(60),
        }
    }

    /// Get an existing session or create a new one
    pub async fn get_or_create_session(&self, lsid: Uuid) -> Arc<Mutex<Session>> {
        let mut sessions = self.sessions.lock().await;

        if let Some(session) = sessions.get(&lsid) {
            // Update last write time
            if let Ok(mut s) = session.try_lock() {
                s.touch();
            }
            return session.clone();
        }

        // Create new session
        let session = Arc::new(Mutex::new(Session::new(lsid)));
        sessions.insert(lsid, session.clone());
        session
    }

    /// Get an existing session without creating one
    pub async fn get_session(&self, lsid: Uuid) -> Option<Arc<Mutex<Session>>> {
        let sessions = self.sessions.lock().await;
        sessions.get(&lsid).cloned()
    }

    /// End (remove) a session
    pub async fn end_session(&self, lsid: Uuid) {
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.remove(&lsid) {
            // Abort any in-progress transaction
            if let Ok(mut s) = session.try_lock() {
                if s.in_transaction {
                    let _ = s.abort_transaction();
                }
            }
        }
    }

    /// Clean up expired sessions
    pub async fn cleanup_expired_sessions(&self) -> usize {
        let mut sessions = self.sessions.lock().await;
        let before_count = sessions.len();

        // Collect expired session IDs
        let expired: Vec<Uuid> = sessions
            .iter()
            .filter(|(_, session)| {
                if let Ok(s) = session.try_lock() {
                    // Check if session has expired
                    if s.is_expired(self.timeout) {
                        return true;
                    }
                    // Check if transaction has expired
                    if s.in_transaction && s.is_transaction_expired(self.transaction_timeout) {
                        return true;
                    }
                }
                false
            })
            .map(|(id, _)| *id)
            .collect();

        // Remove expired sessions
        for id in &expired {
            if let Some(session) = sessions.remove(id) {
                if let Ok(mut s) = session.try_lock() {
                    if s.in_transaction {
                        let _ = s.abort_transaction();
                    }
                }
            }
        }

        before_count - sessions.len()
    }

    /// Get the number of active sessions
    pub async fn session_count(&self) -> usize {
        let sessions = self.sessions.lock().await;
        sessions.len()
    }

    /// Check if a session exists
    pub async fn has_session(&self, lsid: Uuid) -> bool {
        let sessions = self.sessions.lock().await;
        sessions.contains_key(&lsid)
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_creation() {
        let lsid = Uuid::new_v4();
        let session = Session::new(lsid);
        assert_eq!(session.lsid, lsid);
        assert_eq!(session.txn_number, 0);
        assert!(session.autocommit);
        assert!(!session.in_transaction);
    }

    #[test]
    fn test_session_expiry() {
        let lsid = Uuid::new_v4();
        let session = Session::new(lsid);
        // Session should not be expired immediately
        assert!(!session.is_expired(Duration::from_secs(30 * 60)));
    }

    #[test]
    fn test_retryable_write_storage() {
        let lsid = Uuid::new_v4();
        let mut session = Session::new(lsid);

        let result = WriteResult {
            n: 1,
            n_modified: None,
            upserted: None,
            write_errors: None,
        };

        session.store_retryable_write(1, result.clone());
        assert!(session.get_retryable_write(1).is_some());
        assert!(session.get_retryable_write(2).is_none());
    }

    #[test]
    fn test_validate_txn_number() {
        let lsid = Uuid::new_v4();
        let mut session = Session::new(lsid);

        // First operation should be txn_number 1
        assert!(session.validate_txn_number(1).is_ok());
        // Same txn_number is ok (retry)
        assert!(session.validate_txn_number(1).is_ok());
        // Increment is ok
        assert!(session.validate_txn_number(2).is_ok());
        // Previous txn_number is not ok (must be monotonic)
        assert_eq!(
            session.validate_txn_number(1).unwrap_err(),
            ERROR_ILLEGAL_OPERATION
        );
        // Gap is not ok
        assert_eq!(
            session.validate_txn_number(5).unwrap_err(),
            ERROR_ILLEGAL_OPERATION
        );
    }
}
