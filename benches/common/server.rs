use oxidedb::config::Config;
use oxidedb::server::spawn_with_shutdown;
use std::net::SocketAddr;

use super::postgres::TestDb;

pub struct BenchServer {
    addr: SocketAddr,
    _shutdown_tx: tokio::sync::watch::Sender<bool>,
    _handle: std::thread::JoinHandle<()>,
    testdb: TestDb,
}

impl BenchServer {
    pub fn new(testdb: TestDb) -> Self {
        let (addr_tx, addr_rx) = std::sync::mpsc::channel::<SocketAddr>();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let postgres_url = testdb.url.clone();

        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
            rt.block_on(async move {
                let mut cfg = Config::default();
                cfg.listen_addr = "127.0.0.1:0".into();
                cfg.postgres_url = Some(postgres_url);

                let (_state, addr, server_shutdown_tx, server_handle) =
                    spawn_with_shutdown(cfg).await.unwrap();

                // Send the address back to the main thread
                addr_tx.send(addr).expect("Failed to send server address");

                // Wait for either the shutdown signal from main thread or server completion
                let mut shutdown_rx = shutdown_rx;
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        // Shutdown signal received from main thread
                        let _ = server_shutdown_tx.send(true);
                    }
                    _ = server_handle => {
                        // Server completed on its own
                    }
                }
            });
        });

        // Wait for the server to start and get the address
        let addr = addr_rx.recv().expect("Failed to receive server address");

        Self {
            addr,
            _shutdown_tx: shutdown_tx,
            _handle: handle,
            testdb,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn dbname(&self) -> &str {
        &self.testdb.dbname
    }
}

impl Drop for BenchServer {
    fn drop(&mut self) {
        // Signal shutdown - this will be received by the background thread
        let _ = self._shutdown_tx.send(true);
    }
}
