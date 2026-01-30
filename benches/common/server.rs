use oxidedb::config::Config;
use oxidedb::server::{AppState, spawn_with_shutdown};
use std::net::SocketAddr;
use tokio::net::TcpStream;

use super::postgres::TestDb;

pub struct BenchServer {
    _state: std::sync::Arc<AppState>,
    addr: SocketAddr,
    _shutdown: tokio::sync::watch::Sender<bool>,
    _handle: tokio::task::JoinHandle<oxidedb::error::Result<()>>,
    testdb: TestDb,
}

impl BenchServer {
    pub async fn start(testdb: TestDb) -> Self {
        let mut cfg = Config::default();
        cfg.listen_addr = "127.0.0.1:0".into();
        cfg.postgres_url = Some(testdb.url.clone());

        let (state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();

        Self {
            _state: state,
            addr,
            _shutdown: shutdown,
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

    pub async fn connect(&self) -> TcpStream {
        TcpStream::connect(self.addr).await.unwrap()
    }
}
