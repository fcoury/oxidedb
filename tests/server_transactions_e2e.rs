use bson::doc;
use oxidedb::config::Config;
use oxidedb::protocol::{MessageHeader, OP_MSG, decode_op_msg_section0, encode_op_msg};
use oxidedb::server::spawn_with_shutdown;
use rand::{Rng, distributions::Alphanumeric};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

#[path = "common/postgres.rs"]
mod pg;

fn rand_suffix(n: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

async fn read_one_op_msg(stream: &mut TcpStream) -> bson::Document {
    let mut header = [0u8; 16];
    stream.read_exact(&mut header).await.unwrap();
    let (hdr, _) = MessageHeader::parse(&header).unwrap();
    assert_eq!(hdr.op_code, OP_MSG);
    let mut body = vec![0u8; (hdr.message_length as usize) - 16];
    stream.read_exact(&mut body).await.unwrap();
    let (_flags, doc) = decode_op_msg_section0(&body).unwrap();
    doc
}

fn create_lsid() -> bson::Document {
    let uuid = Uuid::new_v4();
    doc! {
        "id": bson::Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Uuid,
            bytes: uuid.as_bytes().to_vec(),
        })
    }
}

#[tokio::test]
async fn e2e_transaction_basic_commit() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };

    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("txn_commit_{}", rand_suffix(6));
    let lsid = create_lsid();

    // create collection
    let create = doc! {"create": "test", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // startTransaction
    let start_txn = doc! {
        "startTransaction": 1i32,
        "lsid": lsid.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&start_txn, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(
        doc.get_f64("ok").unwrap_or(0.0),
        1.0,
        "startTransaction failed: {:?}",
        doc
    );

    // insert within transaction
    let docs = vec![doc! {"_id": "tx1", "value": 100}];
    let ins = doc! {
        "insert": "test",
        "documents": docs,
        "lsid": lsid.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&ins, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(
        doc.get_f64("ok").unwrap_or(0.0),
        1.0,
        "insert in transaction failed: {:?}",
        doc
    );

    // commitTransaction
    let commit_txn = doc! {
        "commitTransaction": 1i32,
        "lsid": lsid.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&commit_txn, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(
        doc.get_f64("ok").unwrap_or(0.0),
        1.0,
        "commitTransaction failed: {:?}",
        doc
    );

    // verify document exists after commit
    let find = doc! {"find": "test", "filter": {}, "$db": &dbname};
    let msg = encode_op_msg(&find, 0, 5);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(batch.len(), 1);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_transaction_basic_abort() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };

    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("txn_abort_{}", rand_suffix(6));
    let lsid = create_lsid();

    // create collection
    let create = doc! {"create": "test", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // startTransaction
    let start_txn = doc! {
        "startTransaction": 1i32,
        "lsid": lsid.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&start_txn, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // insert within transaction
    let docs = vec![doc! {"_id": "tx_abort", "value": 200}];
    let ins = doc! {
        "insert": "test",
        "documents": docs,
        "lsid": lsid.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&ins, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // abortTransaction
    let abort_txn = doc! {
        "abortTransaction": 1i32,
        "lsid": lsid.clone(),
        "$db": &dbname
    };
    let msg = encode_op_msg(&abort_txn, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(
        doc.get_f64("ok").unwrap_or(0.0),
        1.0,
        "abortTransaction failed: {:?}",
        doc
    );

    // verify document does NOT exist after abort
    let find = doc! {"find": "test", "filter": {}, "$db": &dbname};
    let msg = encode_op_msg(&find, 0, 5);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(batch.len(), 0, "Document should not exist after abort");

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_transaction_read_your_writes() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };

    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("txn_ryw_{}", rand_suffix(6));
    let lsid = create_lsid();

    // create collection
    let create = doc! {"create": "test", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // startTransaction
    let start_txn = doc! {
        "startTransaction": 1i32,
        "lsid": lsid.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&start_txn, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // insert within transaction
    let docs = vec![doc! {"_id": "ryw_test", "value": 300}];
    let ins = doc! {
        "insert": "test",
        "documents": docs,
        "lsid": lsid.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&ins, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // find within transaction - should see the inserted document (read-your-writes)
    let find = doc! {
        "find": "test",
        "filter": {},
        "lsid": lsid.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&find, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(batch.len(), 1, "Should see own writes within transaction");

    // commitTransaction
    let commit_txn = doc! {
        "commitTransaction": 1i32,
        "lsid": lsid.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&commit_txn, 0, 5);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_transaction_concurrent_sessions() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };

    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();

    let dbname = format!("txn_concurrent_{}", rand_suffix(6));

    // Session 1
    let lsid1 = create_lsid();
    let mut stream1 = TcpStream::connect(addr).await.unwrap();

    // Session 2
    let lsid2 = create_lsid();
    let mut stream2 = TcpStream::connect(addr).await.unwrap();

    // create collection from session 1
    let create = doc! {"create": "test", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream1.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream1).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // startTransaction on session 1
    let start_txn1 = doc! {
        "startTransaction": 1i32,
        "lsid": lsid1.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&start_txn1, 0, 2);
    stream1.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream1).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // insert from session 1
    let docs = vec![doc! {"_id": "concurrent1", "value": 400}];
    let ins = doc! {
        "insert": "test",
        "documents": docs,
        "lsid": lsid1.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&ins, 0, 3);
    stream1.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream1).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // startTransaction on session 2
    let start_txn2 = doc! {
        "startTransaction": 1i32,
        "lsid": lsid2.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&start_txn2, 0, 4);
    stream2.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream2).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // insert from session 2
    let docs = vec![doc! {"_id": "concurrent2", "value": 500}];
    let ins = doc! {
        "insert": "test",
        "documents": docs,
        "lsid": lsid2.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&ins, 0, 5);
    stream2.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream2).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // find from session 2 - should only see its own writes
    let find = doc! {
        "find": "test",
        "filter": {},
        "lsid": lsid2.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&find, 0, 6);
    stream2.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream2).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(batch.len(), 1, "Session 2 should only see its own writes");

    // commit session 1
    let commit_txn1 = doc! {
        "commitTransaction": 1i32,
        "lsid": lsid1.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&commit_txn1, 0, 7);
    stream1.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream1).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // commit session 2
    let commit_txn2 = doc! {
        "commitTransaction": 1i32,
        "lsid": lsid2.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&commit_txn2, 0, 8);
    stream2.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream2).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // verify both documents exist after both commits
    let find = doc! {"find": "test", "filter": {}, "$db": &dbname};
    let msg = encode_op_msg(&find, 0, 9);
    stream1.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream1).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(batch.len(), 2, "Both documents should exist after commits");

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_transaction_end_sessions() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };

    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("txn_end_{}", rand_suffix(6));
    let lsid = create_lsid();

    // create collection
    let create = doc! {"create": "test", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // startTransaction
    let start_txn = doc! {
        "startTransaction": 1i32,
        "lsid": lsid.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    let msg = encode_op_msg(&start_txn, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Verify session exists
    let session_count_before = state.session_manager.session_count().await;
    assert!(session_count_before >= 1);

    // endSessions
    let end_sessions = doc! {
        "endSessions": [lsid.clone()],
        "$db": &dbname
    };
    let msg = encode_op_msg(&end_sessions, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Verify session was removed
    let uuid = if let Some(bson::Bson::Binary(binary)) = lsid.get("id") {
        Uuid::from_slice(&binary.bytes).unwrap()
    } else {
        panic!("lsid.id is not a Binary");
    };
    let has_session = state.session_manager.has_session(uuid).await;
    assert!(!has_session, "Session should be removed after endSessions");

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_transaction_no_txn_without_autocommit_false() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };

    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("txn_no_autocommit_{}", rand_suffix(6));
    let lsid = create_lsid();

    // create collection
    let create = doc! {"create": "test", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Try to start transaction without autocommit=false - should still work
    let start_txn = doc! {
        "startTransaction": 1i32,
        "lsid": lsid.clone(),
        "txnNumber": 1i64,
        "$db": &dbname
    };
    let msg = encode_op_msg(&start_txn, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    // This should succeed - autocommit defaults to true but we can still start a transaction
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_transaction_missing_lsid() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };

    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("txn_no_lsid_{}", rand_suffix(6));

    // Try to start transaction without lsid - should fail
    let start_txn = doc! {
        "startTransaction": 1i32,
        "txnNumber": 1i64,
        "$db": &dbname
    };
    let msg = encode_op_msg(&start_txn, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    // Should fail with error
    assert_eq!(
        doc.get_f64("ok").unwrap_or(0.0),
        0.0,
        "Should fail without lsid"
    );
    assert!(doc.get_str("errmsg").unwrap().contains("lsid"));

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_transaction_update_and_delete_abort() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };

    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let dbname = format!("txn_update_delete_{}", rand_suffix(6));
    let lsid = create_lsid();

    let create = doc! {"create": "items", "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&create, 0, 1))
        .await
        .unwrap();
    assert_eq!(
        read_one_op_msg(&mut stream)
            .await
            .get_f64("ok")
            .unwrap_or(0.0),
        1.0
    );

    let insert = doc! {
        "insert": "items",
        "documents": [
            {"_id": "update-me", "value": 1i32},
            {"_id": "delete-me", "value": 2i32}
        ],
        "$db": &dbname
    };
    stream
        .write_all(&encode_op_msg(&insert, 0, 2))
        .await
        .unwrap();
    assert_eq!(
        read_one_op_msg(&mut stream).await.get_i32("n").unwrap_or(0),
        2
    );

    let start = doc! {
        "startTransaction": 1i32,
        "lsid": lsid.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    stream
        .write_all(&encode_op_msg(&start, 0, 3))
        .await
        .unwrap();
    assert_eq!(
        read_one_op_msg(&mut stream)
            .await
            .get_f64("ok")
            .unwrap_or(0.0),
        1.0
    );

    let update = doc! {
        "update": "items",
        "updates": [{"q": {"_id": "update-me"}, "u": {"$inc": {"value": 10i32}}}],
        "lsid": lsid.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    stream
        .write_all(&encode_op_msg(&update, 0, 4))
        .await
        .unwrap();
    assert_eq!(
        read_one_op_msg(&mut stream)
            .await
            .get_i32("nModified")
            .unwrap_or(0),
        1
    );

    let delete = doc! {
        "delete": "items",
        "deletes": [{"q": {"_id": "delete-me"}, "limit": 1i32}],
        "lsid": lsid.clone(),
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    stream
        .write_all(&encode_op_msg(&delete, 0, 5))
        .await
        .unwrap();
    assert_eq!(
        read_one_op_msg(&mut stream).await.get_i32("n").unwrap_or(0),
        1
    );

    let abort = doc! {
        "abortTransaction": 1i32,
        "lsid": lsid,
        "txnNumber": 1i64,
        "autocommit": false,
        "$db": &dbname
    };
    stream
        .write_all(&encode_op_msg(&abort, 0, 6))
        .await
        .unwrap();
    assert_eq!(
        read_one_op_msg(&mut stream)
            .await
            .get_f64("ok")
            .unwrap_or(0.0),
        1.0
    );

    let find = doc! {"find": "items", "filter": {}, "sort": {"_id": 1i32}, "$db": &dbname};
    stream.write_all(&encode_op_msg(&find, 0, 7)).await.unwrap();
    let reply = read_one_op_msg(&mut stream).await;
    let documents = reply
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(documents.len(), 2);
    let update_me = documents
        .iter()
        .filter_map(bson::Bson::as_document)
        .find(|document| document.get_str("_id") == Ok("update-me"))
        .unwrap();
    assert_eq!(update_me.get_i32("value").unwrap(), 1);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
