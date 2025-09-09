use oxidedb::config::ShadowConfig;
use oxidedb::protocol::{MessageHeader, OP_QUERY};
use oxidedb::shadow::ShadowSession;
use bson::doc;
use std::sync::Arc;

fn upstream_addr_from_env() -> Option<String> {
    std::env::var("OXIDEDB_TEST_MONGODB_ADDR").ok()
}

#[tokio::test]
async fn shadow_op_msg_hello_ping_buildinfo() {
    let addr = match upstream_addr_from_env() {
        Some(a) => a,
        None => { eprintln!("skipping: set OXIDEDB_TEST_MONGODB_ADDR"); return; }
    };
    let mut cfg = ShadowConfig::default();
    cfg.enabled = true;
    cfg.addr = addr;
    // 7.0, no auth per requirements; sample rate irrelevant here
    let sh = ShadowSession::new(Arc::new(cfg));

    // hello
    let doc = doc!{"hello": 1i32, "$db": "admin"};
    let wire = oxidedb::protocol::encode_op_msg(&doc, 0, 1);
    let (hdr, _) = MessageHeader::parse(&wire[0..16]).unwrap();
    let body = &wire[16..];
    let up = sh.forward_and_read_doc(&hdr, (&wire[0..16]).try_into().unwrap(), body).await.unwrap();
    let up = up.expect("expected doc");
    assert_eq!(up.get_f64("ok").unwrap_or(0.0), 1.0);

    // ping
    let doc = doc!{"ping": 1i32, "$db": "admin"};
    let wire = oxidedb::protocol::encode_op_msg(&doc, 0, 2);
    let (hdr, _) = MessageHeader::parse(&wire[0..16]).unwrap();
    let body = &wire[16..];
    let up = sh.forward_and_read_doc(&hdr, (&wire[0..16]).try_into().unwrap(), body).await.unwrap();
    let up = up.expect("expected doc");
    assert_eq!(up.get_f64("ok").unwrap_or(0.0), 1.0);

    // buildInfo
    let doc = doc!{"buildInfo": 1i32, "$db": "admin"};
    let wire = oxidedb::protocol::encode_op_msg(&doc, 0, 3);
    let (hdr, _) = MessageHeader::parse(&wire[0..16]).unwrap();
    let body = &wire[16..];
    let up = sh.forward_and_read_doc(&hdr, (&wire[0..16]).try_into().unwrap(), body).await.unwrap();
    let up = up.expect("expected doc");
    assert_eq!(up.get_f64("ok").unwrap_or(0.0), 1.0);
}

#[tokio::test]
async fn shadow_op_query_ismaster_admin_cmd() {
    let addr = match upstream_addr_from_env() {
        Some(a) => a,
        None => { eprintln!("skipping: set OXIDEDB_TEST_MONGODB_ADDR"); return; }
    };
    let mut cfg = ShadowConfig::default();
    cfg.enabled = true;
    cfg.addr = addr;
    let sh = ShadowSession::new(Arc::new(cfg));

    // OP_QUERY body for { ismaster: 1 } on admin.$cmd
    let flags: u32 = 0;
    let fqn = b"admin.$cmd\0"; // cstring
    let skip: i32 = 0;
    let nret: i32 = -1;
    let qdoc = bson::to_vec(&doc!{"ismaster": 1i32}).unwrap();
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&flags.to_le_bytes());
    body.extend_from_slice(fqn);
    body.extend_from_slice(&skip.to_le_bytes());
    body.extend_from_slice(&nret.to_le_bytes());
    body.extend_from_slice(&qdoc);

    let msg_len = 16 + body.len() as i32;
    let mut header = Vec::new();
    header.extend_from_slice(&msg_len.to_le_bytes());
    header.extend_from_slice(&10i32.to_le_bytes()); // request id
    header.extend_from_slice(&0i32.to_le_bytes());
    header.extend_from_slice(&OP_QUERY.to_le_bytes());
    let (hdr, _) = MessageHeader::parse(&header[0..16]).unwrap();

    let up = sh.forward_and_read_doc(&hdr, (&header[0..16]).try_into().unwrap(), &body).await.unwrap();
    let up = up.expect("expected doc");
    assert_eq!(up.get_f64("ok").unwrap_or(0.0), 1.0);
}
