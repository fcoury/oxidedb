use bson::doc;
use oxidedb::config::Config;
use oxidedb::protocol::{MessageHeader, OP_MSG, decode_op_msg_section0, encode_op_msg};
use oxidedb::server::spawn_with_shutdown;
use rand::{Rng, distributions::Alphanumeric};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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

#[tokio::test]
async fn e2e_facet_basic() {
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

    let dbname = format!("agg_facet_{}", rand_suffix(6));

    // Create collection
    let create = doc! {"create": "items", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert test documents
    let docs = vec![
        doc! {"category": "A", "price": 10, "name": "item1"},
        doc! {"category": "A", "price": 20, "name": "item2"},
        doc! {"category": "B", "price": 30, "name": "item3"},
        doc! {"category": "B", "price": 40, "name": "item4"},
        doc! {"category": "C", "price": 50, "name": "item5"},
    ];
    let ins = doc! {"insert": "items", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // $facet with multiple sub-pipelines
    let pipeline = vec![bson::Bson::Document(doc! {
        "$facet": {
            "allItems": [],
            "categoryA": [{"$match": {"category": "A"}}],
            "categoryB": [{"$match": {"category": "B"}}],
            "highPrice": [{"$match": {"price": {"$gte": 30}}}]
        }
    })];
    let agg = doc! {"aggregate": "items", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let fb = cursor.get_array("firstBatch").unwrap();
    assert_eq!(fb.len(), 1, "$facet should return exactly one document");

    let facet_result = fb[0].as_document().unwrap();

    // Check allItems facet (should have all 5 items)
    let all_items = facet_result.get_array("allItems").unwrap();
    assert_eq!(all_items.len(), 5, "allItems should have 5 items");

    // Check categoryA facet (should have 2 items)
    let category_a = facet_result.get_array("categoryA").unwrap();
    assert_eq!(category_a.len(), 2, "categoryA should have 2 items");

    // Check categoryB facet (should have 2 items)
    let category_b = facet_result.get_array("categoryB").unwrap();
    assert_eq!(category_b.len(), 2, "categoryB should have 2 items");

    // Check highPrice facet (should have 3 items with price >= 30)
    let high_price = facet_result.get_array("highPrice").unwrap();
    assert_eq!(high_price.len(), 3, "highPrice should have 3 items");

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_facet_with_match() {
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

    let dbname = format!("agg_facet_{}", rand_suffix(6));

    // Create collection
    let create = doc! {"create": "products", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert test documents
    let docs = vec![
        doc! {"type": "electronics", "price": 100, "inStock": true},
        doc! {"type": "electronics", "price": 200, "inStock": false},
        doc! {"type": "clothing", "price": 50, "inStock": true},
        doc! {"type": "clothing", "price": 80, "inStock": true},
        doc! {"type": "food", "price": 10, "inStock": false},
    ];
    let ins = doc! {"insert": "products", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // $facet with $match in sub-pipelines
    let pipeline = vec![bson::Bson::Document(doc! {
        "$facet": {
            "inStockItems": [{"$match": {"inStock": true}}],
            "expensiveItems": [{"$match": {"price": {"$gte": 100}}}],
            "electronicsInStock": [
                {"$match": {"type": "electronics"}},
                {"$match": {"inStock": true}}
            ]
        }
    })];
    let agg = doc! {"aggregate": "products", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let fb = cursor.get_array("firstBatch").unwrap();
    assert_eq!(fb.len(), 1);

    let facet_result = fb[0].as_document().unwrap();

    // Check inStockItems (should have 3 items)
    let in_stock = facet_result.get_array("inStockItems").unwrap();
    assert_eq!(in_stock.len(), 3);

    // Check expensiveItems (should have 2 items with price >= 100)
    let expensive = facet_result.get_array("expensiveItems").unwrap();
    assert_eq!(expensive.len(), 2);

    // Check electronicsInStock (should have 1 item)
    let elec_in_stock = facet_result.get_array("electronicsInStock").unwrap();
    assert_eq!(elec_in_stock.len(), 1);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_facet_with_group() {
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

    let dbname = format!("agg_facet_{}", rand_suffix(6));

    // Create collection
    let create = doc! {"create": "sales", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert test documents
    let docs = vec![
        doc! {"region": "North", "amount": 100},
        doc! {"region": "North", "amount": 200},
        doc! {"region": "South", "amount": 150},
        doc! {"region": "South", "amount": 250},
        doc! {"region": "East", "amount": 300},
    ];
    let ins = doc! {"insert": "sales", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // $facet with $group in sub-pipelines
    let pipeline = vec![bson::Bson::Document(doc! {
        "$facet": {
            "byRegion": [
                {"$group": {"_id": "$region", "total": {"$sum": "$amount"}}},
                {"$sort": {"_id": 1}}
            ],
            "totalSales": [
                {"$group": {"_id": null, "total": {"$sum": "$amount"}}}
            ],
            "countByRegion": [
                {"$group": {"_id": "$region", "count": {"$sum": 1}}}
            ]
        }
    })];
    let agg = doc! {"aggregate": "sales", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let fb = cursor.get_array("firstBatch").unwrap();
    assert_eq!(fb.len(), 1);

    let facet_result = fb[0].as_document().unwrap();

    // Check byRegion (should have 3 groups)
    let by_region = facet_result.get_array("byRegion").unwrap();
    assert_eq!(by_region.len(), 3, "byRegion should have 3 regions");

    // Verify the group results
    let region_0 = by_region[0].as_document().unwrap();
    assert!(region_0.contains_key("_id"));
    assert!(region_0.contains_key("total"));

    // Check totalSales (should have 1 document with total = 1000)
    let total_sales = facet_result.get_array("totalSales").unwrap();
    assert_eq!(total_sales.len(), 1);
    let total_doc = total_sales[0].as_document().unwrap();
    assert_eq!(total_doc.get_f64("total").unwrap(), 1000.0);

    // Check countByRegion (should have 3 groups)
    let count_by_region = facet_result.get_array("countByRegion").unwrap();
    assert_eq!(count_by_region.len(), 3);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_facet_not_last_stage_error() {
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

    let dbname = format!("agg_facet_{}", rand_suffix(6));

    // Create collection
    let create = doc! {"create": "items", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert test document
    let ins = doc! {"insert": "items", "documents": [{"name": "test"}], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // $facet followed by $match (should error)
    let pipeline = vec![
        bson::Bson::Document(doc! {
            "$facet": {
                "all": []
            }
        }),
        bson::Bson::Document(doc! {
            "$match": {"name": "test"}
        }),
    ];
    let agg = doc! {"aggregate": "items", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    // Should return an error
    assert_eq!(
        doc.get_f64("ok").unwrap_or(1.0),
        0.0,
        "Should return error when $facet is not last stage"
    );
    let errmsg = doc.get_str("errmsg").unwrap();
    assert!(
        errmsg.contains("$facet must be the last stage"),
        "Error message should indicate $facet must be last"
    );

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
