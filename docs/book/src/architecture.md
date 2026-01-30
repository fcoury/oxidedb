# System Architecture

OxideDB is a protocol translation layer that sits between MongoDB clients and PostgreSQL, providing MongoDB compatibility while leveraging PostgreSQL's storage engine.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        MongoDB Client                           │
│                   (Driver, Shell, Application)                  │
└──────────────────────┬──────────────────────────────────────────┘
                       │ MongoDB Wire Protocol
                       │ (OP_MSG, OP_QUERY, OP_COMPRESSED)
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                          OxideDB                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │   Protocol   │  │   Command    │  │    Query Translator  │  │
│  │   Handler    │──│   Router     │──│    (BSON → SQL)      │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
│         │                 │                    │               │
│         ▼                 ▼                    ▼               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │   Session    │  │ Aggregation  │  │   Shadow Mode        │  │
│  │   Manager    │  │   Engine     │  │   (Optional)         │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└──────────────────────┬──────────────────────────────────────────┘
                       │ PostgreSQL Wire Protocol
                       │ (SQL queries, transactions)
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                         PostgreSQL                              │
│              (Storage, Indexing, ACID Transactions)             │
└─────────────────────────────────────────────────────────────────┘
```

## Wire Protocol Implementation

OxideDB implements the MongoDB wire protocol to accept connections from any MongoDB client:

### Supported Operations

| Op Code | Name | Description |
|---------|------|-------------|
| 2013 | OP_MSG | Modern message-based protocol (MongoDB 3.6+) |
| 2004 | OP_QUERY | Legacy query operation |
| 2012 | OP_COMPRESSED | Compressed message support (Snappy, zlib, zstd) |

### Protocol Flow

1. **Connection Establishment**: Client connects via TCP to OxideDB's listen address (default: 127.0.0.1:27017)
2. **Handshake**: Client sends `hello` or `ismaster` command
3. **Command Processing**: Commands are parsed from OP_MSG or OP_QUERY payloads
4. **Response Encoding**: Results are encoded back into MongoDB wire format

### Example Wire Flow

```
Client                                    OxideDB
   │                                         │
   │──── OP_MSG (hello command)─────────────▶│
   │                                         │
   │◀─── OP_MSG (hello response)────────────│
   │    { ismaster: true, ok: 1 }            │
   │                                         │
   │──── OP_MSG (insert command)────────────▶│
   │    { insert: "users", documents: [...] }│
   │                                         │
   │◀─── OP_MSG (insert response)───────────│
   │    { n: 1, ok: 1 }                      │
```

## PostgreSQL Storage Layer

### Schema Design

OxideDB maps MongoDB concepts to PostgreSQL constructs:

| MongoDB | PostgreSQL |
|---------|------------|
| Database | Schema (`mdb_<dbname>`) |
| Collection | Table |
| Document | Row (JSONB + BSON binary) |
| Index | PostgreSQL Index (expression indexes on JSONB) |
| _id | BYTEA PRIMARY KEY |

### Document Storage

Each document is stored with three columns:

```sql
CREATE TABLE mdb_test.users (
    id BYTEA PRIMARY KEY,           -- BSON _id as binary
    doc JSONB NOT NULL,             -- Document as JSONB for querying
    doc_bson BYTEA NOT NULL         -- Original BSON for fidelity
);
```

**Why both JSONB and BSON?**
- **JSONB**: Enables PostgreSQL's powerful querying and indexing capabilities
- **BSON**: Preserves MongoDB-specific types (ObjectId, Decimal128, Binary data) and field order

### Metadata Schema

OxideDB maintains metadata tables in the `mdb_meta` schema:

```sql
-- Track databases
CREATE TABLE mdb_meta.databases (
    db TEXT PRIMARY KEY
);

-- Track collections
CREATE TABLE mdb_meta.collections (
    db TEXT NOT NULL,
    coll TEXT NOT NULL,
    PRIMARY KEY (db, coll)
);

-- Track indexes
CREATE TABLE mdb_meta.indexes (
    db TEXT NOT NULL,
    coll TEXT NOT NULL,
    name TEXT NOT NULL,
    spec JSONB NOT NULL,    -- Original index specification
    sql TEXT,               -- Generated SQL DDL
    PRIMARY KEY (db, coll, name)
);
```

## BSON to JSONB Mapping

OxideDB converts BSON documents to PostgreSQL JSONB for storage and querying:

### Type Mapping

| BSON Type | JSONB Representation | Notes |
|-----------|---------------------|-------|
| Double | Number | Direct mapping |
| String | String | Direct mapping |
| Document | Object | Nested JSONB |
| Array | Array | JSONB array |
| Binary | String (Base64) | Stored as string in JSONB |
| ObjectId | String | 24-character hex string |
| Boolean | Boolean | Direct mapping |
| DateTime | String (ISO 8601) | Timestamp as string |
| Null | null | Direct mapping |
| Regular Expression | String | Pattern as string |
| JavaScript Code | String | Code as string |
| 32-bit Integer | Number | Direct mapping |
| Timestamp | String | Special MongoDB timestamp |
| 64-bit Integer | Number | Direct mapping |
| Decimal128 | String | Preserved as string |

### Example Conversion

**BSON Document:**
```bson
{
    _id: ObjectId("507f1f77bcf86cd799439011"),
    name: "Alice",
    age: 30,
    scores: [85, 92, 78],
    metadata: {
        created: ISODate("2024-01-15T10:30:00Z"),
        active: true
    }
}
```

**JSONB Representation:**
```json
{
    "_id": "507f1f77bcf86cd799439011",
    "name": "Alice",
    "age": 30,
    "scores": [85, 92, 78],
    "metadata": {
        "created": "2024-01-15T10:30:00Z",
        "active": true
    }
}
```

## Query Translation Pipeline

### Overview

MongoDB queries are translated to PostgreSQL SQL in multiple stages:

```
MongoDB Query Filter
        │
        ▼
┌──────────────────┐
│  Parse BSON      │
│  Document        │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Extract Logical │
│  Operators       │
│  ($or, $and)     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Build WHERE     │
│  Clauses         │
│  (jsonb_path_*)  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Generate SQL    │
│  Query           │
└────────┬─────────┘
         │
         ▼
PostgreSQL SQL
```

### Filter Translation Examples

**Simple Equality:**
```javascript
// MongoDB
{ name: "Alice" }

// SQL
SELECT doc_bson, doc FROM mdb_test.users 
WHERE jsonb_path_exists(doc, '$."name" ? (@ == "Alice")')
```

**Comparison Operators:**
```javascript
// MongoDB
{ age: { $gte: 18, $lt: 65 } }

// SQL
SELECT doc_bson, doc FROM mdb_test.users 
WHERE jsonb_path_exists(doc, '$."age" ? (@ >= 18)')
  AND jsonb_path_exists(doc, '$."age" ? (@ < 65)')
```

**Logical Operators:**
```javascript
// MongoDB
{ $or: [{ status: "active" }, { age: { $lt: 18 } }] }

// SQL
SELECT doc_bson, doc FROM mdb_test.users 
WHERE (jsonb_path_exists(doc, '$."status" ? (@ == "active")')
   OR jsonb_path_exists(doc, '$."age" ? (@ < 18)'))
```

**Array Operators:**
```javascript
// MongoDB
{ tags: { $all: ["mongodb", "postgresql"] } }

// SQL
SELECT doc_bson, doc FROM mdb_test.users 
WHERE (jsonb_path_exists(doc, '$."tags"[*] ? (@ == "mongodb")')
   AND jsonb_path_exists(doc, '$."tags"[*] ? (@ == "postgresql")'))
```

### Index Utilization

OxideDB creates expression indexes to speed up common queries:

```sql
-- Single field index
CREATE INDEX idx_users_name ON mdb_test.users 
USING btree ((doc->>'name'));

-- Compound index
CREATE INDEX idx_users_status_age ON mdb_test.users 
USING btree ((doc->>'status'), (doc->>'age'));

-- GIN index for containment queries
CREATE INDEX idx_users_doc_gin ON mdb_test.users 
USING GIN (doc jsonb_path_ops);
```

## Aggregation Pipeline Translation

### SQL Pushdown Strategy

OxideDB attempts to push aggregation stages to PostgreSQL SQL when possible:

**Pushdown-Capable Stages:**
- `$match` → SQL WHERE clause
- `$project` → SQL SELECT with jsonb_build_object
- `$sort` → SQL ORDER BY
- `$limit` → SQL LIMIT
- `$skip` → SQL OFFSET
- `$group` → SQL GROUP BY with aggregations
- `$unwind` → LATERAL JOIN with jsonb_array_elements
- `$sample` → SQL ORDER BY random() LIMIT

**Engine-Executed Stages:**
- `$facet` (multiple sub-pipelines)
- `$unionWith` (UNION ALL)
- `$lookup` (joins with external collections)
- `$out` and `$merge` (write operations)

### Example Pipeline Translation

**MongoDB Aggregation:**
```javascript
db.orders.aggregate([
    { $match: { status: "completed" } },
    { $group: { 
        _id: "$customer_id", 
        total: { $sum: "$amount" },
        count: { $sum: 1 }
    }},
    { $sort: { total: -1 } },
    { $limit: 10 }
])
```

**Generated SQL:**
```sql
WITH cte_0 AS (
    SELECT id, doc FROM mdb_test.orders 
    WHERE jsonb_path_exists(doc, '$."status" ? (@ == "completed")')
),
cte_1 AS (
    SELECT MIN(id) as id, 
           jsonb_build_object(
               '_id', doc #>> '{"customer_id"}',
               'total', SUM(((doc #>> '{"amount"}'))::numeric),
               'count', COUNT(*)
           ) AS doc 
    FROM cte_0 
    GROUP BY doc #>> '{"customer_id"}'
)
SELECT doc FROM cte_1 
ORDER BY (doc->>'total')::numeric DESC 
LIMIT 10
```

## Session and Transaction Management

### Logical Sessions

OxideDB implements MongoDB's logical session protocol:

```rust
pub struct Session {
    pub lsid: Uuid,              // Logical Session ID
    pub txn_number: i64,         // Monotonic transaction counter
    pub autocommit: bool,        // Auto-commit mode
    pub in_transaction: bool,    // Transaction state
    pub postgres_client: Option<deadpool_postgres::Object>,
    pub retryable_writes: HashMap<i64, WriteResult>,
}
```

### Transaction Flow

```
Client                     OxideDB                  PostgreSQL
  │                          │                          │
  │─ startTransaction ──────▶│                          │
  │                          │─ BEGIN ─────────────────▶│
  │                          │◀─ OK ────────────────────│
  │◀─ ok: 1 ─────────────────│                          │
  │                          │                          │
  │─ insert (txnNumber: 1) ─▶│                          │
  │                          │─ INSERT (in txn) ───────▶│
  │◀─ ok: 1 ─────────────────│                          │
  │                          │                          │
  │─ update (txnNumber: 2) ─▶│                          │
  │                          │─ UPDATE (in txn) ───────▶│
  │◀─ ok: 1 ─────────────────│                          │
  │                          │                          │
  │─ commitTransaction ─────▶│                          │
  │                          │─ COMMIT ────────────────▶│
  │                          │◀─ OK ────────────────────│
  │◀─ ok: 1 ─────────────────│                          │
```

## Shadow Mode Architecture

Shadow mode enables correctness testing by comparing OxideDB responses with an upstream MongoDB:

```
                    ┌─────────────┐
                    │   Client    │
                    └──────┬──────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           ▼               ▼               ▼
    ┌────────────┐  ┌────────────┐  ┌────────────┐
    │  Request   │  │   OxideDB  │  │   Shadow   │
    │  Router    │  │  (Primary) │  │  MongoDB   │
    └─────┬──────┘  └─────┬──────┘  └─────┬──────┘
          │               │               │
          │               │               │
          │               ▼               │
          │          ┌─────────┐          │
          │          │PostgreSQL│         │
          │          └────┬────┘          │
          │               │               │
          │               ▼               │
          │          ┌─────────┐          │
          └─────────▶│Comparer │◀─────────┘
                     └────┬────┘
                          │
                          ▼
                     ┌─────────┐
                     │ Metrics │
                     │ & Logs  │
                     └─────────┘
```

### Shadow Mode Configuration

```toml
[shadow]
enabled = true
addr = "127.0.0.1:27018"
db_prefix = "shadow_test"
timeout_ms = 800
sample_rate = 1.0
mode = "CompareOnly"  # or "CompareAndFail"

[shadow.compare]
ignore_fields = ["$clusterTime", "operationTime", "topologyVersion"]
numeric_equivalence = false
```

## Performance Considerations

### Connection Pooling

- OxideDB maintains a connection pool to PostgreSQL (default: 16 connections)
- Each client connection to OxideDB can use pooled PostgreSQL connections
- Transactions hold dedicated connections until committed/aborted

### Caching Strategy

1. **Schema Cache**: Known databases and collections are cached to avoid metadata queries
2. **Query Planning**: Complex aggregations use CTEs (Common Table Expressions) for optimization
3. **BSON Cache**: Documents are stored as BSON binary to avoid re-serialization

### Query Optimization

- **Containment Queries**: Use PostgreSQL's `@>` operator when possible
- **Expression Indexes**: Automatically create indexes for frequently queried fields
- **Projection Pushdown**: Only select required fields at the database level

## Security Architecture

### Authentication

OxideDB supports SCRAM-SHA-256 authentication:

```
Client                          OxideDB
  │                               │
  │── saslStart (SCRAM-SHA-256) ─▶│
  │                               │
  │◀─ server-first-message ───────│
  │                               │
  │── client-final-message ──────▶│
  │                               │
  │◀─ server-final-message ───────│
  │    { ok: 1 }                  │
```

### TLS Support

- **Client TLS**: Encrypt connections from MongoDB clients to OxideDB
- **Upstream TLS**: Encrypt connections from OxideDB to PostgreSQL
- **Mutual TLS**: Certificate-based authentication for both directions

## Monitoring and Observability

### Metrics

OxideDB exposes metrics via the `oxidedbShadowMetrics` command:

```javascript
db.adminCommand({ oxidedbShadowMetrics: 1 })
// Returns:
{
    shadow: {
        attempts: 1000,
        matches: 980,
        mismatches: 20,
        timeouts: 0
    },
    ok: 1
}
```

### Logging

Structured logging with tracing:

```
2024-01-15T10:30:00.123Z  INFO oxidedb::server: command=find db=test elapsed_ms=5 rows=100
2024-01-15T10:30:00.456Z  DEBUG oxidedb::translate: filter={"status":"active"} where_sql=...
```

## Next Steps

- Learn about [Query Operators](./features/queries.md)
- Explore [Aggregation Pipeline](./features/aggregation.md)
- Understand [Transaction Support](./features/transactions.md)
- Review [Configuration Options](./reference/config.md)
