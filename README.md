# OxideDB

A minimal MongoDB wire-protocol server that executes a subset of MongoDB commands and stores data on PostgreSQL (jsonb). It also includes a shadowing mode that forwards each incoming MongoDB request to a real MongoDB (e.g., 7.0) to compare responses for protocol/semantic compatibility.

**📚 Full documentation is available in the [mdBook](https://opencode.ai/oxidedb/docs/).**

Status: early, evolving. Useful for experimentation, compatibility exploration, and growing test coverage.

## Documentation

For comprehensive documentation including architecture, API reference, configuration, and examples, visit the **[OxideDB mdBook](https://opencode.ai/oxidedb/docs/)**.

The mdBook covers:
- Getting started guide
- Complete feature reference
- Configuration options
- Query operators and aggregation pipeline
- Transactions and authentication
- Shadowing mode details
- Troubleshooting and FAQ

## Features

- **MongoDB Wire Protocol**
  - OP_MSG and legacy OP_QUERY request handling
  - OP_COMPRESSED support (decompression)
  - Basic commands: `hello`/`ismaster`, `ping`, `buildInfo`, `listDatabases`, `listCollections`, `serverStatus`, `create`, `drop`, `dropDatabase`, `insert`, `find`, `getMore`, `createIndexes`, `dropIndexes`, `killCursors`

- **Query Operators**
  - Comparison: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`
  - Logical: `$and`, `$or`, `$not`, `$nor`
  - Element: `$exists`, `$type`
  - Evaluation: `$regex`, `$mod`, `$where`
  - Array: `$all`, `$size`, `$elemMatch`
  - Bitwise: `$bitsAllSet`, `$bitsAnySet`, `$bitsAllClear`, `$bitsAnyClear`

- **Aggregation Pipeline**
  - Stages: `$match`, `$project`, `$group`, `$sort`, `$limit`, `$skip`, `$unwind`, `$sample`, `$lookup`, `$facet`, `$bucket`, `$bucketAuto`, `$unionWith`, `$replaceRoot`, `$replaceWith`
  - Accumulators: `$sum`, `$avg`, `$min`, `$max`, `$push`, `$addToSet`, `$first`, `$last`, `$stdDevPop`, `$stdDevSamp`
  - Expression operators: arithmetic, string, date, array, conditional, comparison

- **Storage**
  - PostgreSQL jsonb backend (indexes persisted in metadata tables)
  - Cursor TTL with periodic sweeper
  - Multi-document ACID transactions

- **Security**
  - TLS/SSL support for encrypted connections
  - SCRAM-SHA-256 authentication

- **Shadowing (optional)**
  - Forwards original wire bytes to a real MongoDB (default: 7.0) and compares replies
  - Wire-compatible forwarding (OP_MSG stays OP_MSG; OP_QUERY stays OP_QUERY)
  - Safe by default: logs mismatches only; does not affect client responses

## Requirements

- Rust (stable) and Cargo
- Optional: PostgreSQL (for persistence)
- Optional: MongoDB 7.0 (for shadowing tests)
- Optional: `mongosh` for manual testing

## Build

- `cargo build`

## Run

- Minimal (no Postgres, no shadow):
  - `cargo run`
- With PostgreSQL storage:
  - `OXIDEDB_POSTGRES_URL=postgres://USER:PASS@HOST:PORT/DB cargo run`
- With shadowing (compare-only, no failures):
  - `OXIDEDB_SHADOW_ENABLED=true OXIDEDB_SHADOW_ADDR=127.0.0.1:27018 cargo run`
  - Optional: `OXIDEDB_SHADOW_SAMPLE_RATE=1.0` (default 1.0) to control percentage of requests shadowed
  - Optional: `OXIDEDB_SHADOW_DB_PREFIX=test` to isolate upstream DB names (rewrites `$db`/namespace)
- Logging:
  - `RUST_LOG=info,oxidedb=debug` for more details

The server listens on `127.0.0.1:27017` by default. Example with `mongosh`:

```
mongosh mongodb://127.0.0.1:27017
> db.runCommand({ hello: 1 })
> db.runCommand({ ping: 1 })
> db.runCommand({ buildInfo: 1 })
```

## Configuration

OxideDB loads configuration from `config.toml` when present, then applies CLI/env overrides.

Example `config.toml`:

```toml
listen_addr = "127.0.0.1:27017"
postgres_url = "postgres://user:pass@localhost:5432/oxidedb"
log_level = "info"

[cursor]
# (currently exposed as top-level fields)
# cursor_timeout_secs = 300
# cursor_sweep_interval_secs = 30

[shadow]
# Enable shadowing
enabled = true
# Upstream MongoDB address
addr = "127.0.0.1:27018"
# Optional: rewrite $db / fullCollectionName to "<prefix>_<db>"
# db_prefix = "test"
# Timeouts and sampling
timeout_ms = 800
sample_rate = 1.0
# Compare options
[shadow.compare]
# Ignore non-deterministic fields (top-level)
ignore_fields = ["$clusterTime", "operationTime", "topologyVersion", "localTime", "connectionId"]
# Do not treat 2 and 2.0 as equal
numeric_equivalence = false
```

## CLI Flags and Environment

- `--listen-addr` / `OXIDEDB_LISTEN_ADDR` (default `127.0.0.1:27017`)
- `--postgres-url` / `OXIDEDB_POSTGRES_URL`
- `--log-level` / `OXIDEDB_LOG_LEVEL`
- Shadow:
  - `--shadow-enabled` / `OXIDEDB_SHADOW_ENABLED` (default false)
  - `--shadow-addr` / `OXIDEDB_SHADOW_ADDR`
  - `--shadow-db-prefix` / `OXIDEDB_SHADOW_DB_PREFIX`
  - `--shadow-timeout-ms` / `OXIDEDB_SHADOW_TIMEOUT_MS` (default 800)
  - `--shadow-sample-rate` / `OXIDEDB_SHADOW_SAMPLE_RATE` (default 1.0)

## Shadowing Details

- Purpose: measure compatibility by forwarding the exact original request to a real MongoDB and comparing replies.
- Wire compatibility: the forwarded op matches the incoming op (no translation). OP_COMPRESSED is currently not supported and is skipped if encountered.
- Rewriting: when `db_prefix` is set, rewrite `$db` (OP*MSG) and `fullCollectionName` (OP_QUERY) to `<prefix>*<db>` to isolate upstream state.
- Comparison: by default ignores non-deterministic fields only; numeric type equality (2 vs 2.0) is not applied.
- Sampling: shadow every request by default (`sample_rate=1.0`); set to a fraction (e.g., `0.1`) to reduce load.
- Safety: shadow errors/timeouts are logged and never affect client responses in compare-only mode.

More details: see `docs/shadow.md` for the full plan, configuration, wire behavior, comparer, metrics, and current progress.

## Tests

- Run all tests (some may skip if env vars are unset):
  - `cargo test`
- Store-level integration tests (require admin Postgres URL):
  - `export OXIDEDB_TEST_POSTGRES_URL=postgres://USER:PASS@HOST:PORT/postgres`
  - Tests will create and drop ephemeral databases per test.
- Shadow forwarder tests (require MongoDB 7.0):
  - `export OXIDEDB_TEST_MONGODB_ADDR=127.0.0.1:27018`
  - `cargo test --test shadow_smoke -- --nocapture`

## MongoDB 7 with Docker (for shadow)

Quickly start a MongoDB 7.0 for shadow testing on port 27018.

```
docker run -d \
  --name mongo7 \
  -p 27018:27017 \
  mongo:7
```

Then set `export OXIDEDB_TEST_MONGODB_ADDR=127.0.0.1:27018` and run tests or start OxideDB with shadow enabled.

## Roadmap

- Comparer upgrades: nested ignore paths, path-based diffs, redaction of sensitive fields
- End-to-end tests: spin up the server with graceful shutdown and assert shadow match/mismatch counters
- Broader wire coverage: OP_COMPRESSED (decompression), OP_MSG additional sections
- Command completeness and stricter comparisons as feature parity grows
- Optional: deterministic sampling via request hashing

## Troubleshooting

- Client reports unsupported op: some drivers may use OP_COMPRESSED. Use a client/config that avoids compression for now, or open an issue.
- Shadow upstream timeouts: increase `OXIDEDB_SHADOW_TIMEOUT_MS` (default 800ms), verify connectivity to the MongoDB instance.

## Contributing

PRs and issues are welcome while the project evolves. Please include repro steps and versions for protocol-level issues.
