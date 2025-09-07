# OxideDB Master Plan

A Rust server that speaks the MongoDB Wire Protocol and translates operations into PostgreSQL SQL using `jsonb` as the primary storage format.

This plan is iterative and deliverable-focused. It describes the architecture, data model, translation strategy (including aggregation), supported commands, milestones, test strategy, and risks. Treat it as a living document; update each phase with learnings and decisions.

---

## Goals

- Serve standard MongoDB clients over the wire protocol (OP_MSG-first).
- Translate CRUD and admin commands to PostgreSQL using `jsonb`.
- Full aggregation pipeline compatibility at the functional level:
  - Push down stages to PostgreSQL whenever possible for performance.
  - Fallback to an in-engine pipeline executor to preserve exact semantics where pushdown is not feasible.
- Preserve MongoDB semantics; document deliberate deviations and performance caveats.
- Ship incrementally with tests, metrics, and clear compatibility notes.

### Non-Goals (initially)

- Exact performance parity with upstream MongoDB across all workloads.
- Replication/sharding and journaling semantics.
- Map-Reduce (legacy) — encourage the aggregation pipeline instead.
- Atlas-only stages (e.g., `$search`) unless explicitly requested.

---

## Architecture

- Runtime: async Rust with `tokio` and `tracing`.
- Listener: TCP (TLS optional) speaking MongoDB Wire Protocol, focusing on OP_MSG.
- Protocol: Frame parser + BSON document reader/writer (`bson` crate).
- Dispatch: Command router (hello/ping/CRUD/DDL/indexes/aggregate) with per-connection context.
- Translation: Mongo filter/update/projection/sort/aggregation → SQL + parameters for PostgreSQL.
- Aggregation Engine: Planner selects pushdown vs. in-engine execution per stage; streaming, backpressure-aware.
- Storage: PostgreSQL with one schema per Mongo database, one table per collection, `jsonb` doc column + `_id` as PK.
- Cursors: Server-managed cursor map with batched results, `getMore`, timeouts; pipeline cursors supported.
- Observability: Structured logging, metrics, request/connection spans; per-stage timings for pipelines.

```text
+---------------------------+           +-----------------------------+
| MongoDB Client/Driver     |  OP_MSG   |  OxideDB Server (Rust)      |
| (Node/Rust/Python/Go)     +---------->+  - TCP/TLS Listener         |
|                           |           |  - Wire Protocol Parser     |
+---------------------------+           |  - Command Dispatch         |
                                       |  - Translator (Mongo→SQL)   |
                                       |  - Aggregation Planner/Exec |
                                       |  - Cursor Manager           |
                                       |  - Postgres Pool            |
                                       +-------------+---------------+
                                                     |
                                                     | SQL (prepared)
                                                     v
                                       +-----------------------------+
                                       | PostgreSQL                  |
                                       | - Schemas per DB            |
                                       | - Table per Collection      |
                                       | - jsonb docs + id PK        |
                                       | - Indexes (GIN/B-Tree)      |
                                       +-----------------------------+
```

---

## Data Model & Schema

- Postgres schema per Mongo database: `"mdb_<db>"`.
- Postgres table per collection: `"mdb_<db>"."<coll>"`.
- Table layout:
  - `id bytea PRIMARY KEY` (Mongo `_id` stored as 12-byte ObjectId or encoded form for other types).
  - `doc jsonb NOT NULL` (full document, including `_id`).
- Default indexes:
  - `PRIMARY KEY (id)` (B-Tree)
  - `CREATE INDEX ... ON <table> USING GIN (doc jsonb_path_ops)`
- Metadata schema (cluster-wide): `mdb_meta`
  - `databases(db TEXT PRIMARY KEY)`
  - `collections(db TEXT, coll TEXT, PRIMARY KEY (db, coll))`
  - `indexes(db TEXT, coll TEXT, name TEXT, spec jsonb, sql text, PRIMARY KEY (db, coll, name))`
- Naming/quoting: Always quote identifiers to preserve case and special chars; sanitize to avoid invalid names.
- Aggregation spooling: when `allowDiskUse` is true or planner decides, use Postgres temp tables or CTE materialization.

Example DDL for first doc in `db=app`, `coll=users`:

```sql
CREATE SCHEMA IF NOT EXISTS "mdb_app";
CREATE TABLE IF NOT EXISTS "mdb_app"."users" (
  id bytea PRIMARY KEY,
  doc jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_users_doc_gin" ON "mdb_app"."users" USING GIN (doc jsonb_path_ops);
```

---

## Wire Protocol Support

- Focus on OP_MSG (sections 0/1), minimal OP_QUERY for older drivers if needed.
- Maintain `requestID`/`responseTo` pairing; support cursor semantics for aggregate.
- Handshake: respond to `hello`/`ismaster` with conservative features, standalone topology.
- Implement commands: `aggregate`, `explain` (basic shape), `ping`, `buildInfo`, `listDatabases`, `listCollections`, CRUD.
- Error replies follow MongoDB error codes/categories where feasible.

Handshake response sketch:

```json
{
  "ismaster": true,
  "helloOk": true,
  "maxWireVersion": 6,
  "minWireVersion": 0,
  "readOnly": false,
  "ok": 1
}
```

---

## Command Coverage (Phased)

- Admin/Handshake: `hello`/`ismaster`, `ping`, `buildInfo`, `serverStatus` (stub), `listDatabases`, `listCollections`.
- CRUD: `insert`, `find`, `getMore`, `killCursors`, `update`, `delete`.
- Aggregation: `aggregate` (full pipeline), `explain` (basic), `$out`/`$merge` semantics.
- DDL/Indexes: `create`, `drop`, `createIndexes`, `dropIndexes` (basic).

---

## Translation Strategy (Mongo → SQL)

### Namespaces

- Mongo `db.collection` → Postgres `"mdb_<db>"."<coll>"`.
- Lazily create schemas/tables on first use to match Mongo ergonomics.

### Inserts

- If `_id` absent, generate ObjectId (12-byte) server-side.
- Map `_id` to `id bytea`; full doc encoded as `jsonb` (with `_id` round-tripped inside `doc`).
- Ordered vs unordered: surface writeErrors/writeConcernError arrays accordingly.

SQL:

```sql
INSERT INTO "mdb_<db>"."<coll>" (id, doc)
VALUES ($1::bytea, $2::jsonb)
ON CONFLICT (id) DO NOTHING; -- or DO UPDATE when semantics require
```

### Finds

- Shortcut `_id` equality → `WHERE id = $1`.
- Equality subtree → `doc @> $json_fragment::jsonb`.
- Nested paths and operators → `jsonb_path_exists(doc, '$.path ? (@ <op> <value>)')` or field extraction with casts.
- Projection: initially apply in application layer; optimize later.
- Sort: `_id` or simple scalar fields via `ORDER BY (doc->>'field')::type`.
- Limit/skip → `LIMIT/OFFSET`.

### Updates

- Replacement: `UPDATE ... SET doc = $1::jsonb WHERE ...`.
- Operators (initial): `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$rename`, `$setOnInsert`.
- MVP approach: read-modify-write in app layer with optimistic concurrency on `id`; batch for performance.
- Upsert: when `_id` known, prefer `INSERT ... ON CONFLICT (id) DO UPDATE`.

### Deletes

- Build `WHERE` as in `find`; `DELETE FROM ... WHERE ...`.

---

## Aggregation Pipeline

Planner chooses between:
- SQL pushdown: translate sequences into CTEs/SELECT chains with JSONB ops, lateral joins, group-bys, window functions.
- Engine fallback: evaluate stages in Rust over streamed batches when translation would be incorrect or prohibitively complex.
- Hybrid: push down early `$match`/`$project`/`$sort` and execute complex tail stages in-engine.

Stage mappings (pushdown-first; fallback when needed):
- `$match`: WHERE clause using `doc @>` and `jsonb_path_exists`.
- `$project` / `$addFields` / `$set` / `$unset`: SELECT list building with `jsonb_build_object`, `jsonb_strip_nulls`, `-` operator to remove keys.
- `$sort`: ORDER BY on extracted fields; ensure stable sort with `_id` tiebreaker.
- `$limit` / `$skip`: LIMIT/OFFSET.
- `$unwind`: `CROSS JOIN LATERAL jsonb_array_elements(doc #> '{path}') AS elem(value)` with `preserveNullAndEmptyArrays` handling.
- `$group`: GROUP BY expressions on extracted keys; aggregates:
  - `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$count`, `$push` (array_agg), `$addToSet` (array_agg DISTINCT).
- `$lookup` (simple equality): LEFT JOIN to target collection on extracted fields; pipeline variant uses lateral subquery.
- `$facet`: run subpipelines via multiple CTEs and aggregate into a single doc; fallback if shape is complex.
- `$replaceRoot` / `$replaceWith`: switch to a different subdocument via `jsonb` extraction/building.
- `$count`: `SELECT COUNT(*)` and shape `{ count: <n> }`.
- `$sample`: `TABLESAMPLE SYSTEM` or `ORDER BY random()`; note randomness quality.
- `$bucket` / `$bucketAuto`: `width_bucket` and quantile approximations; document boundaries.
- `$unionWith`: SQL `UNION ALL` across two collections (aligned projections first).
- `$setWindowFields`: map to SQL window functions when possible, else fallback.
- `$redact`, `$densify`, `$fill`: engine fallback initially; add pushdown later where possible.
- `$geoNear`: require PostGIS; map to `ST_DWithin`/`ST_Distance` with GIST indexes.
- `$out` / `$merge`: materialize results into a target collection table; implement idempotent DDL and upsert semantics.

Pipeline execution details:
- Streaming: batches fetched from SQL and/or processed in-engine with bounded memory.
- Spooling: when `allowDiskUse` is true or planner decides, spill intermediate results to Postgres temp tables.
- Explain: return a JSON plan tree showing pushdown vs. engine stages.

Example snippets:

Unwind:
```sql
SELECT jsonb_set(doc, '{item}', elem.value) AS doc
FROM "mdb_app"."orders"
CROSS JOIN LATERAL jsonb_array_elements(doc #> '{items}') AS elem(value);
```

Group:
```sql
SELECT jsonb_build_object(
  '_id', doc->'userId',
  'total', SUM((doc->>'amount')::numeric)
) AS doc
FROM "mdb_app"."payments"
GROUP BY doc->'userId';
```

Lookup (equality):
```sql
SELECT jsonb_set(o.doc, '{profile}', p.doc, true) AS doc
FROM "mdb_app"."orders" o
LEFT JOIN "mdb_app"."profiles" p
  ON (o.doc->>'userId') = (p.doc->>'userId');
```

---

## Type Mapping

- `_id`: stored as `bytea` for ObjectId (12 bytes). For non-ObjectId `_id` types, encode consistently:
  - String: UTF-8 bytes
  - Int64/Double/Bool/Date: canonical byte encoding; documented
  - MVP: support ObjectId and String `_id`; extend as needed
- BSON → JSONB (MVP): null, bool, string, int32/int64, double, object, array, ObjectId (string in JSON for read-path), date (ISO-8601 or millis), timestamp (as date initially).
- Optional `doc_bson bytea` later for perfect fidelity; read-path reconstruction from BSON.

---

## Cursors

- Server-side cursor map keyed by `cursorId` (i64), owned by connection.
- Pipeline cursors support `batchSize`, `getMore`, and `killCursors`.
- Use keyset pagination when sorting by `_id`; otherwise `LIMIT/OFFSET`.
- Idle timeout (e.g., 10 minutes) and `killCursors` support.

---

## Indexes

- Default GIN on `doc` for broad queries.
- Single-field indexes: `CREATE INDEX ON ... ((doc->>'field'))` with optional casts.
- Join helpers: indexes on join keys used by `$lookup`.
- Geospatial: PostGIS `GEOGRAPHY/GEOMETRY` computed from `doc` for `$geoNear`, with GIST indexes.
- `_id` index implied by PK.
- Metadata for indexes stored in `mdb_meta.indexes`.

---

## Error Mapping

- Unique violation on `_id` → duplicate key error (code 11000) with Mongo-shaped message.
- Translation errors → Mongo error codes/categories (e.g., `FailedToParse`, `Location11001` for bad pipelines).
- Aggregation memory errors → `QueryExceededMemoryLimitNoDiskUseAllowed` unless `allowDiskUse=true`.
- Enforce limits: `maxBsonObjectSize`, `maxMessageSizeBytes`, batch sizes.

---

## Observability

- Logging: `tracing` spans for connection, request, translation, SQL execution; per-stage pipeline spans.
- Metrics: counters for ops, latencies; gauges for connections/cursors; stage-level metrics.
- Request IDs: propagate wire `requestID` through logs.

---

## Configuration

- File: `config.toml`; env overrides.
- Keys:
  - `listen_addr`, `tls.{enabled,cert_path,key_path}`
  - `postgres.url`, `pool.size`
  - `auth.mode` (none, scram256)
  - `limits.{max_message_bytes,max_batch_size}`
  - `compat.maxWireVersion`
  - `aggregation.allow_disk_use_default` (bool)
  - `features.postgis` (bool) for `$geoNear`

---

## Testing Strategy

- Unit: protocol parsing/serialization; translator for filters, updates, projections; pipeline stage compilers.
- Integration: spin Postgres, start server, exercise via MongoDB drivers for handshake + CRUD + aggregate.
- Aggregation correctness: stage-by-stage suites; mixed pipelines; `$lookup` joins; `$unwind` edge cases; `$group` accumulators.
- Property tests: generate docs and filters/pipelines; compare results to a reference evaluator where feasible.
- Performance: bulk insert/find; representative pipelines (group-by, unwind+lookup); track throughput/latency goals.

---

## Performance & Limits

- Initial targets (modest hardware):
  - Inserts: 5–10k docs/sec
  - Finds: 2–5k docs/sec (simple filters)
  - Aggregations: depends on stage mix; group-by on indexed keys should approach native SQL performance
- Limits:
  - `maxBsonObjectSize` (e.g., 16MB default)
  - Cursor batch sizes; pipeline in-memory buffer caps
  - Pool sizing: 4–8 Postgres connections per CPU (tune)

---

## Security (Phased)

- Phase 1: No auth (local/dev), optional TLS off.
- Phase 2+: SCRAM-SHA-256 (SASL) against `mdb_meta.users` with `saslStart`/`saslContinue` flows; per-connection auth state.
- TLS: rustls-based listener with configurable certs.

---

## Crate/Module Layout (Workspace)

- `oxidedb-server`: binary, listener, dispatch, cursors, config, metrics.
- `oxidedb-protocol`: wire types, OP_MSG framing, BSON helpers, error codes.
- `oxidedb-translate`: filter/update/projection/sort to SQL builder.
- `oxidedb-aggregate`: pipeline AST, planner (pushdown vs. engine), stage compilers, engine executor.
- `oxidedb-store`: Postgres pool, DDL bootstrap, metadata, query execution.
- `oxidedb-common`: types, ids, config structs, logging/metrics setup.

MVP can start as a single crate, but structure code by modules mirroring the above.

---

## Phased Roadmap & Acceptance Criteria

### Phase 0: Scaffolding (1–2 days)
- Cargo workspace/modules, `tracing`, ` anyhow/thiserror`, `bson`, `bytes`, `tokio`, `tokio-util`.
- Config loader, basic logging, CI with fmt/clippy.
- Acceptance: `oxidedb` starts, loads config, logs startup.

### Phase 1: Protocol & Handshake (3–5 days)
- TCP listener, OP_MSG parsing, BSON extraction.
- Implement `hello`/`ismaster`, `ping`, `buildInfo` (static), proper headers.
- Acceptance: Mongo driver can `ping`; returns sane `hello`.

### Phase 2: Storage & Basic CRUD (1–2 weeks)
- Postgres connector + pool; metadata schemas; lazy create db/collection tables.
- `insert` (single/bulk), `_id` generation, ordered/unordered handling.
- `find` with `_id` equality and simple equality on fields; limit/skip; projection in app; sort by `_id`.
- `getMore`/`killCursors` with in-memory cursor cache.
- `listDatabases`/`listCollections`.
- Acceptance: CRUD integration tests pass with drivers on simple docs.

### Phase 3: Aggregation Core (3–5 weeks)
- Pipeline AST, planner (pushdown vs. engine), executor skeleton.
- Pushdown stages: `$match`, `$project`/`$addFields`/`$set`/`$unset`, `$sort`, `$limit`, `$skip`, `$unwind` (basic), `$group` (common accumulators), `$count`, `$replaceRoot`/`$replaceWith`, `$sample`.
- Engine fallback for complex expressions; `allowDiskUse` temp table spooling.
- Acceptance: Core pipelines pass correctness tests; explain shows pushdown.

### Phase 4: Joins & Facets (2–3 weeks)
- `$lookup` (equality join and pipeline form), `$unionWith`, `$facet`.
- Index-aware join planning; join key extraction.
- Acceptance: Lookup/facet suites pass; performance sane with indexes.

### Phase 5: Advanced Stages (3–4 weeks)
- `$bucket`/`$bucketAuto`, `$setWindowFields`, `$redact` (engine), `$densify`/`$fill` (window functions), `$sortByCount`.
- `$geoNear` via PostGIS (behind feature flag); GIST indexes.
- Acceptance: Extended pipeline suites pass; document performance trade-offs.

### Phase 6: Indexes & Planner Hints (1–2 weeks)
- `createIndexes`/`dropIndexes` (single-field, simple compound); persist specs; DDL idempotency.
- Use `_id` and simple-field indexes in translator; join key indexes for `$lookup`.
- Acceptance: Indexes created, used, and reflected via metadata; explain logs index choices.

### Phase 7: Auth & TLS (1–2 weeks)
- SCRAM-SHA-256 SASL; per-connection auth state machine.
- TLS listener with config.
- Acceptance: Auth/TLS tests pass; unauthenticated access rejected when enabled.

### Phase 8: Compatibility & Stabilization (ongoing)
- Improve error codes/messages; timeouts/backpressure; prepared statements.
- Performance tuning; keyset pagination; robust cursor lifecycle.
- Acceptance: Benchmarks meet targets; long-running tests stable.

---

## Open Questions / Decisions to Make

- Exact `_id` byte encoding for non-ObjectId types (string, numbers, dates). MVP: ObjectId + String.
- `$search` (Atlas-only) requirements? If needed, consider external search integration.
- Projection pushdown vs. engine trade-offs; thresholds for pushing expressions into SQL.
- How aggressive should planner be with temp tables vs. large CTEs?
- Collation/text search behavior (defer initially; consider ICU/PG trigram).

---

## Risks & Mitigations

- BSON ↔ JSONB fidelity gaps: Start with supported subset; add `doc_bson` for exact round-trip.
- Aggregation memory/latency blow-ups: streaming executor, `allowDiskUse` spooling, planner cost heuristics.
- Join performance with `$lookup`: require/select indexes; limit cross-product with selective `$match` pushdown first.
- Geospatial correctness/perf: depend on PostGIS; fallback to engine when unavailable.
- Driver quirks across versions: Conservative handshake; CI against multiple drivers.

---

## References

- MongoDB Wire Protocol: https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/
- Aggregation Pipeline: https://www.mongodb.com/docs/manual/core/aggregation-pipeline/
- Extended JSON: https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/
- Postgres JSONB: https://www.postgresql.org/docs/current/functions-json.html
- JSONPath: https://www.postgresql.org/docs/current/datatype-json.html#DATATYPE-JSONPATH
- PostGIS: https://postgis.net/documentation/

---

## Next Steps

- Confirm crate layout (single crate vs workspace now) and whether PostGIS support is desired initially.
- Bootstrap `oxidedb-server` with config/logging and a stub OP_MSG responder.
- Add Postgres connector and metadata bootstrap in `oxidedb-store`.
- Implement `hello`, `ping`, then `insert` + `_id` generation to unlock end-to-end path.
- Draft aggregation planner skeleton and pushdown for `$match` + `$project` as groundwork.
