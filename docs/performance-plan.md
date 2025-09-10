Performance Optimization Plan for oxidedb

Overview

- Goal: Improve latency and throughput with low-risk, high-impact changes first, then iterate with data.
- Approach: add lightweight instrumentation, remove unnecessary DDL from hot paths, increase concurrency, and make queries more index-friendly.

Phases

Phase 0: Instrumentation

- Add debug-level timing around store operations (insert/find/update/delete/index/ensure).
- Emit operation name, db, collection (when available), and elapsed_ms.
- Use this to identify real hotspots before and after each change.

Phase 1: Avoid DDL on Hot Paths (Ensure Cache)

- Problem: insert_one() indirectly runs CREATE SCHEMA/TABLE/INDEX IF NOT EXISTS through ensure_collection(), adding overhead to steady-state writes.
- Solution: Add in-memory caches in PgStore:
  - databases_cache: set of known database names.
  - collections_cache: set of (db, coll) pairs.
- ensure_database/ensure_collection check caches first; only run DDL when not present, then update caches.
- Outcome: eliminate repeated DDL in steady state.

Phase 2: Connection Pool

- Replace single tokio_postgres::Client with a pool (e.g., deadpool-postgres).
- All store calls borrow a client from the pool; transactions use begin on a pooled client (remove per-request connect).
- Configurable pool size via config/env.

Phase 3: Equality Filter Pushdown with @>

- Detect simple top-level equality filters (and $eq forms) and use WHERE doc @> $1::jsonb.
- Fallback to current jsonb_path_exists() for complex/dotted cases.
- Benefit: better use of GIN index on doc; faster, simpler SQL.

Phase 4: Index-Aligned Sort

- For single-field sorts where an expression index exists, switch ORDER BY to use (doc->>'field') ASC|DESC without CASE/regex heuristics.
- Fallback to existing heuristic otherwise.

Phase 5: Small Wins / Cleanup

- Trim unnecessary Document clones where safe.
- Prefer prepared statements across hot paths.
- Optional: tighten error handling for does-not-exist branches.

Phase 6 (Optional Later):

- Streaming cursors (fetch-on-demand) for large result sets.
- Bulk inserts (multi VALUES / COPY) for batch workloads.
- Compression (OP_COMPRESSED) if desired.

Acceptance Criteria

- Steady-state inserts show no DDL in logs and improved latency.
- Concurrent requests do not serialize on a single connection.
- Equality-only reads use @> and benefit from GIN.
- Indexed single-field sorts use index-friendly ORDER BY.
- No functional regressions (existing tests pass).

Implementation Order

1) Phase 0 and Phase 1 (this PR/iteration).
2) Re-measure typical app flows (tenant add/list).
3) Phase 2 connection pool.
4) Phase 3 equality pushdown.
5) Phase 4 sort alignment.

