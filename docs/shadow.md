# Shadow (Upstream Comparison) — Plan and Progress

This document captures the design, configuration, and progress of the shadow feature: sending the same MongoDB wire request that OxideDB receives to a real MongoDB instance ("upstream") and comparing replies to measure wire- and semantic-compatibility.

## Goals

- Wire-compatible forwarding:
  - Forward the original message type unchanged: OP_MSG stays OP_MSG, OP_QUERY stays OP_QUERY.
  - Preserve flags/sections and only mutate when explicitly asked (db prefix rewrite).
- Safe, side-effect-free for clients:
  - OxideDB processes and replies as usual; shadow path runs out-of-band and never affects client responses.
- Comparisons that are useful by default:
  - Ignore only non-deterministic fields; no numeric equivalence by default (2 != 2.0).
  - Path-based ignores configurable when needed.
- Observability and control:
  - Sampling to control costs (default 1.0 in dev/tests).
  - Timeouts to bound latency.
  - Structured logs and basic metrics (attempts/matches/mismatches/timeouts).
- Testability:
  - Forwarder tests that talk directly to MongoDB.
  - End-to-end tests with the server running and shadow enabled.

## Configuration

Top-level `Config` has an optional `shadow` section (TOML/env/CLI supported):

- `enabled` (bool; default false)
- `addr` (string; `host:port` of upstream MongoDB)
- `db_prefix` (string, optional): when set, rewrite `$db`/namespace to `<prefix>_<db>` on the upstream request
- `timeout_ms` (u64; default 800)
- `sample_rate` (f64; 0.0–1.0; default 1.0)
- `mode` (`compare_only` | `compare_and_fail` | `record_only`); default `compare_only` (only compare + log)
- `compare`:
  - `ignore_fields` (list of dot-path patterns; defaults include `$clusterTime`, `operationTime`, `topologyVersion`, `localTime`, `connectionId`)
  - `numeric_equivalence` (bool; default false)

CLI/env overrides:

- `--shadow-enabled` / `OXIDEDB_SHADOW_ENABLED`
- `--shadow-addr` / `OXIDEDB_SHADOW_ADDR`
- `--shadow-db-prefix` / `OXIDEDB_SHADOW_DB_PREFIX`
- `--shadow-timeout-ms` / `OXIDEDB_SHADOW_TIMEOUT_MS`
- `--shadow-sample-rate` / `OXIDEDB_SHADOW_SAMPLE_RATE`

## Wire Behavior

- Forward as-is:
  - OP_MSG → forward OP_MSG (request header/body), reusing requestId.
  - OP_QUERY → forward OP_QUERY (fullCollectionName/flags preserved).
- Rewrites (only when `db_prefix` is set):
  - OP_MSG: update `$db` inside section-0 doc to `<prefix>_<db>`.
  - OP_QUERY: rewrite `fullCollectionName` C-string from `db.$cmd` or `db.<coll>` to `<prefix>_<db>.$cmd`/`<prefix>_<db>.<coll>`.
  - TODO: rewrite explicit namespace fields for commands that carry them (e.g., getMore/killCursors/insert/find). Not yet implemented.
- OP_COMPRESSED:
  - Currently not supported: replies with `OP_COMPRESSED` are detected and skipped (no comparison). Future: add decompression and compare.

## Comparer

- Default ignores: top-level `$clusterTime`, `operationTime`, `topologyVersion`, `localTime`, `connectionId`.
- Path-based ignores: dot-paths with `*` wildcard per segment are supported (e.g., `cursor.firstBatch.*`, `$clusterTime.signature`).
- Numeric equivalence: optional (disabled by default). When enabled, considers `2` and `2.0` equal across int32/int64/double.
- Diffs:
  - Path-based messages like `/cursor/ns ours=... theirs=...`
  - Values summarized and truncated; sensitive field names (password/credential/secret/token/sasl) are redacted.

## Observability

- Logs (via `tracing`):
  - Debug: shadow match, basic timings.
  - Info: shadow mismatch with summary and truncated details.
  - Debug: timeouts/errors.
- Metrics (in-memory counters on `AppState`):
  - `shadow_attempts`, `shadow_matches`, `shadow_mismatches`, `shadow_timeouts`.
  - Exposed programmatically (tests read via `spawn_with_shutdown`); no public endpoint yet.
- Sampling:
  - Bernoulli per-request (`rand::random::<f64>() < sample_rate`). Default `1.0` in dev/tests.
  - Future: optional deterministic sampling based on a request hash.

## Failure Handling

- Shadow send/recv errors or timeouts are logged and do not impact client responses.
- Upstream reconnect: next attempt will reconnect on connection failure; first pass uses a simple lazy reconnect.

## Security

- Redact sensitive fields in diffs.
- Mutations do impact upstream. In tests/dev, use `db_prefix` or a dedicated upstream MongoDB.
- Auth/TLS to upstream: not implemented yet.

## Test Strategy

- Shadow forwarder tests (direct to Mongo):
  - `tests/shadow_smoke.rs`: OP_MSG hello/ping/buildInfo; legacy OP_QUERY ismaster.
- End-to-end server tests with shadow enabled:
  - `tests/server_shadow_e2e.rs`: boots server on an ephemeral port; hello/ping/buildInfo; asserts shadow attempts and ok:1.
  - `tests/server_shadow_crud_e2e.rs`: boots server with Postgres + upstream Mongo; create/insert/find happy path; asserts ok:1 and shadow attempts.
- Running tests:
  - Set upstream Mongo: `export OXIDEDB_TEST_MONGODB_ADDR=127.0.0.1:27018`
  - Optional Postgres (for CRUD e2e): `export OXIDEDB_TEST_POSTGRES_URL=postgres://USER:PASS@HOST:PORT/postgres`
  - Run: `cargo test --test shadow_smoke`, `cargo test --test server_shadow_e2e`, `cargo test --test server_shadow_crud_e2e`

## Milestones and Status

- Config + CLI surfaces [x]
- ShadowSession forwarder (OP_MSG/OP_QUERY) [x]
- OP_REPLY first-doc decoding [x]
- Comparer (ignores, path-diff, redaction) [x]
- Server hook (non-blocking compare; sampling + timeout) [x]
- Metrics counters [x]
- OP_COMPRESSED detection/skip [x]
- Forwarder tests (hello/ping/buildInfo; ismaster) [x]
- E2E tests (hello/ping/buildInfo) [x]
- E2E CRUD test (create/insert/find) [x]
- Namespace rewrites for explicit ns fields when `db_prefix` [ ]
- Upstream auth (SCRAM-SHA-256) [ ]
- Deterministic sampling (hash-based) [ ]
- Metrics exposure/endpoint [ ]
- OP_COMPRESSED full support (decompression) [ ]
- Additional e2e coverage (getMore/killCursors/list*/indexes) [ ]

## Next Steps

1) Namespace rewriting (db_prefix) for commands with explicit collection/ns fields (getMore, killCursors, insert, find).
2) Extend e2e coverage to getMore/killCursors and listDatabases/listCollections; align shapes as needed.
3) Optional: deterministic sampling and a simple metrics dump endpoint or admin command.
4) Investigate OP_COMPRESSED support for full parity with modern drivers.
5) Consider upstream auth (SCRAM-SHA-256) to enable shadow against secured clusters.

---

If you spot noisy diffs in a specific command, add a dot-path ignore to `shadow.compare.ignore_fields` and propose a per-command normalizer if necessary.

