# Aggregation Pipeline Mapping (MongoDB → PostgreSQL)

This document details how each MongoDB aggregation stage and common expressions map to PostgreSQL constructs when using `jsonb` as the primary storage format. It also calls out edge cases, semantic gaps, and when the execution will fall back to the in-engine pipeline executor instead of SQL pushdown.

Legend:
- Pushdown: stage is translated to SQL whenever possible.
- Hybrid: partial pushdown; remaining logic in engine.
- Engine: evaluated in Rust engine (no SQL translation yet) to preserve semantics.

---

## Core Assumptions

- Collection layout: `"mdb_<db>"."<coll>"(id bytea PRIMARY KEY, doc jsonb NOT NULL)`
- JSONB operators/functions used extensively: `->`, `->>`, `#>`, `@>`, `?`, `jsonb_path_exists`, `jsonb_array_elements`, `jsonb_set`, `jsonb_build_object`, `jsonb_agg`, `jsonb_strip_nulls`.
- Expressions extract scalar values with casts where needed, e.g., `(doc->>'price')::numeric`.
- Stable sorts add `_id` tiebreaker: `ORDER BY <keys>, id`.
- Null vs. missing: Mongo distinguishes them. SQL pushdown uses `jsonb_path_exists` and careful checks to emulate missing behavior.

---

## Stage-by-Stage Mapping

### $match — Pushdown

- Simple equality/nested subtree: `doc @> '<fragment>'::jsonb`.
- Operators: use `jsonb_path_exists` and/or extracted fields with casts.
- `$expr`: Hybrid — pushdown arithmetic/comparison subset; fallback for complex expressions.
- `$regex`: map to `~` / `~*` with anchors handled in the pattern; flags `i`/`m`/`s` supported partially.

Examples:
- `{ a: 5 }` → `WHERE doc @> '{"a":5}'::jsonb`
- `{ a: { $gt: 3 } }` → `WHERE jsonb_path_exists(doc, '$.a ? (@ > 3)')`
- `{ name: { $regex: '^A', $options: 'i' } }` → `WHERE (doc->>'name') ~* '^A'`

Edge cases:
- Type comparisons (e.g., 2 vs 2.0) — treat as numeric equal; ensure casts align.
- Missing vs null: `$exists:false` must exclude nulls and missing; emulate with JSONPath `!exists` checks.

---

### $project / $addFields / $set / $unset — Pushdown/Hybrid

- Build documents via `jsonb_build_object` for selected fields and computed expressions.
- Add/update fields with `jsonb_set(doc, '{path}', <expr>::jsonb, true)`.
- Remove fields via `doc - 'field'` or `doc #- '{path}'`.
- Complex expression operators not easily mapped (e.g., `$map`, `$filter`, `$reduce`) → Engine.

Example:
```sql
SELECT jsonb_build_object(
  '_id', id,
  'name', doc->'name',
  'total', ((doc->>'qty')::int * (doc->>'price')::numeric)::jsonb
) AS doc
FROM "mdb_app"."orders";
```

---

### $sort — Pushdown

- Extract fields and cast: `ORDER BY (doc->>'k1')::numeric ASC, (doc->>'k2')::text DESC, id ASC`.
- Stable sorting guaranteed by appending `id`.

Limitations:
- Mixed-type sorts follow Mongo semantics (numbers before strings, etc.) — Engine when mixed types detected.

---

### $limit / $skip — Pushdown

- `LIMIT/OFFSET`.
- For large offsets, prefer keyset pagination when a sort key exists (`_id`).

---

### $unwind — Pushdown/Hybrid

- Use lateral expansion: `CROSS JOIN LATERAL jsonb_array_elements(doc #> '{path}') AS elem(value)`.
- `preserveNullAndEmptyArrays`: add `LEFT JOIN LATERAL` and COALESCE logic.
- `includeArrayIndex`: add column from `WITH ORDINALITY`.

Example:
```sql
SELECT jsonb_set(doc, '{item}', elem.value, true) AS doc
FROM "mdb_app"."orders"
CROSS JOIN LATERAL jsonb_array_elements(doc #> '{items}') AS elem(value);
```

---

### $group — Pushdown (common accumulators)

- Group key `_id`: expression or object → SELECT list + GROUP BY list.
- Accumulators:
  - `$sum`: `SUM(expr)`
  - `$avg`: `AVG(expr)`
  - `$min`/`$max`: `MIN(expr)` / `MAX(expr)`
  - `$count`: `COUNT(*)` or `COUNT(expr)`
  - `$first`/`$last`: use window functions over sorted subqueries; otherwise Engine.
  - `$push`: `jsonb_agg(expr)`
  - `$addToSet`: `jsonb_agg(DISTINCT expr)`

Example:
```sql
SELECT jsonb_build_object(
  '_id', doc->'userId',
  'total', SUM((doc->>'amount')::numeric)
) AS doc
FROM "mdb_app"."payments"
GROUP BY doc->'userId';
```

Limitations:
- Expression-rich group keys/accumulators may require Engine or pre-computed subqueries.

---

### $lookup — Pushdown (equality)/Hybrid (pipeline)

- Equality join: `LEFT JOIN` on extracted keys.
- Pipeline form: `LEFT JOIN LATERAL (SELECT ...) sub ON true` with correlated filter/pipeline.

Example:
```sql
SELECT jsonb_set(o.doc, '{profile}', p.doc, true) AS doc
FROM "mdb_app"."orders" o
LEFT JOIN "mdb_app"."profiles" p
  ON (o.doc->>'userId') = (p.doc->>'userId');
```

Limitations:
- Non-equality join conditions → Engine.
- Large fan-outs may need batching and/or temp tables.

---

### $facet — Hybrid

- Execute each sub-pipeline as a CTE; aggregate results into a single doc with arrays.
- If sub-pipelines require Engine, run them in-engine and combine.

---

### $replaceRoot / $replaceWith — Pushdown

- Use `doc #> '{path}'` or `jsonb_build_object(...)` to switch the root.

---

### $count — Pushdown

- `SELECT COUNT(*)` and wrap as `{ count: <n> }`.

---

### $sortByCount — Pushdown

- Desugars to `$group` by the expression/key + `$sort` by count desc.

---

### $sample — Pushdown

- `TABLESAMPLE SYSTEM(p)` for approximate sampling or `ORDER BY random()` for exact count (slower).

---

### $bucket / $bucketAuto — Pushdown/Hybrid

- `$bucket`: `width_bucket(expr, lower, upper, nbuckets)` and group.
- `$bucketAuto`: approximate quantiles in SQL or Engine for exact boundaries.

---

### $setWindowFields — Pushdown (subset)

- Map to SQL window functions for supported accumulators (`sum`, `avg`, `min`, `max`, `count`, rank/dense_rank/row_number analogs).
- Complex document reshaping → Hybrid/Engine.

---

### $out / $merge — Pushdown

- `$out`: materialize into target collection; overwrite or create.
- `$merge`: upsert/update/delete semantics mapped to `INSERT ... ON CONFLICT ... DO UPDATE` and `DELETE` as needed.
- Ensure idempotent DDL and metadata updates.

---

### $geoNear — Pushdown (with PostGIS) / Engine

- If `features.postgis=true`, map to `ST_DWithin` + `ORDER BY ST_Distance`.
- Else Engine (or unsupported) with degraded performance.

---

### $redact / $densify / $fill / $graphLookup

- `$redact`: Engine initially.
- `$densify` / `$fill`: Pushdown via window functions if possible; else Engine.
- `$graphLookup`: Plan via `WITH RECURSIVE` for pushdown; start as Engine.

---

## Expression Mapping (subset)

- Arithmetic: `$add`, `$subtract`, `$multiply`, `$divide`, `$mod` → `+ - * / %` with casts.
- Comparison: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte` → corresponding SQL ops; handle null/missing per Mongo semantics.
- Logical: `$and`, `$or`, `$not` → SQL Boolean logic; careful with three-valued logic.
- String: `$concat` → `||`, `$substrBytes`/`$substrCP` → `substr`, `$toUpper`/`$toLower`.
- Array: `$size` → `jsonb_array_length`, `$in`/`$nin` → membership checks using JSONPath or unnested arrays.
- Date: `$toDate`, `$year`, `$month`, etc. — require canonical date storage; pushdown if stored as timestamp; else Engine.
- Conditional: `$cond`, `$ifNull`, `$coalesce` → `CASE WHEN`, `COALESCE`.

Unsupported/Engine initially:
- `$map`, `$filter`, `$reduce`, `$let` with deep nested expressions.
- `$switch` with complex branches → can map to `CASE`, but start as Engine.

---

## Null vs Missing Semantics

- Mongo: `null` value differs from missing field.
- Pushdown strategy:
  - Missing: `NOT jsonb_path_exists(doc, '$.a')`.
  - Null: `jsonb_path_exists(doc, '$.a') AND (doc->'a') IS NULL` or `(doc->>'a') IS NULL AND doc ? 'a'`.
- `$exists: false` means either missing or null depending on driver version; adhere to Mongo spec tests.

---

## Arrays vs Scalars

- `{ a: 5 }` matches docs where `a` equals 5 OR any array element equals 5.
- Pushdown via `jsonb_path_exists(doc, '$.a[*] ? (@ == 5)') OR doc @> '{"a":5}'`.
- `$elemMatch` requires per-element predicate — use `jsonb_path_exists` with nested conditions or `CROSS JOIN LATERAL` + WHERE.

---

## Planner Rules (Initial)

- Always push down early `$match` and `$project` that prune payload.
- Push down `$sort` only when sort keys are extracted scalars; add `_id` tiebreaker.
- Prefer keyset pagination with `_id` when possible.
- For `$lookup`, require indexed join keys or cap row estimates; otherwise consider Engine.
- Use temp tables when `allowDiskUse=true` and estimated memory > threshold.

---

## Examples (End-to-End)

1) Match → Project → Sort → Limit
```sql
WITH base AS (
  SELECT id, doc FROM "mdb_app"."users"
  WHERE jsonb_path_exists(doc, '$.age ? (@ >= 18)')
)
SELECT jsonb_build_object('_id', id, 'name', doc->'name') AS doc
FROM base
ORDER BY (doc->>'name')::text ASC, id ASC
LIMIT 20;
```

2) Unwind → Group → SortByCount
```sql
WITH unwound AS (
  SELECT (doc->>'tag') AS tag
  FROM "mdb_app"."articles"
  CROSS JOIN LATERAL jsonb_array_elements(doc #> '{tags}') AS t(tag)
)
SELECT jsonb_build_object('_id', tag, 'count', COUNT(*)) AS doc
FROM unwound
GROUP BY tag
ORDER BY COUNT(*) DESC;
```

3) Lookup (equality) → Project
```sql
SELECT jsonb_build_object(
  '_id', o.id,
  'order', o.doc,
  'user', p.doc
) AS doc
FROM "mdb_app"."orders" o
LEFT JOIN "mdb_app"."profiles" p
  ON (o.doc->>'userId')=(p.doc->>'userId');
```

---

## Test Coverage Outline

- Stage suites: unit tests for each stage compiler and engine fallback.
- Mixed pipelines: integration tests comparing against Mongo for correctness.
- Edge cases: null vs missing, arrays vs scalars, type coercion, regex flags.
- Performance: large `$unwind` + `$group`, `$lookup` with/without indexes, `$facet`.

---

## Not Yet Implemented (Trackers)

- Full `$graphLookup` pushdown via recursive CTEs.
- `$search` (Atlas) — out of scope unless requested.
- Rich date/time operator pushdown without typed columns.
- Expression ops: `$map`, `$filter`, `$reduce` pushdown.

---

## References

- Mongo Aggregation Pipeline: https://www.mongodb.com/docs/manual/core/aggregation-pipeline/
- Postgres jsonb: https://www.postgresql.org/docs/current/functions-json.html
- Postgres JSONPath: https://www.postgresql.org/docs/current/datatype-json.html#DATATYPE-JSONPATH
- LATERAL: https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-LATERAL
