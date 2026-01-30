# Introduction

**OxideDB** is a MongoDB-compatible database that uses PostgreSQL as its storage engine. It provides a translation layer that speaks the MongoDB wire protocol while leveraging PostgreSQL's reliability, ACID transactions, and mature ecosystem.

## What is OxideDB?

OxideDB allows you to use MongoDB drivers and tools with a PostgreSQL backend. This gives you:

- **MongoDB Compatibility**: Use existing MongoDB drivers, ORMs, and tools
- **PostgreSQL Reliability**: ACID transactions, proven storage engine, backups
- **SQL Power**: Access your data via SQL when needed
- **No Migration**: Keep your existing MongoDB application code

## Key Features

### Query & CRUD Operations
- Full CRUD support (insert, find, update, delete)
- Rich query operators: `$eq`, `$ne`, `$gt`, `$lt`, `$in`, `$nin`, `$regex`, `$or`, `$and`, `$all`, `$size`, and more
- Projection, sorting, and pagination
- Array operations and nested document queries

### Aggregation Pipeline
- **Stages**: `$match`, `$project`, `$sort`, `$limit`, `$skip`, `$group`, `$unwind`, `$lookup`, `$addFields`, `$sample`, `$facet`, `$unionWith`, `$bucket`, `$out`, `$merge`, `$count`
- **Expressions**: `$concat`, `$concatArrays`, `$toString`, `$toInt`, `$toDouble`, `$cond`, `$ifNull`, arithmetic operators
- **Accumulators**: `$sum`, `$avg`, `$min`, `$max`, `$count`, `$push`, `$addToSet`

### Transactions
- Multi-document ACID transactions
- Session management with logical session IDs
- Retryable writes for idempotency
- Read-your-writes consistency

### Advanced Features
- **Shadow Mode**: Compare OxideDB responses with upstream MongoDB
- **Namespace Rewriting**: Transparent database prefixing for testing
- **OP_COMPRESSED**: Support for Snappy, zlib, and zstd compression
- **SCRAM-SHA-256**: Authentication support
- **TLS**: Both client and upstream TLS connections

## Use Cases

### Migration Path
Gradually migrate from MongoDB to PostgreSQL without rewriting your application:
1. Deploy OxideDB pointing to your existing PostgreSQL
2. Switch your app to use OxideDB
3. Access data via SQL for analytics, reporting, or complex queries

### Testing & Development
- Use `db_prefix` to isolate test databases
- Shadow mode to validate correctness against MongoDB
- Lightweight alternative to running MongoDB in CI/CD

### Polyglot Persistence
Use MongoDB drivers for application development while keeping data in PostgreSQL for:
- Complex SQL analytics
- Integration with BI tools
- Compliance and auditing requirements

## Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────────┐
│  MongoDB Driver │────▶│   OxideDB   │────▶│   PostgreSQL    │
│   (Any language)│     │  (Wire Protocol│   │   (Storage)     │
└─────────────────┘     │  Translation) │     └─────────────────┘
                        └─────────────┘
```

OxideDB implements the MongoDB wire protocol, translating MongoDB commands into PostgreSQL SQL queries. BSON documents are stored as JSONB in PostgreSQL, preserving MongoDB's flexible schema while gaining PostgreSQL's query capabilities.

## Getting Started

Check out the [Quick Start](./quickstart.md) guide to get OxideDB running in minutes.

## Compatibility

See the [MongoDB Compatibility Matrix](./reference/compatibility.md) for detailed information on supported features and any limitations.

## License

OxideDB is open source and available under the MIT License. See [License](./appendix/license.md) for details.
