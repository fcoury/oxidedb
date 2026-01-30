# Quick Start

Get OxideDB running in minutes with this quick start guide.

## Prerequisites

- **PostgreSQL 14+** with a database ready for OxideDB
- **Rust toolchain** (if building from source)
- Or use the **pre-built binary** (coming soon)

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/fcoury/oxidedb.git
cd oxidedb

# Build release binary
cargo build --release

# Binary will be at:
# target/release/oxidedb
```

### Using Cargo

```bash
cargo install oxidedb
```

## Configuration

Create a configuration file `config.toml`:

```toml
# OxideDB Configuration
listen_addr = "0.0.0.0:27017"
postgres_url = "postgres://user:password@localhost:5432/oxidedb"
log_level = "info"

[shadow]
enabled = false
addr = "127.0.0.1:27018"
db_prefix = "shadow"
timeout_ms = 2000
sample_rate = 1.0
```

Or use environment variables:

```bash
export OXIDEDB_LISTEN_ADDR="0.0.0.0:27017"
export OXIDEDB_POSTGRES_URL="postgres://user:password@localhost:5432/oxidedb"
export OXIDEDB_LOG_LEVEL="info"
```

## Running OxideDB

### Basic Start

```bash
# Using config file
./oxidedb --config config.toml

# Using environment variables
./oxidedb

# With explicit postgres URL
./oxidedb --postgres-url "postgres://user:password@localhost:5432/oxidedb"
```

### With Shadow Mode

Shadow mode compares OxideDB responses with an upstream MongoDB:

```toml
[shadow]
enabled = true
addr = "mongodb://localhost:27018"
db_prefix = "shadow"
timeout_ms = 2000
sample_rate = 1.0
username = "admin"
password = "secret"
auth_db = "admin"
tls_enabled = false
```

## First Connection

Connect using any MongoDB driver:

### Node.js (mongodb driver)

```javascript
const { MongoClient } = require('mongodb');

const client = new MongoClient('mongodb://localhost:27017');
await client.connect();

const db = client.db('myapp');
const collection = db.collection('users');

// Insert
await collection.insertOne({ name: 'Alice', age: 30 });

// Find
const user = await collection.findOne({ name: 'Alice' });
console.log(user);

await client.close();
```

### Python (pymongo)

```python
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017')
db = client['myapp']
collection = db['users']

# Insert
collection.insert_one({'name': 'Alice', 'age': 30})

# Find
user = collection.find_one({'name': 'Alice'})
print(user)

client.close()
```

### MongoDB Shell

```bash
mongosh mongodb://localhost:27017

> use myapp
> db.users.insertOne({ name: 'Alice', age: 30 })
> db.users.findOne({ name: 'Alice' })
```

## Verify Installation

Check that OxideDB is responding:

```javascript
// Using Node.js
const admin = client.db('admin');
const result = await admin.command({ ping: 1 });
console.log(result); // { ok: 1 }
```

## Next Steps

- Learn about [Query Operators](./features/queries.md)
- Explore the [Aggregation Pipeline](./features/aggregation.md)
- Set up [Transactions](./features/transactions.md)
- Configure [Shadow Mode](./features/shadow_mode.md) for testing
- Read about [Architecture](./architecture.md)

## Troubleshooting

### Connection Refused

```
Error: connect ECONNREFUSED 127.0.0.1:27017
```

- Check if OxideDB is running: `ps aux | grep oxidedb`
- Verify the listen address in config
- Check firewall settings

### PostgreSQL Connection Failed

```
Error: failed to connect to postgres
```

- Verify PostgreSQL is running
- Check connection string format
- Ensure database exists
- Check user permissions

### Authentication Errors

If using shadow mode with authentication:

- Verify username/password in config
- Check auth_db is correct (usually "admin")
- Ensure upstream MongoDB accepts the credentials

## Getting Help

- Check the [FAQ](./reference/faq.md)
- Review [Error Codes](./reference/errors.md)
- Open an issue on [GitHub](https://github.com/fcoury/oxidedb/issues)
