# Configuration Reference

Complete reference for OxideDB configuration options.

## Configuration Overview

OxideDB can be configured via:
1. **Configuration file** (`config.toml`)
2. **Environment variables**
3. **Command-line arguments**

Configuration precedence (highest to lowest):
1. Command-line arguments
2. Environment variables
3. Configuration file
4. Default values

## Configuration File

### Basic Structure

```toml
# OxideDB Configuration File
# Save as: config.toml

# Server settings
listen_addr = "127.0.0.1:27017"
postgres_url = "postgres://user:password@localhost:5432/oxidedb"
log_level = "info"

# Cursor settings
cursor_timeout_secs = 300
cursor_sweep_interval_secs = 30

# Shadow mode settings
[shadow]
enabled = false
addr = "127.0.0.1:27018"
db_prefix = "shadow"
timeout_ms = 800
sample_rate = 1.0
mode = "CompareOnly"

[shadow.compare]
ignore_fields = ["$clusterTime", "operationTime", "topologyVersion", "localTime", "connectionId"]
numeric_equivalence = false
```

## Configuration Options

### Server Settings

#### listen_addr

**Type:** `string`
**Default:** `"127.0.0.1:27017"`
**Environment:** `OXIDEDB_LISTEN_ADDR`
**CLI:** `--listen-addr`

The address and port where OxideDB listens for MongoDB client connections.

```toml
# Listen on all interfaces
listen_addr = "0.0.0.0:27017"

# Listen on specific interface
listen_addr = "192.168.1.100:27017"

# Use different port
listen_addr = "127.0.0.1:27018"
```

#### postgres_url

**Type:** `string` (optional)
**Default:** `null`
**Environment:** `OXIDEDB_POSTGRES_URL`
**CLI:** `--postgres-url`

PostgreSQL connection string for data storage.

```toml
# Basic connection
postgres_url = "postgres://user:password@localhost:5432/oxidedb"

# With SSL
postgres_url = "postgres://user:password@localhost:5432/oxidedb?sslmode=require"

# With connection parameters
postgres_url = "postgres://user:password@localhost:5432/oxidedb?connect_timeout=10&application_name=oxidedb"

# Connection URI format
postgres_url = "postgresql://user:password@host:port/db?param1=value1"
```

**Connection String Components:**
- `user` - PostgreSQL username
- `password` - PostgreSQL password
- `host` - PostgreSQL server hostname
- `port` - PostgreSQL server port (default: 5432)
- `dbname` - Database name
- `sslmode` - SSL mode (disable, prefer, require, verify-ca, verify-full)

#### log_level

**Type:** `string`
**Default:** `"info"`
**Environment:** `OXIDEDB_LOG_LEVEL`
**CLI:** `--log-level`

Logging level for OxideDB.

```toml
# Available levels
log_level = "error"   # Only errors
log_level = "warn"    # Warnings and errors
log_level = "info"    # General information (default)
log_level = "debug"   # Detailed debugging
log_level = "trace"   # Very verbose tracing
```

### Cursor Settings

#### cursor_timeout_secs

**Type:** `integer`
**Default:** `300` (5 minutes)
**Environment:** `OXIDEDB_CURSOR_TIMEOUT_SECS`

Time in seconds before an idle cursor is automatically closed.

```toml
# Short timeout for development
cursor_timeout_secs = 60

# Long timeout for batch processing
cursor_timeout_secs = 3600

# Disable timeout (not recommended)
cursor_timeout_secs = 0
```

#### cursor_sweep_interval_secs

**Type:** `integer`
**Default:** `30`
**Environment:** `OXIDEDB_CURSOR_SWEEP_INTERVAL_SECS`

Interval in seconds between cursor cleanup sweeps.

```toml
# Frequent cleanup
cursor_sweep_interval_secs = 10

# Infrequent cleanup (less CPU usage)
cursor_sweep_interval_secs = 60
```

## Shadow Mode Configuration

Shadow mode forwards requests to an upstream MongoDB for comparison.

### [shadow] Section

#### enabled

**Type:** `boolean`
**Default:** `false`
**Environment:** `OXIDEDB_SHADOW_ENABLED`
**CLI:** `--shadow-enabled`

Enable or disable shadow mode.

```toml
[shadow]
enabled = true
```

#### addr

**Type:** `string`
**Default:** `"127.0.0.1:27018"`
**Environment:** `OXIDEDB_SHADOW_ADDR`
**CLI:** `--shadow-addr`

Upstream MongoDB address for shadow comparisons.

```toml
[shadow]
addr = "127.0.0.1:27018"
# Or with hostname
addr = "mongodb.example.com:27017"
```

#### db_prefix

**Type:** `string` (optional)
**Default:** `null`
**Environment:** `OXIDEDB_SHADOW_DB_PREFIX`
**CLI:** `--shadow-db-prefix`

Prefix added to database names when forwarding to upstream MongoDB. Useful for isolating test data.

```toml
[shadow]
db_prefix = "shadow_test"
# Request to "mydb" becomes "shadow_test_mydb" upstream
```

#### timeout_ms

**Type:** `integer`
**Default:** `800`
**Environment:** `OXIDEDB_SHADOW_TIMEOUT_MS`
**CLI:** `--shadow-timeout-ms`

Timeout in milliseconds for shadow requests.

```toml
[shadow]
timeout_ms = 2000  # 2 seconds
timeout_ms = 500   # 500 milliseconds
```

#### sample_rate

**Type:** `float`
**Default:** `1.0`
**Environment:** `OXIDEDB_SHADOW_SAMPLE_RATE`
**CLI:** `--shadow-sample-rate`

Fraction of requests to shadow (0.0 to 1.0).

```toml
[shadow]
sample_rate = 1.0   # Shadow all requests
sample_rate = 0.5   # Shadow 50% of requests
sample_rate = 0.1   # Shadow 10% of requests
sample_rate = 0.01  # Shadow 1% of requests
```

#### mode

**Type:** `string`
**Default:** `"CompareOnly"`
**Environment:** `OXIDEDB_SHADOW_MODE`

Shadow mode behavior.

```toml
[shadow]
# Log differences only (default)
mode = "CompareOnly"

# Log differences and fail on mismatch (testing)
mode = "CompareAndFail"

# Record only, no comparison
mode = "RecordOnly"
```

**Modes:**
- `CompareOnly` - Compare responses and log differences
- `CompareAndFail` - Compare and return error on mismatch (for testing)
- `RecordOnly` - Forward requests without comparison

#### deterministic_sampling

**Type:** `boolean`
**Default:** `false`
**Environment:** `OXIDEDB_SHADOW_DETERMINISTIC_SAMPLING`

Use deterministic sampling based on request ID and database name hash.

```toml
[shadow]
deterministic_sampling = true
# Same request always sampled or not sampled consistently
```

### Shadow Authentication

#### username

**Type:** `string` (optional)
**Default:** `null`
**Environment:** `OXIDEDB_SHADOW_USERNAME`

Username for upstream MongoDB authentication.

```toml
[shadow]
username = "admin"
```

#### password

**Type:** `string` (optional)
**Default:** `null`
**Environment:** `OXIDEDB_SHADOW_PASSWORD`

Password for upstream MongoDB authentication.

```toml
[shadow]
password = "secretpassword"
```

#### auth_db

**Type:** `string`
**Default:** `"admin"`
**Environment:** `OXIDEDB_SHADOW_AUTH_DB`

Authentication database for upstream MongoDB.

```toml
[shadow]
auth_db = "admin"
```

### Shadow TLS Settings

#### tls_enabled

**Type:** `boolean`
**Default:** `false`
**Environment:** `OXIDEDB_SHADOW_TLS_ENABLED`

Enable TLS for upstream MongoDB connections.

```toml
[shadow]
tls_enabled = true
```

#### tls_ca_file

**Type:** `string` (optional)
**Default:** `null`
**Environment:** `OXIDEDB_SHADOW_TLS_CA_FILE`

Path to CA certificate file for TLS verification.

```toml
[shadow]
tls_ca_file = "/path/to/ca.pem"
```

#### tls_client_cert

**Type:** `string` (optional)
**Default:** `null`
**Environment:** `OXIDEDB_SHADOW_TLS_CLIENT_CERT`

Path to client certificate file for mutual TLS.

```toml
[shadow]
tls_client_cert = "/path/to/client.crt"
```

#### tls_client_key

**Type:** `string` (optional)
**Default:** `null`
**Environment:** `OXIDEDB_SHADOW_TLS_CLIENT_KEY`

Path to client private key file for mutual TLS.

```toml
[shadow]
tls_client_key = "/path/to/client.key"
```

#### tls_allow_invalid_certs

**Type:** `boolean`
**Default:** `false`
**Environment:** `OXIDEDB_SHADOW_TLS_ALLOW_INVALID_CERTS`

Allow invalid certificates (development only, not recommended for production).

```toml
[shadow]
tls_allow_invalid_certs = true
```

### [shadow.compare] Section

#### ignore_fields

**Type:** `array of strings`
**Default:** `["$clusterTime", "operationTime", "topologyVersion", "localTime", "connectionId"]`

Fields to ignore when comparing responses.

```toml
[shadow.compare]
ignore_fields = [
    "$clusterTime",
    "operationTime",
    "topologyVersion",
    "localTime",
    "connectionId",
    "customField"
]
```

#### numeric_equivalence

**Type:** `boolean`
**Default:** `false`
**Environment:** `OXIDEDB_SHADOW_NUMERIC_EQUIVALENCE`

Treat different numeric types as equivalent (e.g., 2 and 2.0).

```toml
[shadow.compare]
numeric_equivalence = true
# 2 (int) and 2.0 (double) considered equal
```

## Environment Variables

All configuration options can be set via environment variables:

```bash
# Server settings
export OXIDEDB_LISTEN_ADDR="0.0.0.0:27017"
export OXIDEDB_POSTGRES_URL="postgres://user:pass@localhost:5432/oxidedb"
export OXIDEDB_LOG_LEVEL="debug"

# Cursor settings
export OXIDEDB_CURSOR_TIMEOUT_SECS="600"
export OXIDEDB_CURSOR_SWEEP_INTERVAL_SECS="60"

# Shadow mode
export OXIDEDB_SHADOW_ENABLED="true"
export OXIDEDB_SHADOW_ADDR="127.0.0.1:27018"
export OXIDEDB_SHADOW_DB_PREFIX="test"
export OXIDEDB_SHADOW_TIMEOUT_MS="1000"
export OXIDEDB_SHADOW_SAMPLE_RATE="0.5"
export OXIDEDB_SHADOW_MODE="CompareOnly"
export OXIDEDB_SHADOW_DETERMINISTIC_SAMPLING="true"

# Shadow authentication
export OXIDEDB_SHADOW_USERNAME="admin"
export OXIDEDB_SHADOW_PASSWORD="secret"
export OXIDEDB_SHADOW_AUTH_DB="admin"

# Shadow TLS
export OXIDEDB_SHADOW_TLS_ENABLED="true"
export OXIDEDB_SHADOW_TLS_CA_FILE="/path/to/ca.pem"
export OXIDEDB_SHADOW_TLS_CLIENT_CERT="/path/to/client.crt"
export OXIDEDB_SHADOW_TLS_CLIENT_KEY="/path/to/client.key"
export OXIDEDB_SHADOW_TLS_ALLOW_INVALID_CERTS="false"

# Shadow comparison
export OXIDEDB_SHADOW_NUMERIC_EQUIVALENCE="false"
```

## Command-Line Arguments

```bash
oxidedb [OPTIONS]

Options:
    --config <PATH>                    Path to configuration file
    --listen-addr <ADDR>               Server listen address
    --postgres-url <URL>               PostgreSQL connection URL
    --log-level <LEVEL>                Logging level
    --shadow-enabled                   Enable shadow mode
    --shadow-addr <ADDR>               Shadow MongoDB address
    --shadow-db-prefix <PREFIX>        Database prefix for shadow
    --shadow-timeout-ms <MS>           Shadow timeout in milliseconds
    --shadow-sample-rate <RATE>        Shadow sampling rate (0.0-1.0)
    -h, --help                         Print help
    -V, --version                      Print version
```

### CLI Examples

```bash
# Basic start with config file
oxidedb --config /etc/oxidedb/config.toml

# Start with PostgreSQL
oxidedb --postgres-url "postgres://user:pass@localhost/oxidedb"

# Start with shadow mode
oxidedb \
    --postgres-url "postgres://user:pass@localhost/oxidedb" \
    --shadow-enabled \
    --shadow-addr "127.0.0.1:27018" \
    --shadow-sample-rate 0.1

# Development mode with debug logging
oxidedb \
    --listen-addr "127.0.0.1:27017" \
    --log-level debug \
    --postgres-url "postgres://user:pass@localhost/oxidedb"
```

## Example Configurations

### Development Configuration

```toml
# config.development.toml
listen_addr = "127.0.0.1:27017"
postgres_url = "postgres://dev:dev@localhost:5432/oxidedb_dev"
log_level = "debug"

cursor_timeout_secs = 60
cursor_sweep_interval_secs = 10

[shadow]
enabled = true
addr = "127.0.0.1:27018"
db_prefix = "dev_shadow"
timeout_ms = 2000
sample_rate = 1.0
mode = "CompareOnly"

[shadow.compare]
ignore_fields = ["$clusterTime", "operationTime", "topologyVersion", "localTime", "connectionId"]
numeric_equivalence = true
```

### Production Configuration

```toml
# config.production.toml
listen_addr = "0.0.0.0:27017"
postgres_url = "postgres://oxidedb:${PG_PASSWORD}@postgres.internal:5432/oxidedb"
log_level = "info"

cursor_timeout_secs = 600
cursor_sweep_interval_secs = 60

# Shadow mode disabled in production
[shadow]
enabled = false
```

### Testing Configuration

```toml
# config.testing.toml
listen_addr = "127.0.0.1:27017"
postgres_url = "postgres://test:test@localhost:5432/oxidedb_test"
log_level = "warn"

cursor_timeout_secs = 30
cursor_sweep_interval_secs = 5

[shadow]
enabled = true
addr = "127.0.0.1:27018"
db_prefix = "test"
timeout_ms = 500
sample_rate = 1.0
mode = "CompareAndFail"
deterministic_sampling = true

[shadow.compare]
ignore_fields = ["$clusterTime", "operationTime", "topologyVersion", "localTime", "connectionId"]
numeric_equivalence = false
```

### High-Performance Configuration

```toml
# config.highperf.toml
listen_addr = "0.0.0.0:27017"
postgres_url = "postgres://oxidedb:password@postgres-primary.internal:5432/oxidedb?pool_size=50"
log_level = "error"

cursor_timeout_secs = 300
cursor_sweep_interval_secs = 30

# Shadow mode with low sample rate for monitoring
[shadow]
enabled = true
addr = "mongodb-reference.internal:27017"
timeout_ms = 100
sample_rate = 0.001
mode = "CompareOnly"
deterministic_sampling = true
```

### Docker Configuration

```toml
# config.docker.toml
listen_addr = "0.0.0.0:27017"
postgres_url = "postgres://${PG_USER}:${PG_PASSWORD}@postgres:5432/${PG_DATABASE}"
log_level = "${LOG_LEVEL:-info}"

cursor_timeout_secs = 300
cursor_sweep_interval_secs = 30

[shadow]
enabled = "${SHADOW_ENABLED:-false}"
addr = "${SHADOW_ADDR:-mongodb:27017}"
db_prefix = "${SHADOW_DB_PREFIX:-}"
timeout_ms = "${SHADOW_TIMEOUT_MS:-800}"
sample_rate = "${SHADOW_SAMPLE_RATE:-1.0}"
```

## Configuration Validation

OxideDB validates configuration on startup and reports errors:

```bash
# Invalid configuration
$ oxidedb --shadow-sample-rate 2.0
Error: Invalid configuration: shadow.sample_rate must be between 0.0 and 1.0

# Missing required value
$ oxidedb --postgres-url ""
Error: Invalid configuration: postgres_url cannot be empty
```

## Runtime Configuration

Some settings can be viewed at runtime:

```javascript
// Check shadow metrics
db.adminCommand({ oxidedbShadowMetrics: 1 })

// Response:
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

## Security Best Practices

### PostgreSQL Connection

```toml
# Use SSL in production
postgres_url = "postgres://user:password@host:5432/db?sslmode=require"

# Use connection pooling
postgres_url = "postgres://user:password@host:5432/db?pool_size=20"

# Set application name for monitoring
postgres_url = "postgres://user:password@host:5432/db?application_name=oxidedb"
```

### Shadow Mode Security

```toml
[shadow]
# Use TLS for shadow connections
tls_enabled = true
tls_ca_file = "/etc/ssl/certs/ca.crt"

# Don't allow invalid certs in production
tls_allow_invalid_certs = false

# Use specific credentials
username = "shadow_user"
password = "${SHADOW_PASSWORD}"  # Use environment variable
auth_db = "admin"
```

### Environment Variable Security

```bash
# Use a .env file (not committed to version control)
# .env
OXIDEDB_POSTGRES_URL="postgres://user:password@localhost/oxidedb"
OXIDEDB_SHADOW_PASSWORD="secret"

# Load before starting
set -a && source .env && set +a && oxidedb
```

## Troubleshooting Configuration

### Common Issues

**PostgreSQL Connection Failed:**
```
Error: failed to connect to postgres
```
- Check PostgreSQL is running
- Verify connection string format
- Ensure database exists
- Check firewall rules

**Shadow Mode Timeout:**
```
shadow timeout
```
- Increase `timeout_ms`
- Check network connectivity to upstream MongoDB
- Verify upstream MongoDB is running

**Invalid Sample Rate:**
```
Error: sample_rate must be between 0.0 and 1.0
```
- Ensure sample_rate is within valid range
- Check for typos in configuration

## Next Steps

- Review [MongoDB Compatibility Matrix](./compatibility.md)
- Learn about [Architecture](../architecture.md)
- Explore [Query Operators](../features/queries.md)
