# Configuration

OxideDB can be configured through multiple methods: configuration files, environment variables, and command-line arguments.

## Configuration Methods

### 1. Configuration File

Create a `config.toml` file in the working directory:

```toml
listen_addr = "127.0.0.1:27017"
postgres_url = "postgres://user:password@localhost:5432/oxidedb"
log_level = "info"
```

### 2. Environment Variables

Set configuration via environment variables:

```bash
export OXIDEDB_LISTEN_ADDR="127.0.0.1:27017"
export OXIDEDB_POSTGRES_URL="postgres://user:password@localhost:5432/oxidedb"
export OXIDEDB_LOG_LEVEL="info"
```

### 3. Command-Line Arguments

Pass configuration directly:

```bash
oxidedb --listen-addr "127.0.0.1:27017" --postgres-url "postgres://user:password@localhost:5432/oxidedb"
```

## Configuration Precedence

Configuration is applied in the following order (later overrides earlier):

1. Default values
2. Configuration file (`config.toml`)
3. Environment variables (`OXIDEDB_*`)
4. Command-line arguments

## Quick Configuration Examples

### Development Setup

```toml
listen_addr = "127.0.0.1:27017"
postgres_url = "postgres://dev:dev@localhost:5432/oxidedb_dev"
log_level = "debug"
```

### Production Setup

```toml
listen_addr = "0.0.0.0:27017"
postgres_url = "postgres://oxidedb:${PG_PASSWORD}@postgres.internal:5432/oxidedb"
log_level = "info"
```

### With Shadow Mode

```toml
listen_addr = "127.0.0.1:27017"
postgres_url = "postgres://user:password@localhost:5432/oxidedb"

[shadow]
enabled = true
addr = "127.0.0.1:27018"
db_prefix = "test"
sample_rate = 1.0
```

## Complete Reference

For detailed configuration options, see the [Configuration Reference](./reference/config.md).

## Environment Variables Reference

| Variable | Description |
|----------|-------------|
| `OXIDEDB_LISTEN_ADDR` | Server listen address |
| `OXIDEDB_POSTGRES_URL` | PostgreSQL connection URL |
| `OXIDEDB_LOG_LEVEL` | Logging level (error, warn, info, debug, trace) |
| `OXIDEDB_CURSOR_TIMEOUT_SECS` | Cursor timeout in seconds |
| `OXIDEDB_CURSOR_SWEEP_INTERVAL_SECS` | Cursor cleanup interval |
| `OXIDEDB_SHADOW_ENABLED` | Enable shadow mode |
| `OXIDEDB_SHADOW_ADDR` | Shadow MongoDB address |
| `OXIDEDB_SHADOW_DB_PREFIX` | Database prefix for shadow |
| `OXIDEDB_SHADOW_TIMEOUT_MS` | Shadow timeout in milliseconds |
| `OXIDEDB_SHADOW_SAMPLE_RATE` | Shadow sampling rate (0.0-1.0) |

## Next Steps

- Read the [Configuration Reference](./reference/config.md) for all options
- Learn about [Shadow Mode](./features/shadow_mode.md) configuration
- See [Quick Start](./quickstart.md) for setup examples
