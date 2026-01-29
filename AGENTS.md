# AGENTS.md

## Code hygiene

- After each feature implementation, run `cargo fmt --all` and `cargo clippy -- -D warnings`.
  - Fix all the reported issues.
- Fix all the warnings reported by `cargo check --all`.

## Build/Lint/Test Commands

- Build: `cargo build`
- Run: `cargo run`
- Test all: `cargo test`
- Test single: `cargo test --test <test_name>`
- Lint: `cargo clippy`
- Format: `cargo fmt`
- Format check: `cargo fmt --check`

### Test Environment Variables

- PostgreSQL tests: `OXIDEDB_TEST_POSTGRES_URL=postgres://USER:PASS@HOST:PORT/postgres`
- Shadow tests: `OXIDEDB_TEST_MONGODB_ADDR=127.0.0.1:27018`

### Examples

- Run specific test: `cargo test --test server_indexes_e2e`
- Run with verbose output: `cargo test -- --nocapture`
- Check format without changes: `cargo fmt --check`
- Apply clippy suggestions: `cargo clippy --fix`

## Code Style Guidelines

### Imports

- Group std imports first, then external crates, then internal modules
- Use explicit imports rather than glob imports when possible
- Sort imports alphabetically within each group

### Formatting

- Use rustfmt for consistent code formatting
- Max line length: 100 characters
- Use 4 spaces for indentation (no tabs)

### Types

- Use explicit types in function signatures
- Prefer `&str` over `String` for function parameters when possible
- Use `Option<T>` for values that may be absent
- Use `Result<T, E>` for operations that may fail

### Naming Conventions

- Use snake_case for variables and functions
- Use PascalCase for types and structs
- Use SCREAMING_SNAKE_CASE for constants
- Use descriptive names that convey purpose

### Error Handling

- Use `anyhow::Result` for most error handling
- Use `thiserror` for defining custom error types
- Return specific error types with context when appropriate
- Log errors with tracing rather than printing

### Documentation

- Document public APIs with rustdoc comments
- Include examples in documentation when helpful
- Keep documentation up-to-date with code changes

## Project Structure

- `src/` - Main source code
- `tests/` - Integration tests
- `docs/` - Documentation files
- `config.toml` - Optional configuration file

## Cursor Rules

None found.

## Copilot Instructions

None found.

