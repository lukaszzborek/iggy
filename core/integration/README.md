# Integration Tests - Dual Mode (Sync/Async)

This integration test crate supports both synchronous and asynchronous client testing modes.

## Architecture

### Async Mode (Default)
- **Server**: Embedded TestServer (runs in-process)
- **Protocols**: TCP, HTTP, QUIC
- **Features**: `--features async`
- **Use case**: Full integration testing with all protocols

### Sync Mode
- **Server**: ExternalServer (spawns `cargo run --bin iggy-server` as separate process)
- **Protocols**: TCP only
- **Features**: `--no-default-features --features sync`
- **Use case**: Testing sync SDK, debugging (server runs in separate process)

## Usage

### Running Async Tests
```bash
# Library tests
cargo test --features async --package integration --lib

# All tests (including server-specific)
cargo test --features async --package integration
```

### Running Sync Tests
```bash
# SDK tests only (client tests)
cargo test --no-default-features --features sync --package integration --test mod -- sdk::

# Note: Server will be automatically started by ExternalServer
# You can also start server manually: cargo run --bin iggy-server
```

## Debugging

### Sync Mode Advantages
- Server runs as separate process - can attach debugger
- Process isolation - easier to debug client/server interaction
- Simple `cargo run` - standard Rust debugging workflow

### Async Mode Advantages
- Faster test execution (no process spawn)
- All protocols supported (HTTP, QUIC, TCP)
- Full server control from test code

## Dependencies

### Async Mode Dependencies
- `server` - Full server implementation
- `iggy_common` - With `async` and `tokio_lock` features
- All protocol clients (HTTP, QUIC, TCP)

### Sync Mode Dependencies
- No `server` dependency (runs external process)
- `iggy` - With `sync` feature
- TCP client only

## Notes

- Tests using TestServer-specific APIs are only available in async mode
- Sync tests should use ExternalServer or connect to manually started server
- ExternalServer automatically cleans up server process on Drop
