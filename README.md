# ipc-ring

A zero-copy fan-out ring buffer for interprocess communication.

The Rust implementation provides single-producer rings on Linux, macOS, and
Windows. Named rings support up to 64 independent readers, each receiving bytes
published after its attachment completes. The slowest active reader provides
backpressure; with no readers, the producer remains fully writable. One local
server routes clients to any number of rings by UTF-8 port name.

Endpoint constructors accept a native path. On Unix this is the exact
Unix-domain socket pathname; callers secure its parent directory and remove
stale or retired socket files. On Windows it is a complete local named-pipe
path such as `\\.\pipe\my-ring`.

```rust,no_run
use ipc_ring::{Server, ring::Consumer};

# async fn example() -> std::io::Result<()> {
let (server, task) = Server::bind("/tmp/my-service.sock")?;
let router = tokio::spawn(task);

let producer = server.register("telemetry", 64 * 1024)?;
let first = Consumer::connect("/tmp/my-service.sock", "telemetry").await?;
let second = Consumer::connect("/tmp/my-service.sock", "telemetry").await?;
# drop((producer, first, second));
router.abort();
# Ok(())
# }
```

Listener binding is synchronous. Consumer connection, routing, ring waits, and
grant completion are asynchronous and run entirely on the caller's Tokio
runtime. Immediate availability checks remain synchronous.

Each complete handshake has a one-second default timeout.
`ServerOptions::handshake_timeout` and
`ring::ConnectOptions::handshake_timeout` configure the server and consumer
independently.

See [SPEC.md](SPEC.md) for the memory layout and synchronization protocol.
