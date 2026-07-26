# ipc-ring

A zero-copy magic ring buffer for interprocess communication.

The initial Rust implementation provides SPSC rings on Linux, macOS, and
Windows. One local server routes clients to any number of rings by UTF-8 port
name. Each ring uses anonymous shared memory and retains its notification
stream for data and space alerts.

Endpoint constructors accept a native path. On Unix this is the exact
Unix-domain socket pathname; callers secure its parent directory and remove
stale or retired socket files. On Windows it is a complete local named-pipe
path such as `\\.\pipe\my-ring`.

```rust,no_run
use ipc_ring::{Server, spsc::Consumer};

# async fn example() -> std::io::Result<()> {
let (server, task) = Server::bind("/tmp/my-service.sock")?;
let router = tokio::spawn(task);

let producer = server.register("telemetry").spsc(64 * 1024)?;
let consumer = Consumer::connect("/tmp/my-service.sock", "telemetry").await?;
# drop((producer, consumer));
router.abort();
# Ok(())
# }
```

Listener binding is synchronous. Consumer connection, routing, ring waits, and
grant completion are asynchronous and run entirely on the caller's Tokio
runtime. Immediate availability checks remain synchronous.

Each complete handshake has a one-second default timeout.
`ServerOptions::handshake_timeout` and
`spsc::ConnectOptions::handshake_timeout` configure the server and consumer
independently.

See [SPEC.md](SPEC.md) for the memory layout and synchronization protocol.
