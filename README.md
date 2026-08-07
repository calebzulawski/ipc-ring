# ipc-ring

A zero-copy fan-out ring buffer for interprocess communication.

The Rust implementation provides single-producer rings on Linux, macOS, and
Windows. Server-backed and process-local rings support up to 64 independent
readers. New server-backed readers receive bytes published after attachment;
cloned local readers inherit the source reader's current cursor. The slowest
active reader provides backpressure. A server-backed producer remains writable
with no readers, while a local producer reports `BrokenPipe` after its final
reader disappears. One local server routes clients to any number of rings by
UTF-8 port name.

IPC connection functions accept a native path. On Unix this is the exact
Unix-domain socket pathname; callers secure its parent directory and remove
stale or retired socket files. On Windows it is a complete local named-pipe
path such as `\\.\pipe\my-ring`.

```rust,no_run
use ipc_ring::ipc::{ConnectOptions, Server};

# async fn example() -> std::io::Result<()> {
let (server, task) = Server::bind("/tmp/my-service.sock")?;
let router = tokio::spawn(task);

let mut producer = server.register("telemetry", 64 * 1024)?;
let mut consumer = ConnectOptions::new()
    .connect("/tmp/my-service.sock", "telemetry")
    .await?;

producer.reserve(4).await?;
producer.view_mut()[..4].copy_from_slice(b"ping");
producer.advance(4)?;

consumer.reserve(4).await?;
assert_eq!(&consumer.view()[..4], b"ping");
consumer.advance(4)?;
# drop((producer, consumer));
router.abort();
# Ok(())
# }
```

For an in-process ring, `local::create(capacity)` returns safe views over a
`local::Producer` and `local::Consumer`. Additional local readers can be
created with `try_clone` on the consumer view:

```rust
use ipc_ring::local;

# fn example() -> std::io::Result<()> {
let (mut producer, mut consumer) = local::create(1024)?;
let mut second_consumer = consumer.try_clone()?;

producer.try_reserve(4)?;
producer.view_mut()[..4].copy_from_slice(b"ping");
producer.advance(4)?;

consumer.try_reserve(4)?;
second_consumer.try_reserve(4)?;
# Ok(())
# }
```

Listener binding is synchronous. Consumer connection, routing, and ring waits
run entirely on the caller's Tokio runtime. Immediate reservations and cursor
advancement remain synchronous. The local and IPC producer and consumer types implement
`raw::Cursor`; constructors wrap them in `view::View`, which retains one safe
reservation. Producers additionally implement `raw::CursorMut`, enabling
in-place modification through `view_mut`. Reservations request a minimum
length and expose the full availability snapshot observed by their successful
check.

Each complete handshake has a one-second default timeout.
`ipc::ServerOptions::handshake_timeout` and
`ipc::ConnectOptions::handshake_timeout` configure the server and consumer
independently.

See [SPEC.md](SPEC.md) for the memory layout and synchronization protocol.
