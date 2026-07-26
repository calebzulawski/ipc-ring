# IPC Ring ABI and routing protocol v1

This specifies a platform-specific shared-memory SPSC byte ring, its alert
protocol, and the local server that routes clients to rings by port name.
Framing, resizing, and SPMC fan-out are outside this version.

## Shared layout

All fields are naturally aligned native-endian atomics on supported 64-bit
little-endian targets.

| Offset | Type | Field |
| ---: | --- | --- |
| 0 | atomic `u64` | ABI version |
| 8 | `u64` | payload capacity |
| 16 | atomic `u64` | write position |
| 24 | atomic `u64` | read position |
| 32 | atomic `u32` | data waiter state |
| 36 | atomic `u32` | space waiter state |

The 40-byte header uses `IDLE = 0` and `WAITING = 1` for each waiter. Version
zero means initialization is incomplete. The producer writes all fields before
release-storing version 1; a consumer acquire-loads and requires version 1
exactly.

The payload starts at the next native mapping-page boundary. The object length
is exactly `payload_offset + capacity`. Capacity is a nonzero power of two, no
greater than `2^63`, and a multiple of the platform mapping granularity.
Creation rounds a positive requested minimum up to the smallest valid
capacity.

Each participant maps the header once and the payload twice into adjacent
virtual ranges. Position `p` addresses:

```text
payload_alias_base + (p & (capacity - 1))
```

The producer alone writes `write_position`; the sole consumer alone writes
`read_position`. `write_position wrapping_sub read_position` must not exceed
capacity. Payload writes precede a release-store of the write cursor, and the
consumer acquire-loads it before reading. Reads finish before a release-store
of the read cursor, and the producer acquire-loads it before reuse.

Grants have lengths from 1 through capacity. Partial commit or release advances
only that amount. Dropping a grant advances nothing.

## Server and ports

Binding returns a cloneable `Server` for ring registration and one move-only
future that owns the Tokio Unix listener or Windows named-pipe listener.
Callers explicitly spawn or await that future, so the listener cannot be run
twice. `Server` clones hold weak registry access and cannot run or stop the
listener.

A registration has a nonempty, case-sensitive UTF-8 port name of at most 255
bytes. Characters have no special meaning. The registry stores weak
registrations, so dropping a producer retires its port without explicit
deregistration. A second live registration of the same port returns
`AlreadyExists`.

The listener future accepts handshakes into a `JoinSet` with at most 64 in
flight. At that limit it pauses acceptance and relies on the listener backlog
for backpressure. Ending the future cancels incomplete handshakes, drops the
registry, and makes `Server::register` fail with `BrokenPipe`. Notification
streams already installed in rings are independent of the listener and remain
usable.

The setup path is opaque and native. Unix passes it directly to Tokio as a UDS
pathname. The library neither creates parent directories nor removes occupied,
stale, or retired socket files. Callers own directory security and pathname
lifecycle. Windows expects a complete local pipe path such as
`\\.\pipe\my-service`; remote clients are rejected.

## Routing and attachment

Every client request is:

```text
[protocol version: u8, port byte length: u8, UTF-8 port bytes...]
```

The server response is:

```text
[protocol version: u8, status: u8]
```

Version 1 statuses are `Ok`, `Busy`, `NotFound`, `Invalid`, and
`Incompatible`. Unknown statuses are malformed. `Busy` means the selected
SPSC ring's server-local consumer admission permit is already held.

For `Ok`, the server acquires the ring's sole consumer permit and transfers the
anonymous shared-memory mapping. Unix sends one private carrier byte with
exactly one `SCM_RIGHTS` descriptor and the receiver sets `CLOEXEC`. Windows
pins the pipe's kernel-reported client process, duplicates one mapping handle
into it, and sends that numeric handle value.

The client maps and validates the complete ABI before sending the eight-byte
`Ready` frame. There is no final acknowledgement. The server then moves the
accepted stream into the ring. The complete handshake has a one-second default
deadline. `ServerOptions` and `ConnectOptions` configure that deadline
independently for each peer. Failure or cancellation before the notification
stream is stored drops the owned permit and restores consumer admission.

Because Ready has no response, a client may begin a wait just before the
server stores its notification stream. Producer publications made
before routing finishes update their cursor while holding the notification
stream mutex. Stream insertion holds the same async mutex, clears the data
waiter atomic, writes one zero-valued wake byte unconditionally, and places the
stream and its admission permit in the slot. New publications therefore wait
for stream insertion instead of updating without a stream. Cancellation or
failure drops the not-yet-stored stream, its permit, and the mutex guard. The
connection byte is durable and harmless when no waiter or data exists because
every wait rechecks its cursor predicate.

## Notifications and stream ownership

Each endpoint owns separate consumer-data and producer-space notification
values. Anonymous pairs back them with independent Tokio wakes. Cross-process
values share one notification-stream slot containing the full-duplex stream
and no background task. A ring operation holds the slot's Tokio mutex
exclusively for one notification read or write and leaves the stream in the
guarded slot afterward.

A waiter:

1. locks the notification stream;
2. exchanges its waiter atomic to WAITING with acquire-release ordering;
3. rechecks the cursor predicate;
4. reads the fixed zero-valued wake byte only if still blocked.

A completing grant:

1. locks the notification stream when its slot contains one;
2. publishes its cursor with release ordering;
3. exchanges the matching waiter atomic to IDLE with acquire-release ordering;
4. writes the fixed zero-valued wake byte only if it observed WAITING;
5. releases the stream lock.

The atomic exchanges have one modification order. Either completion observes
the armed waiter and writes a durable byte, or the waiter's recheck acquires
the preceding cursor publication. Extra zero bytes cause harmless cursor
rechecks; nonzero bytes are malformed. EOF, malformed bytes, and I/O failures
remove the notification stream from its slot and return `BrokenPipe`.

Completion awaits socket or pipe writability. If it is cancelled after cursor
publication, its locked-stream guard removes the stream, so the peer wakes
through EOF. All endpoint and alert I/O runs on the caller's Tokio runtime; the
library creates no runtime or background thread.

Before the first consumer, a producer may fill the ring without a stream. If
it then needs space, locking the notification stream waits for routing to place
one in the slot. Stream insertion's unconditional connection byte wakes any
consumer data waiter armed during that interval.

Liveness is intentionally operation-driven. Closing a consumer does not run a
producer-side monitor. Its admission permit remains held by the installed
stream until a producer operation encounters EOF or another stream error; that
operation returns `BrokenPipe`, removes the stream, and drops the permit so a
later replacement can connect. Existing cursors and buffered bytes are never
reset. A consumer may drain already published bytes after producer loss and
observes `BrokenPipe` when it next needs an alert.

Anonymous pairs use the same waiter atomics and async public methods but create
no stream. One Tokio notification per direction retains a wake that arrives
after arming but before the operation begins awaiting it. A stale retained wake
may cause a harmless cursor recheck.
