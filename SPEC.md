# IPC Ring ABI and routing protocol v1

This specifies a platform-specific shared-memory, single-producer byte ring,
how one producer wakes multiple readers, and the local server that routes
clients to rings by port name. Framing and resizing are outside this version.

## Shared layout

All fields are naturally aligned native-endian atomics on supported 64-bit
little-endian targets.

| Offset | Type | Field |
| ---: | --- | --- |
| 0 | atomic `u64` | ABI version |
| 8 | `u64` | payload capacity |
| 16 | atomic `u64` | write position |
| 24 | atomic `u64` | data-waiter bitmap |
| 32 | atomic `u64` | space-waiter bitmap |
| 40 | 64 atomic `u64` values | reader positions |

The 552-byte header uses one bitmap bit per reader slot. Version zero means
initialization is incomplete. The producer initializes the object before
release-storing version 1; consumers acquire-load and require version 1.

The payload starts at the next native mapping-page boundary. Capacity is a
nonzero power of two, no greater than `2^63`, and a multiple of the platform
mapping granularity. A requested minimum, including zero, is rounded up to the
smallest valid capacity. An attached shared-memory object may contain unused
trailing storage but must cover the declared header and payload.

Each participant maps the header once and the payload twice into adjacent
virtual ranges. Position `p` addresses:

```text
payload_alias_base + (p & (capacity - 1))
```

The producer alone advances `write_position`. Active reader slot `i` alone
advances `read_positions[i]`. Every active reader must satisfy:

```text
write_position wrapping_sub read_positions[i] <= capacity
```

The producer writes payload bytes before release-storing the write cursor.
Readers acquire-load that cursor before accessing the payload, finish reading
before release-storing their cursor, and the producer acquire-loads reader
cursors before reuse. Grants may have lengths from zero through capacity.
Partial commit or release advances only the supplied amount.

## Registration and attachment

Binding returns a cloneable `Server` and one move-only listener future.
`Server::register(port, minimum_capacity)` creates and publishes a ring under a
nonempty, case-sensitive UTF-8 port of at most 255 bytes. Dropping its producer
retires the port.

Each request is:

```text
[protocol version: u8, port byte length: u8, UTF-8 port bytes...]
```

Every response begins:

```text
[protocol version: u8, status: u8]
```

Version 1 statuses are `Ok`, `Busy`, `NotFound`, `Invalid`, and
`Incompatible`. `Busy` means all 64 reader slots are reserved. `Ok` is followed
by the assigned reader slot:

```text
[reader slot: u8]
```

The server reserves a slot before transferring the anonymous mapping. Unix
transfers one `SCM_RIGHTS` descriptor; Windows duplicates one mapping handle
into the pipe client's pinned process. The client validates the mapping and
sends the eight-byte `Ready` frame.

After `Ready`, the server initializes the slot from the current write cursor,
makes the reader active, and sends the one-byte zero-valued attachment
acknowledgement. Without another suspension point, it then writes one
unconditional wake and publishes the stream. That wake reconciles any
notification requested between activation and stream installation; an
unnecessary wake only causes a harmless state check. The client does not access
the ring or return a `Consumer` until it receives the acknowledgement.

The server process owns one atomic 64-bit reservation bitmap; it is admission
state and is not stored in the shared mapping. Each reservation is owned by its
connection and is released automatically on handshake failure or cancellation.
An incomplete handshake does not participate in ring backpressure. Completed
membership is published as one immutable bitmap-and-connection snapshot and a
reader remains active until its wakeup stream is found disconnected.

Each newly attached reader starts at the write cursor observed during final
attachment and therefore receives future bytes only. It does not replay bytes
published before that point. With no active readers, a named producer always
has one full capacity available and may overwrite bytes that nobody observed.
Stopping the listener prevents registration and attachment but does not disable
existing named producers.

The complete handshake has a one-second default deadline configurable through
`ServerOptions` and `ConnectOptions`.

## Backpressure and notifications

Each active IPC reader has one full-duplex socket or pipe used only for
wakeups. A wakeup is one zero byte: it carries no payload and tells the receiver
to recheck shared positions. Anonymous rings use the same cursor and bitmap
protocol with one reader slot and two process-local Tokio notifications.

The producer's writable length is the capacity minus the greatest buffered
distance among active readers. A reservation succeeds only when every active
reader leaves enough space, so the slowest active reader provides backpressure.
When no named readers are active, the writable length is the full capacity.

A reader waiting for data:

1. checks whether enough data is readable;
2. sets its bit in `data_waiters`;
3. rechecks the readable byte count;
4. reads its stream only if still blocked.

After publishing, the producer takes the waiter bitmap, loads the current
immutable connection snapshot, and wakes the active connections represented by
the captured bits. Loading membership after taking the bits prevents slot reuse
from routing a new reader's request through an old connection. A nonblocking
wake write that reports `WouldBlock` is safe because a wake byte is already
queued. Every wake causes the receiver to recheck shared cursors.

There is one producer and therefore at most one set bit in `space_waiters`.
The producer may wait on any reader that independently blocks its requested
length. It sets that reader's bit, rechecks the request and connection, and
reads that reader's stream only if still blocked. The reader release-stores its
cursor, clears its own bit, and writes a wake byte only if the bit was set.

An extra wake byte causes a harmless state check. Nonzero bytes are malformed.
EOF and I/O failure remove that reader; healthy readers continue. Anonymous
endpoint loss returns `BrokenPipe` because no replacement can attach. All
waits and wakeup I/O run on the caller's Tokio runtime. The library creates no
runtime, background thread, or persistent monitoring task, so disconnect
detection is operation-driven.
