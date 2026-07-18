# IPC Ring ABI v1

This specifies a platform-specific shared-memory SPSC byte ring. The Rust
binding includes named discovery, but liveness, framing, crash recovery,
resizing, and SPMC fan-out are outside this protocol.

## Layout

All fields are naturally aligned native-endian atomics on supported 64-bit
little-endian targets.

| Offset | Type | Field |
| ---: | --- | --- |
| 0 | atomic `u64` | ABI version |
| 8 | `u64` | payload capacity |
| 16 | atomic `u64` | write position |
| 24 | atomic `u64` | read position |
| 32 | atomic `u64` | consumer claim |

The consumer claim values are `FREE = 0` and `CLAIMED = 1`; every other
value is corrupt.

Linux appends atomic `u32 data_wait_state` and `u32 space_wait_state` at
offsets 40 and 44 (48 bytes total). macOS appends atomic `u64` states at
offsets 40 and 48 (56 bytes). Windows uses the 40-byte common header and two
auto-reset events associated with the named mapping.

Version zero means initialization is in progress. The initializer writes all
other fields and release-stores version 1 last; connectors acquire-load and
require version 1 exactly. A named producer initializes the consumer claim to
FREE. Anonymous pair creation initializes it to CLAIMED.

The payload starts at the next native mapping-page boundary. The shared-memory
object has length exactly `payload_offset + capacity`. Capacity is a power of two, at most
`2^63`, and a multiple of platform mapping granularity.

Creation APIs accept a positive minimum capacity. They round it up to the
smallest capacity satisfying these requirements and store that actual capacity
in the header.

## Endpoint ownership

Named-object create-new semantics and the non-cloneable Rust producer type
provide the sole producer. A connecting consumer performs an acquire-release
compare-exchange from FREE to CLAIMED before returning an endpoint. If it
observes CLAIMED, connection fails. Dropping a consumer release-stores FREE
after any borrowed read grant has ended.

A process crash does not clear shared atomic memory. A crashed consumer
therefore leaves CLAIMED behind and prevents replacement until an external
control plane establishes a recovery policy. The claim is endpoint ownership,
not a liveness indication.

## Mapping and cursors

Each participant maps the header once and payload twice into adjacent virtual
ranges. Position `p` is at:

```text
payload_alias_base + (p & (capacity - 1))
```

The producer alone writes `write_position`; the claimed consumer alone writes
`read_position`. `used = write_position wrapping_sub read_position` must
not exceed capacity. Payload publication precedes a release-store of write
position; the consumer acquire-loads it before reading. Reads finish before a
release-store of read position; the producer acquire-loads it before reuse.

Grants have lengths from 1 through capacity. Partial commit/release advances
only that portion; dropping a grant advances nothing.

## Wait handshake

Linux/macOS states are `IDLE = 0`, `WAITING = 1`, with one waiter per state.
A waiter checks its cursor predicate, acquire-release exchanges the state to
WAITING, rechecks, then asks the OS to wait only while WAITING. After any return
it exchanges IDLE and repeats. A publisher release-stores its cursor,
acquire-release exchanges the state to IDLE, and wakes one if it observed
WAITING. The exchange happens after every positive cursor advancement.

Linux binds this to shared `FUTEX_WAIT`/`FUTEX_WAKE`. macOS 14.4+ binds it
to 8-byte `os_sync_wait_on_address` with shared flags. Windows signals the
corresponding auto-reset event after cursor publication and loops on predicates.

## Named binding lifetime

Linux and macOS use a create-new POSIX shared-memory name owned by the producer.
The producer unlinks that name when dropped; already mapped consumers remain
valid until they unmap. Windows named mappings and events remain alive until
their final handles close. Producer crashes can leave stale POSIX names, and
consumer crashes can leave stale claims.

