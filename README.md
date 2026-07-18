# ipc-ring

A zero-copy magic ring buffer for interprocess communication.

The initial Rust implementation provides SPSC endpoints on Linux, macOS, and Windows. Fan-out and control-plane features are future work.

See [SPEC.md](SPEC.md) for the memory layout and synchronization protocol.
