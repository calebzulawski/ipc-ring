//! Ring buffers for inter-process communication.
//!
//! The write side binds and runs a server on a local socket or named pipe, then
//! registers a ring buffer under a named port. Readers connect to the server's
//! address and request that port.
//!
//! # Write side
//!
//! ```rust,no_run
//! use ipc_ring::ipc::Server;
//!
//! # async fn write_side() -> std::io::Result<()> {
//! let (server, server_task) = Server::bind("/tmp/example.sock")?;
//! tokio::spawn(server_task);
//! let _producer = server.register("events", 64 * 1024)?;
//! # Ok(())
//! # }
//! ```
//!
//! # Read side
//!
//! ```rust,no_run
//! use ipc_ring::ipc::ConnectOptions;
//!
//! # async fn read_side() -> std::io::Result<()> {
//! let _consumer = ConnectOptions::new()
//!     .connect("/tmp/example.sock", "events")
//!     .await?;
//! # Ok(())
//! # }
//! ```

mod consumer;
mod handshake;
mod notification;
mod producer;
mod reader;
mod registered;
mod server;
mod socket;

pub use consumer::{ConnectOptions, Consumer};
pub use producer::Producer;
pub use server::{Server, ServerOptions};
