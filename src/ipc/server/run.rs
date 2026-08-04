use super::{Server, ServerRegistry};
use crate::ipc::handshake;
use crate::ipc::socket::{self, Listener};
use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

const MAX_CONCURRENT_HANDSHAKES: usize = 64;

/// Options for running an IPC server.
#[derive(Clone, Copy, Debug)]
pub struct ServerOptions {
    handshake_timeout: Duration,
}

impl ServerOptions {
    /// Creates server options with default settings.
    pub const fn new() -> Self {
        Self {
            handshake_timeout: handshake::DEFAULT_TIMEOUT,
        }
    }

    /// Sets how long a reader may take to connect.
    pub const fn handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Binds a server to `path` and returns the server and the future that runs it.
    pub fn bind(
        self,
        path: impl AsRef<Path>,
    ) -> io::Result<(
        Server,
        impl Future<Output = io::Result<()>> + Send + 'static,
    )> {
        let listener = socket::bind(path.as_ref())?;
        let registry = Arc::new(ServerRegistry::new());
        let server = Server {
            registry: Arc::downgrade(&registry),
        };
        let task = run_listener(listener, registry, self.handshake_timeout);
        Ok((server, task))
    }
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    /// Binds a server with default options and returns the server and the future that runs it.
    pub fn bind(
        path: impl AsRef<Path>,
    ) -> io::Result<(Self, impl Future<Output = io::Result<()>> + Send + 'static)> {
        ServerOptions::new().bind(path)
    }
}

async fn run_listener(
    mut listener: Listener,
    registry: Arc<ServerRegistry>,
    handshake_timeout: Duration,
) -> io::Result<()> {
    let mut incomplete_handshakes = JoinSet::new();

    loop {
        while incomplete_handshakes.len() >= MAX_CONCURRENT_HANDSHAKES {
            let _ = incomplete_handshakes.join_next().await;
        }

        if incomplete_handshakes.is_empty() {
            let stream = socket::accept(&mut listener).await?;
            let registry = Arc::downgrade(&registry);
            incomplete_handshakes.spawn(async move {
                let _ = handshake::route(stream, registry, handshake_timeout).await;
            });
        } else {
            tokio::select! {
                accepted = socket::accept(&mut listener) => {
                    let stream = accepted?;
                    let registry = Arc::downgrade(&registry);
                    incomplete_handshakes.spawn(async move {
                        let _ = handshake::route(stream, registry, handshake_timeout).await;
                    });
                }
                _ = incomplete_handshakes.join_next() => {}
            }
        }
    }
}
