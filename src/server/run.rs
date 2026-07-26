use super::{Server, ServerRegistry};
use crate::local_socket::{self, Listener};
use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

const MAX_CONCURRENT_HANDSHAKES: usize = 64;

/// Configures listener-wide policy applied to each accepted handshake.
#[derive(Clone, Copy, Debug)]
pub struct ServerOptions {
    handshake_timeout: Duration,
}

impl ServerOptions {
    pub const fn new() -> Self {
        Self {
            handshake_timeout: crate::handshake::DEFAULT_TIMEOUT,
        }
    }

    /// Limits each accepted client's complete handshake without affecting established rings.
    pub const fn handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Binds the endpoint and returns registration access plus its listener task.
    pub fn bind(
        self,
        path: impl AsRef<Path>,
    ) -> io::Result<(
        Server,
        impl Future<Output = io::Result<()>> + Send + 'static,
    )> {
        let listener = local_socket::bind(path.as_ref())?;
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
    /// Returns registration access and the unique listener future for this path.
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
            let stream = local_socket::accept(&mut listener).await?;
            let registry = Arc::downgrade(&registry);
            incomplete_handshakes.spawn(async move {
                let _ = crate::handshake::route(stream, registry, handshake_timeout).await;
            });
        } else {
            tokio::select! {
                accepted = local_socket::accept(&mut listener) => {
                    let stream = accepted?;
                    let registry = Arc::downgrade(&registry);
                    incomplete_handshakes.spawn(async move {
                        let _ = crate::handshake::route(stream, registry, handshake_timeout).await;
                    });
                }
                _ = incomplete_handshakes.join_next() => {}
            }
        }
    }
}
