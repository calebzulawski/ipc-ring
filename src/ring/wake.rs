//! Transport-independent wake behavior used by the shared ring engine.

use std::io;

/// Producer-side wake behavior used by one monomorphic reader registry.
pub(crate) trait ProducerWake: Send + Sync + 'static {
    fn notify_data(&self) -> io::Result<()>;
    async fn wait_for_space(&self) -> io::Result<()>;
    /// Invalidates this reader even if registry snapshots still retain it.
    fn close(&self);
}

/// Consumer-side wake behavior used by the shared view implementation.
pub(crate) trait ConsumerWake {
    async fn wait_for_data(&mut self) -> io::Result<()>;
    fn notify_space(&self) -> io::Result<()>;
}
