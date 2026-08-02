//! Routes connections from one local listener to registered shared rings.

mod registry;
mod run;

pub use registry::Server;
pub use run::ServerOptions;

pub(crate) use registry::ServerRegistry;
