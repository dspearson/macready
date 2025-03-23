use log::debug;

mod buffer;
mod collector;
mod config;
mod connection;
mod entity;
mod error;
mod process;
mod retry;
mod storage;
mod util;

// Re-export public types for external use
pub use self::config::{AgentConfig, DatabaseConfig, LogLevel, SslMode, StorageType};
pub use self::entity::Entity;

/// Reexport all error types and common interfaces
pub mod prelude {
    pub use crate::error::{AgentError, Result};
}

/// Initialize the logging system
pub fn init_logging(log_level: &config::LogLevel) {
    debug!("Initializing logging with level: {:?}", log_level);
    util::logging::init(log_level);
}
