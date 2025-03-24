
use log::debug;

#[cfg(feature = "buffer")]
pub mod buffer;
#[cfg(feature = "collector")]
pub mod collector;
pub mod config;
#[cfg(feature = "connection")]
pub mod connection;
pub mod entity;
pub mod error;
#[cfg(feature = "process")]
pub mod process;
pub mod retry;
#[cfg(feature = "storage")]
pub mod storage;
pub mod util;

// Re-export public types for external use
pub use self::config::{AgentConfig, DatabaseConfig, LogLevel, SslMode, StorageType};
pub use self::entity::Entity;

// Main feature modules
#[cfg(feature = "buffer")]
pub use buffer::MetricsBuffer;

#[cfg(feature = "collector")]
pub use collector::{Collector, MetricBatch, MetricPoint};

#[cfg(all(feature = "storage", feature = "postgres"))]
pub use storage::postgres_impl::{PostgresBatchExt, PostgresStorage, PostgresStorageExt};

#[cfg(all(feature = "storage", feature = "memory"))]
pub use storage::memory::MemoryStorage;

/// Reexport all error types and common interfaces
pub mod prelude {
    pub use crate::error::{AgentError, Result};
}

/// Initialize the logging system
pub fn init_logging(log_level: &config::LogLevel) {
    debug!("Initializing logging with level: {:?}", log_level);
    util::logging::init(log_level);
}
