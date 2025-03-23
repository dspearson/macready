//! A core library for building metric-collecting agents

pub mod buffer;
pub mod config;
pub mod connection;
pub mod error;
pub mod process;
pub mod retry;
pub mod entity;
pub mod collector;
pub mod storage;
pub mod util;

/// Re-export of commonly used types for convenience
pub mod prelude {
    pub use crate::entity::{Entity, EntityRegistry};
    pub use crate::collector::{Collector, MetricPoint, MetricBatch};
    pub use crate::storage::Storage;
    pub use crate::storage::postgres_impl::{PostgresStorage, PostgresStorageConfig, PostgresStorageConfigBuilder};
    pub use crate::error::{AgentError, Result};
    pub use crate::buffer::{MetricsBuffer, BufferConfig};
    pub use crate::retry::{execute_with_retry, RetryConfig, RetryBuilder};
    pub use crate::connection::health::{HealthCheck, HealthStatus, HealthCheckConfig};
    pub use crate::connection::postgres::{PgConfig, PgConfigBuilder, SslMode};
    pub use crate::process::{Command, CommandBuilder, ProcessSource, ProcessConfig, spawn_process};
    pub use crate::process::stream::{StreamParser, LineStream, RecordStream};
    pub use crate::process::stream::DelimitedTextParser;
}

/// Library version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
