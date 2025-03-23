//! A core library for building metric-collecting agents

pub mod buffer;
pub mod config;
pub mod connection;
pub mod error;
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
    pub use crate::error::{AgentError, Result};
    pub use crate::buffer::MetricsBuffer;
    pub use crate::retry::{execute_with_retry, RetryConfig};
    pub use crate::connection::health::HealthCheck;
}

/// Library version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
