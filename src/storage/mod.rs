use async_trait::async_trait;
use std::time::Duration;

use crate::error::Result;

/// PostgreSQL storage implementation
pub mod postgres_impl;
pub use postgres_impl::{PostgresStorage, PostgresStorageExt, PostgresBatchExt};

/// In-memory storage for testing
pub mod memory;
