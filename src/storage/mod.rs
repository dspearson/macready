use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

use crate::collector::MetricPoint;
use crate::entity::Entity;
use crate::error::Result;

/// Generic trait for storage backends
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    /// The type of metrics this storage handles
    type MetricType: MetricPoint;

    /// The type of entities this storage handles
    type EntityType: Entity;

    /// Store a single metric
    async fn store_metric(&self, metric: &Self::MetricType) -> Result<()>;

    /// Store multiple metrics in a batch
    async fn store_metrics(&self, metrics: &[Self::MetricType]) -> Result<usize>;

    /// Register a new entity
    async fn register_entity(&self, entity: &Self::EntityType) -> Result<()>;

    /// Update an existing entity
    async fn update_entity(&self, entity: &Self::EntityType) -> Result<()>;

    /// Get an entity by ID
    async fn get_entity(&self, id: &<Self::EntityType as Entity>::Id) -> Result<Self::EntityType>;

    /// Get an entity by name
    async fn get_entity_by_name(&self, name: &str) -> Result<Self::EntityType>;

    /// Check if an entity exists
    async fn entity_exists(&self, id: &<Self::EntityType as Entity>::Id) -> Result<bool>;

    /// Check the connection health
    async fn health_check(&self) -> Result<bool>;

    /// Get a name for this storage backend
    fn name(&self) -> &str;
}

/// PostgreSQL storage implementation
mod postgres_impl;
pub use postgres_impl::{PostgresStorage, PostgresStorageConfig, PostgresStorageConfigBuilder};

/// In-memory storage for testing
pub mod memory;
