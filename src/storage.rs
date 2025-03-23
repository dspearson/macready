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

/// Module for PostgreSQL storage implementation
pub mod postgres {
    use super::*;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use log::{debug, error, info, warn};
    use std::collections::HashMap;
    use std::marker::PhantomData;
    use std::sync::Arc;
    use tokio_postgres::{Client, NoTls};
    use uuid::Uuid;

    use crate::connection::postgres::{establish_connection, PgConfig};
    use crate::retry::{execute_with_retry, RetryConfig};

    /// Configuration for PostgreSQL storage
    pub struct PostgresConfig {
        /// Database connection configuration
        pub connection: PgConfig,

        /// Retry configuration for database operations
        pub retry: RetryConfig,

        /// Table name for storing metrics
        pub metrics_table: String,

        /// Table name for storing entities
        pub entities_table: String,
    }

    impl Default for PostgresConfig {
        fn default() -> Self {
            Self {
                connection: PgConfig::default(),
                retry: RetryConfig::default(),
                metrics_table: "metrics".to_string(),
                entities_table: "entities".to_string(),
            }
        }
    }

    /// PostgreSQL storage implementation
    pub struct PostgresStorage<M, E>
    where
        M: MetricPoint<EntityType = E>,
        E: Entity,
    {
        /// Client connection
        client: Arc<Client>,

        /// Configuration
        config: PostgresConfig,

        /// Storage name
        name: String,

        /// Phantom data for generics
        _metric_type: PhantomData<M>,
        _entity_type: PhantomData<E>,
    }

    impl<M, E> PostgresStorage<M, E>
    where
        M: MetricPoint<EntityType = E>,
        E: Entity,
    {
        /// Create a new PostgreSQL storage
        pub async fn new(config: PostgresConfig, name: impl Into<String>) -> Result<Self> {
            let client = establish_connection(&config.connection).await?;

            Ok(Self {
                client,
                config,
                name: name.into(),
                _metric_type: PhantomData,
                _entity_type: PhantomData,
            })
        }

        /// Get the underlying client
        pub fn client(&self) -> Arc<Client> {
            Arc::clone(&self.client)
        }
    }

    #[async_trait]
    impl<M, E> Storage for PostgresStorage<M, E>
    where
        M: MetricPoint<EntityType = E>,
        E: Entity,
    {
        type MetricType = M;
        type EntityType = E;

        async fn store_metric(&self, metric: &Self::MetricType) -> Result<()> {
            // This is a simplified implementation - in practice, you would need to
            // build proper SQL statements based on your schema and metric type

            execute_with_retry(
                || {
                    let client = Arc::clone(&self.client);
                    let metric = metric.clone();
                    let table = self.config.metrics_table.clone();

                    Box::pin(async move {
                        // In a real implementation, construct parameterized SQL based on metric type
                        let sql = format!("INSERT INTO {} (...) VALUES (...)", table);

                        // Execute the statement with parameters from the metric
                        // client.execute(&sql, &[...]).await?;

                        // For now, just simulate success
                        Ok(())
                    })
                },
                self.config.retry.clone(),
                "store_metric",
            )
            .await
        }

        async fn store_metrics(&self, metrics: &[Self::MetricType]) -> Result<usize> {
            if metrics.is_empty() {
                return Ok(0);
            }

            // In practice, you would use a more efficient batch insert here

            let mut count = 0;
            for metric in metrics {
                if let Ok(()) = self.store_metric(metric).await {
                    count += 1;
                }
            }

            Ok(count)
        }

        async fn register_entity(&self, entity: &Self::EntityType) -> Result<()> {
            // Simplified implementation

            execute_with_retry(
                || {
                    let client = Arc::clone(&self.client);
                    let entity_json = entity.to_json();
                    let table = self.config.entities_table.clone();

                    Box::pin(async move {
                        // In a real implementation, construct parameterized SQL based on entity type
                        let sql = format!("INSERT INTO {} (...) VALUES (...)", table);

                        // Execute the statement with parameters from the entity
                        // client.execute(&sql, &[...]).await?;

                        // For now, just simulate success
                        Ok(())
                    })
                },
                self.config.retry.clone(),
                "register_entity",
            )
            .await
        }

        async fn update_entity(&self, entity: &Self::EntityType) -> Result<()> {
            // Simplified implementation

            execute_with_retry(
                || {
                    let client = Arc::clone(&self.client);
                    let entity_json = entity.to_json();
                    let table = self.config.entities_table.clone();

                    Box::pin(async move {
                        // In a real implementation, construct parameterized SQL based on entity type
                        let sql = format!("UPDATE {} SET ... WHERE id = ...", table);

                        // Execute the statement with parameters from the entity
                        // client.execute(&sql, &[...]).await?;

                        // For now, just simulate success
                        Ok(())
                    })
                },
                self.config.retry.clone(),
                "update_entity",
            )
            .await
        }

        async fn get_entity(&self, id: &<Self::EntityType as Entity>::Id) -> Result<Self::EntityType> {
            // Simplified implementation - would need actual entity construction from query results
            Err(crate::error::AgentError::Other("Not implemented".to_string()))
        }

        async fn get_entity_by_name(&self, name: &str) -> Result<Self::EntityType> {
            // Simplified implementation - would need actual entity construction from query results
            Err(crate::error::AgentError::Other("Not implemented".to_string()))
        }

        async fn entity_exists(&self, id: &<Self::EntityType as Entity>::Id) -> Result<bool> {
            // Simplified implementation

            execute_with_retry(
                || {
                    let client = Arc::clone(&self.client);
                    let id_clone = id.clone(); // Clone ID for the closure
                    let table = self.config.entities_table.clone();

                    Box::pin(async move {
                        // In a real implementation, construct parameterized SQL
                        let sql = format!("SELECT 1 FROM {} WHERE id = $1 LIMIT 1", table);

                        // Execute the query and check if a row is returned
                        // let row = client.query_opt(&sql, &[&id_clone]).await?;
                        // Ok(row.is_some())

                        // For now, just simulate a result
                        Ok(false)
                    })
                },
                self.config.retry.clone(),
                "entity_exists",
            )
            .await
        }

        async fn health_check(&self) -> Result<bool> {
            execute_with_retry(
                || {
                    let client = Arc::clone(&self.client);

                    Box::pin(async move {
                        // Simple query to check connection health
                        let row = client.query_one("SELECT 1", &[]).await?;
                        let value: i32 = row.get(0);
                        Ok(value == 1)
                    })
                },
                self.config.retry.clone(),
                "health_check",
            )
            .await
        }

        fn name(&self) -> &str {
            &self.name
        }
    }
}

/// In-memory storage implementation for testing
pub mod memory {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    /// In-memory storage for metrics and entities
    pub struct MemoryStorage<M, E>
    where
        M: MetricPoint<EntityType = E>,
        E: Entity,
    {
        /// Stored metrics
        metrics: RwLock<Vec<M>>,

        /// Stored entities
        entities: RwLock<HashMap<<E as Entity>::Id, E>>,

        /// Entity name to ID mapping
        entity_names: RwLock<HashMap<String, <E as Entity>::Id>>,

        /// Storage name
        name: String,
    }

    impl<M, E> MemoryStorage<M, E>
    where
        M: MetricPoint<EntityType = E>,
        E: Entity,
    {
        /// Create a new memory storage
        pub fn new(name: impl Into<String>) -> Self {
            Self {
                metrics: RwLock::new(Vec::new()),
                entities: RwLock::new(HashMap::new()),
                entity_names: RwLock::new(HashMap::new()),
                name: name.into(),
            }
        }
    }

    #[async_trait]
    impl<M, E> Storage for MemoryStorage<M, E>
    where
        M: MetricPoint<EntityType = E>,
        E: Entity,
    {
        type MetricType = M;
        type EntityType = E;

        async fn store_metric(&self, metric: &Self::MetricType) -> Result<()> {
            let mut metrics = self.metrics.write().map_err(|_| crate::error::AgentError::Other("Lock poisoned".to_string()))?;
            metrics.push(metric.clone());
            Ok(())
        }

        async fn store_metrics(&self, metrics: &[Self::MetricType]) -> Result<usize> {
            let mut storage = self.metrics.write().map_err(|_| crate::error::AgentError::Other("Lock poisoned".to_string()))?;
            storage.extend_from_slice(metrics);
            Ok(metrics.len())
        }

        async fn register_entity(&self, entity: &Self::EntityType) -> Result<()> {
            let id = entity.id().clone();
            let name = entity.name().to_string();

            let mut entities = self.entities.write().map_err(|_| crate::error::AgentError::Other("Lock poisoned".to_string()))?;
            let mut names = self.entity_names.write().map_err(|_| crate::error::AgentError::Other("Lock poisoned".to_string()))?;

            entities.insert(id.clone(), entity.clone());
            names.insert(name, id);

            Ok(())
        }

        async fn update_entity(&self, entity: &Self::EntityType) -> Result<()> {
            let id = entity.id().clone();
            let name = entity.name().to_string();

            let mut entities = self.entities.write().map_err(|_| crate::error::AgentError::Other("Lock poisoned".to_string()))?;
            let mut names = self.entity_names.write().map_err(|_| crate::error::AgentError::Other("Lock poisoned".to_string()))?;

            // Update the mapping if the name changed
            if let Some(existing) = entities.get(&id) {
                if existing.name() != name {
                    names.remove(existing.name());
                }
            }

            entities.insert(id.clone(), entity.clone());
            names.insert(name, id);

            Ok(())
        }

        async fn get_entity(&self, id: &<Self::EntityType as Entity>::Id) -> Result<Self::EntityType> {
            let entities = self.entities.read().map_err(|_| crate::error::AgentError::Other("Lock poisoned".to_string()))?;

            entities.get(id)
                .cloned()
                .ok_or_else(|| crate::error::AgentError::EntityNotFound(format!("Entity with ID {:?} not found", id)))
        }

        async fn get_entity_by_name(&self, name: &str) -> Result<Self::EntityType> {
            let names = self.entity_names.read().map_err(|_| crate::error::AgentError::Other("Lock poisoned".to_string()))?;
            let entities = self.entities.read().map_err(|_| crate::error::AgentError::Other("Lock poisoned".to_string()))?;

            let id = names.get(name)
                .ok_or_else(|| crate::error::AgentError::EntityNotFound(format!("Entity with name '{}' not found", name)))?;

            entities.get(id)
                .cloned()
                .ok_or_else(|| crate::error::AgentError::EntityNotFound(format!("Entity with ID {:?} not found", id)))
        }

        async fn entity_exists(&self, id: &<Self::EntityType as Entity>::Id) -> Result<bool> {
            let entities = self.entities.read().map_err(|_| crate::error::AgentError::Other("Lock poisoned".to_string()))?;
            Ok(entities.contains_key(id))
        }

        async fn health_check(&self) -> Result<bool> {
            // Memory storage is always healthy
            Ok(true)
        }

        fn name(&self) -> &str {
            &self.name
        }
    }
}
