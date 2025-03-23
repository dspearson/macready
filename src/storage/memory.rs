use async_trait::async_trait;
use log::{debug, info, trace, warn};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::collector::MetricPoint;
use crate::entity::Entity;
use crate::error::{AgentError, Result};
use crate::storage::Storage;

/// In-memory storage for testing
pub struct MemoryStorage<M, E>
where
    M: MetricPoint<EntityType = E>,
    E: Entity,
{
    /// Name of the storage
    name: String,

    /// Entities stored by ID
    entities: RwLock<HashMap<<E as Entity>::Id, E>>,

    /// Entities indexed by name
    entity_names: RwLock<HashMap<String, <E as Entity>::Id>>,

    /// Metrics stored by ID
    metrics: RwLock<Vec<M>>,

    /// Health status
    healthy: RwLock<bool>,
}

impl<M, E> MemoryStorage<M, E>
where
    M: MetricPoint<EntityType = E>,
    E: Entity,
{
    /// Create a new in-memory storage
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            entities: RwLock::new(HashMap::new()),
            entity_names: RwLock::new(HashMap::new()),
            metrics: RwLock::new(Vec::new()),
            healthy: RwLock::new(true),
        }
    }

    /// Set the health status
    pub fn set_healthy(&self, healthy: bool) {
        if let Ok(mut lock) = self.healthy.write() {
            *lock = healthy;
        }
    }

    /// Get all stored metrics
    pub fn get_metrics(&self) -> Result<Vec<M>> {
        match self.metrics.read() {
            Ok(metrics) => Ok(metrics.clone()),
            Err(_) => Err(AgentError::Other("Failed to read metrics".to_string())),
        }
    }

    /// Clear all stored metrics
    pub fn clear_metrics(&self) -> Result<()> {
        match self.metrics.write() {
            Ok(mut metrics) => {
                metrics.clear();
                Ok(())
            },
            Err(_) => Err(AgentError::Other("Failed to clear metrics".to_string())),
        }
    }

    /// Get all stored entities
    pub fn get_entities(&self) -> Result<Vec<E>> {
        match self.entities.read() {
            Ok(entities) => Ok(entities.values().cloned().collect()),
            Err(_) => Err(AgentError::Other("Failed to read entities".to_string())),
        }
    }

    /// Clear all stored entities
    pub fn clear_entities(&self) -> Result<()> {
        match self.entities.write() {
            Ok(mut entities) => {
                entities.clear();
                if let Ok(mut names) = self.entity_names.write() {
                    names.clear();
                }
                Ok(())
            },
            Err(_) => Err(AgentError::Other("Failed to clear entities".to_string())),
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
        match self.metrics.write() {
            Ok(mut metrics) => {
                metrics.push(metric.clone());
                Ok(())
            },
            Err(_) => Err(AgentError::Other("Failed to store metric".to_string())),
        }
    }

    async fn store_metrics(&self, metrics: &[Self::MetricType]) -> Result<usize> {
        match self.metrics.write() {
            Ok(mut stored_metrics) => {
                stored_metrics.extend_from_slice(metrics);
                Ok(metrics.len())
            },
            Err(_) => Err(AgentError::Other("Failed to store metrics".to_string())),
        }
    }

    async fn register_entity(&self, entity: &Self::EntityType) -> Result<()> {
        match (self.entities.write(), self.entity_names.write()) {
            (Ok(mut entities), Ok(mut names)) => {
                let id = entity.id().clone();
                let name = entity.name().to_string();

                entities.insert(id.clone(), entity.clone());
                names.insert(name, id);

                Ok(())
            },
            _ => Err(AgentError::Other("Failed to register entity".to_string())),
        }
    }

    async fn update_entity(&self, entity: &Self::EntityType) -> Result<()> {
        // For an in-memory implementation, update is the same as register
        self.register_entity(entity).await
    }

    async fn get_entity(&self, id: &<Self::EntityType as Entity>::Id) -> Result<Self::EntityType> {
        match self.entities.read() {
            Ok(entities) => {
                entities.get(id)
                    .cloned()
                    .ok_or_else(|| AgentError::EntityNotFound(format!("Entity with ID {:?} not found", id)))
            },
            Err(_) => Err(AgentError::Other("Failed to read entities".to_string())),
        }
    }

    async fn get_entity_by_name(&self, name: &str) -> Result<Self::EntityType> {
        match (self.entity_names.read(), self.entities.read()) {
            (Ok(names), Ok(entities)) => {
                let id = names.get(name)
                    .ok_or_else(|| AgentError::EntityNotFound(format!("Entity with name '{}' not found", name)))?;

                entities.get(id)
                    .cloned()
                    .ok_or_else(|| AgentError::EntityNotFound(format!("Entity with ID {:?} not found", id)))
            },
            _ => Err(AgentError::Other("Failed to read entities".to_string())),
        }
    }

    async fn entity_exists(&self, id: &<Self::EntityType as Entity>::Id) -> Result<bool> {
        match self.entities.read() {
            Ok(entities) => Ok(entities.contains_key(id)),
            Err(_) => Err(AgentError::Other("Failed to read entities".to_string())),
        }
    }

    async fn health_check(&self) -> Result<bool> {
        match self.healthy.read() {
            Ok(healthy) => Ok(*healthy),
            Err(_) => Err(AgentError::Other("Failed to read health status".to_string())),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}
