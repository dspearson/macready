use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

use crate::error::{AgentError, Result};

/// Trait for identifiable entities that metrics can be collected for
#[async_trait]
pub trait Entity: Clone + Send + Sync + Debug + 'static {
    /// The type used as the entity ID (usually String or Uuid)
    type Id: Clone + Eq + Hash + Send + Sync + Debug + 'static;

    /// Get the entity's unique identifier
    fn id(&self) -> &Self::Id;

    /// Get the entity's name (used as reference in metric collection)
    fn name(&self) -> &str;

    /// Get the entity's type (e.g., "interface", "cpu", "zone")
    fn entity_type(&self) -> &str;

    /// Check if this entity is active/available
    fn is_active(&self) -> bool;

    /// Optional method to get the parent entity ID if applicable
    fn parent_id(&self) -> Option<&Self::Id> {
        None
    }

    /// Convert the entity to a JSON-compatible format
    fn to_json(&self) -> serde_json::Value;

    /// Get entity metadata as key-value pairs
    fn metadata(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}

/// Registry for tracking entities
pub struct EntityRegistry<E: Entity> {
    entities: RwLock<HashMap<E::Id, Arc<E>>>,
    name_index: RwLock<HashMap<String, E::Id>>,
}

impl<E: Entity> EntityRegistry<E> {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            entities: RwLock::new(HashMap::new()),
            name_index: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new entity
    pub fn register(&self, entity: E) -> Result<Arc<E>> {
        let id = entity.id().clone();
        let name = entity.name().to_string();
        let entity = Arc::new(entity);

        let mut entities = self
            .entities
            .write()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        let mut names = self
            .name_index
            .write()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;

        entities.insert(id.clone(), Arc::clone(&entity));
        names.insert(name, id);

        Ok(entity)
    }

    /// Update an existing entity
    pub fn update(&self, entity: E) -> Result<Arc<E>> {
        let id = entity.id().clone();
        let name = entity.name().to_string();
        let entity = Arc::new(entity);

        let mut entities = self
            .entities
            .write()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        let mut names = self
            .name_index
            .write()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;

        // Update the entity map
        entities.insert(id.clone(), Arc::clone(&entity));

        // Find and update name index if needed
        let mut old_name = None;

        // Collect entries to remove first
        for (existing_name, existing_id) in names.iter() {
            if existing_id == &id && existing_name != &name {
                old_name = Some(existing_name.clone());
                break;
            }
        }

        // Then remove as needed
        if let Some(name_to_remove) = old_name {
            names.remove(&name_to_remove);
        }

        // Add the new entry
        names.insert(name, id);

        Ok(entity)
    }

    /// Get an entity by ID
    pub fn get(&self, id: &E::Id) -> Result<Arc<E>> {
        let entities = self
            .entities
            .read()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        entities
            .get(id)
            .cloned()
            .ok_or_else(|| AgentError::EntityNotFound(format!("Entity with ID {:?} not found", id)))
    }

    /// Get an entity by name
    pub fn get_by_name(&self, name: &str) -> Result<Arc<E>> {
        let names = self
            .name_index
            .read()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        let entities = self
            .entities
            .read()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;

        let id = names.get(name).ok_or_else(|| {
            AgentError::EntityNotFound(format!("Entity with name '{}' not found", name))
        })?;

        entities
            .get(id)
            .cloned()
            .ok_or_else(|| AgentError::EntityNotFound(format!("Entity with ID {:?} not found", id)))
    }

    /// Get all entities
    pub fn get_all(&self) -> Result<Vec<Arc<E>>> {
        let entities = self
            .entities
            .read()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        Ok(entities.values().cloned().collect())
    }

    /// Get all entities of a specific type
    pub fn get_by_type(&self, entity_type: &str) -> Result<Vec<Arc<E>>> {
        let entities = self
            .entities
            .read()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        Ok(entities
            .values()
            .filter(|e| e.entity_type() == entity_type)
            .cloned()
            .collect())
    }

    /// Get entities with a specific parent
    pub fn get_by_parent(&self, parent_id: &E::Id) -> Result<Vec<Arc<E>>> {
        let entities = self
            .entities
            .read()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        Ok(entities
            .values()
            .filter(|e| e.parent_id().map_or(false, |id| id == parent_id))
            .cloned()
            .collect())
    }

    /// Remove an entity by ID
    pub fn remove(&self, id: &E::Id) -> Result<Option<Arc<E>>> {
        let mut entities = self
            .entities
            .write()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        let mut names = self
            .name_index
            .write()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;

        // Remove from name index first
        if let Some(entity) = entities.get(id) {
            names.remove(entity.name());
        }

        // Then remove from entities map
        Ok(entities.remove(id))
    }

    /// Count the number of entities
    pub fn count(&self) -> Result<usize> {
        let entities = self
            .entities
            .read()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        Ok(entities.len())
    }

    /// Check if an entity exists by ID
    pub fn contains(&self, id: &E::Id) -> Result<bool> {
        let entities = self
            .entities
            .read()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        Ok(entities.contains_key(id))
    }

    /// Check if an entity exists by name
    pub fn contains_name(&self, name: &str) -> Result<bool> {
        let names = self
            .name_index
            .read()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        Ok(names.contains_key(name))
    }
}

/// Default implementation
impl<E: Entity> Default for EntityRegistry<E> {
    fn default() -> Self {
        Self::new()
    }
}
