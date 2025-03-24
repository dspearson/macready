use crate::error::Result;
use std::collections::HashMap;
use std::hash::Hash;
use std::fmt::Debug;
use std::sync::Arc;

/// A change in an entity
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityChange<K, V> {
    /// Entity was added
    Added(V),
    /// Entity was updated
    Updated {
        /// The old value
        old: V,
        /// The new value
        new: V,
        /// Map of changed fields
        changes: HashMap<String, (Option<String>, Option<String>)>,
    },
    /// Entity was removed
    Removed(V),
}

/// Trait for a function that handles entity changes
#[async_trait::async_trait]
pub trait ChangeHandler<K, V>: Send + Sync + 'static {
    /// Handle a change to an entity
    async fn handle_change(&self, key: &K, change: &EntityChange<K, V>) -> Result<()>;
}

/// A change tracker that detects entity changes
pub struct ChangeTracker<K, V> {
    name: String,
    known_entities: tokio::sync::RwLock<HashMap<K, V>>,
    handlers: Vec<Box<dyn ChangeHandler<K, V>>>,
    diff_fn: Box<dyn Fn(&V, &V) -> HashMap<String, (Option<String>, Option<String>)> + Send + Sync>,
}

impl<K, V> ChangeTracker<K, V>
where
    K: Eq + Hash + Clone + Debug + Send + Sync + 'static,
    V: Clone + Debug + Send + Sync + 'static,
{
    /// Create a new change tracker
    pub fn new<F>(name: impl Into<String>, diff_fn: F) -> Self
    where
        F: Fn(&V, &V) -> HashMap<String, (Option<String>, Option<String>)> + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            known_entities: tokio::sync::RwLock::new(HashMap::new()),
            handlers: Vec::new(),
            diff_fn: Box::new(diff_fn),
        }
    }

    /// Add a change handler
    pub fn add_handler<H: ChangeHandler<K, V>>(&mut self, handler: H) -> &mut Self {
        self.handlers.push(Box::new(handler));
        self
    }

    /// Track a set of entities and detect changes
    pub async fn track(&self, entities: HashMap<K, V>) -> Result<()> {
        // Get current known entities
        let mut current = self.known_entities.write().await;

        // Track which entities we've seen
        let mut seen = std::collections::HashSet::new();

        // Process each entity
        for (key, value) in entities.iter() {
            seen.insert(key.clone());

            // Check if this is a known entity
            if let Some(existing) = current.get(key) {
                // Detect changes
                let changes = (self.diff_fn)(existing, value);

                if !changes.is_empty() {
                    // Entity changed
                    let change = EntityChange::Updated {
                        old: existing.clone(),
                        new: value.clone(),
                        changes,
                    };

                    // Notify handlers
                    for handler in &self.handlers {
                        handler.handle_change(key, &change).await?;
                    }

                    // Update known entities
                    current.insert(key.clone(), value.clone());
                }
            } else {
                // New entity
                let change = EntityChange::Added(value.clone());

                // Notify handlers
                for handler in &self.handlers {
                    handler.handle_change(key, &change).await?;
                }

                // Add to known entities
                current.insert(key.clone(), value.clone());
            }
        }

        // Check for removed entities
        let mut removed = Vec::new();
        for key in current.keys() {
            if !seen.contains(key) {
                removed.push(key.clone());
            }
        }

        // Process removed entities
        for key in removed {
            // Get the entity
            let value = current.remove(&key).unwrap();

            // Create change
            let change = EntityChange::Removed(value);

            // Notify handlers
            for handler in &self.handlers {
                handler.handle_change(&key, &change).await?;
            }
        }

        Ok(())
    }

    /// Access the current known entities
    pub async fn get_known_entities(&self) -> Result<HashMap<K, V>> {
        let entities = self.known_entities.read().await;
        Ok(entities.clone())
    }

    /// Get a specific entity by key
    pub async fn get_entity(&self, key: &K) -> Result<Option<V>> {
        let entities = self.known_entities.read().await;
        Ok(entities.get(key).cloned())
    }

    /// Get the name of this tracker
    pub fn name(&self) -> &str {
        &self.name
    }
}
