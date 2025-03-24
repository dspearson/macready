use std::collections::HashMap;
use std::sync::RwLock;

use crate::error::{AgentError, Result};

/// A minimal in-memory storage for testing
pub struct MemoryStorage<T> {
    data: RwLock<HashMap<String, T>>,
    name: String,
}

impl<T> MemoryStorage<T> {
    /// Create a new memory storage
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            name: name.into(),
        }
    }

    /// Store data with a key
    pub fn store(&self, key: impl Into<String>, value: T) -> Result<()> {
        let mut data = self
            .data
            .write()
            .map_err(|_| AgentError::Storage("Lock poisoned".to_string()))?;
        data.insert(key.into(), value);
        Ok(())
    }

    /// Retrieve data by key
    pub fn get(&self, key: &str) -> Result<Option<T>>
    where
        T: Clone,
    {
        let data = self
            .data
            .read()
            .map_err(|_| AgentError::Storage("Lock poisoned".to_string()))?;
        Ok(data.get(key).cloned())
    }

    /// Remove data by key
    pub fn remove(&self, key: &str) -> Result<Option<T>> {
        let mut data = self
            .data
            .write()
            .map_err(|_| AgentError::Storage("Lock poisoned".to_string()))?;
        Ok(data.remove(key))
    }

    /// Clear all data
    pub fn clear(&self) -> Result<()> {
        let mut data = self
            .data
            .write()
            .map_err(|_| AgentError::Storage("Lock poisoned".to_string()))?;
        data.clear();
        Ok(())
    }

    /// Get all keys
    pub fn keys(&self) -> Result<Vec<String>> {
        let data = self
            .data
            .read()
            .map_err(|_| AgentError::Storage("Lock poisoned".to_string()))?;
        Ok(data.keys().cloned().collect())
    }

    /// Get storage name
    pub fn name(&self) -> &str {
        &self.name
    }
}
