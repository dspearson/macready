use chrono::{Duration, Utc};
use log::{debug, info, trace};
use std::collections::HashMap;
use std::sync::RwLock;

use crate::collector::MetricPoint;
use crate::error::{AgentError, Result};

/// Configuration for metrics buffer
pub struct BufferConfig {
    /// Maximum age of buffered metrics in minutes
    pub max_age_minutes: i64,

    /// Maximum metrics to buffer per entity
    pub max_per_entity: usize,

    /// Total maximum buffer size across all entities
    pub max_total: usize,

    /// Minimum metrics to trigger auto-detection
    pub detection_threshold: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_age_minutes: 10,
            max_per_entity: 100,
            max_total: 1000,
            detection_threshold: 5,
        }
    }
}

/// A buffer for metrics that don't have a registered entity yet
pub struct MetricsBuffer<M: MetricPoint> {
    /// Buffer of unknown metrics keyed by entity name
    buffer: RwLock<HashMap<String, Vec<M>>>,

    /// Configuration for buffer management
    config: BufferConfig,
}

impl<M: MetricPoint> MetricsBuffer<M> {
    /// Create a new metrics buffer with default configuration
    pub fn new() -> Self {
        Self {
            buffer: RwLock::new(HashMap::new()),
            config: BufferConfig::default(),
        }
    }

    /// Create a new metrics buffer with custom configuration
    pub fn with_config(config: BufferConfig) -> Self {
        Self {
            buffer: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Add a metric to the buffer
    pub fn add(&self, metric: M) -> Result<()> {
        let entity_name = metric.entity_name().to_string();

        let mut buffer = self
            .buffer
            .write()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;

        buffer
            .entry(entity_name.clone())
            .or_insert_with(Vec::new)
            .push(metric);

        trace!(
            "Added metric for unknown entity '{}' to buffer",
            entity_name
        );

        Ok(())
    }

    /// Add multiple metrics to the buffer
    pub fn add_batch(&self, metrics: Vec<M>) -> Result<usize> {
        let mut count = 0;

        for metric in metrics {
            if self.add(metric).is_ok() {
                count += 1;
            }
        }

        Ok(count)
    }

    /// Get all entity names in the buffer
    pub fn get_entity_names(&self) -> Result<Vec<String>> {
        let buffer = self
            .buffer
            .read()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        Ok(buffer.keys().cloned().collect())
    }

    /// Get and remove metrics for a specific entity
    pub fn take_for_entity(&self, entity_name: &str) -> Result<Vec<M>> {
        let mut buffer = self
            .buffer
            .write()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;

        match buffer.remove(entity_name) {
            Some(metrics) => {
                debug!(
                    "Removed {} buffered metrics for entity '{}'",
                    metrics.len(),
                    entity_name
                );
                Ok(metrics)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Get entity names that have enough metrics to warrant auto-detection
    pub fn get_autodetect_candidates(&self) -> Result<Vec<String>> {
        let buffer = self
            .buffer
            .read()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;

        Ok(buffer
            .iter()
            .filter(|(_, metrics)| metrics.len() >= self.config.detection_threshold)
            .map(|(name, _)| name.clone())
            .collect())
    }

    /// Clean up the buffer based on age and size limits
    pub fn cleanup(&self) -> Result<usize> {
        let mut total_removed = 0;

        // Age-based cleanup
        let cutoff_time = Utc::now() - Duration::minutes(self.config.max_age_minutes);

        let mut buffer = self
            .buffer
            .write()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;

        // First pass: age-based cleanup and per-entity size limits
        for (entity_name, metrics) in buffer.iter_mut() {
            let original_len = metrics.len();

            // Age-based cleanup
            metrics.retain(|m| m.timestamp() > cutoff_time);
            let age_removed = original_len - metrics.len();

            // Size-based cleanup for this entity
            if metrics.len() > self.config.max_per_entity {
                // Sort by timestamp (newest first) and keep only the most recent
                metrics.sort_by(|a, b| b.timestamp().cmp(&a.timestamp()));
                let size_removed = metrics.len() - self.config.max_per_entity;
                metrics.truncate(self.config.max_per_entity);

                if size_removed > 0 {
                    debug!(
                        "Truncated {} excess buffered metrics for entity '{}'",
                        size_removed, entity_name
                    );
                    total_removed += size_removed;
                }
            }

            if age_removed > 0 {
                debug!(
                    "Removed {} old buffered metrics for entity '{}'",
                    age_removed, entity_name
                );
                total_removed += age_removed;
            }
        }

        // Remove empty entries
        buffer.retain(|_, metrics| !metrics.is_empty());

        // Second pass: global buffer size limit
        let total_buffered: usize = buffer.values().map(|v| v.len()).sum();

        if total_buffered > self.config.max_total {
            // We need to remove oldest metrics across all entities
            let to_remove = total_buffered - self.config.max_total;
            info!(
                "Total buffer size ({}) exceeds maximum ({}), removing {} oldest metrics",
                total_buffered, self.config.max_total, to_remove
            );

            // Flatten all metrics into one vector with entity name
            let mut all_metrics = Vec::new();
            for (entity, metrics) in buffer.iter() {
                for metric in metrics {
                    all_metrics.push((entity.clone(), metric.timestamp()));
                }
            }

            // Sort by timestamp (oldest first)
            all_metrics.sort_by(|a, b| a.1.cmp(&b.1));

            // Get the cutoff timestamp
            if let Some((_, cutoff_ts)) = all_metrics.get(to_remove - 1) {
                // Remove all metrics older than or equal to this timestamp
                for (entity_name, metrics) in buffer.iter_mut() {
                    let original_len = metrics.len();
                    metrics.retain(|m| m.timestamp() > *cutoff_ts);
                    let removed = original_len - metrics.len();
                    if removed > 0 {
                        debug!(
                            "Removed {} oldest metrics from buffer for entity '{}'",
                            removed, entity_name
                        );
                    }
                }
            }

            // Remove empty entries again
            buffer.retain(|_, metrics| !metrics.is_empty());

            total_removed += to_remove;
        }

        Ok(total_removed)
    }

    /// Get the total number of buffered metrics
    pub fn total_count(&self) -> Result<usize> {
        let buffer = self
            .buffer
            .read()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        Ok(buffer.values().map(|v| v.len()).sum())
    }

    /// Get the number of unique entities in the buffer
    pub fn entity_count(&self) -> Result<usize> {
        let buffer = self
            .buffer
            .read()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        Ok(buffer.len())
    }

    /// Get count of metrics for a specific entity
    pub fn count_for_entity(&self, entity_name: &str) -> Result<usize> {
        let buffer = self
            .buffer
            .read()
            .map_err(|_| AgentError::Other("Lock poisoned".to_string()))?;
        Ok(buffer.get(entity_name).map_or(0, |v| v.len()))
    }
}

impl<M: MetricPoint> Default for MetricsBuffer<M> {
    fn default() -> Self {
        Self::new()
    }
}
