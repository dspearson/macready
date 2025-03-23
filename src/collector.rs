use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use crate::entity::Entity;
use crate::error::{AgentError, Result};

/// Trait for a single metric point
pub trait MetricPoint: Clone + Send + Sync + Debug + 'static {
    /// The type of entity this metric is associated with
    type EntityType: Entity;

    /// Get the entity ID this metric belongs to
    fn entity_id(&self) -> Option<&<Self::EntityType as Entity>::Id>;

    /// Get the entity name (e.g., interface name) this metric belongs to
    fn entity_name(&self) -> &str;

    /// Get the timestamp when this metric was collected
    fn timestamp(&self) -> DateTime<Utc>;

    /// Get the metric values as key-value pairs
    fn values(&self) -> HashMap<String, i64>;

    /// Convert the metric to a JSON-compatible format
    fn to_json(&self) -> serde_json::Value;

    /// Get the metric collection method/source
    fn collection_method(&self) -> &str;
}

/// A batch of metrics sent from a collector
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricBatch<M: MetricPoint> {
    pub metrics: Vec<M>,
    pub timestamp: DateTime<Utc>,
}

impl<M: MetricPoint> MetricBatch<M> {
    pub fn new(metrics: Vec<M>) -> Self {
        Self {
            metrics,
            timestamp: Utc::now(),
        }
    }

    pub fn len(&self) -> usize {
        self.metrics.len()
    }

    pub fn is_empty(&self) -> bool {
        self.metrics.is_empty()
    }
}

/// Trait for metric collectors
#[async_trait]
pub trait Collector: Send + Sync + 'static {
    /// The type of metric point this collector produces
    type MetricType: MetricPoint;

    /// Start collecting metrics and return a receiver channel
    async fn start(&self) -> Result<mpsc::Receiver<MetricBatch<Self::MetricType>>>;

    /// Stop collection (implementation is optional)
    async fn stop(&self) -> Result<()> {
        Ok(())
    }

    /// Get the collection interval
    fn interval(&self) -> Duration;

    /// Get the collector name
    fn name(&self) -> &str;

    /// Get the entity type this collector is for
    fn entity_type(&self) -> &str;
}

/// Base implementation for common collector functionality
pub struct CollectorConfig {
    /// Name of the collector
    pub name: String,

    /// Type of entity this collector monitors
    pub entity_type: String,

    /// Collection interval
    pub interval: Duration,

    /// Channel buffer size
    pub buffer_size: usize,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            name: "default_collector".to_string(),
            entity_type: "unknown".to_string(),
            interval: Duration::from_secs(60),
            buffer_size: 100,
        }
    }
}

/// Helper struct for building collectors
pub struct CollectorBuilder {
    config: CollectorConfig,
}

impl CollectorBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            config: CollectorConfig {
                name: name.into(),
                ..Default::default()
            },
        }
    }

    pub fn entity_type(mut self, entity_type: impl Into<String>) -> Self {
        self.config.entity_type = entity_type.into();
        self
    }

    pub fn interval(mut self, interval: Duration) -> Self {
        self.config.interval = interval;
        self
    }

    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.config.buffer_size = buffer_size;
        self
    }

    pub fn build(self) -> CollectorConfig {
        self.config
    }
}
