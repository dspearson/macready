//! Core collector traits and types
use crate::entity::Entity;
use crate::error::Result;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::sync::mpsc;

/// A trait for metric types
pub trait MetricType: Clone + Send + Sync + Debug + 'static {}

// Implement for any type that meets the requirements
impl<T> MetricType for T where T: Clone + Send + Sync + Debug + 'static {}

/// A trait for metrics associated with entities
pub trait MetricPoint: Clone + Send + Sync + Debug + 'static {
    /// The type of entity this metric is associated with
    type EntityType: Entity;

    /// Get the entity ID this metric belongs to
    fn entity_id(&self) -> Option<&<Self::EntityType as Entity>::Id>;

    /// Get the entity name this metric belongs to
    fn entity_name(&self) -> &str;

    /// Get the timestamp when this metric was collected
    fn timestamp(&self) -> DateTime<Utc>;

    /// Get the metric values as key-value pairs
    fn values(&self) -> HashMap<String, i64>;

    /// Convert the metric to a JSON-compatible format
    fn to_json(&self) -> serde_json::Value;

    /// Get the metric collection method/source
    fn collection_method(&self) -> &str {
        "default"
    }
}


/// A batch of metrics
#[derive(Debug, Clone)]
pub struct MetricBatch<T: MetricType> {
    /// The metrics in this batch
    pub metrics: Vec<T>,
    /// When this batch was collected
    pub timestamp: DateTime<Utc>,
    /// The source of these metrics
    pub source: String,
}

impl<T: MetricType> MetricBatch<T> {
    /// Create a new metric batch
    pub fn new(metrics: Vec<T>, source: impl Into<String>) -> Self {
        Self {
            metrics,
            timestamp: Utc::now(),
            source: source.into(),
        }
    }
}

/// Base trait for all metric collectors
#[async_trait::async_trait]
pub trait Collector: Send + Sync + 'static {
    /// The type of metric this collector produces
    type MetricType: MetricType;

    /// Start collecting metrics
    async fn start(&self) -> Result<mpsc::Receiver<MetricBatch<Self::MetricType>>>;

    /// Stop collecting metrics
    async fn stop(&self) -> Result<()>;

    /// Get the collector name
    fn name(&self) -> &str;
}
