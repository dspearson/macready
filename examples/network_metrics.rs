use async_trait::async_trait;
use chrono::{DateTime, Utc};
use metrics_agent_core::buffer::MetricsBuffer;
use metrics_agent_core::collector::{Collector, CollectorBuilder, MetricBatch, MetricPoint};
use metrics_agent_core::connection::postgres::{
    PgConfig, PgConfigBuilder, SslMode, establish_connection,
};
use metrics_agent_core::entity::{Entity, EntityRegistry};
use metrics_agent_core::prelude::*;
use metrics_agent_core::retry::RetryBuilder;
use metrics_agent_core::storage::memory::MemoryStorage;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use uuid::Uuid;

// Define a network interface entity
#[derive(Debug, Clone)]
struct NetworkInterface {
    interface_id: Uuid,
    host_id: Uuid,
    interface_name: String,
    interface_type: String,
    is_active: bool,
}

#[async_trait]
impl Entity for NetworkInterface {
    type Id = Uuid;

    fn id(&self) -> &Self::Id {
        &self.interface_id
    }

    fn name(&self) -> &str {
        &self.interface_name
    }

    fn entity_type(&self) -> &str {
        "network_interface"
    }

    fn is_active(&self) -> bool {
        self.is_active
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "interface_id": self.interface_id,
            "host_id": self.host_id,
            "interface_name": self.interface_name,
            "interface_type": self.interface_type,
            "is_active": self.is_active,
        })
    }
}

// Define a network metric
#[derive(Debug, Clone)]
struct NetworkMetric {
    interface_name: String,
    interface_id: Option<Uuid>,
    input_bytes: i64,
    input_packets: i64,
    output_bytes: i64,
    output_packets: i64,
    timestamp: DateTime<Utc>,
}

impl MetricPoint for NetworkMetric {
    type EntityType = NetworkInterface;

    fn entity_id(&self) -> Option<&<Self::EntityType as Entity>::Id> {
        self.interface_id.as_ref()
    }

    fn entity_name(&self) -> &str {
        &self.interface_name
    }

    fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    fn values(&self) -> HashMap<String, i64> {
        let mut values = HashMap::new();
        values.insert("input_bytes".to_string(), self.input_bytes);
        values.insert("input_packets".to_string(), self.input_packets);
        values.insert("output_bytes".to_string(), self.output_bytes);
        values.insert("output_packets".to_string(), self.output_packets);
        values
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "interface_name": self.interface_name,
            "interface_id": self.interface_id.map(|id| id.to_string()),
            "input_bytes": self.input_bytes,
            "input_packets": self.input_packets,
            "output_bytes": self.output_bytes,
            "output_packets": self.output_packets,
            "timestamp": self.timestamp.to_rfc3339(),
        })
    }

    fn collection_method(&self) -> &str {
        "example"
    }
}

// Create a simple network metrics collector
struct NetworkMetricsCollector {
    config: metrics_agent_core::collector::CollectorConfig,
}

impl NetworkMetricsCollector {
    fn new() -> Self {
        Self {
            config: CollectorBuilder::new("network_collector")
                .entity_type("network_interface")
                .interval(Duration::from_secs(5))
                .buffer_size(100)
                .build(),
        }
    }
}

#[async_trait]
impl Collector for NetworkMetricsCollector {
    type MetricType = NetworkMetric;

    async fn start(
        &self,
    ) -> metrics_agent_core::error::Result<mpsc::Receiver<MetricBatch<Self::MetricType>>> {
        let (tx, rx) = mpsc::channel(self.config.buffer_size);

        // Spawn a task to simulate metric collection
        tokio::spawn(async move {
            loop {
                // Generate some fake metrics
                let metrics = vec![
                    NetworkMetric {
                        interface_name: "eth0".to_string(),
                        interface_id: None, // We don't know the ID yet
                        input_bytes: 1024,
                        input_packets: 8,
                        output_bytes: 512,
                        output_packets: 4,
                        timestamp: Utc::now(),
                    },
                    NetworkMetric {
                        interface_name: "wlan0".to_string(),
                        interface_id: None, // We don't know the ID yet
                        input_bytes: 2048,
                        input_packets: 16,
                        output_bytes: 1024,
                        output_packets: 8,
                        timestamp: Utc::now(),
                    },
                ];

                // Send the batch
                let batch = MetricBatch::new(metrics);
                if tx.send(batch).await.is_err() {
                    break;
                }

                // Wait for next collection interval
                sleep(Duration::from_secs(5)).await;
            }
        });

        Ok(rx)
    }

    fn interval(&self) -> Duration {
        self.config.interval
    }

    fn name(&self) -> &str {
        &self.config.name
    }

    fn entity_type(&self) -> &str {
        &self.config.entity_type
    }
}

// Simulated network interface discovery
async fn discover_interfaces() -> Vec<NetworkInterface> {
    // In a real collector, this would query the system for interfaces
    vec![
        NetworkInterface {
            interface_id: Uuid::new_v4(),
            host_id: Uuid::new_v4(),
            interface_name: "eth0".to_string(),
            interface_type: "physical".to_string(),
            is_active: true,
        },
        NetworkInterface {
            interface_id: Uuid::new_v4(),
            host_id: Uuid::new_v4(),
            interface_name: "wlan0".to_string(),
            interface_type: "wireless".to_string(),
            is_active: true,
        },
    ]
}

#[tokio::main]
async fn main() -> metrics_agent_core::error::Result<()> {
    // Initialize logger
    env_logger::init();

    // Create entity registry
    let registry = Arc::new(EntityRegistry::<NetworkInterface>::new());

    // Create metrics buffer
    let buffer = Arc::new(MetricsBuffer::<NetworkMetric>::new());

    // Create a memory storage backend for this example
    let storage = Arc::new(MemoryStorage::<NetworkMetric, NetworkInterface>::new(
        "example_storage",
    ));

    // Start the collector
    let collector = NetworkMetricsCollector::new();
    let mut metrics_rx = collector.start().await?;

    // Discover interfaces after a short delay
    tokio::spawn({
        let registry = Arc::clone(&registry);
        let storage = Arc::clone(&storage);

        async move {
            // Wait a bit before discovering interfaces
            sleep(Duration::from_secs(10)).await;

            // Discover interfaces
            log::info!("Discovering network interfaces...");
            let interfaces = discover_interfaces().await;

            for interface in interfaces {
                log::info!("Discovered interface: {}", interface.name());

                // Register the interface
                registry.register(interface.clone()).unwrap();

                // Also register with storage
                storage.register_entity(&interface).await.unwrap();
            }
        }
    });

    // Process metrics
    log::info!("Starting metrics processing loop");
    while let Some(batch) = metrics_rx.recv().await {
        log::info!("Received batch of {} metrics", batch.metrics.len());

        for metric in batch.metrics {
            let entity_name = metric.entity_name();

            // Check if we know this entity
            match registry.get_by_name(entity_name) {
                Ok(entity) => {
                    // We know this entity, update the metric with the entity ID
                    let mut updated_metric = metric.clone();
                    updated_metric.interface_id = Some(entity.id().clone());

                    // Store the metric
                    if let Err(e) = storage.store_metric(&updated_metric).await {
                        log::error!("Failed to store metric: {}", e);
                    } else {
                        log::debug!("Stored metric for {}", entity_name);
                    }
                }
                Err(_) => {
                    // We don't know this entity yet, buffer the metric
                    log::debug!("Buffering metric for unknown entity: {}", entity_name);
                    buffer.add(metric).unwrap();
                }
            }
        }

        // Process any buffered metrics for entities we now know
        for entity_name in buffer.get_entity_names().unwrap() {
            if let Ok(entity) = registry.get_by_name(&entity_name) {
                log::info!("Processing buffered metrics for {}", entity_name);

                // Take metrics for this entity
                let buffered_metrics = buffer.take_for_entity(&entity_name).unwrap();

                if !buffered_metrics.is_empty() {
                    log::info!(
                        "Processing {} buffered metrics for {}",
                        buffered_metrics.len(),
                        entity_name
                    );

                    // Update metrics with entity ID and store them
                    for metric in buffered_metrics {
                        let mut updated_metric = metric;
                        updated_metric.interface_id = Some(entity.id().clone());

                        if let Err(e) = storage.store_metric(&updated_metric).await {
                            log::error!("Failed to store buffered metric: {}", e);
                        }
                    }
                }
            }
        }

        // Clean up the buffer periodically
        let removed = buffer.cleanup().unwrap();
        if removed > 0 {
            log::info!("Cleaned up {} old buffered metrics", removed);
        }
    }

    Ok(())
}
