# MacReady

A reusable core library for shapeshifting metric-collecting agents.

## Overview

This library provides the foundational components needed to build a metric-collecting agent. It was extracted from the Crossbow Metrics project to enable code reuse across different components for various platforms and use-cases.

Key features:

- **Entity Management**: Track entities (interfaces, zones, hosts, etc.)
- **Metric Collection**: Abstract interfaces for metrics collection
- **Metrics Buffering**: Buffer metrics for entities not yet discovered
- **Database Connectivity**: Resilient PostgreSQL connection handling with health monitoring
- **Retry Logic**: Robust retry mechanisms with exponential backoff
- **Configuration Handling**: Flexible configuration loading from various sources

## Usage

### Basic Usage

```rust
use macready::prelude::*;
use macready::connection::postgres::{establish_connection, PgConfig};
use macready::buffer::MetricsBuffer;
use macready::storage::postgres::PostgresStorage;

#[tokio::main]
async fn main() -> Result<()> {
    // Set up database connection
    let db_config = PgConfig::new()
        .username("postgres")
        .password("password")
        .database("metrics")
        .host("localhost")
        .build();

    let client = establish_connection(&db_config).await?;

    // Create storage backend
    let storage = PostgresStorage::new(db_config, "my-agent").await?;

    // Start metrics collection
    let collector = MyCustomCollector::new();
    let metrics_rx = collector.start().await?;

    // Process metrics
    while let Some(batch) = metrics_rx.recv().await {
        storage.store_metrics(&batch.metrics).await?;
    }

    Ok(())
}
```

### Implementing Custom Entities

To implement your own entity type:

```rust
use async_trait::async_trait;
use macready::entity::Entity;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct NetworkInterface {
    interface_id: Uuid,
    host_id: Uuid,
    interface_name: String,
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
            "interface_id": self.interface_id.to_string(),
            "host_id": self.host_id.to_string(),
            "interface_name": self.interface_name,
            "is_active": self.is_active,
        })
    }
}
```

### Implementing Custom Metrics

To implement your own metric type:

```rust
use chrono::{DateTime, Utc};
use macready::collector::MetricPoint;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct NetworkMetric {
    interface_name: String,
    interface_id: Option<Uuid>,
    input_bytes: i64,
    output_bytes: i64,
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
        values.insert("output_bytes".to_string(), self.output_bytes);
        values
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "interface_name": self.interface_name,
            "interface_id": self.interface_id.map(|id| id.to_string()),
            "input_bytes": self.input_bytes,
            "output_bytes": self.output_bytes,
            "timestamp": self.timestamp,
        })
    }

    fn collection_method(&self) -> &str {
        "custom"
    }
}
```

## Components

### Entity Registry

The `EntityRegistry` provides a thread-safe store for entities that metrics can be collected for:

```rust
let registry = EntityRegistry::<NetworkInterface>::new();
let interface = NetworkInterface { /* ... */ };
registry.register(interface)?;

// Later, lookup by ID or name
let interface = registry.get_by_name("eth0")?;
```

### Metrics Buffer

The `MetricsBuffer` handles buffering metrics for entities that haven't been discovered yet:

```rust
let buffer = MetricsBuffer::<NetworkMetric>::new();

// Add metrics for unknown entities
buffer.add(metric)?;

// When an entity is discovered, take its buffered metrics
let metrics = buffer.take_for_entity("eth0")?;

// Process the metrics
storage.store_metrics(&metrics).await?;
```

### Storage Backends

The library provides a `PostgresStorage` implementation, but you can also create your own by implementing the `Storage` trait:

```rust
#[async_trait]
impl<M, E> Storage for MyCustomStorage<M, E>
where
    M: MetricPoint<EntityType = E>,
    E: Entity,
{
    type MetricType = M;
    type EntityType = E;

    async fn store_metric(&self, metric: &Self::MetricType) -> Result<()> {
        // Store the metric
    }

    // ... other required methods
}
```

## Configuration

The library provides flexible configuration handling:

```rust
#[derive(Debug, Deserialize)]
struct MyConfig {
    database: DatabaseConfig,
    agent_name: String,
    collection_interval: u64,
}

impl ConfigLoader for MyConfig {
    fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        load_config(vec![ConfigSource::File(path.as_ref().to_string_lossy().to_string())])
    }

    fn load_from_sources(sources: Vec<ConfigSource>) -> Result<Self> {
        load_config(sources)
    }
}
```

## Retry Logic

The library includes a robust retry mechanism:

```rust
let result = execute_with_retry(
    || async {
        // Async operation that might fail
        client.query_one("SELECT 1", &[]).await
    },
    RetryConfig::default(),
    "database_operation"
).await?;
```

## Health Checking

The library provides a health checking system to monitor connection health:

```rust
let health_status = Arc::new(HealthStatus::new("database"));
let health_check = GenericHealthCheck::new(Arc::clone(&client), "database");

let health_events = start_health_check(
    Arc::new(health_check),
    Arc::clone(&health_status),
    HealthCheckConfig::default()
);

// React to health events
tokio::spawn(async move {
    while let Some(event) = health_events.recv().await {
        match event {
            HealthEvent::Healthy { name } => {
                info!("Connection {} is healthy", name);
            },
            HealthEvent::Unhealthy { name, reason } => {
                error!("Connection {} is unhealthy: {}", name, reason);
            }
        }
    }
});
```

## Notes

Norway: /ˈswiːdən/ (pronunciation guide)

## License

ISC License

Copyright (c) 2025 Dominic Pearson
