use async_trait::async_trait;
use chrono::Utc;
use macready::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Simple disk usage entity
#[derive(Debug, Clone)]
struct DiskEntity {
    id: String,
    name: String,
    mount_point: String,
    filesystem: String,
    is_active: bool,
}

impl Entity for DiskEntity {
    type Id = String;

    fn id(&self) -> &Self::Id {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn entity_type(&self) -> &str {
        "disk"
    }

    fn is_active(&self) -> bool {
        self.is_active
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "name": self.name,
            "mount_point": self.mount_point,
            "filesystem": self.filesystem,
            "is_active": self.is_active,
        })
    }

    fn metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert("mount_point".to_string(), self.mount_point.clone());
        metadata.insert("filesystem".to_string(), self.filesystem.clone());
        metadata
    }
}

/// Disk usage metric
#[derive(Debug, Clone)]
struct DiskMetric {
    disk_id: String,
    disk_name: String,
    total_bytes: i64,
    used_bytes: i64,
    available_bytes: i64,
    percent_used: f64,
    timestamp: chrono::DateTime<Utc>,
}

impl MetricPoint for DiskMetric {
    type EntityType = DiskEntity;

    fn entity_id(&self) -> Option<&<Self::EntityType as Entity>::Id> {
        Some(&self.disk_id)
    }

    fn entity_name(&self) -> &str {
        &self.disk_name
    }

    fn timestamp(&self) -> chrono::DateTime<Utc> {
        self.timestamp
    }

    fn values(&self) -> HashMap<String, i64> {
        let mut values = HashMap::new();
        values.insert("total_bytes".to_string(), self.total_bytes);
        values.insert("used_bytes".to_string(), self.used_bytes);
        values.insert("available_bytes".to_string(), self.available_bytes);
        values.insert("percent_used".to_string(), (self.percent_used * 100.0) as i64);
        values
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "disk_id": self.disk_id,
            "disk_name": self.disk_name,
            "total_bytes": self.total_bytes,
            "used_bytes": self.used_bytes,
            "available_bytes": self.available_bytes,
            "percent_used": self.percent_used,
            "timestamp": self.timestamp,
        })
    }

    fn collection_method(&self) -> &str {
        "df"
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logger
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .init();

    println!("Starting PostgreSQL storage example...");

    // Configure PostgreSQL connection
    let pg_config = PgConfigBuilder::new()
        .username("postgres")
        .password("postgres")
        .host("localhost")
        .port(5432)
        .database("metrics")
        .ssl_mode(SslMode::Disable)
        .application_name("macready-example")
        .build();

    // Configure the storage
    let storage_config = PostgresStorageConfigBuilder::new()
        .connection(pg_config)
        .metrics_table("disk_metrics")
        .entities_table("disk_entities")
        .batch_size(100)
        .create_tables(true)
        .build();

    // Create the storage
    println!("Connecting to PostgreSQL...");
    let storage = match PostgresStorage::<DiskMetric, DiskEntity>::new(storage_config, "disk_metrics").await {
        Ok(storage) => {
            println!("Connected to PostgreSQL");
            storage
        },
        Err(e) => {
            eprintln!("Failed to connect to PostgreSQL: {}", e);
            println!("This example requires a running PostgreSQL server.");
            println!("You can start one with Docker:");
            println!("  docker run --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres");
            return Err(e);
        }
    };

    // Create an entity registry
    let registry = Arc::new(EntityRegistry::<DiskEntity>::new());

    // Register some disk entities
    let root_disk = DiskEntity {
        id: "root".to_string(),
        name: "root",
        mount_point: "/",
        filesystem: "ext4",
        is_active: true,
    };

    let home_disk = DiskEntity {
        id: "home".to_string(),
        name: "home",
        mount_point: "/home",
        filesystem: "ext4",
        is_active: true,
    };

    // Register with storage
    println!("Registering entities...");
    storage.register_entity(&root_disk).await?;
    storage.register_entity(&home_disk).await?;

    // Register with local registry
    registry.register(root_disk.clone())?;
    registry.register(home_disk.clone())?;

    // Create some metrics
    println!("Creating sample metrics...");
    let now = Utc::now();

    let metrics = vec![
        DiskMetric {
            disk_id: "root".to_string(),
            disk_name: "root".to_string(),
            total_bytes: 100 * 1024 * 1024 * 1024, // 100 GB
            used_bytes: 50 * 1024 * 1024 * 1024,   // 50 GB
            available_bytes: 50 * 1024 * 1024 * 1024, // 50 GB
            percent_used: 0.5, // 50%
            timestamp: now,
        },
        DiskMetric {
            disk_id: "home".to_string(),
            disk_name: "home".to_string(),
            total_bytes: 500 * 1024 * 1024 * 1024, // 500 GB
            used_bytes: 100 * 1024 * 1024 * 1024,  // 100 GB
            available_bytes: 400 * 1024 * 1024 * 1024, // 400 GB
            percent_used: 0.2, // 20%
            timestamp: now,
        },
    ];

    // Store the metrics
    println!("Storing metrics...");
    let stored = storage.store_metrics(&metrics).await?;
    println!("Stored {} metrics", stored);

    // Check entity exists
    println!("Checking if entities exist...");
    let root_exists = storage.entity_exists(&"root".to_string()).await?;
    let home_exists = storage.entity_exists(&"home".to_string()).await?;

    println!("Root disk exists: {}", root_exists);
    println!("Home disk exists: {}", home_exists);

    // Health check
    println!("Performing health check...");
    let healthy = storage.health_check().await?;
    println!("Storage is healthy: {}", healthy);

    println!("Example completed successfully");
    Ok(())
}
