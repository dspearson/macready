use chrono::Utc;
use macready::prelude::*;
use macready::storage::postgres_impl::{PostgresStorage, PostgresStorageExt};
use std::time::Duration;
use tokio_postgres::types::ToSql;

// Define a simple metric structure - but remember that with the new approach,
// this could be any structure the user wants
#[derive(Debug, Clone)]
struct DiskMetric {
    disk_name: String,
    used_bytes: i64,
    total_bytes: i64,
    timestamp: chrono::DateTime<Utc>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logger
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .init();

    println!("Starting PostgreSQL storage example...");

    // Configure PostgreSQL connection
    let pg_config = DatabaseConfig {
        host: "localhost".to_string(),
        port: 5432,
        database: "metrics".to_string(),
        username: "postgres".to_string(),
        password: "postgres".to_string(),
        ssl_mode: SslMode::Disable,
        ca_cert: None,
        client_cert: None,
        client_key: None,
    };

    // Create the storage
    println!("Connecting to PostgreSQL...");
    let storage = match PostgresStorage::<DiskMetric>::new(pg_config, "disk_metrics").await {
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

    // Create a table for metrics if it doesn't exist
    storage.execute_sql(
        "CREATE TABLE IF NOT EXISTS disk_metrics (
            id SERIAL PRIMARY KEY,
            disk_name TEXT NOT NULL,
            used_bytes BIGINT NOT NULL,
            total_bytes BIGINT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
        &[],
    ).await?;

    // Create some sample metrics
    println!("Creating sample metrics...");
    let now = Utc::now();

    let metrics = vec![
        DiskMetric {
            disk_name: "root".to_string(),
            total_bytes: 100 * 1024 * 1024 * 1024, // 100 GB
            used_bytes: 50 * 1024 * 1024 * 1024,   // 50 GB
            timestamp: now,
        },
        DiskMetric {
            disk_name: "home".to_string(),
            total_bytes: 500 * 1024 * 1024 * 1024, // 500 GB
            used_bytes: 100 * 1024 * 1024 * 1024,  // 100 GB
            timestamp: now,
        },
    ];

    // Store metrics using user-defined closure
    println!("Storing metrics...");

    for metric in &metrics {
        storage.store(metric.clone(), |client, metric| async move {
            let stmt = "INSERT INTO disk_metrics (disk_name, used_bytes, total_bytes, timestamp)
                        VALUES ($1, $2, $3, $4) RETURNING id";

            let row = client.query_one(
                stmt,
                &[
                    &metric.disk_name as &(dyn ToSql + Sync),
                    &metric.used_bytes as &(dyn ToSql + Sync),
                    &metric.total_bytes as &(dyn ToSql + Sync),
                    &metric.timestamp as &(dyn ToSql + Sync),
                ],
            ).await.map_err(|e| AgentError::Database(e.to_string()))?;

            let id: i32 = row.get(0);
            Ok(id)
        }).await?;
    }

    // Now retrieve the metrics
    println!("Retrieving metrics...");

    let retrieved_metrics = storage.retrieve("root", |client, disk_name| async move {
        let stmt = "SELECT disk_name, used_bytes, total_bytes, timestamp
                   FROM disk_metrics WHERE disk_name = $1";

        let rows = client.query(
            stmt,
            &[&disk_name as &(dyn ToSql + Sync)],
        ).await.map_err(|e| AgentError::Database(e.to_string()))?;

        let metrics = rows.iter().map(|row| {
            DiskMetric {
                disk_name: row.get(0),
                used_bytes: row.get(1),
                total_bytes: row.get(2),
                timestamp: row.get(3),
            }
        }).collect::<Vec<_>>();

        Ok(metrics)
    }).await?;

    println!("Retrieved {} metrics for root disk", retrieved_metrics.len());

    // Show the metrics
    for metric in retrieved_metrics {
        println!(
            "Disk: {}, Used: {:.2} GB, Total: {:.2} GB, Usage: {:.1}%",
            metric.disk_name,
            metric.used_bytes as f64 / 1_073_741_824.0,
            metric.total_bytes as f64 / 1_073_741_824.0,
            (metric.used_bytes as f64 / metric.total_bytes as f64) * 100.0
        );
    }

    // Demonstrate health check
    println!("Performing health check...");
    let healthy = storage.check_health().await?;
    println!("Storage is healthy: {}", healthy);

    println!("Example completed successfully");
    Ok(())
}
