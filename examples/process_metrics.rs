use chrono::Utc;
use macready::prelude::*;
use macready::process::{Command, ProcessConfig, ProcessError, ProcessSource};
use macready::storage::postgres_impl::{PostgresStorage, PostgresStorageExt};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::types::ToSql;

/// A simple metric for CPU usage
#[derive(Debug, Clone)]
struct CpuMetric {
    cpu: String,
    user: f64,
    system: f64,
    idle: f64,
    timestamp: chrono::DateTime<Utc>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logger
    env_logger::init();

    println!("Starting process metrics example with PostgreSQL storage...");

    // Setup PostgreSQL storage
    let db_config = DatabaseConfig {
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

    let storage = PostgresStorage::<CpuMetric>::new(db_config, "cpu_metrics")
        .await
        .expect("Failed to connect to PostgreSQL");

    // Create table if not exists
    storage
        .execute_sql(
            "CREATE TABLE IF NOT EXISTS cpu_metrics (
            id SERIAL PRIMARY KEY,
            cpu TEXT NOT NULL,
            user_pct FLOAT NOT NULL,
            system_pct FLOAT NOT NULL,
            idle_pct FLOAT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
            &[],
        )
        .await
        .expect("Failed to create table");

    // Create a metrics collector
    let metrics = Arc::new(Mutex::new(Vec::<CpuMetric>::new()));

    // Collect some metrics
    // (This is a simplified example - in a real app, you'd use ProcessSource to collect metrics)
    {
        let mut metrics_guard = metrics.lock().unwrap();
        metrics_guard.push(CpuMetric {
            cpu: "cpu0".to_string(),
            user: 0.35,
            system: 0.15,
            idle: 0.50,
            timestamp: Utc::now(),
        });

        metrics_guard.push(CpuMetric {
            cpu: "cpu1".to_string(),
            user: 0.25,
            system: 0.10,
            idle: 0.65,
            timestamp: Utc::now(),
        });
    }

    // Store the metrics using our custom closure
    let metrics_vec = metrics.lock().unwrap().clone();
    for metric in &metrics_vec {
        let id =
            storage
                .store(metric.clone(), |client, metric| async move {
                    client.query_one(
                "INSERT INTO cpu_metrics (cpu, user_pct, system_pct, idle_pct, timestamp)
                 VALUES ($1, $2, $3, $4, $5) RETURNING id",
                &[
                    &metric.cpu as &(dyn ToSql + Sync),
                    &metric.user as &(dyn ToSql + Sync),
                    &metric.system as &(dyn ToSql + Sync),
                    &metric.idle as &(dyn ToSql + Sync),
                    &metric.timestamp as &(dyn ToSql + Sync),
                ],
            )
            .await
            .map(|row| row.get::<_, i32>(0))
            .map_err(|e| AgentError::Database(format!("Failed to insert CPU metric: {}", e)))
                })
                .await
                .expect("Failed to store metric");

        println!("Stored CPU metric with ID: {}", id);
    }

    // Retrieve metrics
    let retrieved = storage
        .retrieve("cpu0", |client, cpu| async move {
            client
                .query(
                    "SELECT cpu, user_pct, system_pct, idle_pct, timestamp FROM cpu_metrics
             WHERE cpu = $1 ORDER BY timestamp DESC",
                    &[&cpu as &(dyn ToSql + Sync)],
                )
                .await
                .map(|rows| {
                    rows.iter()
                        .map(|row| CpuMetric {
                            cpu: row.get(0),
                            user: row.get(1),
                            system: row.get(2),
                            idle: row.get(3),
                            timestamp: row.get(4),
                        })
                        .collect::<Vec<_>>()
                })
                .map_err(|e| AgentError::Database(format!("Failed to query CPU metrics: {}", e)))
        })
        .await
        .expect("Failed to retrieve metrics");

    println!("Retrieved {} metrics for cpu0", retrieved.len());

    // Display metrics
    for metric in retrieved {
        println!(
            "CPU: {}, User: {:.1}%, System: {:.1}%, Idle: {:.1}%",
            metric.cpu,
            metric.user * 100.0,
            metric.system * 100.0,
            metric.idle * 100.0
        );
    }

    Ok(())
}
