// Example of a simple uptime collector using macready

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use clap::Parser;
use log::{debug, error, info};
use macready::collector::{Collector, CollectorConfig, MetricBatch, PeriodicCollector};
use macready::config::{AgentConfig, load_config};
use macready::error::AgentError;
use macready::error::Result as MacreadyResult;
use macready::storage::postgres_impl::{PostgresStorage, PostgresStorageExt};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_postgres::types::ToSql;

/// Command line arguments for the uptime example
#[derive(Parser, Debug)]
#[command(name = "uptime", about = "Macready uptime monitoring example")]
struct Args {
    /// Path to the configuration file (required)
    #[arg(short, long)]
    config: PathBuf,

    /// Interval between metric collections in seconds
    #[arg(short, long, default_value = "60")]
    interval: u64,
}

// Define an uptime metric
#[derive(Debug, Clone)]
struct UptimeMetric {
    hostname: String,
    uptime_seconds: i64,
    timestamp: chrono::DateTime<Utc>,
}

// Uptime metrics collector using macready's PeriodicCollector
struct UptimeCollector {
    storage: Arc<PostgresStorage<UptimeMetric>>,
    hostname: String,
    interval: Duration,
    config: CollectorConfig,
}

impl UptimeCollector {
    fn new(storage: PostgresStorage<UptimeMetric>, hostname: String, interval_secs: u64) -> Self {
        Self {
            storage: Arc::new(storage),
            hostname,
            interval: Duration::from_secs(interval_secs),
            config: CollectorConfig {
                name: "uptime_collector".to_string(),
                buffer_size: 10,
                interval: Some(Duration::from_secs(interval_secs)),
            },
        }
    }

    // Helper to get the uptime
    async fn get_uptime() -> Result<i64, AgentError> {
        if cfg!(target_os = "linux") {
            // For Linux, use /proc/uptime
            let output = tokio::process::Command::new("cat")
                .arg("/proc/uptime")
                .output()
                .await
                .map_err(|e| AgentError::Process(format!("Failed to execute command: {}", e)))?;

            let output_str = String::from_utf8_lossy(&output.stdout);
            parse_uptime(&output_str)
        } else {
            // For illumos, use kstat
            let output = tokio::process::Command::new("kstat")
                .arg("-p")
                .arg("unix:0:system_misc:boot_time")
                .output()
                .await
                .map_err(|e| AgentError::Process(format!("Failed to execute command: {}", e)))?;

            let output_str = String::from_utf8_lossy(&output.stdout);
            parse_uptime(&output_str)
        }
    }
}

#[async_trait]
impl Collector for UptimeCollector {
    type MetricType = UptimeMetric;

    async fn start(&self) -> MacreadyResult<mpsc::Receiver<MetricBatch<Self::MetricType>>> {
        let (tx, rx) = mpsc::channel::<MetricBatch<Self::MetricType>>(self.config.buffer_size);
        let interval = self.interval;
        let hostname = self.hostname.clone();
        let storage = self.storage.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;

                match Self::get_uptime().await {
                    Ok(uptime_seconds) => {
                        let metric = UptimeMetric {
                            hostname: hostname.clone(),
                            uptime_seconds,
                            timestamp: Utc::now(),
                        };

                        info!("Collected uptime: {} seconds", uptime_seconds);

                        // Store the metric in the database
                        if let Err(e) = storage
                            .store(metric.clone(), |client, metric| async move {
                                client
            .query_one(
                "INSERT INTO uptime_metrics (hostname, uptime_seconds, timestamp)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (hostname)
                 DO UPDATE SET uptime_seconds = $2
                 RETURNING id",
                &[
                    &metric.hostname as &(dyn ToSql + Sync),
                    &metric.uptime_seconds as &(dyn ToSql + Sync),
                    &metric.timestamp as &(dyn ToSql + Sync),
                ],
            )
            .await
            .map(|row| row.get::<_, uuid::Uuid>(0))
            .map_err(|e| AgentError::Database(format!("Failed to insert uptime metric: {}", e)))
                            })
                            .await
                        {
                            error!("Failed to store metric: {}", e);
                        }

                        // Also send the metric to the channel for anyone listening
                        let batch = MetricBatch::new(vec![metric], "uptime_collector".to_string());
                        if tx.send(batch).await.is_err() {
                            // Channel closed, exit loop
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Failed to get uptime: {}", e);
                    }
                }
            }
        });

        Ok(rx)
    }

    async fn stop(&self) -> MacreadyResult<()> {
        // Nothing special to clean up
        Ok(())
    }

    fn name(&self) -> &str {
        &self.config.name
    }
}

#[async_trait]
impl PeriodicCollector for UptimeCollector {
    async fn collect(&self) -> MacreadyResult<Vec<Self::MetricType>> {
        match Self::get_uptime().await {
            Ok(uptime_seconds) => {
                let metric = UptimeMetric {
                    hostname: self.hostname.clone(),
                    uptime_seconds,
                    timestamp: Utc::now(),
                };

                Ok(vec![metric])
            }
            Err(e) => {
                error!("Failed to get uptime: {}", e);
                Ok(vec![])
            }
        }
    }

    fn interval(&self) -> Duration {
        self.interval
    }
}

// Function to parse uptime from command output
fn parse_uptime(line: &str) -> Result<i64, macready::error::AgentError> {
    if cfg!(target_os = "linux") {
        // Linux /proc/uptime format: "43200.42 12345.67"
        // First number is uptime in seconds
        let parts: Vec<&str> = line.split_whitespace().collect();
        if !parts.is_empty() {
            if let Ok(seconds) = parts[0].parse::<f64>() {
                return Ok(seconds as i64);
            }
        }
    } else {
        // illumos kstat output format:
        // "unix:0:system_misc:boot_time    1613445789"
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            if let Ok(boot_time) = parts[1].parse::<i64>() {
                // Calculate uptime as current time - boot time
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;

                return Ok(now - boot_time);
            }
        }
    }

    Err(AgentError::Process("Failed to parse uptime".to_string()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // Verify the configuration file exists
    if !args.config.exists() {
        // Initialize logger with default level for error reporting
        macready::init_logging(&macready::config::LogLevel::Error);
        error!("Configuration file not found: {}", args.config.display());
        return Err(anyhow::anyhow!(
            "Configuration file not found: {}",
            args.config.display()
        ));
    }

    // Load configuration from the specified file
    let config_path = &args.config;
    let config: AgentConfig = match load_config(config_path) {
        Ok(config) => {
            // Initialize logger with config-specified level
            macready::init_logging(&config.log_level);
            info!("Configuration loaded from {}", config_path.display());
            config
        }
        Err(e) => {
            // Initialize logger with default level for error reporting
            macready::init_logging(&macready::config::LogLevel::Error);
            error!("Failed to load configuration: {}", e);
            return Err(anyhow::anyhow!("Failed to load configuration: {}", e));
        }
    };

    info!("Starting uptime metrics collector...");

    // Create storage
    let storage =
        match PostgresStorage::<UptimeMetric>::new(config.connection.clone(), "uptime_metrics")
            .await
        {
            Ok(storage) => {
                info!("Connected to PostgreSQL");
                storage
            }
            Err(e) => {
                error!("Failed to connect to PostgreSQL: {}", e);
                return Err(e.into());
            }
        };

    // Create the uptime metrics table if it doesn't exist
    storage
        .execute_sql(
            "CREATE TABLE IF NOT EXISTS uptime_metrics (
             id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
             hostname TEXT NOT NULL,
             uptime_seconds BIGINT NOT NULL,
             timestamp TIMESTAMPTZ NOT NULL,
             created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
             UNIQUE (hostname)
            )",
            &[],
        )
        .await?;

    // Get the hostname
    let hostname = hostname::get()?.to_string_lossy().into_owned();

    // Create the uptime collector
    let collector = UptimeCollector::new(storage, hostname, args.interval);

    // Start the collector
    info!("Starting uptime collection every {} seconds", args.interval);
    let mut metrics_rx = collector.start().await?;

    // Wait for metrics indefinitely (or until error)
    info!("Waiting for metrics...");
    let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("Failed to create signal handler");

    tokio::select! {
        _ = signal.recv() => {
            info!("Received interrupt signal, shutting down...");
        }
        result = async {
            while let Some(batch) = metrics_rx.recv().await {
                debug!("Received batch of {} metrics", batch.metrics.len());
            }
            Ok::<_, anyhow::Error>(())
        } => {
            if let Err(e) = result {
                error!("Error receiving metrics: {}", e);
            }
        }
    }

    // Stop the collector
    collector.stop().await?;

    info!("Uptime collection completed");

    Ok(())
}
