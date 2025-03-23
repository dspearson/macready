use async_trait::async_trait;
use chrono::Utc;
use macready::prelude::*;
use macready::process::{Command, ProcessConfig, ProcessError, ProcessSource};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;

/// A simple metric for CPU usage
#[derive(Debug, Clone)]
struct CpuMetric {
    cpu: String,
    user: f64,
    system: f64,
    idle: f64,
    timestamp: chrono::DateTime<Utc>,
}

/// A simple CPU entity
#[derive(Debug, Clone)]
struct CpuEntity {
    id: String,
    name: String,
    is_active: bool,
}

impl Entity for CpuEntity {
    type Id = String;

    fn id(&self) -> &Self::Id {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn entity_type(&self) -> &str {
        "cpu"
    }

    fn is_active(&self) -> bool {
        self.is_active
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "name": self.name,
            "is_active": self.is_active,
        })
    }
}

impl MetricPoint for CpuMetric {
    type EntityType = CpuEntity;

    fn entity_id(&self) -> Option<&<Self::EntityType as Entity>::Id> {
        Some(&self.cpu)
    }

    fn entity_name(&self) -> &str {
        &self.cpu
    }

    fn timestamp(&self) -> chrono::DateTime<Utc> {
        self.timestamp
    }

    fn values(&self) -> HashMap<String, i64> {
        let mut values = HashMap::new();
        // Convert to integer percentage for storage
        values.insert("user".to_string(), (self.user * 100.0) as i64);
        values.insert("system".to_string(), (self.system * 100.0) as i64);
        values.insert("idle".to_string(), (self.idle * 100.0) as i64);
        values
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "cpu": self.cpu,
            "user": self.user,
            "system": self.system,
            "idle": self.idle,
            "timestamp": self.timestamp,
        })
    }

    fn collection_method(&self) -> &str {
        "mpstat"
    }
}

/// Process source for mpstat
struct MpstatSource {
    /// Buffer for collected metrics
    metrics: Arc<Mutex<Vec<CpuMetric>>>,
}

impl MpstatSource {
    fn new() -> Self {
        Self {
            metrics: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_metrics(&self) -> Vec<CpuMetric> {
        let metrics = self.metrics.lock().unwrap();
        metrics.clone()
    }
}

#[async_trait]
impl ProcessSource for MpstatSource {
    fn command(&self) -> Command {
        // On Linux, use mpstat command for CPU metrics
        Command::new("mpstat")
            .args(["1", "1", "-P", "ALL"])
            .capture_stdout(true)
            .capture_stderr(true)
    }

    async fn process_line(&self, line: String) -> macready::error::Result<()> {
        // Example mpstat output line:
        // 11:30:38 AM  CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest  %gnice   %idle
        // 11:30:39 AM  all    3.03    0.00    1.01    0.00    0.00    0.00    0.00    0.00    0.00   95.96
        // 11:30:39 AM    0    2.00    0.00    2.00    0.00    0.00    0.00    0.00    0.00    0.00   96.00
        // 11:30:39 AM    1    4.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   96.00

        // Skip header lines and empty lines
        if line.trim().is_empty() || line.contains("CPU") || line.contains("Average") {
            return Ok(());
        }

        // Split the line into fields and parse
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 12 {
            // Not enough fields
            return Ok(());
        }

        // Try to parse the values
        let cpu = fields[2].to_string();
        let user = fields[3].parse::<f64>().unwrap_or(0.0) / 100.0;
        let system = fields[5].parse::<f64>().unwrap_or(0.0) / 100.0;
        let idle = fields[11].parse::<f64>().unwrap_or(0.0) / 100.0;

        // Create the metric
        let metric = CpuMetric {
            cpu,
            user,
            system,
            idle,
            timestamp: Utc::now(),
        };

        // Store the metric
        let mut metrics = self.metrics.lock().unwrap();
        metrics.push(metric);

        Ok(())
    }

    async fn handle_error(&self, error: ProcessError) -> macready::error::Result<()> {
        eprintln!("Process error: {}", error);
        Ok(())
    }

    async fn on_exit(&self, exit_status: std::process::ExitStatus) -> macready::error::Result<()> {
        println!("Process exited with status: {}", exit_status);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> macready::error::Result<()> {
    // Initialize logger
    env_logger::init();

    println!("Starting process metrics example...");

    // Create the mpstat source
    let source = MpstatSource::new();
    let source_clone = source;

    // Create a process config
    let config = ProcessConfig {
        start_timeout: Duration::from_secs(5),
        exit_timeout: Duration::from_secs(5),
        restart_on_exit: false,
        max_restart_attempts: 3,
        restart_delay: Duration::from_secs(1),
        environment: HashMap::new(),
        working_dir: None,
    };

    // Spawn the process
    let handle = macready::process::spawn_process(source, config).await?;

    // Wait for the process to complete
    println!("Waiting for metrics collection to complete...");
    sleep(Duration::from_secs(5)).await;

    // Get the collected metrics
    let metrics = source_clone.get_metrics();
    println!("Collected {} metrics:", metrics.len());

    for metric in metrics {
        println!(
            "CPU: {}, User: {:.2}%, System: {:.2}%, Idle: {:.2}%",
            metric.cpu,
            metric.user * 100.0,
            metric.system * 100.0,
            metric.idle * 100.0
        );
    }

    // Wait for the process to complete
    if let Err(e) = handle.await.unwrap() {
        eprintln!("Process error: {}", e);
    }

    Ok(())
}
