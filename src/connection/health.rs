use async_trait::async_trait;
use log::{debug, error, info, trace, warn};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use anyhow::Result;

/// Trait for health check functionality
#[async_trait]
pub trait HealthCheck: Send + Sync + 'static {
    /// Perform a health check
    async fn check_health(&self) -> Result<bool>;

    /// Get the name of this health check
    fn name(&self) -> &str;
}

/// Common health check status
pub struct HealthStatus {
    /// Is the connection healthy
    healthy: AtomicBool,

    /// Name of the connection
    name: String,
}

impl HealthStatus {
    /// Create a new health status
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            healthy: AtomicBool::new(true), // Assume healthy initially
            name: name.into(),
        }
    }

    /// Check if the connection is healthy
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// Set the health status
    pub fn set_healthy(&self, healthy: bool) {
        self.healthy.store(healthy, Ordering::Relaxed);
    }

    /// Get the name
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Configuration for health checking
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Check interval in seconds
    pub interval: u64,

    /// How many consecutive failures to mark as unhealthy
    pub failure_threshold: usize,

    /// How many consecutive successes to mark as healthy again
    pub recovery_threshold: usize,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval: 30,
            failure_threshold: 3,
            recovery_threshold: 2,
        }
    }
}

/// Start a background health check task
pub fn start_health_check<H: HealthCheck>(
    checker: Arc<H>,
    status: Arc<HealthStatus>,
    config: HealthCheckConfig,
) -> mpsc::Receiver<HealthEvent> {
    // Channel for health status notifications
    let (tx, rx) = mpsc::channel(10);

    tokio::spawn(async move {
        run_health_checks(checker, status, config, tx).await;
    });

    rx
}

/// Health check events
#[derive(Debug, Clone)]
pub enum HealthEvent {
    /// Connection is healthy
    Healthy { name: String },

    /// Connection is unhealthy
    Unhealthy { name: String, reason: String },
}

/// Run health check loop
async fn run_health_checks<H: HealthCheck>(
    checker: Arc<H>,
    status: Arc<HealthStatus>,
    config: HealthCheckConfig,
    tx: mpsc::Sender<HealthEvent>,
) {
    let name = checker.name().to_string();
    let mut check_interval = interval(Duration::from_secs(config.interval));

    let mut consecutive_failures = 0;
    let mut consecutive_successes = 0;
    let mut current_healthy = status.is_healthy();

    info!(
        "Starting health checks for {} (interval: {}s)",
        name, config.interval
    );

    loop {
        check_interval.tick().await;

        match checker.check_health().await {
            Ok(true) => {
                consecutive_failures = 0;
                consecutive_successes += 1;
                trace!("Health check passed for {}", name);

                // If we've recovered after being unhealthy
                if !current_healthy && consecutive_successes >= config.recovery_threshold {
                    info!("Connection {} has recovered", name);
                    status.set_healthy(true);
                    current_healthy = true;

                    // Send recovery event
                    let _ = tx.send(HealthEvent::Healthy { name: name.clone() }).await;
                }
            }
            Ok(false) => {
                handle_failed_check(
                    &name,
                    "Check returned false",
                    &status,
                    &mut current_healthy,
                    &mut consecutive_failures,
                    &mut consecutive_successes,
                    &config,
                    &tx,
                )
                .await;
            }
            Err(e) => {
                let reason = format!("Health check error: {}", e);
                handle_failed_check(
                    &name,
                    &reason,
                    &status,
                    &mut current_healthy,
                    &mut consecutive_failures,
                    &mut consecutive_successes,
                    &config,
                    &tx,
                )
                .await;
            }
        }
    }
}

/// Handle a failed health check
async fn handle_failed_check(
    name: &str,
    reason: &str,
    status: &HealthStatus,
    current_healthy: &mut bool,
    consecutive_failures: &mut usize,
    consecutive_successes: &mut usize,
    config: &HealthCheckConfig,
    tx: &mpsc::Sender<HealthEvent>,
) {
    *consecutive_successes = 0;
    *consecutive_failures += 1;

    let failure_count = *consecutive_failures;

    if failure_count == 1 {
        // First failure
        debug!("Health check failed for {}: {}", name, reason);
    } else {
        warn!(
            "Health check failed for {} ({} consecutive failures): {}",
            name, failure_count, reason
        );
    }

    // Check if we need to mark as unhealthy
    if *current_healthy && failure_count >= config.failure_threshold {
        error!(
            "Connection {} is now marked as unhealthy after {} consecutive failures",
            name, failure_count
        );
        status.set_healthy(false);
        *current_healthy = false;

        // Send unhealthy event
        let _ = tx
            .send(HealthEvent::Unhealthy {
                name: name.to_string(),
                reason: reason.to_string(),
            })
            .await;
    }
}

/// Generic implementation of health check for any type with a check_health method
pub struct GenericHealthCheck<T> {
    inner: Arc<T>,
    name: String,
}

impl<T> GenericHealthCheck<T> {
    pub fn new(inner: Arc<T>, name: impl Into<String>) -> Self {
        Self {
            inner,
            name: name.into(),
        }
    }
}

#[async_trait]
impl<T> HealthCheck for GenericHealthCheck<T>
where
    T: Send + Sync + 'static,
    T: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool>> + Send>>,
{
    async fn check_health(&self) -> Result<bool> {
        (self.inner)().await
    }

    fn name(&self) -> &str {
        &self.name
    }
}
