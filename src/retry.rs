use log::{debug, warn};
use rand::random;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

use crate::error::{AgentError, Result};

/// Configuration for retry behavior
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: usize,

    /// Initial delay before first retry in milliseconds
    pub initial_delay_ms: u64,

    /// Multiplier for exponential backoff
    pub backoff_factor: f64,

    /// Maximum delay in milliseconds
    pub max_delay_ms: u64,

    /// Whether to add jitter to delays
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay_ms: 100,
            backoff_factor: 1.5,
            max_delay_ms: 30_000, // 30 seconds
            jitter: true,
        }
    }
}

/// Execute a future with retry logic
pub async fn execute_with_retry<F, Fut, T, E>(
    operation: F,
    config: RetryConfig,
    context: &str,
) -> Result<T>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = std::result::Result<T, E>> + Send,
    E: std::error::Error + Send + Sync + 'static,
{
    let mut attempts = 0;
    let mut delay = Duration::from_millis(config.initial_delay_ms);

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(err) => {
                attempts += 1;

                if attempts >= config.max_attempts {
                    return Err(AgentError::retry(context, attempts, err));
                }

                warn!(
                    "{} (attempt {}/{}): {}",
                    context, attempts, config.max_attempts, err
                );

                sleep(delay).await;

                // Calculate next delay with exponential backoff
                let next_delay_ms = (delay.as_millis() as f64 * config.backoff_factor) as u64;

                // Apply jitter if configured
                if config.jitter {
                    delay = Duration::from_millis(
                        next_delay_ms.min(config.max_delay_ms) + random::<u64>() % 100,
                    );
                } else {
                    delay = Duration::from_millis(next_delay_ms.min(config.max_delay_ms));
                }

                debug!("Retrying after {:?} delay", delay);
            }
        }
    }
}

/// A simplified version of execute_with_retry that uses default config
pub async fn retry<F, Fut, T, E>(operation: F, context: &str) -> Result<T>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = std::result::Result<T, E>> + Send,
    E: std::error::Error + Send + Sync + 'static,
{
    execute_with_retry(operation, RetryConfig::default(), context).await
}

/// Builder for custom retry configurations
pub struct RetryBuilder {
    config: RetryConfig,
}

impl RetryBuilder {
    pub fn new() -> Self {
        Self {
            config: RetryConfig::default(),
        }
    }

    pub fn max_attempts(mut self, attempts: usize) -> Self {
        self.config.max_attempts = attempts;
        self
    }

    pub fn initial_delay(mut self, delay_ms: u64) -> Self {
        self.config.initial_delay_ms = delay_ms;
        self
    }

    pub fn backoff_factor(mut self, factor: f64) -> Self {
        self.config.backoff_factor = factor;
        self
    }

    pub fn max_delay(mut self, delay_ms: u64) -> Self {
        self.config.max_delay_ms = delay_ms;
        self
    }

    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.config.jitter = jitter;
        self
    }

    pub fn build(self) -> RetryConfig {
        self.config
    }
}
