use super::core::{Collector, MetricBatch, MetricType};
use super::config::CollectorConfig;
use crate::error::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time;

/// A collector that polls for metrics at a fixed interval
#[async_trait::async_trait]
pub trait PeriodicCollector: Collector {
    /// Collect a batch of metrics
    async fn collect(&self) -> Result<Vec<Self::MetricType>>;

    /// Get the collection interval
    fn interval(&self) -> Duration;
}

/// A base implementation of a periodic collector
pub struct BasePeriodicCollector<T: MetricType, F>
where
    F: Fn() -> std::pin::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<T>>> + Send>> + Send + Sync + 'static,
{
    /// The collector configuration
    config: CollectorConfig,
    /// The collection function
    collect_fn: F,
    /// Whether the collector is running
    running: RwLock<bool>,
}

impl<T: MetricType, F> BasePeriodicCollector<T, F>
where
    F: Fn() -> std::pin::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<T>>> + Send>> + Send + Sync + 'static,
{
    /// Create a new base periodic collector
    pub fn new(config: CollectorConfig, collect_fn: F) -> Self {
        Self {
            config,
            collect_fn,
            running: RwLock::new(false),
        }
    }
}

#[async_trait::async_trait]
impl<T: MetricType, F> Collector for BasePeriodicCollector<T, F>
where
    F: Fn() -> std::pin::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<T>>> + Send>> + Send + Sync + 'static,
{
    type MetricType = T;

    async fn start(&self) -> Result<mpsc::Receiver<MetricBatch<Self::MetricType>>> {
        let mut running = self.running.write().await;
        *running = true;
        drop(running);

        let (tx, rx) = mpsc::channel(self.config.buffer_size);
        let collect_fn = &self.collect_fn;
        let running = self.running.clone();  // Clone the RwLock, not trying to Arc::clone it
        let source = self.config.name.clone();
        let interval = self.interval();

        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval);

            while *running.read().await {
                interval_timer.tick().await;

                match collect_fn().await {
                    Ok(metrics) => {
                        if !metrics.is_empty() {
                            let batch = MetricBatch::new(metrics, source.clone());
                            if tx.send(batch).await.is_err() {
                                // Channel closed, exit loop
                                break;
                            }
                        }
                    },
                    Err(e) => {
                        log::error!("Error collecting metrics: {}", e);
                        // Continue collecting despite errors
                    }
                }
            }
        });

        Ok(rx)
    }

    async fn stop(&self) -> Result<()> {
        let mut running = self.running.write().await;
        *running = false;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.config.name
    }
}

#[async_trait::async_trait]
impl<T: MetricType, F> PeriodicCollector for BasePeriodicCollector<T, F>
where
    F: Fn() -> std::pin::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<T>>> + Send>> + Send + Sync + 'static,
{
    async fn collect(&self) -> Result<Vec<Self::MetricType>> {
        (self.collect_fn)().await
    }

    fn interval(&self) -> Duration {
        self.config.interval.unwrap_or_else(|| Duration::from_secs(60))
    }
}
