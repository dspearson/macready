use super::config::CollectorConfig;
use super::core::{Collector, MetricBatch, MetricType};
use crate::error::Result;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;

/// A collector that streams metrics continuously
#[async_trait::async_trait]
pub trait StreamingCollector: Collector {
    /// Initialise the streaming collector
    async fn init_stream(&self) -> Result<()>;

    /// Clean up resources when done
    async fn cleanup_stream(&self) -> Result<()>;
}

/// A base implementation of a streaming collector
#[allow(dead_code)]
pub struct BaseStreamingCollector<T: MetricType> {
    /// The collector configuration
    config: CollectorConfig,
    /// The initialisation function
    init_fn: Box<
        dyn Fn() -> Pin<Box<dyn std::future::Future<Output = Result<mpsc::Receiver<T>>> + Send>>
            + Send
            + Sync,
    >,
    /// The cleanup function
    cleanup_fn: Box<
        dyn Fn() -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> + Send + Sync,
    >,
    /// Whether the collector is running
    running: Arc<RwLock<bool>>,
}

#[allow(dead_code)]
impl<T: MetricType> BaseStreamingCollector<T> {
    /// Create a new base streaming collector
    pub fn new<I, C>(config: CollectorConfig, init_fn: I, cleanup_fn: C) -> Self
    where
        I: Fn() -> Pin<Box<dyn std::future::Future<Output = Result<mpsc::Receiver<T>>> + Send>>
            + Send
            + Sync
            + 'static,
        C: Fn() -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        Self {
            config,
            init_fn: Box::new(init_fn),
            cleanup_fn: Box::new(cleanup_fn),
            running: Arc::new(RwLock::new(false)),
        }
    }
}

#[async_trait::async_trait]
impl<T: MetricType> Collector for BaseStreamingCollector<T> {
    type MetricType = T;

    async fn start(&self) -> Result<mpsc::Receiver<MetricBatch<Self::MetricType>>> {
        let mut running = self.running.write().await;
        *running = true;
        drop(running);

        // Initialise the stream
        let input_rx = (self.init_fn)().await?;

        // Create the output channel
        let (tx, rx) = mpsc::channel(self.config.buffer_size);
        let source = self.config.name.clone();
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            let mut input_rx = input_rx;

            while let Some(metric) = input_rx.recv().await {
                if !*running.read().await {
                    break;
                }

                // Create a batch with a single metric
                let batch = MetricBatch::new(vec![metric], source.clone());

                if tx.send(batch).await.is_err() {
                    // Channel closed, exit loop
                    break;
                }
            }
        });

        Ok(rx)
    }

    async fn stop(&self) -> Result<()> {
        let mut running = self.running.write().await;
        *running = false;

        // Clean up the stream
        (self.cleanup_fn)().await?;

        Ok(())
    }

    fn name(&self) -> &str {
        &self.config.name
    }
}

#[async_trait::async_trait]
impl<T: MetricType> StreamingCollector for BaseStreamingCollector<T> {
    async fn init_stream(&self) -> Result<()> {
        // This is handled in the start method
        Ok(())
    }

    async fn cleanup_stream(&self) -> Result<()> {
        // This is handled in the stop method
        Ok(())
    }
}
