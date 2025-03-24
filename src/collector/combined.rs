use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use crate::collector::{Collector, MetricBatch, MetricType};
use crate::error::Result;
use crate::source::MetricSource;
use crate::transformer::MetricTransformer;

/// A collector that combines a source and a transformer
pub struct CombinedCollector<I, O>
where
    I: Send + 'static,
    O: MetricType,
{
    /// The source of raw metrics
    source: Box<dyn MetricSource<Output = I>>,

    /// The transformer to convert raw metrics
    transformer: Box<dyn MetricTransformer<I, O>>,

    /// Name of this collector
    name: String,

    /// Running state
    running: Arc<RwLock<bool>>,

    /// Phantom data for input and output types
    _phantom: PhantomData<(I, O)>,
}

impl<I, O> CombinedCollector<I, O>
where
    I: Send + 'static,
    O: MetricType,
{
    /// Create a new combined collector
    pub fn new(
        name: impl Into<String>,
        source: impl MetricSource<Output = I> + 'static,
        transformer: impl MetricTransformer<I, O> + 'static,
    ) -> Self {
        Self {
            source: Box::new(source),
            transformer: Box::new(transformer),
            name: name.into(),
            running: Arc::new(RwLock::new(false)),
            _phantom: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<I, O> Collector for CombinedCollector<I, O>
where
    I: Send + 'static,
    O: MetricType,
{
    type MetricType = O;

    async fn start(&self) -> Result<mpsc::Receiver<MetricBatch<Self::MetricType>>> {
        // Set running state
        let mut running = self.running.write().await;
        *running = true;
        drop(running);

        // Start the source
        let mut source_rx = self.source.start().await?;

        // Create output channel
        let (tx, rx) = mpsc::channel(100);

        // Clone things needed for the task
        let transformer = self.transformer.clone();
        let running = self.running.clone();
        let source_name = self.source.name().to_string();

        // Start the processing task
        tokio::spawn(async move {
            while let Some(input) = source_rx.recv().await {
                // Check if we're still running
                if !*running.read().await {
                    break;
                }

                // Transform the input
                match transformer.transform(input).await {
                    Ok(outputs) => {
                        if !outputs.is_empty() {
                            // Create a batch and send it
                            let batch = MetricBatch::new(outputs, source_name.clone());
                            if tx.send(batch).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Error transforming metrics: {}", e);
                    }
                }
            }
        });

        Ok(rx)
    }

    async fn stop(&self) -> Result<()> {
        // Stop the source
        self.source.stop().await?;

        // Update running state
        let mut running = self.running.write().await;
        *running = false;

        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}
