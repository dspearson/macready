
use crate::error::Result;
use tokio::sync::mpsc;

/// Trait for sources that provide raw metrics
#[async_trait::async_trait]
pub trait MetricSource: Send + Sync + 'static {
    /// The type of metric this source produces
    type Output: Send + 'static;

    /// Start collecting metrics
    async fn start(&self) -> Result<mpsc::Receiver<Self::Output>>;

    /// Stop collecting metrics
    async fn stop(&self) -> Result<()>;

    /// Get the source name
    fn name(&self) -> &str;
}
