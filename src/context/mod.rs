
use crate::error::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// A provider of context information needed for metric collection
#[async_trait::async_trait]
pub trait ContextProvider: Send + Sync + 'static {
    /// Initialize the context provider
    async fn initialize(&self) -> Result<()> {
        Ok(()) // Default no-op implementation
    }

    /// Update the context
    async fn update(&self) -> Result<()>;

    /// Get the update interval
    fn interval(&self) -> Duration;

    /// Get the provider name
    fn name(&self) -> &str;

    /// Shutdown the provider
    async fn shutdown(&self) -> Result<()> {
        Ok(()) // Default no-op implementation
    }
}

/// A manager for context providers
pub struct ContextManager {
    providers: Vec<Arc<dyn ContextProvider>>,
    running: RwLock<bool>,
}

impl ContextManager {
    /// Create a new context manager
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
            running: RwLock::new(false),
        }
    }

    /// Add a context provider
    pub fn add_provider<P: ContextProvider>(&mut self, provider: P) -> &mut Self {
        self.providers.push(Arc::new(provider));
        self
    }

    /// Start all context providers
    pub async fn start(&self) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>> {
        let mut handles = Vec::new();

        // Mark as running
        let mut running = self.running.write().await;
        *running = true;
        drop(running);

        // Initialize and start each provider
        for provider in &self.providers {
            let provider_clone = Arc::clone(provider);

            // Initialize the provider
            provider_clone.initialize().await?;

            // Start the provider in its own task
            let handle = tokio::spawn({
                let provider = Arc::clone(&provider_clone);
                let running = Arc::clone(&self.running);

                async move {
                    let mut interval_timer = tokio::time::interval(provider.interval());

                    log::info!("Starting context provider: {}", provider.name());

                    // Initialize
                    if let Err(e) = provider.update().await {
                        log::error!("Error during initial context update for {}: {}", provider.name(), e);
                    }

                    // Run update loop
                    while *running.read().await {
                        interval_timer.tick().await;

                        if let Err(e) = provider.update().await {
                            log::error!("Error updating context for {}: {}", provider.name(), e);
                        }
                    }

                    // Shutdown
                    if let Err(e) = provider.shutdown().await {
                        log::error!("Error shutting down context provider {}: {}", provider.name(), e);
                    }

                    Ok(())
                }
            });

            handles.push(handle);
        }

        Ok(handles)
    }

    /// Stop all context providers
    pub async fn stop(&self) -> Result<()> {
        let mut running = self.running.write().await;
        *running = false;
        Ok(())
    }
}
