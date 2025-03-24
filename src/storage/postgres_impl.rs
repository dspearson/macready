use log::{debug, info};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::config::DatabaseConfig;
use crate::connection::postgres::PostgresProvider;
use crate::connection::health::HealthCheck;
use crate::error::{AgentError, Result};
use crate::retry::{execute_with_retry, RetryConfig};

/// PostgreSQL storage that allows maximum flexibility by accepting user-defined handlers
pub struct PostgresStorage<T: Send + Sync + Clone + 'static> {
    /// Postgres connection provider
    provider: Arc<PostgresProvider>,

    /// Name of this storage
    name: String,

    /// Type marker for user's storage type
    _phantom: PhantomData<T>,

    /// Retry configuration
    retry_config: RetryConfig,
}

impl<T: Send + Sync + Clone + 'static> PostgresStorage<T> {
    /// Create a new PostgreSQL storage with a connection configuration
    pub async fn new(db_config: DatabaseConfig, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        let provider = Arc::new(PostgresProvider::new(db_config, format!("{}_provider", &name)).await?);

        Ok(Self {
            provider,
            name,
            _phantom: PhantomData,
            retry_config: RetryConfig::default(),
        })
    }

    /// Set the retry configuration
    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }

    /// Get a raw client from the pool
    pub async fn get_client(&self) -> Result<deadpool_postgres::Client> {
        self.provider.get_client().await
    }

    /// Execute a database operation with retry logic
    pub async fn execute<F, Fut, R>(&self, operation: F, context: &str) -> Result<R>
    where
        F: Fn(deadpool_postgres::Client) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<R>> + Send + 'static,
        R: Send + 'static,
    {
        execute_with_retry(
            || {
                let provider = Arc::clone(&self.provider);
                let operation = operation.clone();  // Clone the operation
                Box::pin(async move {
                    let client = provider.get_client().await?;
                    operation(client).await
                })
            },
            self.retry_config.clone(),
            context,
        ).await
    }

    /// Execute a transaction with retry logic
/// Execute a transaction with retry logic
    pub async fn execute_in_transaction<F, Fut, R>(&self, operation: F, context: &str) -> Result<R>
    where
        F: Fn(&mut deadpool_postgres::Transaction<'_>) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<R>> + Send + 'static,
        R: Send + 'static,
    {
        execute_with_retry(
            || {
                let provider = Arc::clone(&self.provider);
                let operation = operation.clone();
                Box::pin(async move {
                    let mut client = provider.get_client().await?;
                    let mut tx = client.transaction().await
                                                     .map_err(|e| AgentError::Database(format!("Failed to begin transaction: {}", e)))?;

                    match operation(&mut tx).await {
                        Ok(result) => {
                            tx.commit().await
                                       .map_err(|e| AgentError::Database(format!("Failed to commit transaction: {}", e)))?;
                            Ok(result)
                        },
                        Err(e) => {
                            // Try to roll back, but don't mask the original error
                            let _ = tx.rollback().await;
                            Err(e)
                        }
                    }
                })
            },
            self.retry_config.clone(),
            context,
        ).await
    }

    /// Verify database connectivity
    pub async fn check_health(&self) -> Result<bool> {
        HealthCheck::check_health(&*self.provider).await
    }

    /// Get storage name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Execute a raw query
    pub async fn query(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<Vec<tokio_postgres::Row>> {
        self.provider.query(sql, params).await
    }

    /// Execute a statement
    pub async fn execute_sql(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<u64> {
        self.provider.execute(sql, params).await
    }

    /// Execute a query to get a single row
    pub async fn query_one(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<tokio_postgres::Row> {
        self.provider.query_one(sql, params).await
    }

    /// Execute a query to get an optional row
    pub async fn query_opt(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<Option<tokio_postgres::Row>> {
        self.provider.query_opt(sql, params).await
    }
}


/// Extension trait for executing database operations with user-defined functions
pub trait PostgresStorageExt<T: Send + Sync + Clone + 'static> {
    /// Store data with a user-defined function
    async fn store<F, Fut, R>(&self, data: T, store_fn: F) -> Result<R>
    where
        F: Fn(deadpool_postgres::Client, T) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<R>> + Send + 'static,
        R: Send + 'static;

    /// Store data in a transaction with a user-defined function
    async fn store_in_transaction<F, Fut, R>(&self, data: T, store_fn: F) -> Result<R>
    where
        F: Fn(&mut deadpool_postgres::Transaction<'_>, T) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<R>> + Send + 'static,
        R: Send + 'static;

    /// Retrieve data with a user-defined function
    async fn retrieve<F, Fut, R, Q: Send + Sync + Clone + 'static>(&self, query: Q, retrieve_fn: F) -> Result<R>
    where
        F: Fn(deadpool_postgres::Client, Q) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<R>> + Send + 'static,
        R: Send + 'static;

    /// Delete data with a user-defined function
    async fn delete<F, Fut, R, Q: Send + Sync + Clone + 'static>(&self, query: Q, delete_fn: F) -> Result<R>
    where
        F: Fn(deadpool_postgres::Client, Q) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<R>> + Send + 'static,
        R: Send + 'static;
}

impl<T: Send + Sync + Clone + 'static> PostgresStorageExt<T> for PostgresStorage<T> {
    async fn store<F, Fut, R>(&self, data: T, store_fn: F) -> Result<R>
    where
        F: Fn(deadpool_postgres::Client, T) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<R>> + Send + 'static,
        R: Send + 'static,
    {
        self.execute(
            move |client| {
                let data = data.clone();
                let store_fn = store_fn.clone();
                store_fn(client, data)
            },
            "store_data",
        ).await
    }

    async fn store_in_transaction<F, Fut, R>(&self, data: T, store_fn: F) -> Result<R>
    where
        F: Fn(&mut deadpool_postgres::Transaction<'_>, T) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<R>> + Send + 'static,
        R: Send + 'static,
    {
        self.execute_in_transaction(
            move |tx| {
                let data = data.clone();
                let store_fn = store_fn.clone();
                store_fn(tx, data)
            },
            "store_data_in_transaction",
        ).await
    }

    async fn retrieve<F, Fut, R, Q: Send + Sync + Clone + 'static>(&self, query: Q, retrieve_fn: F) -> Result<R>
    where
        F: Fn(deadpool_postgres::Client, Q) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<R>> + Send + 'static,
        R: Send + 'static,
    {
        self.execute(
            move |client| {
                let query = query.clone();
                let retrieve_fn = retrieve_fn.clone();
                retrieve_fn(client, query)
            },
            "retrieve_data",
        ).await
    }

    async fn delete<F, Fut, R, Q: Send + Sync + Clone + 'static>(&self, query: Q, delete_fn: F) -> Result<R>
    where
        F: Fn(deadpool_postgres::Client, Q) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<R>> + Send + 'static,
        R: Send + 'static,
    {
        self.execute(
            move |client| {
                let query = query.clone();
                let delete_fn = delete_fn.clone();
                delete_fn(client, query)
            },
            "delete_data",
        ).await
    }
}

/// Batch processing helpers
pub trait PostgresBatchExt<T: Send + Sync + Clone + 'static> {
    /// Process a batch of items with a user-defined function
    async fn process_batch<F, Fut, R>(&self, items: Vec<T>, batch_size: usize, process_fn: F) -> Result<Vec<R>>
    where
        F: Fn(&mut deadpool_postgres::Transaction<'_>, Vec<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<Vec<R>>> + Send + 'static,
        R: Send + 'static;
}

impl<T: Send + Sync + Clone + 'static> PostgresBatchExt<T> for PostgresStorage<T> {
    async fn process_batch<F, Fut, R>(&self, items: Vec<T>, batch_size: usize, process_fn: F) -> Result<Vec<R>>
    where
        F: Fn(&mut deadpool_postgres::Transaction<'_>, Vec<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<Vec<R>>> + Send + 'static,
        R: Send + 'static,
    {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(items.len());

        // Process in batches
        for chunk in items.chunks(batch_size) {
            let chunk_vec = chunk.to_vec();
            let process_fn = process_fn.clone();

            let chunk_results = self.execute_in_transaction(
                move |tx| {
                    let chunk_vec = chunk_vec.clone();
                    process_fn(tx, chunk_vec)
                },
                "process_batch",
            ).await?;

            results.extend(chunk_results);
        }

        Ok(results)
    }
}
