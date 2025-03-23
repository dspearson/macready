use async_trait::async_trait;
use chrono::Utc;
use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls, Statement};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;

use crate::collector::MetricPoint;
use crate::connection::postgres::{PgConfig, PgPool, SslMode};
use crate::entity::Entity;
use crate::error::{AgentError, Result};
use crate::retry::RetryConfig;
use crate::storage::Storage;

enum TlsMode {
    NoTls(NoTls),
    WithTls(MakeTlsConnector),
}

/// Configuration for PostgreSQL storage
#[derive(Debug, Clone)]
pub struct PostgresStorageConfig {
    /// Database connection configuration
    pub connection: PgConfig,

    /// Retry configuration for database operations
    pub retry: RetryConfig,

    /// Table name for storing metrics
    pub metrics_table: String,

    /// Table name for storing entities
    pub entities_table: String,

    /// Batch size for inserting metrics
    pub batch_size: usize,

    /// Whether to create tables if they don't exist
    pub create_tables: bool,

    /// Custom type mapping for entity and metric fields
    pub type_mappings: HashMap<String, String>,

    /// Database connection pool size
    pub pool_size: usize,

    /// Database connection pool recycling method
    pub pool_recycling: deadpool_postgres::RecyclingMethod,
}

impl Default for PostgresStorageConfig {
    fn default() -> Self {
        Self {
            connection: PgConfig::default(),
            retry: RetryConfig::default(),
            metrics_table: "metrics".to_string(),
            entities_table: "entities".to_string(),
            batch_size: 1000,
            create_tables: true,
            type_mappings: HashMap::new(),
            pool_size: 16,
            pool_recycling: deadpool_postgres::RecyclingMethod::Fast,
        }
    }
}

/// Builder for PostgresStorageConfig
pub struct PostgresStorageConfigBuilder {
    config: PostgresStorageConfig,
}

impl PostgresStorageConfigBuilder {
    /// Create a new builder with default config
    pub fn new() -> Self {
        Self {
            config: PostgresStorageConfig::default(),
        }
    }

    /// Set the connection configuration
    pub fn connection(mut self, connection: PgConfig) -> Self {
        self.config.connection = connection;
        self
    }

    /// Set the retry configuration
    pub fn retry(mut self, retry: RetryConfig) -> Self {
        self.config.retry = retry;
        self
    }

    /// Set the metrics table name
    pub fn metrics_table(mut self, table: impl Into<String>) -> Self {
        self.config.metrics_table = table.into();
        self
    }

    /// Set the entities table name
    pub fn entities_table(mut self, table: impl Into<String>) -> Self {
        self.config.entities_table = table.into();
        self
    }

    /// Set the batch size for inserting metrics
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set whether to create tables if they don't exist
    pub fn create_tables(mut self, create: bool) -> Self {
        self.config.create_tables = create;
        self
    }

    /// Add a type mapping
    pub fn type_mapping(mut self, field: impl Into<String>, pg_type: impl Into<String>) -> Self {
        self.config.type_mappings.insert(field.into(), pg_type.into());
        self
    }

    /// Set the pool size
    pub fn pool_size(mut self, size: usize) -> Self {
        self.config.pool_size = size;
        self
    }

    /// Set the pool recycling method
    pub fn pool_recycling(mut self, recycling: deadpool_postgres::RecyclingMethod) -> Self {
        self.config.pool_recycling = recycling;
        self
    }

    /// Build the configuration
    pub fn build(self) -> PostgresStorageConfig {
        self.config
    }
}

/// PostgreSQL schema information
#[derive(Debug, Clone)]
struct PostgresSchema {
    /// Metric columns
    metric_columns: Vec<ColumnInfo>,

    /// Entity columns
    entity_columns: Vec<ColumnInfo>,

    /// Metric value columns
    metric_value_columns: Vec<String>,
}

/// Column information
#[derive(Debug, Clone)]
struct ColumnInfo {
    /// Column name
    name: String,

    /// PostgreSQL type
    pg_type: String,
}

// Function to build TLS connector (since the one in the postgres module isn't accessible)
fn build_tls_connector(config: &PgConfig) -> Result<TlsConnector> {
    let mut builder = TlsConnector::builder();

    let tls_mode = match config.connection.ssl_mode {
        SslMode::Disable => TlsMode::NoTls(NoTls),
        _ => {
            let connector = build_tls_connector(&config.connection)?;
            TlsMode::WithTls(MakeTlsConnector::new(connector))
        }
    };
    match config.ssl_mode {
        SslMode::Require => {
            // Don't verify certificates
            builder.danger_accept_invalid_certs(true);
        },
        SslMode::VerifyCa => {
            // Verify CA but not hostname
            builder.danger_accept_invalid_hostnames(true);
        },
        SslMode::VerifyFull => {
            // Full verification (default)
        },
        SslMode::Disable => {
            // Shouldn't get here
            return Err(AgentError::Connection("Invalid SSL mode configuration".to_string()));
        }
    }

    builder.build().map_err(|e| AgentError::Tls(e.to_string()))
}

/// PostgreSQL storage implementation
pub struct PostgresStorage<M, E>
where
    M: MetricPoint<EntityType = E>,
    E: Entity,
{
    /// Connection pool
    pool: PgPool,

    /// Client connection
    client: Arc<Client>,

    /// Configuration
    config: PostgresStorageConfig,

    /// Storage name
    name: String,

    /// Prepared statements
    statements: HashMap<String, Statement>,

    /// Schema information
    schema: PostgresSchema,

    /// Phantom data for generics
    _metric_type: PhantomData<M>,
    _entity_type: PhantomData<E>,

    /// Database connection pool size
    pub pool_size: usize,

    /// Database connection pool recycling method
    pub pool_recycling: deadpool_postgres::RecyclingMethod,
}

impl<M, E> PostgresStorage<M, E>
where
    M: MetricPoint<EntityType = E>,
    E: Entity,
{
    /// Create a new PostgreSQL storage
    pub async fn new(config: PostgresStorageConfig, name: impl Into<String>) -> Result<Self> {
        // Create the connection pool
        let pool = PgPool::new(&config.connection).await?;

        // Create a direct connection for the schema setup and prepared statements
        let (direct_client, connection) = tokio_postgres::connect(
            &config.connection.connection_string(),
            match config.connection.ssl_mode {
                SslMode::Disable => NoTls,
                _ => {
                    let connector = build_tls_connector(&config.connection)?;
                    MakeTlsConnector::new(connector)
                }
            }
        ).await.map_err(|e| AgentError::Connection(format!("Failed to connect to database: {}", e)))?;

        // Drive the connection
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Database connection error: {}", e);
            }
        });

        let client = Arc::new(direct_client);

        // Initialize schema
        let schema = Self::initialize_schema(&client, &config).await?;

        // Create tables if configured
        if config.create_tables {
            Self::create_tables(&client, &config, &schema).await?;
        }

        // Prepare statements
        let statements = Self::prepare_statements(&client, &config, &schema).await?;

        Ok(Self {
            pool,
            client,
            config,
            name: name.into(),
            statements,
            schema,
            _metric_type: PhantomData,
            _entity_type: PhantomData,
            pool_size: config.pool_size,
            pool_recycling: config.pool_recycling,
        })
    }

    /// Initialize schema information based on metric and entity types
    async fn initialize_schema(_client: &Client, config: &PostgresStorageConfig) -> Result<PostgresSchema> {
        // This would typically involve inspecting the database schema
        // For now, we'll create a basic schema based on the entity and metric types

        // Entity columns
        let mut entity_columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                pg_type: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "name".to_string(),
                pg_type: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "entity_type".to_string(),
                pg_type: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "is_active".to_string(),
                pg_type: "BOOLEAN".to_string(),
            },
            ColumnInfo {
                name: "metadata".to_string(),
                pg_type: "JSONB".to_string(),
            },
            ColumnInfo {
                name: "created_at".to_string(),
                pg_type: "TIMESTAMPTZ".to_string(),
            },
            ColumnInfo {
                name: "updated_at".to_string(),
                pg_type: "TIMESTAMPTZ".to_string(),
            },
        ];

        // Metric columns
        let mut metric_columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                pg_type: "UUID".to_string(),
            },
            ColumnInfo {
                name: "entity_id".to_string(),
                pg_type: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "entity_name".to_string(),
                pg_type: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "collection_method".to_string(),
                pg_type: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "timestamp".to_string(),
                pg_type: "TIMESTAMPTZ".to_string(),
            },
        ];

        // Common metric value columns that we'll assume exist
        // In a real implementation, we'd inspect the metrics to determine these
        let metric_value_columns = vec![
            "value".to_string(),
            "min".to_string(),
            "max".to_string(),
            "avg".to_string(),
            "count".to_string(),
        ];

        // Add value columns to metric columns
        for column in &metric_value_columns {
            metric_columns.push(ColumnInfo {
                name: column.clone(),
                pg_type: "DOUBLE PRECISION".to_string(),
            });
        }

        // Apply custom type mappings from config
        for (field, pg_type) in &config.type_mappings {
            // Update entity columns
            for col in &mut entity_columns {
                if col.name == *field {
                    col.pg_type = pg_type.clone();
                    break;
                }
            }

            // Update metric columns
            for col in &mut metric_columns {
                if col.name == *field {
                    col.pg_type = pg_type.clone();
                    break;
                }
            }
        }

        Ok(PostgresSchema {
            metric_columns,
            entity_columns,
            metric_value_columns,
        })
    }

    /// Create tables if they don't exist
    async fn create_tables(client: &Client, config: &PostgresStorageConfig, schema: &PostgresSchema) -> Result<()> {
        // Create entities table
        let mut entity_columns_sql = String::new();
        for (i, col) in schema.entity_columns.iter().enumerate() {
            if i > 0 {
                entity_columns_sql.push_str(", ");
            }
            entity_columns_sql.push_str(&format!("{} {}", col.name, col.pg_type));
        }

        let create_entities_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY (id))",
            config.entities_table,
            entity_columns_sql
        );

        debug!("Creating entities table with SQL: {}", create_entities_sql);
        client.execute(&create_entities_sql, &[]).await.map_err(|e| {
            AgentError::Database(e.to_string())
        })?;

        // Create metrics table
        let mut metric_columns_sql = String::new();
        for (i, col) in schema.metric_columns.iter().enumerate() {
            if i > 0 {
                metric_columns_sql.push_str(", ");
            }
            metric_columns_sql.push_str(&format!("{} {}", col.name, col.pg_type));
        }

        let create_metrics_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY (id))",
            config.metrics_table,
            metric_columns_sql
        );

        debug!("Creating metrics table with SQL: {}", create_metrics_sql);
        client.execute(&create_metrics_sql, &[]).await.map_err(|e| {
            AgentError::Database(e.to_string())
        })?;

        // Create index on entity_id for metrics table
        let create_index_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_entity_id ON {} (entity_id)",
            config.metrics_table.replace(".", "_"),
            config.metrics_table
        );

        debug!("Creating index with SQL: {}", create_index_sql);
        client.execute(&create_index_sql, &[]).await.map_err(|e| {
            AgentError::Database(e.to_string())
        })?;

        // Create index on timestamp for metrics table
        let create_timestamp_index_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_timestamp ON {} (timestamp)",
            config.metrics_table.replace(".", "_"),
            config.metrics_table
        );

        debug!("Creating timestamp index with SQL: {}", create_timestamp_index_sql);
        client.execute(&create_timestamp_index_sql, &[]).await.map_err(|e| {
            AgentError::Database(e.to_string())
        })?;

        Ok(())
    }

    /// Prepare statements for common operations
    async fn prepare_statements(
        client: &Client,
        config: &PostgresStorageConfig,
        schema: &PostgresSchema,
    ) -> Result<HashMap<String, Statement>> {
        let mut statements = HashMap::new();

        // Insert entity statement
        let mut entity_columns = String::new();
        let mut entity_params = String::new();

        for (i, col) in schema.entity_columns.iter().enumerate() {
            if i > 0 {
                entity_columns.push_str(", ");
                entity_params.push_str(", ");
            }
            entity_columns.push_str(&col.name);
            entity_params.push_str(&format!("${}", i + 1));
        }

        let insert_entity_sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (id) DO UPDATE SET {}",
            config.entities_table,
            entity_columns,
            entity_params,
            // For ON CONFLICT UPDATE, set all columns except id
            schema.entity_columns.iter().enumerate()
                .filter(|(_, col)| col.name != "id")
                .map(|(i, col)| format!("{} = ${}", col.name, i + 1))
                .collect::<Vec<_>>()
                .join(", ")
        );

        debug!("Preparing insert entity statement: {}", insert_entity_sql);

        let stmt = client.prepare(&insert_entity_sql).await.map_err(|e| {
            AgentError::Database(e.to_string())
        })?;

        statements.insert("insert_entity".to_string(), stmt);

        // Insert metric statement
        let mut metric_columns = String::new();
        let mut metric_params = String::new();

        for (i, col) in schema.metric_columns.iter().enumerate() {
            if i > 0 {
                metric_columns.push_str(", ");
                metric_params.push_str(", ");
            }
            metric_columns.push_str(&col.name);
            metric_params.push_str(&format!("${}", i + 1));
        }

        let insert_metric_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            config.metrics_table,
            metric_columns,
            metric_params
        );

        debug!("Preparing insert metric statement: {}", insert_metric_sql);

        let stmt = client.prepare(&insert_metric_sql).await.map_err(|e| {
            AgentError::Database(e.to_string())
        })?;

        statements.insert("insert_metric".to_string(), stmt);

        // Get entity by ID statement
        let get_entity_by_id_sql = format!(
            "SELECT * FROM {} WHERE id = $1",
            config.entities_table
        );

        debug!("Preparing get entity by ID statement: {}", get_entity_by_id_sql);

        let stmt = client.prepare(&get_entity_by_id_sql).await.map_err(|e| {
            AgentError::Database(e.to_string())
        })?;

        statements.insert("get_entity_by_id".to_string(), stmt);

        // Get entity by name statement
        let get_entity_by_name_sql = format!(
            "SELECT * FROM {} WHERE name = $1",
            config.entities_table
        );

        debug!("Preparing get entity by name statement: {}", get_entity_by_name_sql);

        let stmt = client.prepare(&get_entity_by_name_sql).await.map_err(|e| {
            AgentError::Database(e.to_string())
        })?;

        statements.insert("get_entity_by_name".to_string(), stmt);

        // Check if entity exists statement
        let entity_exists_sql = format!(
            "SELECT 1 FROM {} WHERE id = $1 LIMIT 1",
            config.entities_table
        );

        debug!("Preparing entity exists statement: {}", entity_exists_sql);

        let stmt = client.prepare(&entity_exists_sql).await.map_err(|e| {
            AgentError::Database(e.to_string())
        })?;

        statements.insert("entity_exists".to_string(), stmt);

        Ok(statements)
    }

    /// Get the underlying client
    pub fn client(&self) -> Arc<Client> {
        Arc::clone(&self.client)
    }

    /// Convert an entity to database parameters
    fn entity_to_params(&self, entity: &E) -> Vec<Box<dyn tokio_postgres::types::ToSql + Sync>> {
        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>> = Vec::new();

        // Add ID, ensure it's a string
        let id_str = format!("{:?}", entity.id());
        params.push(Box::new(id_str));

        // Add name
        params.push(Box::new(entity.name().to_string()));

        // Add entity type
        params.push(Box::new(entity.entity_type().to_string()));

        // Add is_active
        params.push(Box::new(entity.is_active()));

        // Add metadata as JSON string instead of JSON value directly
        let metadata = serde_json::to_value(entity.to_json()).unwrap_or(serde_json::Value::Null);
        let metadata_str = serde_json::to_string(&metadata).unwrap_or_default();
        params.push(Box::new(metadata_str));

        // Add created_at and updated_at
        let now = Utc::now();
        params.push(Box::new(now));
        params.push(Box::new(now));

        params
    }

    /// Convert a metric to database parameters
    fn metric_to_params(&self, metric: &M) -> Vec<Box<dyn tokio_postgres::types::ToSql + Sync>> {
        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>> = Vec::new();

        // Add ID (generate a new UUID)
        params.push(Box::new(uuid::Uuid::new_v4()));

        // Add entity ID if present, otherwise null
        if let Some(entity_id) = metric.entity_id() {
            let id_str = format!("{:?}", entity_id);
            params.push(Box::new(id_str));
        } else {
            params.push(Box::new(Option::<String>::None));
        }

        // Add entity name
        params.push(Box::new(metric.entity_name().to_string()));

        // Add collection method
        params.push(Box::new(metric.collection_method().to_string()));

        // Add timestamp
        params.push(Box::new(metric.timestamp()));

        // Add values
        let values = metric.values();
        for column in &self.schema.metric_value_columns {
            if let Some(value) = values.get(column) {
                params.push(Box::new(*value as f64));
            } else {
                params.push(Box::new(Option::<f64>::None));
            }
        }

        params
    }
}

#[async_trait]
impl<M, E> Storage for PostgresStorage<M, E>
where
    M: MetricPoint<EntityType = E>,
    E: Entity,
{
    type MetricType = M;
    type EntityType = E;

    async fn store_metric(&self, metric: &Self::MetricType) -> Result<()> {
        // Get a client from the pool
        let pool_client = self.pool.get().await?;

        // Get the prepared statement
        let stmt_opt = self.statements.get("insert_metric").cloned();
        let stmt = match stmt_opt {
            Some(s) => s,
            None => return Err(AgentError::Other("Insert metric statement not prepared".to_string())),
        };

        // Convert the metric to parameters
        let params = self.metric_to_params(metric);

        // Maps from Box<dyn ToSql + Sync> to &(dyn ToSql + Sync)
        let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params.iter()
            .map(AsRef::as_ref)
            .collect();

        // Execute the statement
        pool_client.execute(&stmt, &params_refs[..]).await
            .map_err(|e| AgentError::Database(e.to_string()))?;

        Ok(())
    }

    async fn store_metrics(&self, metrics: &[Self::MetricType]) -> Result<usize> {
        if metrics.is_empty() {
            return Ok(0);
        }

        let batch_size = self.config.batch_size;
        let mut stored_count = 0;

        // Get the prepared statement
        let stmt_opt = self.statements.get("insert_metric").cloned();
        let stmt = match stmt_opt {
            Some(s) => s,
            None => return Err(AgentError::Other("Insert metric statement not prepared".to_string())),
        };

        // Process in batches to avoid excessive memory usage or statement size limits
        for chunk in metrics.chunks(batch_size) {
            // Get a client from the pool
            let pool_client = self.pool.get().await?;

            // Start a transaction for the batch
            let tx = pool_client.transaction().await
                .map_err(|e| AgentError::Database(e.to_string()))?;

            // Insert each metric
            for metric in chunk {
                let params = self.metric_to_params(metric);
                let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params.iter()
                    .map(AsRef::as_ref)
                    .collect();

                tx.execute(&stmt, &params_refs[..]).await
                    .map_err(|e| AgentError::Database(e.to_string()))?;
            }

            // Commit the transaction
            tx.commit().await
                .map_err(|e| AgentError::Database(e.to_string()))?;

            stored_count += chunk.len();
        }

        Ok(stored_count)
    }

    async fn register_entity(&self, entity: &Self::EntityType) -> Result<()> {
        // Get a client from the pool
        let pool_client = self.pool.get().await?;

        // Get the prepared statement
        let stmt_opt = self.statements.get("insert_entity").cloned();
        let stmt = match stmt_opt {
            Some(s) => s,
            None => return Err(AgentError::Other("Insert entity statement not prepared".to_string())),
        };

        // Convert entity to parameters
        let params = self.entity_to_params(entity);
        let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params.iter()
            .map(AsRef::as_ref)
            .collect();

        // Execute statement
        pool_client.execute(&stmt, &params_refs[..]).await
            .map_err(|e| AgentError::Database(e.to_string()))?;

        Ok(())
    }

    async fn update_entity(&self, entity: &Self::EntityType) -> Result<()> {
        // We're using upsert in register_entity, so this is the same operation
        self.register_entity(entity).await
    }

    async fn get_entity(&self, id: &<Self::EntityType as Entity>::Id) -> Result<Self::EntityType> {
        // Get the prepared statement
        let stmt_opt = self.statements.get("get_entity_by_id").cloned();
        let stmt = match stmt_opt {
            Some(s) => s,
            None => return Err(AgentError::Other("Get entity by ID statement not prepared".to_string())),
        };

        let id_str = format!("{:?}", id);

        let row = self.client.query_opt(&stmt, &[&id_str]).await
            .map_err(|e| AgentError::Database(e.to_string()))?;

        match row {
            Some(_row) => {
                // In a real implementation, we'd deserialize the entity from the row
                // For now, we'll just return an error
                Err(AgentError::Other("Entity deserialization not implemented".to_string()))
            },
            None => Err(AgentError::EntityNotFound(format!("Entity with ID {:?} not found", id))),
        }
    }

    async fn get_entity_by_name(&self, name: &str) -> Result<Self::EntityType> {
        // Get the prepared statement
        let stmt_opt = self.statements.get("get_entity_by_name").cloned();
        let stmt = match stmt_opt {
            Some(s) => s,
            None => return Err(AgentError::Other("Get entity by name statement not prepared".to_string())),
        };

        let row = self.client.query_opt(&stmt, &[&name]).await
            .map_err(|e| AgentError::Database(e.to_string()))?;

        match row {
            Some(_row) => {
                // In a real implementation, we'd deserialize the entity from the row
                // For now, we'll just return an error
                Err(AgentError::Other("Entity deserialization not implemented".to_string()))
            },
            None => Err(AgentError::EntityNotFound(format!("Entity with name '{}' not found", name))),
        }
    }

    async fn entity_exists(&self, id: &<Self::EntityType as Entity>::Id) -> Result<bool> {
        // Get the prepared statement
        let stmt_opt = self.statements.get("entity_exists").cloned();
        let stmt = match stmt_opt {
            Some(s) => s,
            None => return Err(AgentError::Other("Entity exists statement not prepared".to_string())),
        };

        let id_str = format!("{:?}", id);

        let row = self.client.query_opt(&stmt, &[&id_str]).await
            .map_err(|e| AgentError::Database(e.to_string()))?;

        Ok(row.is_some())
    }

    async fn health_check(&self) -> Result<bool> {
        // Simple query to check connection health
        let row = self.client.query_one("SELECT 1", &[]).await
            .map_err(|e| AgentError::Database(e.to_string()))?;

        let value: i32 = row.get(0);
        Ok(value == 1)
    }

    fn name(&self) -> &str {
        &self.name
    }
}
