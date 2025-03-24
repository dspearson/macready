use chrono::{DateTime, Utc};
use log::{debug, error};
use native_tls::{Certificate, Identity, TlsConnector};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::tls::{MakeTlsConnector, NoTls};
use tokio_postgres::{Client, Statement};
use tokio_postgres::types::{to_sql_checked, ToSql};

use crate::prelude::{AgentError, Result};
use crate::config::{AgentConfig, DatabaseConfig, SslMode};
use crate::entity::Entity;
use crate::storage::{Metric, Storage};

// Define a TlsMode enum to handle both NoTls and MakeTlsConnector
enum TlsMode {
    NoTls(NoTls),
    WithTls(MakeTlsConnector),
}

/// PostgreSQL storage implementation
pub struct PostgresStorage {
    /// PostgreSQL client pool
    pool: deadpool_postgres::Pool,
    /// Statement cache
    statements: Arc<Mutex<HashMap<String, Statement>>>,
}

impl PostgresStorage {
    /// Create a new PostgreSQL storage
    pub async fn new(config: &AgentConfig) -> Result<Self> {
        // Create the postgres config
        let postgres_config = create_postgres_config(&config.connection)?;

        // Create a TLS connector if needed
        let tls_mode = match config.connection.ssl_mode {
            SslMode::Disable => TlsMode::NoTls(NoTls),
            _ => {
                let connector = build_tls_connector(&config.connection)?;
                TlsMode::WithTls(MakeTlsConnector::new(connector))
            }
        };

        // Create the pool manager and connect
        let pool = match tls_mode {
            TlsMode::NoTls(no_tls) => {
                let manager = deadpool_postgres::Manager::new(postgres_config.clone(), no_tls);
                deadpool_postgres::Pool::new(manager, 10)
            },
            TlsMode::WithTls(with_tls) => {
                let manager = deadpool_postgres::Manager::new(postgres_config.clone(), with_tls);
                deadpool_postgres::Pool::new(manager, 10)
            }
        };

        // Test the connection
        let client = pool.get().await
            .map_err(|e| AgentError::Connection(format!("Failed to connect to database: {}", e)))?;

        // Initialize the database
        Self::init_db(&client).await?;

        Ok(Self {
            pool,
            statements: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Initialize the database
    async fn init_db(client: &Client) -> Result<()> {
        // Create the metrics table if it doesn't exist
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS metrics (
                    id SERIAL PRIMARY KEY,
                    entity_id VARCHAR(255) NOT NULL,
                    metric_type VARCHAR(255) NOT NULL,
                    value DOUBLE PRECISION NOT NULL,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    metadata JSONB
                )",
                &[],
            )
            .await?;

        // Create the entities table if it doesn't exist
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS entities (
                    id VARCHAR(255) PRIMARY KEY,
                    entity_type VARCHAR(255) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    metadata JSONB
                )",
                &[],
            )
            .await?;

        // Create indexes
        client
            .execute(
                "CREATE INDEX IF NOT EXISTS metrics_entity_id_idx ON metrics (entity_id)",
                &[],
            )
            .await?;

        client
            .execute(
                "CREATE INDEX IF NOT EXISTS metrics_metric_type_idx ON metrics (metric_type)",
                &[],
            )
            .await?;

        client
            .execute(
                "CREATE INDEX IF NOT EXISTS metrics_timestamp_idx ON metrics (timestamp)",
                &[],
            )
            .await?;

        client
            .execute(
                "CREATE INDEX IF NOT EXISTS entities_entity_type_idx ON entities (entity_type)",
                &[],
            )
            .await?;

        Ok(())
    }

    /// Get or prepare a statement
    async fn get_statement(&self, client: &Client, sql: &str) -> Result<Statement> {
        // Check if we have the statement in the cache
        let mut statements = self.statements.lock().await;
        if let Some(stmt) = statements.get(sql) {
            return Ok(stmt.clone());
        }

        // Prepare the statement
        let stmt = client.prepare(sql).await?;
        statements.insert(sql.to_string(), stmt.clone());
        Ok(stmt)
    }

    /// Convert a metric to parameters
    fn metric_to_params(&self, metric: &Metric) -> Vec<Box<dyn ToSql + Sync + Send>> {
        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();

        // Add the entity ID
        params.push(Box::new(metric.entity_id.clone()));

        // Add the metric type
        params.push(Box::new(metric.metric_type.clone()));

        // Add the value
        params.push(Box::new(metric.value));

        // Add the timestamp
        params.push(Box::new(metric.timestamp));

        // Add the metadata
        params.push(Box::new(
            serde_json::to_value(&metric.metadata).unwrap_or(serde_json::Value::Null),
        ));

        params
    }

    /// Convert an entity to parameters
    fn entity_to_params(&self, entity: &Entity) -> Vec<Box<dyn ToSql + Sync + Send>> {
        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();

        // Add the entity ID
        params.push(Box::new(entity.id.clone()));

        // Add the entity type
        params.push(Box::new(entity.entity_type.clone()));

        // Add the name
        params.push(Box::new(entity.name.clone()));

        // Add the created_at
        params.push(Box::new(entity.created_at));

        // Add the metadata
        params.push(Box::new(
            serde_json::to_value(&entity.metadata).unwrap_or(serde_json::Value::Null),
        ));

        params
    }

    /// Execute a statement with parameters
    async fn execute_with_params(
        client: &Client,
        stmt: &Statement,
        params: Vec<Box<dyn ToSql + Sync + Send>>
    ) -> Result<u64> {
        let params_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();
        client.execute(stmt, &params_refs[..]).await
            .map_err(|e| AgentError::Database(e.to_string()))
    }
}

// This is a marker type to implement the Storage trait for PostgresStorage
// It helps with the associated types in the trait
impl Storage for PostgresStorage {
    type MetricType = Metric;
    type EntityType = Entity;

    async fn store_metric(&self, metric: &Self::MetricType) -> Result<()> {
        // Get a client from the pool
        let pool_client = self.pool.get().await?;

        // Get or prepare the statement
        let stmt = self
            .get_statement(
                &pool_client,
                "INSERT INTO metrics (entity_id, metric_type, value, timestamp, metadata)
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .await?;

        // Convert the metric to parameters
        let params = self.metric_to_params(metric);

        // Execute with parameters
        Self::execute_with_params(&pool_client, &stmt, params).await?;

        Ok(())
    }

    async fn store_metrics(&self, metrics: &[Self::MetricType]) -> Result<usize> {
        if metrics.is_empty() {
            return Ok(0);
        }

        // Start a transaction
        let mut pool_client = self.pool.get().await?;
        let tx = pool_client.transaction().await?;

        // Get or prepare the statement
        let stmt = self
            .get_statement(
                &tx,
                "INSERT INTO metrics (entity_id, metric_type, value, timestamp, metadata)
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .await?;

        let mut stored_count = 0;

        // Store each metric
        for metric in metrics {
            // Convert the metric to parameters
            let params = self.metric_to_params(metric);

            // Execute with parameters
            Self::execute_with_params(&tx, &stmt, params).await?;
            stored_count += 1;
        }

        // Commit the transaction
        tx.commit().await?;

        Ok(stored_count)
    }

    async fn register_entity(&self, entity: &Self::EntityType) -> Result<()> {
        // Get a client from the pool
        let pool_client = self.pool.get().await?;

        // Get or prepare the statement
        let stmt = self
            .get_statement(
                &pool_client,
                "INSERT INTO entities (id, entity_type, name, created_at, metadata)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (id) DO UPDATE SET
                 entity_type = $2,
                 name = $3,
                 metadata = $5",
            )
            .await?;

        // Convert the entity to parameters
        let params = self.entity_to_params(entity);

        // Execute with parameters
        Self::execute_with_params(&pool_client, &stmt, params).await?;

        Ok(())
    }

    async fn get_entity(&self, id: &str) -> Result<Option<Self::EntityType>> {
        // Get a client from the pool
        let pool_client = self.pool.get().await?;

        // Get or prepare the statement
        let stmt = self
            .get_statement(
                &pool_client,
                "SELECT id, entity_type, name, created_at, metadata FROM entities WHERE id = $1",
            )
            .await?;

        // Execute the statement
        let row = pool_client.query_opt(&stmt, &[&id]).await?;

        // Convert the row to an entity
        match row {
            Some(row) => {
                let entity = Entity {
                    id: row.get(0),
                    entity_type: row.get(1),
                    name: row.get(2),
                    created_at: row.get(3),
                    metadata: serde_json::from_value(row.get(4)).unwrap_or_default(),
                };
                Ok(Some(entity))
            }
            None => Ok(None),
        }
    }

    async fn get_metrics(
        &self,
        entity_id: &str,
        metric_type: Option<&str>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> Result<Vec<Self::MetricType>> {
        // Start building the query
        let mut query = "SELECT entity_id, metric_type, value, timestamp, metadata FROM metrics WHERE entity_id = $1".to_string();
        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
        params.push(Box::new(entity_id.to_string()));

        // Add metric type if provided
        if let Some(mt) = metric_type {
            query.push_str(" AND metric_type = $");
            query.push_str(&(params.len() + 1).to_string());
            params.push(Box::new(mt.to_string()));
        }

        // Add start time if provided
        if let Some(st) = start_time {
            query.push_str(" AND timestamp >= $");
            query.push_str(&(params.len() + 1).to_string());
            params.push(Box::new(st));
        }

        // Add end time if provided
        if let Some(et) = end_time {
            query.push_str(" AND timestamp <= $");
            query.push_str(&(params.len() + 1).to_string());
            params.push(Box::new(et));
        }

        // Add ordering
        query.push_str(" ORDER BY timestamp DESC");

        // Add limit if provided
        if let Some(l) = limit {
            query.push_str(" LIMIT $");
            query.push_str(&(params.len() + 1).to_string());
            params.push(Box::new(l as i64));
        }

        // Get a client from the pool
        let pool_client = self.pool.get().await?;

        // Extract references to the parameters for execute
        let params_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

        // Get or prepare the statement
        let stmt = self.get_statement(&pool_client, &query).await?;

        // Execute the statement
        let rows = pool_client.query(&stmt, &params_refs[..]).await?;

        // Convert the rows to metrics
        let mut metrics = Vec::new();
        for row in rows {
            let metric = Metric {
                entity_id: row.get(0),
                metric_type: row.get(1),
                value: row.get(2),
                timestamp: row.get(3),
                metadata: serde_json::from_value(row.get(4)).unwrap_or_default(),
            };
            metrics.push(metric);
        }

        Ok(metrics)
    }
}

/// Create a PostgreSQL config from a database config
fn create_postgres_config(config: &DatabaseConfig) -> Result<tokio_postgres::Config> {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config
        .host(&config.host)
        .port(config.port)
        .dbname(&config.name)
        .user(&config.username)
        .password(&config.password);

    Ok(pg_config)
}

/// Build a TLS connector from a database config
fn build_tls_connector(config: &DatabaseConfig) -> Result<TlsConnector> {
    let mut builder = TlsConnector::builder();

    // Add CA certificate if provided
    if let Some(ca_cert_path) = &config.ca_cert {
        let ca_cert = load_certificate(ca_cert_path)?;
        builder.add_root_certificate(ca_cert);
    }

    // Add client certificate if provided
    if let Some(client_cert_path) = &config.client_cert {
        if let Some(client_key_path) = &config.client_key {
            let identity = load_identity(client_cert_path, client_key_path, "")?;
            builder.identity(identity);
        } else {
            return Err(AgentError::Tls("Client key not provided".to_string()));
        }
    }

    // Set minimum protocol version based on SSL mode
    match config.ssl_mode {
        SslMode::VerifyCa | SslMode::VerifyFull => {
            builder.danger_accept_invalid_certs(false);
        }
        _ => {
            builder.danger_accept_invalid_certs(true);
        }
    }

    builder.build().map_err(|e| AgentError::Tls(e.to_string()))
}

/// Load a certificate from a file
fn load_certificate<P: AsRef<Path>>(path: P) -> Result<Certificate> {
    let cert_data = fs::read(path)?;
    Certificate::from_pem(&cert_data).map_err(|e| AgentError::Tls(e.to_string()))
}

/// Load an identity from certificate and key files
fn load_identity<P: AsRef<Path>>(
    cert_path: P,
    key_path: P,
    password: &str,
) -> Result<Identity> {
    let cert_data = fs::read(cert_path)?;
    let key_data = fs::read(key_path)?;
    Identity::from_pkcs8(&cert_data, &key_data, password).map_err(|e| AgentError::Tls(e.to_string()))
}

/// Parse SSL mode from a string
fn parse_ssl_mode(s: &str) -> Result<SslMode> {
    match s.to_lowercase().as_str() {
        "disable" => Ok(SslMode::Disable),
        "allow" => Ok(SslMode::Allow),
        "prefer" => Ok(SslMode::Prefer),
        "require" => Ok(SslMode::Require),
        "verify-ca" => Ok(SslMode::VerifyCa),
        "verify-full" => Ok(SslMode::VerifyFull),
        _ => Err(AgentError::Config(format!("Invalid SSL mode: {}", s))),
    }
}
