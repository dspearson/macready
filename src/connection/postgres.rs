use log::{debug, info, warn};
use std::pin::Pin;
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::fs;
use std::path::Path;
use anyhow::{Context, Result, anyhow};

use crate::config::{DatabaseConfig, SslMode};
use crate::connection::health::HealthCheck;

/// PostgreSQL connection provider
pub struct PostgresProvider {
    name: String,
    pool: deadpool_postgres::Pool,
}

impl PostgresProvider {
    /// Create a new PostgreSQL connection provider
    pub async fn new(config: DatabaseConfig, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        debug!("Creating PostgreSQL provider: {}", &name);

        // Create the pool
        let pool = Self::create_pool(&config)?;

        // Test connection
        let client = pool
            .get()
            .await
            .context("Failed to connect to database")?;

        // Test basic query
        client
            .execute("SELECT 1", &[])
            .await
            .context("Failed to execute test query")?;

        info!(
            "Successfully connected to PostgreSQL database: {}:{}/{}",
            config.host, config.port, config.name
        );

        Ok(Self { name, pool })
    }

    /// Create a connection pool
    fn create_pool(config: &DatabaseConfig) -> Result<deadpool_postgres::Pool> {
        // Create PostgreSQL config
        let mut pg_config = tokio_postgres::Config::new();
        pg_config
            .host(&config.host)
            .port(config.port)
            .dbname(&config.name) // Changed from database to name
            .user(&config.username)
            .password(&config.password);

        // Create the pool builder based on SSL mode
        let builder = match config.ssl_mode {
            SslMode::Disable => {
                debug!("Creating PostgreSQL pool with SSL disabled");
                let manager = deadpool_postgres::Manager::new(pg_config, tokio_postgres::NoTls);
                deadpool_postgres::Pool::builder(manager)
            }
            _ => {
                debug!(
                    "Creating PostgreSQL pool with SSL enabled (mode: {:?})",
                    config.ssl_mode
                );
                let connector = build_tls_connector(config)?;
                let tls = MakeTlsConnector::new(connector);
                let manager = deadpool_postgres::Manager::new(pg_config, tls);
                deadpool_postgres::Pool::builder(manager)
            }
        };

        // Build the pool
        builder
            .max_size(10)
            .build()
            .context("Failed to create connection pool")
    }

    /// Get a client from the pool
    pub async fn get_client(&self) -> Result<deadpool_postgres::Client> {
        self.pool
            .get()
            .await
            .context("Failed to get client from pool")
    }

    /// Execute a query with error handling
    pub async fn execute(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<u64> {
        let client = self.get_client().await?;
        client
            .execute(sql, params)
            .await
            .context("Query execution error")
    }

    /// Execute a query and get rows
    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<tokio_postgres::Row>> {
        let client = self.get_client().await?;
        client
            .query(sql, params)
            .await
            .context("Query error")
    }

    /// Query for a single row
    pub async fn query_one(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<tokio_postgres::Row> {
        let client = self.get_client().await?;
        client
            .query_one(sql, params)
            .await
            .context("Query one error")
    }

    /// Query for an optional row
    pub async fn query_opt(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Option<tokio_postgres::Row>> {
        let client = self.get_client().await?;
        client
            .query_opt(sql, params)
            .await
            .context("Query opt error")
    }

    /// Begin a transaction
    pub async fn with_transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(
            &'a mut deadpool_postgres::Transaction<'_>,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<R>> + Send + 'a>>,
        R: Send + 'static,
    {
        let mut client = self.get_client().await?;
        let mut tx = client
            .transaction()
            .await
            .context("Failed to begin transaction")?;

        // Execute the function with the transaction
        let result = f(&mut tx).await?;

        // Commit the transaction
        tx.commit()
            .await
            .context("Failed to commit transaction")?;

        Ok(result)
    }

    /// Provider name
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl HealthCheck for PostgresProvider {
    async fn check_health(&self) -> Result<bool> {
        match self.get_client().await {
            Ok(client) => match client.execute("SELECT 1", &[]).await {
                Ok(_) => Ok(true),
                Err(e) => {
                    warn!("Database health check failed: {}", e);
                    Ok(false)
                }
            },
            Err(e) => {
                warn!("Database connection failed: {}", e);
                Ok(false)
            }
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
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
            let identity = load_identity(client_cert_path, client_key_path)?;
            builder.identity(identity);
        } else {
            return Err(anyhow!("Client key not provided"));
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

    builder.build().context("Failed to build TLS connector")
}

/// Load a certificate from a file
pub fn load_certificate<P: AsRef<Path>>(path: P) -> Result<Certificate> {
    let cert_data = fs::read(path).context("Failed to read certificate file")?;
    Certificate::from_pem(&cert_data).context("Invalid certificate format")
}

/// Load an identity from certificate and key files
pub fn load_identity<P: AsRef<Path>>(cert_path: P, key_path: P) -> Result<Identity> {
    let cert_data = fs::read(cert_path).context("Failed to read certificate file")?;
    let key_data = fs::read(key_path).context("Failed to read key file")?;
    Identity::from_pkcs8(&cert_data, &key_data).context("Invalid certificate or key format")
}

/// Parse SSL mode from a string
pub fn parse_ssl_mode(s: &str) -> Result<SslMode> {
    match s.to_lowercase().as_str() {
        "disable" => Ok(SslMode::Disable),
        "allow" => Ok(SslMode::Allow),
        "prefer" => Ok(SslMode::Prefer),
        "require" => Ok(SslMode::Require),
        "verify-ca" => Ok(SslMode::VerifyCa),
        "verify-full" => Ok(SslMode::VerifyFull),
        _ => Err(anyhow!("Invalid SSL mode: {}", s)),
    }
}
