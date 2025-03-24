use log::{debug, info};
use native_tls::{Certificate, Identity, TlsConnector};
use std::fs;
use std::path::Path;
use std::time::Duration;

use crate::config::{DatabaseConfig, SslMode};
use crate::prelude::{AgentError, Result};
use crate::connection::health::Health;

/// PostgreSQL connection
pub struct PostgresConnection {
    /// Connection string
    config_string: String,
    /// Connection configuration
    config: DatabaseConfig,
}

impl PostgresConnection {
    /// Create a new PostgreSQL connection
    pub fn new(config: DatabaseConfig) -> Self {
        let config_string = format!(
            "host={} port={} dbname={} user={} password={}",
            config.host, config.port, config.name, config.username, config.password
        );

        Self {
            config_string,
            config,
        }
    }

    /// Connect to the database
    pub async fn connect(&self) -> Result<tokio_postgres::Client> {
        debug!("Connecting to database: {}", self.config_string);

        // Create a connection pool
        let pool = deadpool_postgres::Pool::builder()
            .max_size(10)
            .connect_timeout(Duration::from_secs(5))
            .build(self.create_manager()?)
            .map_err(|e| AgentError::Connection(format!("Failed to create connection pool: {}", e)))?;

        // Get a client from the pool
        let client = pool.get().await
            .map_err(|e| AgentError::Connection(format!("Failed to connect to database: {}", e)))?;

        // Test the connection
        client
            .execute("SELECT 1", &[])
            .await
            .map_err(|e| AgentError::Connection(format!("Failed to execute test query: {}", e)))?;

        info!("Connected to database: {}", self.config_string);

        // Return a new client
        pool.get().await
            .map_err(|e| AgentError::Connection(format!("Failed to get client from pool: {}", e)))
    }

    /// Create a connection manager
    fn create_manager(&self) -> Result<deadpool_postgres::Manager<tokio_postgres::NoTls>> {
        let mut config = tokio_postgres::config::Config::new();
        config
            .host(&self.config.host)
            .port(self.config.port)
            .dbname(&self.config.name)
            .user(&self.config.username)
            .password(&self.config.password)
            .connect_timeout(Duration::from_secs(5));

        // Create the manager
        let manager = deadpool_postgres::Manager::new(config, tokio_postgres::NoTls);

        Ok(manager)
    }

    /// Create a TLS connection
    async fn connect_with_tls(&self) -> Result<tokio_postgres::Client> {
        // Build TLS connector
        let connector = match &self.config.ssl_mode {
            SslMode::Disable => {
                return self.connect_without_tls().await;
            }
            _ => {
                // Build TLS connector
                if self.config.ca_cert.is_none() && self.config.client_cert.is_none() {
                    return Err(AgentError::Connection("Invalid SSL mode configuration".to_string()));
                }

                // Build TLS connector
                let mut builder = TlsConnector::builder();
                builder.build().map_err(|e| AgentError::Tls(e.to_string()))?
            }
        };

        // Parse connection string
        let config_string = self.config_string.clone();
        debug!("Connecting to database with TLS: {}", config_string);

        // Connect
        let config = self.config_string.parse::<tokio_postgres::Config>()
            .map_err(|e| AgentError::Connection(format!("Failed to parse connection string: {}", e)))?;

        // Set SSL mode
        let ssl_mode = match self.config.ssl_mode {
            SslMode::Disable => "disable",
            SslMode::Allow => "allow",
            SslMode::Prefer => "prefer",
            SslMode::Require => "require",
            SslMode::VerifyCa => "verify-ca",
            SslMode::VerifyFull => "verify-full",
        };

        // Add SSL mode to config string
        let config_string = format!("{} sslmode={}", config_string, ssl_mode);
        debug!("Connecting with SSL mode: {}", ssl_mode);

        // Parse config
        let config = config_string.parse::<tokio_postgres::Config>()
            .map_err(|e| AgentError::Connection(format!("Failed to parse connection string: {}", e)))?;

        // Create TLS connector
        let tls = tokio_postgres::tls::MakeTlsConnector::new(connector);

        // Connect
        let (client, connection) = tokio_postgres::connect(&config_string, tls)
            .await
            .map_err(|e| AgentError::Connection(format!("Failed to connect to database: {}", e)))?;

        // Spawn connection
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok(client)
    }

    /// Connect without TLS
    async fn connect_without_tls(&self) -> Result<tokio_postgres::Client> {
        // Parse connection string
        let config_string = self.config_string.clone();
        debug!("Connecting to database without TLS: {}", config_string);

        // Connect
        let (client, connection) = tokio_postgres::connect(&config_string, tokio_postgres::NoTls)
            .await
            .map_err(|e| AgentError::Connection(format!("Failed to connect to database: {}", e)))?;

        // Spawn connection
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok(client)
    }
}

impl Health for PostgresConnection {
    async fn check_health(&self, timeout: Duration) -> Result<bool> {
        // Create a simple client
        let client = self.connect().await;

        // Check if connection succeeded
        match client {
            Ok(_) => Ok(true),
            Err(e) => {
                debug!("Health check failed: {}", e);
                Err(AgentError::Connection(format!("Database validation failed: {}", e)))
            }
        }
    }

    async fn check_health_with_timeout(&self, timeout: Duration) -> Result<bool> {
        // Create a timeout
        let timeout_future = tokio::time::sleep(timeout);
        let health_future = self.check_health(timeout);

        // Wait for either the health check or the timeout
        tokio::select! {
            health = health_future => health,
            _ = timeout_future => Err(AgentError::Timeout(format!("Database validation timed out after {} seconds", timeout.as_secs())))
        }
    }
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
        _ => Err(AgentError::Config(format!("Invalid SSL mode: {}", s))),
    }
}

/// Load a certificate from a file
pub fn load_certificate<P: AsRef<Path>>(path: P) -> Result<Certificate> {
    let cert_data = fs::read(path)?;
    Certificate::from_pem(&cert_data).map_err(|e| AgentError::Tls(e.to_string()))
}

/// Load an identity from certificate and key files
pub fn load_identity<P: AsRef<Path>>(cert_path: P, key_path: P, password: &str) -> Result<Identity> {
    let cert_data = fs::read(&cert_path)?;
    let key_data = fs::read(&key_path)?;
    Identity::from_pkcs8(&cert_data, &key_data, password).map_err(|e| AgentError::Tls(e.to_string()))
}
