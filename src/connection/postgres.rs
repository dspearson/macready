use log::{debug, error, info, trace};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tokio_postgres::{Client, NoTls};

use crate::error::{AgentError, Result};
use crate::retry::{execute_with_retry, RetryConfig};

/// SSL mode for PostgreSQL connections
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    /// No SSL
    Disable,

    /// Require SSL but don't verify certificate
    Require,

    /// Require SSL and verify certificate
    VerifyCa,

    /// Require SSL and verify certificate hostname matches
    VerifyFull,
}

impl SslMode {
    /// Parse SSL mode from string
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "disable" => Ok(SslMode::Disable),
            "require" => Ok(SslMode::Require),
            "verify-ca" => Ok(SslMode::VerifyCa),
            "verify-full" => Ok(SslMode::VerifyFull),
            _ => Err(AgentError::Config(format!("Invalid SSL mode: {}", s).into())),
        }
    }

    /// Convert to string
    pub fn as_str(&self) -> &'static str {
        match self {
            SslMode::Disable => "disable",
            SslMode::Require => "require",
            SslMode::VerifyCa => "verify-ca",
            SslMode::VerifyFull => "verify-full",
        }
    }
}

/// Configuration for PostgreSQL connection
#[derive(Debug, Clone)]
pub struct PgConfig {
    /// Username for authentication
    pub username: String,

    /// Password for authentication
    pub password: String,

    /// Database host(s)
    pub hosts: Vec<String>,

    /// Database port
    pub port: u16,

    /// Database name
    pub database: String,

    /// SSL mode
    pub ssl_mode: SslMode,

    /// Connection timeout in seconds
    pub connect_timeout: u64,

    /// Application name
    pub application_name: String,
}

impl Default for PgConfig {
    fn default() -> Self {
        Self {
            username: "postgres".to_string(),
            password: "postgres".to_string(),
            hosts: vec!["localhost".to_string()],
            port: 5432,
            database: "postgres".to_string(),
            ssl_mode: SslMode::Disable,
            connect_timeout: 10,
            application_name: "macready".to_string(),
        }
    }
}

impl PgConfig {
    /// Create a new connection config
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the connection string
    pub fn connection_string(&self) -> String {
        // Join multiple hosts with commas
        let hosts_with_ports: Vec<String> = self
            .hosts
            .iter()
            .map(|host| format!("{}:{}", host, self.port))
            .collect();

        let hosts = hosts_with_ports.join(",");

        // Build the connection string
        let mut conn_str = format!(
            "postgresql://{}:{}@{}/{}",
            self.username, self.password, hosts, self.database
        );

        // Add options
        conn_str.push_str(&format!("?application_name={}", self.application_name));
        conn_str.push_str(&format!("&connect_timeout={}", self.connect_timeout));

        conn_str
    }
}

/// Establish a connection to PostgreSQL
pub async fn establish_connection(config: &PgConfig) -> Result<Arc<Client>> {
    match config.ssl_mode {
        SslMode::Disable => connect_without_tls(config).await,
        _ => connect_with_tls(config).await,
    }
}

/// Connect without TLS
async fn connect_without_tls(config: &PgConfig) -> Result<Arc<Client>> {
    debug!("Connecting to database without TLS");

    let conn_str = config.connection_string();

    // Connect without TLS
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .map_err(|e| AgentError::Connection(format!("Failed to connect to database: {}", e)))?;

    spawn_connection_handler(connection);

    // Wrap the client in an Arc
    let client_arc = Arc::new(client);

    // Validate the connection before returning
    validate_connection(Arc::clone(&client_arc)).await?;

    Ok(client_arc)
}

/// Connect with TLS
async fn connect_with_tls(config: &PgConfig) -> Result<Arc<Client>> {
    debug!("Connecting to database with TLS (sslmode={})", config.ssl_mode.as_str());

    // Set up TLS
    let tls_connector = build_tls_connector(config)?;
    let tls = MakeTlsConnector::new(tls_connector);

    let conn_str = config.connection_string();

    // Connect with TLS
    let (client, connection) = tokio_postgres::connect(&conn_str, tls)
        .await
        .map_err(|e| AgentError::Connection(format!("Failed to connect to database with TLS: {}", e)))?;

    spawn_connection_handler(connection);

    // Wrap the client in an Arc
    let client_arc = Arc::new(client);

    // Validate the connection before returning
    validate_connection(Arc::clone(&client_arc)).await?;

    Ok(client_arc)
}

/// Build a TLS connector based on the configuration
fn build_tls_connector(config: &PgConfig) -> Result<TlsConnector> {
    let mut tls_builder = TlsConnector::builder();

    match config.ssl_mode {
        SslMode::Require => {
            // Don't verify certificates
            tls_builder = tls_builder.danger_accept_invalid_certs(true);
        },
        SslMode::VerifyCa => {
            // Verify CA but not hostname
            tls_builder = tls_builder.danger_accept_invalid_hostnames(true);
        },
        SslMode::VerifyFull => {
            // Full verification (default)
        },
        SslMode::Disable => {
            // Shouldn't get here
            return Err(AgentError::Connection("Invalid SSL mode configuration".to_string()));
        }
    }

    tls_builder.build().map_err(|e| AgentError::Tls(e))
}

/// Spawn a task to handle the connection
fn spawn_connection_handler(
    connection: impl Future<Output = Result<(), tokio_postgres::Error>> + Send + 'static,
) {
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Database connection error: {}", e);
        }
    });
}

/// Validate a database connection
pub async fn validate_connection(client: Arc<Client>) -> Result<()> {
    debug!("Validating database connection...");

    // Set a short timeout for the validation query
    let timeout = Duration::from_secs(10);

    // Run a simple query with timeout
    match time::timeout(timeout, client.query_one("SELECT 1", &[])).await {
        Ok(Ok(_)) => {
            debug!("Database connection is valid");
            Ok(())
        }
        Ok(Err(e)) => {
            error!("Database validation failed: {}", e);
            Err(AgentError::Connection(format!("Database validation failed: {}", e)))
        }
        Err(_) => {
            error!("Database validation timed out after {} seconds", timeout.as_secs());
            Err(AgentError::Timeout(format!("Database validation timed out after {} seconds", timeout.as_secs())))
        }
    }
}

/// Builder for PostgreSQL configuration
pub struct PgConfigBuilder {
    config: PgConfig,
}

impl PgConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: PgConfig::default(),
        }
    }

    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.config.username = username.into();
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.config.password = password.into();
        self
    }

    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.config.hosts = vec![host.into()];
        self
    }

    pub fn hosts(mut self, hosts: Vec<String>) -> Self {
        self.config.hosts = hosts;
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }

    pub fn database(mut self, db: impl Into<String>) -> Self {
        self.config.database = db.into();
        self
    }

    pub fn ssl_mode(mut self, mode: SslMode) -> Self {
        self.config.ssl_mode = mode;
        self
    }

    pub fn connect_timeout(mut self, seconds: u64) -> Self {
        self.config.connect_timeout = seconds;
        self
    }

    pub fn application_name(mut self, name: impl Into<String>) -> Self {
        self.config.application_name = name.into();
        self
    }

    pub fn build(self) -> PgConfig {
        self.config
    }
}

impl From<&str> for PgConfig {
    /// Create a PgConfig from a connection string
    /// Note: This is a simplified implementation that doesn't handle all possible options
    fn from(conn_str: &str) -> Self {
        let mut config = PgConfig::default();

        // Extract username, password, host, port, and database from the connection string
        // This is a very simplified parser that doesn't handle all cases
        if let Some(auth_part) = conn_str.strip_prefix("postgresql://") {
            // Split by @ to separate credentials and host parts
            let parts: Vec<&str> = auth_part.split('@').collect();
            if parts.len() >= 2 {
                // Handle credentials
                let creds: Vec<&str> = parts[0].split(':').collect();
                if creds.len() >= 2 {
                    config.username = creds[0].to_string();
                    config.password = creds[1].to_string();
                }

                // Handle host, port, and database
                let host_db: Vec<&str> = parts[1].split('/').collect();
                if !host_db.is_empty() {
                    let host_port: Vec<&str> = host_db[0].split(':').collect();
                    if !host_port.is_empty() {
                        config.hosts = vec![host_port[0].to_string()];

                        if host_port.len() > 1 {
                            if let Ok(port) = host_port[1].parse::<u16>() {
                                config.port = port;
                            }
                        }
                    }

                    if host_db.len() > 1 {
                        // Handle params after ?
                        let db_params: Vec<&str> = host_db[1].split('?').collect();
                        config.database = db_params[0].to_string();

                        // Handle parameters
                        if db_params.len() > 1 {
                            for param in db_params[1].split('&') {
                                let kv: Vec<&str> = param.split('=').collect();
                                if kv.len() == 2 {
                                    match kv[0] {
                                        "sslmode" => {
                                            if let Ok(mode) = SslMode::from_str(kv[1]) {
                                                config.ssl_mode = mode;
                                            }
                                        },
                                        "connect_timeout" => {
                                            if let Ok(timeout) = kv[1].parse::<u64>() {
                                                config.connect_timeout = timeout;
                                            }
                                        },
                                        "application_name" => {
                                            config.application_name = kv[1].to_string();
                                        },
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        config
    }
}
