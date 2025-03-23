use config::{self, File};
use log::{debug, error};
use serde::Deserialize;
use std::path::Path;

use crate::prelude::{AgentError, Result};

/// Database connection configuration
#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    /// Database host
    pub host: String,
    /// Database port
    pub port: u16,
    /// Database name
    pub name: String,
    /// Database username
    pub username: String,
    /// Database password
    pub password: String,
    /// SSL mode
    #[serde(default)]
    pub ssl_mode: SslMode,
    /// CA certificate path
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// Client certificate path
    #[serde(default)]
    pub client_cert: Option<String>,
    /// Client key path
    #[serde(default)]
    pub client_key: Option<String>,
}

/// SSL mode for database connections
#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SslMode {
    /// Disable SSL
    Disable,
    /// Allow SSL
    Allow,
    /// Prefer SSL
    Prefer,
    /// Require SSL
    Require,
    /// Verify CA
    VerifyCa,
    /// Verify full
    VerifyFull,
}

impl Default for SslMode {
    fn default() -> Self {
        SslMode::Disable
    }
}

/// Agent configuration
#[derive(Debug, Deserialize, Clone)]
pub struct AgentConfig {
    /// Database connection configuration
    pub connection: DatabaseConfig,
    /// Collection interval in seconds
    #[serde(default = "default_collection_interval")]
    pub collection_interval: u64,
    /// Storage type
    #[serde(default)]
    pub storage_type: StorageType,
    /// Logging level
    #[serde(default)]
    pub log_level: LogLevel,
}

/// Default collection interval
fn default_collection_interval() -> u64 {
    60
}

/// Storage type
#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum StorageType {
    /// PostgreSQL storage
    Postgres,
    /// In-memory storage
    Memory,
}

impl Default for StorageType {
    fn default() -> Self {
        StorageType::Postgres
    }
}

/// Logging level
#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    /// Error level
    Error,
    /// Warning level
    Warn,
    /// Info level
    Info,
    /// Debug level
    Debug,
    /// Trace level
    Trace,
}

impl Default for LogLevel {
    fn default() -> Self {
        LogLevel::Info
    }
}

/// Load agent configuration from a file
pub fn load_config<P: AsRef<Path>>(path: P) -> Result<AgentConfig> {
    let path = path.as_ref();
    debug!("Loading configuration from {}", path.display());

    // Check if the file exists
    if !path.exists() {
        error!("Configuration file {} does not exist", path.display());
        return Err(AgentError::Config(format!("Configuration file not found: {}", path.display())));
    }

    // Get the file extension
    let extension = match path.extension() {
        Some(ext) => ext.to_string_lossy().to_lowercase(),
        None => {
            error!("Configuration file has no extension");
            return Err(AgentError::Config(format!("Configuration file has no extension: {}", path.display())));
        }
    };

    // Check if the extension is supported and create the appropriate FileFormat
    let format = match extension.as_str() {
        "toml" => config::FileFormat::Toml,
        "json" => config::FileFormat::Json,
        "yaml" | "yml" => config::FileFormat::Yaml,
        format => {
            error!("Unsupported configuration format: {}", format);
            return Err(AgentError::Config(format!("Unsupported config format: {}", format)));
        }
    };

    // Build configuration
    let config = config::Config::builder()
        .add_source(File::with_name(path.to_str().unwrap()).format(format))
        .build()
        .map_err(|e| AgentError::Config(e.to_string()))?;

    // Deserialize configuration
    config.try_deserialize()
        .map_err(|e| AgentError::Config(e.to_string()))
}
