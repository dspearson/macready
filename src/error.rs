use std::error::Error as StdError;
use std::fmt;

/// Result type for agent operations
pub type Result<T> = std::result::Result<T, AgentError>;

/// Agent error
#[derive(Debug, Clone)]  // Added Clone trait
pub enum AgentError {
    /// Database error
    Database(String),
    /// Retry error
    Retry(String),
    /// Collection error
    Collection(String),
    /// TLS error
    Tls(String),
    /// Storage error
    Storage(String),
    /// Entity error
    Entity(String),
    /// Config error
    Config(String),
    /// Metric error
    Metric(String),
    /// Process error
    Process(String),
    /// General error
    General(String),
    /// Connection error - for database connection issues
    Connection(String),
    /// Timeout error
    Timeout(String),
    /// Other general error
    Other(String),
    /// Entity not found error
    EntityNotFound(String),
}

impl AgentError {
    /// Create a retry error
    pub fn retry(context: &str, attempts: usize, err: impl fmt::Display) -> Self {
        AgentError::Retry(format!("{} failed after {} attempts: {}", context, attempts, err))
    }
}

impl fmt::Display for AgentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AgentError::Database(source) => write!(f, "Database error: {}", source),
            AgentError::Retry(source) => write!(f, "Retry error: {}", source),
            AgentError::Collection(source) => write!(f, "Collection error: {}", source),
            AgentError::Tls(source) => write!(f, "TLS error: {}", source),
            AgentError::Storage(source) => write!(f, "Storage error: {}", source),
            AgentError::Entity(source) => write!(f, "Entity error: {}", source),
            AgentError::Config(source) => write!(f, "Config error: {}", source),
            AgentError::Metric(source) => write!(f, "Metric error: {}", source),
            AgentError::Process(source) => write!(f, "Process error: {}", source),
            AgentError::General(source) => write!(f, "General error: {}", source),
            AgentError::Connection(source) => write!(f, "Connection error: {}", source),
            AgentError::Timeout(source) => write!(f, "Timeout error: {}", source),
            AgentError::Other(source) => write!(f, "Other error: {}", source),
            AgentError::EntityNotFound(source) => write!(f, "Entity not found: {}", source),
        }
    }
}

impl StdError for AgentError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

impl From<tokio_postgres::Error> for AgentError {
    fn from(err: tokio_postgres::Error) -> Self {
        AgentError::Database(err.to_string())
    }
}

impl From<native_tls::Error> for AgentError {
    fn from(err: native_tls::Error) -> Self {
        AgentError::Tls(err.to_string())
    }
}

impl From<std::io::Error> for AgentError {
    fn from(err: std::io::Error) -> Self {
        AgentError::General(err.to_string())
    }
}

impl From<config::ConfigError> for AgentError {
    fn from(err: config::ConfigError) -> Self {
        AgentError::Config(err.to_string())
    }
}
