use thiserror::Error;
use std::io;
use std::fmt;

/// Primary error type for metrics agent operations
#[derive(Error, Debug)]
pub enum AgentError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    #[error("TLS error: {0}")]
    Tls(#[from] native_tls::Error),

    #[error("Configuration error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("Entity not found: {0}")]
    EntityNotFound(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Buffer overflow: {0}")]
    BufferOverflow(String),

    #[error("Timeout error: {0}")]
    Timeout(String),

    #[error("Parsing error: {0}")]
    Parse(String),

    #[error("Collection error: {0}")]
    Collection(String),

    #[error("Retry error: {context} (attempts: {attempts}): {source}")]
    Retry {
        context: String,
        attempts: usize,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Other error: {0}")]
    Other(String),
}

/// Type alias for results with AgentError
pub type Result<T> = std::result::Result<T, AgentError>;

impl AgentError {
    /// Create a new retry error with context
    pub fn retry<E>(context: impl Into<String>, attempts: usize, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        AgentError::Retry {
            context: context.into(),
            attempts,
            source: Box::new(source),
        }
    }
}

/// Create an AgentError from a string message
impl From<String> for AgentError {
    fn from(s: String) -> Self {
        AgentError::Other(s)
    }
}

/// Create an AgentError from a static string
impl From<&str> for AgentError {
    fn from(s: &str) -> Self {
        AgentError::Other(s.to_string())
    }
}

/// Create from anyhow error
impl From<anyhow::Error> for AgentError {
    fn from(err: anyhow::Error) -> Self {
        AgentError::Other(err.to_string())
    }
}
