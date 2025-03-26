// src/error.rs
use std::io;
use thiserror::Error;

// Re-export anyhow's Result type
pub use anyhow::Result;

/// Custom Error type for the macready library
#[derive(Error, Debug)]
pub enum AgentError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Retry error: {0}")]
    Retry(String),

    #[error("Collection error: {0}")]
    Collection(String),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Entity error: {0}")]
    Entity(String),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Metric error: {0}")]
    Metric(String),

    #[error("Process error: {0}")]
    Process(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Timeout error: {0}")]
    Timeout(String),

    #[error("Entity not found: {0}")]
    EntityNotFound(String),

    #[error("Other error: {0}")]
    Other(String),
}
