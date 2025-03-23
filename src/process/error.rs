use std::io;
use std::process::ExitStatus;
use std::time::Duration;
use thiserror::Error;

/// Result type for process operations
pub type ProcessResult<T> = std::result::Result<T, ProcessError>;

/// Errors that can occur during process operations
#[derive(Error, Debug)]
pub enum ProcessError {
    #[error("Failed to spawn process: {0}")]
    SpawnError(#[from] io::Error),

    #[error("Process exited with non-zero status: {0}")]
    NonZeroExit(ExitStatus),

    #[error("Process killed: {0}")]
    ProcessKilled(String),

    #[error("Process timed out after {0:?}")]
    Timeout(Duration),

    #[error("Failed to read from process: {0}")]
    ReadError(io::Error),  // Removed the #[from] attribute here

    #[error("Process output parse error: {0}")]
    ParseError(String),

    #[error("Process terminated unexpectedly")]
    UnexpectedTermination,

    #[error("Failed to restart process after {attempts} attempts: {reason}")]
    RestartFailed {
        attempts: usize,
        reason: String,
    },

    #[error("Process channel error: {0}")]
    ChannelError(String),

    #[error("Other process error: {0}")]
    Other(String),
}

impl From<tokio::sync::oneshot::error::RecvError> for ProcessError {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        ProcessError::ChannelError(err.to_string())
    }
}

impl From<tokio::sync::mpsc::error::SendError<String>> for ProcessError {
    fn from(err: tokio::sync::mpsc::error::SendError<String>) -> Self {
        ProcessError::ChannelError(err.to_string())
    }
}

impl From<ProcessError> for crate::error::AgentError {
    fn from(err: ProcessError) -> Self {
        crate::error::AgentError::Collection(err.to_string())
    }
}

impl From<crate::error::AgentError> for ProcessError {
    fn from(err: crate::error::AgentError) -> Self {
        ProcessError::Other(err.to_string())
    }
}
