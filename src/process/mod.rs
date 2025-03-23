//! Safe process management for metric collection
//!
//! This module provides utilities for safely spawning, managing, and processing
//! output from external processes. It's designed to make it easy to collect
//! metrics from command-line tools and system utilities.

mod command;
mod stream;
mod error;

pub use command::{Command, CommandBuilder, ProcessHandle};
pub use stream::{LineStream, RecordStream, StreamParser};
pub use error::{ProcessError, ProcessResult};

/// Trait for sources that provide metrics via external processes
#[async_trait::async_trait]
pub trait ProcessSource: Send + Sync + 'static {
    /// Get the command to execute
    fn command(&self) -> Command;

    /// Process a line of output from the command
    async fn process_line(&self, line: String) -> crate::error::Result<()>;

    /// Handle an error from the process
    async fn handle_error(&self, error: ProcessError) -> crate::error::Result<()>;

    /// Called when the process exits
    async fn on_exit(&self, exit_status: std::process::ExitStatus) -> crate::error::Result<()>;
}

/// Configuration for process execution
#[derive(Debug, Clone)]
pub struct ProcessConfig {
    /// Maximum time to wait for process to start
    pub start_timeout: std::time::Duration,

    /// Maximum time to wait for process to exit
    pub exit_timeout: std::time::Duration,

    /// Whether to restart the process if it exits
    pub restart_on_exit: bool,

    /// Maximum number of restart attempts
    pub max_restart_attempts: usize,

    /// Time to wait between restart attempts
    pub restart_delay: std::time::Duration,

    /// Environment variables to set for the process
    pub environment: std::collections::HashMap<String, String>,

    /// Working directory for the process
    pub working_dir: Option<std::path::PathBuf>,
}

impl Default for ProcessConfig {
    fn default() -> Self {
        Self {
            start_timeout: std::time::Duration::from_secs(10),
            exit_timeout: std::time::Duration::from_secs(10),
            restart_on_exit: false,
            max_restart_attempts: 3,
            restart_delay: std::time::Duration::from_secs(1),
            environment: std::collections::HashMap::new(),
            working_dir: None,
        }
    }
}

/// Spawn a process and process its output
pub async fn spawn_process<S: ProcessSource>(
    source: S,
    config: ProcessConfig,
) -> crate::error::Result<tokio::task::JoinHandle<crate::error::Result<()>>> {
    command::spawn_and_process(source, config).await
}
