use std::collections::HashMap;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::sync::Arc;
use std::time::Duration;

use log::{debug, error, trace, warn};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command as TokioCommand};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::error::{Result, AgentError};
use crate::process::{ProcessConfig, ProcessError, ProcessResult, ProcessSource};

/// Command wrapper for process execution
#[derive(Debug, Clone)]
pub struct Command {
    /// Program to execute
    program: String,

    /// Arguments to pass to the program
    args: Vec<String>,

    /// Whether to capture stdout
    capture_stdout: bool,

    /// Whether to capture stderr
    capture_stderr: bool,

    /// Current working directory
    current_dir: Option<PathBuf>,

    /// Environment variables
    env_vars: HashMap<String, String>,
}

impl Command {
    /// Create a new command
    pub fn new<S: Into<String>>(program: S) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            capture_stdout: false,
            capture_stderr: false,
            current_dir: None,
            env_vars: HashMap::new(),
        }
    }

    /// Add an argument
    pub fn arg<S: Into<String>>(mut self, arg: S) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Add multiple arguments
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for arg in args {
            self.args.push(arg.into());
        }
        self
    }

    /// Set whether to capture stdout
    pub fn capture_stdout(mut self, capture: bool) -> Self {
        self.capture_stdout = capture;
        self
    }

    /// Set whether to capture stderr
    pub fn capture_stderr(mut self, capture: bool) -> Self {
        self.capture_stderr = capture;
        self
    }

    /// Set the current working directory
    pub fn current_dir<P: AsRef<Path>>(mut self, dir: P) -> Self {
        self.current_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    /// Add an environment variable
    pub fn env<K, V>(mut self, key: K, val: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.env_vars.insert(key.into(), val.into());
        self
    }

    /// Add multiple environment variables
    pub fn envs<I, K, V>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (key, val) in vars {
            self.env_vars.insert(key.into(), val.into());
        }
        self
    }

    /// Execute the command and return a handle to the process
    pub async fn spawn(&self) -> ProcessResult<ProcessHandle> {
        debug!("Spawning command: {} {:?}", self.program, self.args);

        let mut cmd = TokioCommand::new(&self.program);
        cmd.args(&self.args);

        if let Some(dir) = &self.current_dir {
            cmd.current_dir(dir);
        }

        for (key, val) in &self.env_vars {
            cmd.env(key, val);
        }

        if self.capture_stdout {
            cmd.stdout(Stdio::piped());
        }

        if self.capture_stderr {
            cmd.stderr(Stdio::piped());
        }

        let child = cmd.spawn().map_err(ProcessError::SpawnError)?;

        Ok(ProcessHandle {
            child,
            program: self.program.clone(),
            args: self.args.clone(),
            capture_stdout: self.capture_stdout,
            capture_stderr: self.capture_stderr,
        })
    }
}

/// Builder for commands
pub struct CommandBuilder {
    /// Program to execute
    program: String,

    /// Arguments to pass to the program
    args: Vec<String>,

    /// Whether to capture stdout
    capture_stdout: bool,

    /// Whether to capture stderr
    capture_stderr: bool,

    /// Current working directory
    current_dir: Option<PathBuf>,

    /// Environment variables
    env_vars: HashMap<String, String>,
}

impl CommandBuilder {
    /// Create a new command builder
    pub fn new<S: Into<String>>(program: S) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            capture_stdout: false,
            capture_stderr: false,
            current_dir: None,
            env_vars: HashMap::new(),
        }
    }

    /// Add an argument
    pub fn arg<S: Into<String>>(mut self, arg: S) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Add multiple arguments
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for arg in args {
            self.args.push(arg.into());
        }
        self
    }

    /// Set whether to capture stdout
    pub fn capture_stdout(mut self, capture: bool) -> Self {
        self.capture_stdout = capture;
        self
    }

    /// Set whether to capture stderr
    pub fn capture_stderr(mut self, capture: bool) -> Self {
        self.capture_stderr = capture;
        self
    }

    /// Set the current working directory
    pub fn current_dir<P: AsRef<Path>>(mut self, dir: P) -> Self {
        self.current_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    /// Add an environment variable
    pub fn env<K, V>(mut self, key: K, val: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.env_vars.insert(key.into(), val.into());
        self
    }

    /// Add multiple environment variables
    pub fn envs<I, K, V>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (key, val) in vars {
            self.env_vars.insert(key.into(), val.into());
        }
        self
    }

    /// Build the command
    pub fn build(self) -> Command {
        Command {
            program: self.program,
            args: self.args,
            capture_stdout: self.capture_stdout,
            capture_stderr: self.capture_stderr,
            current_dir: self.current_dir,
            env_vars: self.env_vars,
        }
    }
}

/// Handle to a running process
pub struct ProcessHandle {
    /// Child process
    child: Child,

    /// Program name
    program: String,

    /// Arguments
    args: Vec<String>,

    /// Whether stdout is captured
    capture_stdout: bool,

    /// Whether stderr is captured
    capture_stderr: bool,
}

impl ProcessHandle {
    /// Get a reference to the child process
    pub fn child(&mut self) -> &mut Child {
        &mut self.child
    }

    /// Wait for the process to exit with a timeout
    pub async fn wait_with_timeout(&mut self, timeout_duration: Duration) -> ProcessResult<std::process::ExitStatus> {
        match timeout(timeout_duration, self.child.wait()).await {
            Ok(Ok(status)) => Ok(status),
            Ok(Err(e)) => Err(ProcessError::SpawnError(e)),
            Err(_) => Err(ProcessError::Timeout(timeout_duration)),
        }
    }

    /// Read lines from stdout and send them to the given channel
    pub async fn read_stdout_lines(&mut self, tx: mpsc::Sender<String>) -> ProcessResult<()> {
        if !self.capture_stdout {
            return Err(ProcessError::Other("Stdout not captured".to_string()));
        }

        let stdout = self.child.stdout.take().ok_or_else(|| {
            ProcessError::Other("Failed to get stdout handle".to_string())
        })?;

        let mut reader = BufReader::new(stdout).lines();
        while let Some(line) = reader.next_line().await.map_err(|e| ProcessError::ReadError(e))? {
            trace!("[{}] stdout: {}", self.program, line);
            tx.send(line).await.map_err(|e| {
                ProcessError::ChannelError(e.to_string())
            })?;
        }

        Ok(())
    }

    /// Read lines from stderr and log them
    pub async fn log_stderr(&mut self) -> ProcessResult<()> {
        if !self.capture_stderr {
            return Err(ProcessError::Other("Stderr not captured".to_string()));
        }

        let stderr = self.child.stderr.take().ok_or_else(|| {
            ProcessError::Other("Failed to get stderr handle".to_string())
        })?;

        let mut reader = BufReader::new(stderr).lines();
        while let Some(line) = reader.next_line().await.map_err(|e| ProcessError::ReadError(e))? {
            debug!("[{}] stderr: {}", self.program, line);
        }

        Ok(())
    }

    /// Kill the process
    pub async fn kill(&mut self) -> ProcessResult<()> {
        self.child.kill().await.map_err(|e| {
            ProcessError::ProcessKilled(format!("Failed to kill process: {}", e))
        })
    }
}

/// Spawn a process and process its output
pub async fn spawn_and_process<S: ProcessSource>(
    source: S,
    config: ProcessConfig,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    let command = source.command();

    // Create a channel for stdout lines
    let (tx, mut rx) = mpsc::channel::<String>(100);

    // Spawn the process - fix the timeout issue
    debug!("Spawning process: {:?}", command);

    // First spawn the command without timeout
    let spawn_result = match command.spawn().await {
        Ok(handle) => handle,
        Err(e) => return Err(e.into()),
    };

    let mut handle = spawn_result;

    // Clone the source for the tokio task
    let source = Arc::new(source);

    // Spawn a task to process stdout lines
    let source_clone = Arc::clone(&source);
    let stdout_task: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
        if let Err(e) = handle.read_stdout_lines(tx).await {
            error!("Error reading stdout: {}", e);
            return Err(e.into());
        }
        Ok(())
    });

    // Spawn a task to log stderr
    let stderr_task = tokio::spawn(async move {
        if let Err(e) = handle.log_stderr().await {
            warn!("Error reading stderr: {}", e);
        }
        Ok(())
    });

    // Spawn the main processing task
    let task = tokio::spawn(async move {
        let mut restart_attempts = 0;

        loop {
            // Process lines from stdout
            while let Some(line) = rx.recv().await {
                if let Err(e) = source_clone.process_line(line).await {
                    error!("Error processing line: {}", e);
                }
            }

            // Wait for the process to exit
            let exit_status = match timeout(config.exit_timeout, stdout_task).await {
                Ok(Ok(Ok(_))) => {
                    debug!("Process exited normally");
                    // Process exited normally, get its exit status
                    match stderr_task.await {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("Error waiting for stderr task: {}", e);
                        }
                    }

                    // Report normal exit to the source
                    let status = std::process::ExitStatus::from_raw(0);
                    if let Err(e) = source_clone.on_exit(status).await {
                        error!("Error handling process exit: {}", e);
                    }

                    break Ok(());
                }
                Ok(Ok(Err(e))) => {
                    error!("Process error: {}", e);

                    // Report error to the source
                    if let Err(e2) = source_clone.handle_error(e.into()).await {
                        error!("Error handling process error: {}", e2);
                    }

                    // Check if we should restart
                    if !config.restart_on_exit || restart_attempts >= config.max_restart_attempts {
                        break Err(AgentError::Collection(format!("Process exited with error after {} restart attempts", restart_attempts)));
                    }

                    restart_attempts += 1;
                    debug!("Restarting process (attempt {}/{})", restart_attempts, config.max_restart_attempts);

                    // Wait before restarting
                    tokio::time::sleep(config.restart_delay).await;
                    continue;
                }
                Ok(Err(e)) => {
                    error!("Error waiting for stdout task: {}", e);
                    break Err(AgentError::Collection(format!("Error waiting for stdout task: {}", e)));
                }
                Err(_) => {
                    error!("Process exit timed out after {:?}", config.exit_timeout);
                    break Err(AgentError::Timeout(format!("Process exit timed out after {:?}", config.exit_timeout)));
                }
            };

            return exit_status;
        }
    });

    Ok(task)
}
