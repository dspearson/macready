use clap::{Parser, ValueEnum};
use config::{Config, File};
use serde::Deserialize;
use std::path::Path;
use std::str::FromStr;
use log::LevelFilter;

use crate::error::{AgentError, Result};

/// Log level enum compatible with clap
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum LogLevel {
    /// Only show errors
    Error,
    /// Show errors and warnings
    Warn,
    /// Show errors, warnings, and info (default)
    Info,
    /// Show errors, warnings, info, and debug messages
    Debug,
    /// Show all messages including trace
    Trace,
}

impl FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "error" => Ok(LogLevel::Error),
            "warn" => Ok(LogLevel::Warn),
            "info" => Ok(LogLevel::Info),
            "debug" => Ok(LogLevel::Debug),
            "trace" => Ok(LogLevel::Trace),
            _ => Err(format!("Unknown log level: {}", s)),
        }
    }
}

impl LogLevel {
    /// Convert to log::LevelFilter
    pub fn to_filter(&self) -> LevelFilter {
        match self {
            LogLevel::Error => LevelFilter::Error,
            LogLevel::Warn => LevelFilter::Warn,
            LogLevel::Info => LevelFilter::Info,
            LogLevel::Debug => LevelFilter::Debug,
            LogLevel::Trace => LevelFilter::Trace,
        }
    }
}

/// Base CLI arguments for metrics agents
#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct BaseArgs {
    /// Path to config file
    #[arg(long, default_value = "config.toml")]
    pub config: String,

    /// Hostname to use for metrics collection
    #[arg(long)]
    pub hostname: Option<String>,

    /// Verbosity level for logging
    #[arg(short, long, default_value = "info")]
    pub log_level: LogLevel,

    /// Quiet mode (errors only, overrides log-level)
    #[arg(short, long)]
    pub quiet: bool,

    /// Show additional detail
    #[arg(short, long)]
    pub verbose: bool,
}

/// Configuration loader trait
pub trait ConfigLoader: Sized {
    /// Load configuration from a file
    fn load<P: AsRef<Path>>(path: P) -> Result<Self>;

    /// Load configuration from multiple sources with optional merging
    fn load_from_sources(sources: Vec<ConfigSource>) -> Result<Self>;
}

/// Configuration source for flexible loading
pub enum ConfigSource {
    /// Load from a file
    File(String),

    /// Load from environment variables
    Environment(String),

    /// Load from command line arguments
    CommandLine,

    /// Load from a string
    String(String, String), // (format, content)
}

/// Helper function to load configuration from multiple sources
pub fn load_config<C: ConfigLoader + serde::de::DeserializeOwned>(
    sources: Vec<ConfigSource>
) -> Result<C> {
    // Create a default config builder
    let mut builder = config::Config::builder();

    // Add each source to the builder
    for source in sources {
        builder = match source {
            ConfigSource::File(path) => {
                if Path::new(&path).exists() {
                    builder.add_source(File::with_name(&path))
                } else {
                    return Err(AgentError::Config(config::ConfigError::NotFound(path)));
                }
            },
            ConfigSource::Environment(prefix) => {
                builder.add_source(config::Environment::with_prefix(&prefix))
            },
            ConfigSource::CommandLine => {
                // For now, we don't have a built-in way to handle command line args via config crate
                builder
            },
            ConfigSource::String(format, content) => {
                match format.to_lowercase().as_str() {
                    "json" => builder.add_source(config::File::from_str(&content, config::FileFormat::Json)),
                    "toml" => builder.add_source(config::File::from_str(&content, config::FileFormat::Toml)),
                    "yaml" => builder.add_source(config::File::from_str(&content, config::FileFormat::Yaml)),
                    _ => return Err(AgentError::Config(config::ConfigError::Message(format!("Unsupported config format: {}", format)))),
                }
            },
        };
    }

    // Build and deserialize
    let config = builder.build()
        .map_err(|e| AgentError::Config(e))?;

    config.try_deserialize()
        .map_err(|e| AgentError::Config(e))
}

/// Initialize the logger based on arguments
pub fn init_logger(args: &BaseArgs) -> Result<()> {
    let level = if args.quiet {
        LogLevel::Error.to_filter()
    } else {
        args.log_level.to_filter()
    };

    // Create a default environment and override the log level
    let env = env_logger::Env::default().default_filter_or(level.to_string());

    // Initialize with custom format
    env_logger::Builder::from_env(env)
        .format(|buf, record| {
            use chrono::Local;
            use std::io::Write;

            let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
            writeln!(
                buf,
                "{} {} [{}] {}",
                timestamp,
                record.level(),
                record.target(),
                record.args()
            )
        })
        .init();

    Ok(())
}

/// Get a unique identifier for the current machine
pub fn get_hostname() -> Result<String> {
    // Try various methods to get a hostname
    if let Ok(hostname) = std::env::var("HOSTNAME") {
        if !hostname.is_empty() {
            return Ok(hostname);
        }
    }

    if let Ok(hostname) = hostname::get() {
        if let Ok(hostname_str) = hostname.into_string() {
            if !hostname_str.is_empty() {
                return Ok(hostname_str);
            }
        }
    }

    // Fallback for Linux/Unix systems
    if cfg!(unix) {
        use std::process::Command;
        if let Ok(output) = Command::new("hostname").output() {
            if output.status.success() {
                if let Ok(hostname) = String::from_utf8(output.stdout) {
                    let trimmed = hostname.trim().to_string();
                    if !trimmed.is_empty() {
                        return Ok(trimmed);
                    }
                }
            }
        }
    }

    // Generate a random identifier as a last resort
    let random_id = uuid::Uuid::new_v4().to_string();
    Ok(format!("unknown-host-{}", random_id))
}

/// Base configuration trait with common settings
pub trait BaseConfig {
    /// Get the maximum number of retries for operations
    fn max_retries(&self) -> usize;

    /// Get the log level setting
    fn log_level(&self) -> LogLevel;

    /// Get the agent name
    fn agent_name(&self) -> &str;
}

/// Database configuration for the agent
#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub username: String,
    pub password: String,
    pub hosts: Vec<String>,
    pub port: u16,
    pub database: String,
    pub sslmode: String,
}

/// Default implementation for common database config
impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            username: "postgres".to_string(),
            password: "postgres".to_string(),
            hosts: vec!["localhost".to_string()],
            port: 5432,
            database: "metrics".to_string(),
            sslmode: "disable".to_string(),
        }
    }
}

impl DatabaseConfig {
    /// Generate a connection string from this config
    pub fn connection_string(&self) -> String {
        // Join multiple hosts with commas
        let hosts_with_ports: Vec<String> = self
            .hosts
            .iter()
            .map(|host| format!("{}:{}", host, self.port))
            .collect();

        let hosts = hosts_with_ports.join(",");

        format!(
            "postgresql://{}:{}@{}/{}",
            self.username, self.password, hosts, self.database
        )
    }
}
