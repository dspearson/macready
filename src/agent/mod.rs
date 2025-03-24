// src/config/mod.rs
use crate::error::{AgentError, Result};
use serde::Deserialize;
use std::fmt::Debug;
use std::fs;
use std::path::{Path, PathBuf};
use log::{debug, info, warn};

/// Source of configuration
#[derive(Debug, Clone)]
pub enum ConfigSource {
    /// File path (TOML format)
    File(String),
    /// Environment variables with a prefix
    Environment(String),
    /// TOML string
    Toml(String),
    /// Default configuration
    Defaults,
}

/// Trait for configuration loading
pub trait ConfigLoader: Sized {
    /// Load from a TOML file
    fn load<P: AsRef<Path>>(path: P) -> Result<Self>;

    /// Load from multiple sources
    fn load_from_sources(sources: Vec<ConfigSource>) -> Result<Self>;
}

/// Helper function to load configuration from various sources
pub fn load_config<T>(sources: Vec<ConfigSource>) -> Result<T>
where
    T: for<'de> Deserialize<'de> + Debug
{
    let mut builder = config::Config::builder();

    // Process each source in order
    for source in sources {
        match source {
            ConfigSource::File(path) => {
                let path = PathBuf::from(path);
                if !path.exists() {
                    warn!("Configuration file not found: {}", path.display());
                    continue;
                }

                debug!("Loading TOML configuration from file: {}", path.display());
                builder = builder.add_source(config::File::with_name(&path.to_string_lossy())
                    .format(config::FileFormat::Toml));
            },
            ConfigSource::Environment(prefix) => {
                debug!("Loading configuration from environment with prefix: {}", prefix);
                builder = builder.add_source(
                    config::Environment::with_prefix(&prefix)
                        .separator("__")
                        .try_parsing(true)
                );
            },
            ConfigSource::Toml(toml_str) => {
                debug!("Loading configuration from TOML string");
                builder = builder.add_source(config::File::from_str(&toml_str,
                    config::FileFormat::Toml));
            },
            ConfigSource::Defaults => {
                // No action needed - defaults are handled by the Deserialize implementation
                debug!("Using default configuration values");
            }
        }
    }

    // Build the config
    let config = builder.build()
        .map_err(|e| AgentError::Config(format!("Failed to build configuration: {}", e)))?;

    // Deserialize into the target type
    let result = config.try_deserialize()
        .map_err(|e| AgentError::Config(format!("Failed to deserialize configuration: {}", e)))?;

    debug!("Configuration loaded successfully: {:?}", result);

    Ok(result)
}

/// Configuration builder
pub struct ConfigBuilder<T: for<'de> Deserialize<'de>> {
    sources: Vec<ConfigSource>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: for<'de> Deserialize<'de>> ConfigBuilder<T> {
    /// Create a new config builder
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Add a TOML file source
    pub fn add_file<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.sources.push(ConfigSource::File(path.as_ref().to_string_lossy().to_string()));
        self
    }

    /// Add environment variables
    pub fn add_env(mut self, prefix: impl Into<String>) -> Self {
        self.sources.push(ConfigSource::Environment(prefix.into()));
        self
    }

    /// Add TOML string
    pub fn add_toml(mut self, toml: impl Into<String>) -> Self {
        self.sources.push(ConfigSource::Toml(toml.into()));
        self
    }

    /// Use default values
    pub fn use_defaults(mut self) -> Self {
        self.sources.push(ConfigSource::Defaults);
        self
    }

    /// Build the configuration
    pub fn build(self) -> Result<T> {
        load_config::<T>(self.sources)
    }
}

/// Helper functions for working with configuration files
pub struct ConfigUtils;

impl ConfigUtils {
    /// Read a TOML configuration file as a string
    pub fn read_file<P: AsRef<Path>>(path: P) -> Result<String> {
        let path = path.as_ref();
        fs::read_to_string(path)
            .map_err(|e| AgentError::Config(format!("Failed to read config file {}: {}",
                path.display(), e)))
    }

    /// Ensure a file has a .toml extension
    pub fn ensure_toml_extension<P: AsRef<Path>>(path: P) -> PathBuf {
        let path = path.as_ref();

        if let Some(ext) = path.extension() {
            if ext == "toml" {
                return path.to_path_buf();
            }
        }

        // Add .toml extension if missing
        let mut new_path = path.to_path_buf();
        new_path.set_extension("toml");
        new_path
    }

    /// Find a TOML configuration file in multiple possible locations
    pub fn find_config_file(name: &str, locations: &[PathBuf]) -> Option<PathBuf> {
        for location in locations {
            let filename = format!("{}.toml", name);
            let path = location.join(filename);

            if path.exists() {
                return Some(path);
            }
        }

        None
    }

    /// Get standard configuration search paths
    pub fn get_standard_search_paths() -> Vec<PathBuf> {
        let mut paths = Vec::new();

        // Current directory
        paths.push(PathBuf::from("."));

        // Config directory in current directory
        paths.push(PathBuf::from("config"));

        // User's home directory
        if let Some(home) = dirs::home_dir() {
            paths.push(home.join(".config"));
        }

        // System configuration directories
        #[cfg(target_os = "linux")]
        {
            paths.push(PathBuf::from("/etc"));
        }

        #[cfg(target_os = "macos")]
        {
            paths.push(PathBuf::from("/etc"));
            paths.push(PathBuf::from("/Library/Application Support"));
        }

        #[cfg(target_os = "windows")]
        {
            if let Some(app_data) = dirs::config_dir() {
                paths.push(app_data);
            }
        }

        paths
    }
}

/// Simple implementation of ConfigLoader for standard use cases
impl<T: for<'de> Deserialize<'de> + Debug> ConfigLoader for T {
    fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let sources = vec![ConfigSource::File(path.as_ref().to_string_lossy().to_string())];
        Self::load_from_sources(sources)
    }

    fn load_from_sources(sources: Vec<ConfigSource>) -> Result<Self> {
        load_config(sources)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestConfig {
        name: String,
        count: i32,
        flag: bool,
    }

    #[test]
    fn test_load_from_toml_file() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, r#"
            name = "test"
            count = 42
            flag = true
        "#).unwrap();

        let config = TestConfig::load(file.path()).unwrap();
        assert_eq!(config.name, "test");
        assert_eq!(config.count, 42);
        assert_eq!(config.flag, true);
    }

    #[test]
    fn test_load_from_toml_string() {
        let toml_str = r#"
        name = "test-toml"
        count = 100
        flag = false
        "#;

        let config = ConfigBuilder::<TestConfig>::new()
            .add_toml(toml_str)
            .build()
            .unwrap();

        assert_eq!(config.name, "test-toml");
        assert_eq!(config.count, 100);
        assert_eq!(config.flag, false);
    }

    #[test]
    fn test_multiple_sources() {
        let toml_str1 = r#"
        name = "config1"
        count = 10
        flag = true
        "#;

        let toml_str2 = r#"
        name = "config2"
        count = 20
        "#;

        // Later sources override earlier ones
        let config = ConfigBuilder::<TestConfig>::new()
            .add_toml(toml_str1)
            .add_toml(toml_str2)
            .build()
            .unwrap();

        assert_eq!(config.name, "config2");
        assert_eq!(config.count, 20);
        assert_eq!(config.flag, true); // From first TOML since second didn't override it
    }
}
