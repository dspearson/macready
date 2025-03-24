use std::time::Duration;

/// Configuration for a collector
#[derive(Debug, Clone)]
pub struct CollectorConfig {
    /// Name of the collector
    pub name: String,
    /// Buffer size for the metrics channel
    pub buffer_size: usize,
    /// Default collection interval (for periodic collectors)
    pub interval: Option<Duration>,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            name: "default_collector".to_string(),
            buffer_size: 100,
            interval: Some(Duration::from_secs(60)),
        }
    }
}

/// Builder for collector configuration
pub struct CollectorConfigBuilder {
    config: CollectorConfig,
}

impl CollectorConfigBuilder {
    /// Create a new collector config builder
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            config: CollectorConfig {
                name: name.into(),
                ..Default::default()
            },
        }
    }

    /// Set the buffer size
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    /// Set the collection interval
    pub fn interval(mut self, interval: Duration) -> Self {
        self.config.interval = Some(interval);
        self
    }

    /// Build the configuration
    pub fn build(self) -> CollectorConfig {
        self.config
    }
}
