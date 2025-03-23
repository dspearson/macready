use env_logger::Builder;
use log::LevelFilter;
use std::io::Write;

// Import LogLevel from the config module, not error module
use crate::config::LogLevel;

/// Initialize the logging system
pub fn init(level: &LogLevel) {
    let log_level = match level {
        LogLevel::Error => LevelFilter::Error,
        LogLevel::Warn => LevelFilter::Warn,
        LogLevel::Info => LevelFilter::Info,
        LogLevel::Debug => LevelFilter::Debug,
        LogLevel::Trace => LevelFilter::Trace,
    };

    let mut builder = Builder::new();
    builder
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, log_level)
        .init();
}
