use chrono::Local;
use log::{Level, LevelFilter};
use std::io::Write;

use crate::config::LogLevel;

/// Initialize a custom logger with timestamp and module information
pub fn init_logger(level: LogLevel, quiet: bool) {
    let filter = if quiet {
        LevelFilter::Error
    } else {
        level.to_filter()
    };

    // Create a default environment and override the log level
    let env = env_logger::Env::default().default_filter_or(filter.to_string());

    // Initialize with custom format
    env_logger::Builder::from_env(env)
        .format(|buf, record| {
            let level_style = buf.default_styled_level(record.level());
            let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");

            writeln!(
                buf,
                "{} {} [{}] {}",
                timestamp,
                level_style,
                record.target(),
                record.args()
            )
        })
        .init();
}

/// Convert string to log level
pub fn parse_log_level(level: &str) -> LogLevel {
    match level.to_lowercase().as_str() {
        "error" => LogLevel::Error,
        "warn" => LogLevel::Warn,
        "info" => LogLevel::Info,
        "debug" => LogLevel::Debug,
        "trace" => LogLevel::Trace,
        _ => LogLevel::Info, // Default to info for unknown values
    }
}

/// Format a duration in a human-readable way
pub fn format_duration(duration: std::time::Duration) -> String {
    let total_secs = duration.as_secs();

    if total_secs < 60 {
        return format!("{:.2}s", duration.as_secs_f32());
    }

    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;

    if hours > 0 {
        format!("{}h {:02}m {:02}s", hours, minutes, seconds)
    } else {
        format!("{}m {:02}s", minutes, seconds)
    }
}

/// Format a file size in a human-readable way
pub fn format_size(size: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if size < KB {
        format!("{} B", size)
    } else if size < MB {
        format!("{:.2} KB", size as f64 / KB as f64)
    } else if size < GB {
        format!("{:.2} MB", size as f64 / MB as f64)
    } else if size < TB {
        format!("{:.2} GB", size as f64 / GB as f64)
    } else {
        format!("{:.2} TB", size as f64 / TB as f64)
    }
}

/// Format a number of metrics in a human-readable way
pub fn format_metric_count(count: usize) -> String {
    if count < 1000 {
        count.to_string()
    } else if count < 1_000_000 {
        format!("{:.1}K", count as f64 / 1000.0)
    } else {
        format!("{:.1}M", count as f64 / 1_000_000.0)
    }
}

/// Format a metric rate (per second) in a human-readable way
pub fn format_metric_rate(rate: f64) -> String {
    if rate < 1.0 {
        format!("{:.2} metrics/sec", rate)
    } else if rate < 1000.0 {
        format!("{:.1} metrics/sec", rate)
    } else if rate < 1_000_000.0 {
        format!("{:.1}K metrics/sec", rate / 1000.0)
    } else {
        format!("{:.1}M metrics/sec", rate / 1_000_000.0)
    }
}
