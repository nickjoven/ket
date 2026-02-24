//! Append-only mutation log for ket operations.

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone, serde::Serialize)]
pub struct LogEntry {
    pub timestamp: String,
    pub event: String,
    pub detail: String,
}

impl fmt::Display for LogEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} | {} | {}", self.timestamp, self.event, self.detail)
    }
}

/// Append a log entry.
pub fn append(log_path: &Path, event: &str, detail: &str) -> Result<(), std::io::Error> {
    let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    writeln!(file, "{timestamp} | {event} | {detail}")?;
    Ok(())
}

/// Read the last N log entries.
pub fn read(log_path: &Path, n: usize) -> Result<Vec<LogEntry>, std::io::Error> {
    if !log_path.exists() {
        return Ok(vec![]);
    }
    let contents = fs::read_to_string(log_path)?;
    let lines: Vec<&str> = contents.lines().filter(|l| !l.is_empty()).collect();

    let start = lines.len().saturating_sub(n);
    let entries = lines[start..]
        .iter()
        .filter_map(|line| {
            let parts: Vec<&str> = line.splitn(3, " | ").collect();
            if parts.len() == 3 {
                Some(LogEntry {
                    timestamp: parts[0].to_string(),
                    event: parts[1].to_string(),
                    detail: parts[2].to_string(),
                })
            } else {
                None
            }
        })
        .collect();

    Ok(entries)
}
