//! Append-only mutation log for ket operations.

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
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
///
/// Guards the trailing newline (#18): if the log's last byte is not
/// `\n` — an interrupted prior write, or any writer that forgot —
/// appending directly would concatenate two entries into one line
/// that every line-anchored reader silently skips, so BOTH entries
/// would vanish from drift enforcement. Five such concatenations
/// were found in a live log (harmonics, 2026-07-30), one of them
/// hiding the only seal of an enforced-spine path. The heal and the
/// entry go down in a single write, then fsync.
pub fn append(log_path: &Path, event: &str, detail: &str) -> Result<(), std::io::Error> {
    let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(log_path)?;
    let needs_newline = file.metadata()?.len() > 0 && {
        file.seek(SeekFrom::End(-1))?;
        let mut last = [0u8; 1];
        file.read_exact(&mut last)?;
        last[0] != b'\n'
    };
    let guard = if needs_newline { "\n" } else { "" };
    // One write: the guard newline and the entry cannot be split by a
    // crash into a state worse than the one being healed.
    writeln!(file, "{guard}{timestamp} | {event} | {detail}")?;
    file.sync_data()?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_heals_a_missing_trailing_newline() {
        let dir = std::env::temp_dir().join(format!("ket-log-test-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let log = dir.join("log");

        // A truncated tail: prior entry with no trailing LF.
        fs::write(&log, "2026-01-01T00:00:00Z | put | a.md -> aaaa").unwrap();
        append(&log, "put", "b.md -> bbbb").unwrap();

        let text = fs::read_to_string(&log).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "healed into two lines: {text:?}");
        assert!(lines[0].ends_with("a.md -> aaaa"));
        assert!(lines[1].contains("| put | b.md -> bbbb"));
        assert!(text.ends_with('\n'), "trailing LF restored");

        // Normal append adds exactly one line, no blank line inserted.
        append(&log, "put", "c.md -> cccc").unwrap();
        let text = fs::read_to_string(&log).unwrap();
        assert_eq!(text.lines().count(), 3);
        assert!(!text.contains("\n\n"), "no spurious blank line");

        // Fresh file: no leading newline.
        let log2 = dir.join("log2");
        append(&log2, "init", "x").unwrap();
        assert!(!fs::read_to_string(&log2).unwrap().starts_with('\n'));

        fs::remove_dir_all(&dir).unwrap();
    }
}
