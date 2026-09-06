//! Append-only mutation log for ket operations.
//!
//! Lives in ket-cas so every writer — the CLI, the MCP server, catbus, any
//! library consumer — appends to the same `.ket/log`. The log is the source
//! of truth for *events*; a writer that bypasses it leaves silent history.
//! `log_path_for(store)` derives the conventional location from a CAS root.

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

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

/// Conventional log location for a CAS store: the sibling of its `cas/` dir
/// (`.ket/cas` → `.ket/log`). Falls back to `<root>/log` for a bare store.
pub fn log_path_for(store: &super::Store) -> PathBuf {
    match store.root().parent() {
        Some(parent) => parent.join("log"),
        None => store.root().join("log"),
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
    // One `write_all`, not `writeln!`. `writeln!` on a File is `write_fmt`,
    // which issues a separate write(2) per format fragment; under O_APPEND
    // each write lands atomically at end-of-file, but concurrent writers then
    // interleave their fragments and tear each other's lines. Building the
    // whole line — heal newline, entry, terminator — into one buffer and
    // emitting it in a single write keeps each entry whole: O_APPEND makes a
    // lone write of a short line atomic. (32 concurrent appenders produced 20
    // malformed lines out of 42 before this; a live-log audit found the same.)
    let line = format!("{guard}{timestamp} | {event} | {detail}\n");
    file.write_all(line.as_bytes())?;
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

    #[test]
    fn concurrent_appends_never_tear_a_line() {
        // Many writers appending at once must each leave one whole, well-formed
        // line — no interleaved fragments. `writeln!` tore lines here (a
        // 32-writer run left 20 of 42 lines malformed); one buffered write per
        // entry, under O_APPEND, does not.
        let dir = std::env::temp_dir().join(format!("ket-log-concurrent-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let log = dir.join("log");

        let writers = 32;
        let per_writer = 8;
        let handles: Vec<_> = (0..writers)
            .map(|w| {
                let log = log.clone();
                std::thread::spawn(move || {
                    for i in 0..per_writer {
                        append(&log, "put", &format!("w{w:02}-{i} -> {}", "a".repeat(64))).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let text = fs::read_to_string(&log).unwrap();
        let lines: Vec<&str> = text.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(
            lines.len(),
            writers * per_writer,
            "one line per append, none torn or merged"
        );
        let re_ok = |l: &str| {
            // "<ts> | put | w##-# -> <64 hex-ish>"
            let parts: Vec<&str> = l.splitn(3, " | ").collect();
            parts.len() == 3
                && parts[1] == "put"
                && parts[2].contains(" -> ")
                && parts[2].ends_with(&"a".repeat(64))
        };
        let malformed: Vec<&&str> = lines.iter().filter(|l| !re_ok(l)).collect();
        assert!(malformed.is_empty(), "torn/merged lines: {malformed:?}");

        fs::remove_dir_all(&dir).unwrap();
    }
}
