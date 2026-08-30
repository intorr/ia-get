//! Shared helpers for unit tests, used by both the library and the binary.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Creates a unique temp directory for a test.
///
/// Directories are not cleaned up, mirroring the existing test style:
/// leftovers under the OS temp dir are harmless.
pub fn temp_dir_for(test_name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "ia-get-test-{}-{}-{}",
        std::process::id(),
        test_name,
        nanos
    ));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

/// Returns a file's mtime as unix seconds, if readable.
pub fn mtime_of(path: &Path) -> Option<u64> {
    std::fs::metadata(path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
}
