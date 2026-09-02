//! Filesystem write-safety shared by the download and metadata paths:
//! refusing to write through a pre-planted symlink.

use crate::{IaGetError, Result};
use std::path::Path;

/// Refuses to write through a pre-planted symlink at `path`: opening or
/// replacing such a path would silently reach the link target, which may
/// live outside the working directory. A missing path passes, so the
/// caller may still create it.
pub fn ensure_not_symlink(path: &Path) -> Result<()> {
    if let Ok(metadata) = std::fs::symlink_metadata(path)
        && metadata.file_type().is_symlink()
    {
        return Err(IaGetError::FileSystem {
            detail: format!(
                "{} is a symlink; refusing to write through it",
                path.display()
            ),
            source: None,
        });
    }
    Ok(())
}

/// The free space (in bytes) of the volume that `path` lives on, or `None`
/// when it cannot be determined.
///
/// Used by the pre-download disk-space check: a `None` result means the
/// check is skipped (best-effort), never that the download is refused —
/// platforms or filesystems where the free-space call fails must not block a
/// download that would otherwise succeed.
pub fn available_space(path: &Path) -> Option<u64> {
    fs2::available_space(path).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn available_space_reports_a_volume() {
        // The current directory's volume must report a free-space figure on
        // every supported platform; only the fact that it resolves (not the
        // exact amount) is asserted, since that varies by machine.
        assert!(available_space(Path::new(".")).is_some());
    }
}
