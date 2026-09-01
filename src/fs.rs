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
