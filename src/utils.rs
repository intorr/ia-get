//! General-purpose helpers shared across the crate: adding the cookie
//! header to requests, refusing to write through pre-planted symlinks, and
//! validating the archive.org details URL.

use crate::constants::URL_PATTERN;
use crate::{IaGetError, Result};
use regex::Regex;
use reqwest::RequestBuilder;
use reqwest::header::{COOKIE, HeaderValue};
use std::path::Path;
use std::sync::LazyLock;

/// Adds the `Cookie` header to a request builder when a cookie value is
/// present, so every authenticated request is built the same way.
pub fn with_cookie(mut request: RequestBuilder, cookie: Option<&HeaderValue>) -> RequestBuilder {
    if let Some(cookie) = cookie {
        request = request.header(COOKIE, cookie);
    }
    request
}

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

/// Compiled regex for URL validation (initialized once)
static URL_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(URL_PATTERN).expect("Invalid URL regex pattern"));

/// Validates an archive.org details URL format
///
/// # Arguments
/// * `url` - The URL to validate
///
/// # Returns
/// * `Ok(())` if the URL is valid
/// * `Err(IaGetError::UrlFormat)` if the URL format is invalid
///
/// # Examples
/// ```
/// use ia_get::utils::validate_archive_url;
///
/// assert!(validate_archive_url("https://archive.org/details/valid-item").is_ok());
/// assert!(validate_archive_url("https://archive.org/details/valid-item/").is_ok());
/// assert!(validate_archive_url("https://example.com/invalid").is_err());
/// ```
pub fn validate_archive_url(url: &str) -> Result<()> {
    // The anchored pattern already requires a non-empty identifier right
    // after "details/" and nothing after it.
    if URL_REGEX.is_match(url) {
        return Ok(());
    }
    Err(IaGetError::UrlFormat(url.to_string()))
}
