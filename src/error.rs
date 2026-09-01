//! Error types for ia-get

use thiserror::Error;

/// Result type alias for ia-get operations
pub type Result<T> = std::result::Result<T, IaGetError>;

/// Main error type for ia-get operations
#[derive(Error, Debug)]
pub enum IaGetError {
    /// Network-related errors including connection failures, timeouts, and HTTP errors
    #[error("Network error: {detail}")]
    Network {
        detail: String,
        /// The underlying error (e.g. the reqwest error), when one exists
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// File system errors during download operations
    #[error("File operation failed: {detail}")]
    FileSystem {
        detail: String,
        /// The underlying error (e.g. the std::io::Error), when one exists
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// URL format or parsing errors
    #[error(
        "Invalid archive.org URL: {0}. Expected format: https://archive.org/details/<identifier>[/]"
    )]
    UrlFormat(String),

    /// XML parsing errors
    #[error("Failed to parse XML: {0}")]
    XmlParsing(String),

    /// The supplied cookie input cannot be turned into a valid HTTP header
    #[error("Invalid cookie header: {0}")]
    InvalidCookie(String),

    /// Server rejected the resume offset (HTTP 416): the local partial file is not a valid prefix
    #[error(
        "Server rejected the resume offset (HTTP 416); the partial file must be re-downloaded from scratch"
    )]
    RangeNotSatisfiable,

    /// The run was interrupted by the user (Ctrl+C)
    #[error("Interrupted by user")]
    Interrupted,

    /// One or more files in a batch could not be downloaded.
    ///
    /// The per-file reasons stay out of the display: the batch loop already
    /// printed them before constructing this error. `details` remains
    /// available for structured access.
    #[error("{count} of {total} file(s) failed to download")]
    BatchFailed {
        count: usize,
        total: usize,
        details: String,
    },
}

impl From<reqwest::Error> for IaGetError {
    fn from(err: reqwest::Error) -> Self {
        // Connection failures and HTTP errors get a more specific prefix;
        // the rest fall back to the reqwest message as is.
        let detail = if err.is_connect() || err.is_timeout() {
            format!("Connection failed: {err}")
        } else if let Some(status) = err.status() {
            format!("HTTP error {status}: {err}")
        } else {
            err.to_string()
        };
        IaGetError::Network {
            detail,
            source: Some(Box::new(err)),
        }
    }
}

impl From<std::io::Error> for IaGetError {
    fn from(err: std::io::Error) -> Self {
        // An OS-level Interrupted (a syscall interrupted by a signal) is a
        // transient I/O problem, not a user request to stop the run: map it
        // to FileSystem so the retry loop can treat it as retryable. The
        // user's Ctrl+C reaches the code through the running flag, never
        // through an io::Error.
        IaGetError::FileSystem {
            detail: err.to_string(),
            source: Some(Box::new(err)),
        }
    }
}

impl From<url::ParseError> for IaGetError {
    fn from(err: url::ParseError) -> Self {
        IaGetError::UrlFormat(err.to_string())
    }
}

impl From<serde_xml_rs::Error> for IaGetError {
    fn from(err: serde_xml_rs::Error) -> Self {
        IaGetError::XmlParsing(err.to_string())
    }
}

/// Converts an `io::Error` into an `IaGetError` naming the file the
/// operation touched. A bare message ("File operation failed: Access is
/// denied") cannot be located in a multi-hundred-file batch; the path can.
pub fn io_error_with_path(path: impl AsRef<std::path::Path>, err: std::io::Error) -> IaGetError {
    IaGetError::FileSystem {
        detail: format!("{}: {}", path.as_ref().display(), err),
        source: Some(Box::new(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn os_interrupted_maps_to_filesystem_error() {
        // A syscall interrupted by a signal must not be mistaken for the
        // user's Ctrl+C.
        let io_err = std::io::Error::from(ErrorKind::Interrupted);
        let err = IaGetError::from(io_err);
        assert!(
            matches!(err, IaGetError::FileSystem { .. }),
            "an OS-level Interrupted must stay a file system error, got {err:?}"
        );
    }
}
