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
    #[error("Invalid archive.org URL: {0}. Expected format: https://archive.org/details/<identifier>[/]")]
    UrlFormat(String),

    /// XML parsing errors
    #[error("Failed to parse XML: {0}")]
    XmlParsing(String),

    /// Server rejected the resume offset (HTTP 416): the local partial file is not a valid prefix
    #[error("Server rejected the resume offset (HTTP 416); the partial file must be re-downloaded from scratch")]
    RangeNotSatisfiable,

    /// The run was interrupted by the user (Ctrl+C)
    #[error("Interrupted by user")]
    Interrupted,

    /// One or more files in a batch could not be downloaded
    #[error("{count} of {total} file(s) failed to download. {details}")]
    BatchFailed {
        count: usize,
        total: usize,
        details: String,
    },
}

impl From<reqwest::Error> for IaGetError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_connect() || err.is_timeout() {
            IaGetError::Network {
                detail: format!("Connection failed: {err}"),
                source: Some(Box::new(err)),
            }
        } else if err.is_status() {
            let status = err.status().map(|s| s.to_string()).unwrap_or_default();
            IaGetError::Network {
                detail: format!("HTTP error {status}: {err}"),
                source: Some(Box::new(err)),
            }
        } else {
            IaGetError::Network {
                detail: err.to_string(),
                source: Some(Box::new(err)),
            }
        }
    }
}

impl From<std::io::Error> for IaGetError {
    fn from(err: std::io::Error) -> Self {
        if err.kind() == std::io::ErrorKind::Interrupted {
            IaGetError::Interrupted
        } else {
            IaGetError::FileSystem {
                detail: err.to_string(),
                source: Some(Box::new(err)),
            }
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
