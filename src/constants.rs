//! Application constants for ia-get

/// User agent string for HTTP requests: tool name and version, as archive.org
/// asks clients to identify themselves with
pub const USER_AGENT: &str = concat!("ia-get/", env!("CARGO_PKG_VERSION"));

/// Regex pattern for validating archive.org details URLs
pub const URL_PATTERN: &str = r"^https://archive\.org/details/[a-zA-Z0-9_\-.@]+/?$";

/// Maximum length for XML content in debug output (characters)
pub const XML_DEBUG_TRUNCATE_LEN: usize = 1000;
