//! Utility functions for ia-get.

use crate::constants::URL_PATTERN;
use crate::{IaGetError, Result};
use colored::*;
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use reqwest::RequestBuilder;
use reqwest::header::{COOKIE, HeaderValue};
use std::path::Path;
use std::sync::LazyLock;
use std::time::Duration;

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

/// Spinner tick interval in milliseconds
pub const SPINNER_TICK_INTERVAL: u64 = 100;

/// Size constants for formatting
const KB: u64 = 1024;
const MB: u64 = KB * 1024;
const GB: u64 = MB * 1024;

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

/// Create a progress bar with consistent styling
///
/// # Arguments
/// * `total` - Total value for the progress bar
/// * `action` - Action text to show at the beginning, pre-styled with the
///   `colored` crate (e.g., "╰╼ Downloading  ")
/// * `color` - Optional bar color style (defaults to "green/green")
/// * `with_eta` - Whether to include ETA in the template
///
/// # Returns
/// A configured progress bar
pub fn create_progress_bar(
    total: u64,
    action: &str,
    color: Option<&str>,
    with_eta: bool,
) -> ProgressBar {
    let pb = ProgressBar::new(total);
    let color_str = color.unwrap_or("green/green");

    let template =
        format!("{action}{{elapsed_precise}} {{bar:40.{color_str}}} {{bytes}}/{{total_bytes}}");
    let template = if with_eta {
        format!("{template} (ETA: {{eta}})")
    } else {
        template
    };

    pb.set_style(
        ProgressStyle::default_bar()
            .template(&template)
            .expect("Failed to set progress bar style")
            .progress_chars("▓▒░"),
    );

    pb
}

/// Tree glyph for a line with more lines following in the file's block.
pub fn branch_glyph() -> ColoredString {
    "├╼".cyan().dimmed()
}

/// Tree glyph for the last line of a file's block.
pub fn last_glyph() -> ColoredString {
    "╰╼".cyan().dimmed()
}

/// Print the "Filename / Count" banner for one file of a numbered list
pub fn print_file_banner(file_path: &str, number: usize, total: usize) {
    println!(
        "{}  {}     {}",
        "▣".bright_cyan().bold(),
        "Filename".white(),
        file_path.bold()
    );
    println!(
        "{} {}        {} {} of {}",
        branch_glyph(),
        "Count".white(),
        "#".blue().bold(),
        number.to_string().bold(),
        total.to_string().bold()
    );
}

/// Create a spinner with braille animation
///
/// # Arguments
/// * `message` - Message to display next to the spinner
///
/// # Returns
/// A configured spinner
pub fn create_spinner(message: &str) -> ProgressBar {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
            .template(&format!("{} {}", "{spinner}".yellow().bold(), message))
            .expect("Failed to set spinner style"),
    );
    spinner.enable_steady_tick(Duration::from_millis(SPINNER_TICK_INTERVAL));
    spinner
}

/// Restyles a running spinner as a static completion message and finishes it
pub fn finish_spinner(spinner: &ProgressBar, message: &str) {
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template(message)
            .expect("Failed to set spinner style"),
    );
    spinner.finish();
}

/// Print the "Downloaded ↓ size" status line for a finished file
///
/// `prefix` is the pre-styled tree glyph: "├╼" when more lines follow in the
/// file's block, "╰╼" for its last line. It is taken by value so the
/// caller's styling survives (a `&ColoredString` coerced to `&str` would
/// deref to the plain, uncoloured text). When `elapsed` is present, the
/// transfer time and rate are appended; it is absent for files that never
/// crossed the network (e.g. the locally saved `_files.xml`).
pub fn print_downloaded_line(prefix: ColoredString, transferred: u64, elapsed: Option<Duration>) {
    let head = format!(
        "{} {}   {} {}",
        prefix,
        "Downloaded".white(),
        "↓".green().bold(),
        format_size(transferred).bold()
    );

    match elapsed {
        Some(elapsed) => {
            let elapsed_secs = elapsed.as_secs_f64();
            let rate = if elapsed_secs > 0.0 {
                transferred as f64 / elapsed_secs
            } else {
                0.0
            };
            let (rate, unit) = format_transfer_rate(rate);
            println!(
                "{head} in {} ({rate:.2} {unit}/s)",
                format_duration(elapsed).bold()
            );
        }
        None => println!("{head}"),
    }
}

/// Format a duration into a human-readable string
pub fn format_duration(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    if total_secs < 60 {
        return format!("{}.{:02}s", total_secs, duration.subsec_millis() / 10);
    }

    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;

    if hours > 0 {
        format!("{}h {}m {}s", hours, mins, secs)
    } else {
        format!("{}m {}s", mins, secs)
    }
}

/// Picks the human-readable unit (B/KB/MB/GB) for a byte count and returns
/// the value scaled to that unit
fn scaled_unit(value: f64) -> (f64, &'static str) {
    let kb = KB as f64;
    let mb = MB as f64;
    let gb = GB as f64;

    if value < kb {
        (value, "B")
    } else if value < mb {
        (value / kb, "KB")
    } else if value < gb {
        (value / mb, "MB")
    } else {
        (value / gb, "GB")
    }
}

/// Format a size in bytes to a human-readable string
pub fn format_size(size: u64) -> String {
    if size < KB {
        format!("{}B", size)
    } else {
        let (value, unit) = scaled_unit(size as f64);
        format!("{value:.2}{unit}")
    }
}

/// Format transfer rate to appropriate units
pub fn format_transfer_rate(bytes_per_sec: f64) -> (f64, &'static str) {
    scaled_unit(bytes_per_sec)
}

/// Windows reserved device names (case-insensitive). A path component whose
/// base name matches one of these gets an underscore appended.
const WINDOWS_RESERVED_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Characters that are invalid in file names on Windows or Unix and are
/// replaced with underscores
fn is_invalid_filename_char(ch: char) -> bool {
    matches!(ch, '<' | '>' | ':' | '"' | '|' | '?' | '*' | '\\')
        || ('\x00'..='\x1F').contains(&ch)
        || ch == '\x7F'
}

/// Replaces every invalid character in a path component with an underscore.
///
/// Returns the replacement and whether anything was changed.
fn replace_invalid_chars(component: &str) -> (String, bool) {
    let mut sanitized = String::with_capacity(component.len());
    let mut was_modified = false;

    for ch in component.chars() {
        if is_invalid_filename_char(ch) {
            sanitized.push('_');
            was_modified = true;
        } else {
            sanitized.push(ch);
        }
    }

    (sanitized, was_modified)
}

/// Appends an underscore to a component whose base name (the part before the
/// first dot) is a Windows reserved name, keeping the extension intact
/// (`CON.txt` → `CON_.txt`).
///
/// Returns whether anything was changed.
fn mark_reserved_name(component: &mut String) -> bool {
    let dot_pos = component.find('.');
    let base_name = dot_pos.map_or(component.as_str(), |pos| &component[..pos]);

    let is_reserved = WINDOWS_RESERVED_NAMES
        .iter()
        .any(|reserved| base_name.eq_ignore_ascii_case(reserved));

    if is_reserved {
        match dot_pos {
            Some(pos) => component.insert(pos, '_'),
            None => component.push('_'),
        }
    }

    is_reserved
}

/// Sanitizes a single non-empty path component: replaces invalid characters,
/// trims surrounding spaces and trailing dots (Windows rejects both),
/// substitutes a placeholder for components that end up empty, and marks
/// Windows reserved names.
///
/// Returns the sanitized component and whether anything was changed.
fn sanitize_component(component: &str) -> (String, bool) {
    let (mut sanitized, mut was_modified) = replace_invalid_chars(component);

    let trimmed = sanitized.trim().trim_end_matches('.');
    if trimmed.len() != sanitized.len() {
        sanitized = trimmed.to_string();
        was_modified = true;
    }

    if sanitized.is_empty() {
        sanitized = "_".to_string();
        was_modified = true;
    }

    was_modified |= mark_reserved_name(&mut sanitized);

    (sanitized, was_modified)
}

/// Sanitizes a filename for cross-platform filesystem compatibility
///
/// Replaces characters that are invalid on Windows or Unix filesystems
/// with underscores, while preserving path separators.
///
/// Invalid characters replaced with underscores:
/// - Windows: `< > : " | ? *` and control characters (0-31)
/// - Unix: null character (\0)
/// - Both: leading/trailing spaces, trailing dots in path components
///
/// Also handles Windows reserved names (CON, PRN, AUX, NUL, COM1-9, LPT1-9)
/// by appending an underscore, and drops `.`/`..` path components: kept,
/// they would let a server-controlled name escape the working directory.
///
/// # Arguments
/// * `filename` - The original filename (may include path components separated by `/`)
///
/// # Returns
/// * `(sanitized_filename, was_modified)` - Tuple of cleaned filename and whether it was changed
///
/// # Examples
/// ```
/// use ia_get::utils::sanitize_filename;
///
/// let (sanitized, modified) = sanitize_filename("normal_file.txt");
/// assert_eq!(sanitized, "normal_file.txt");
/// assert!(!modified);
///
/// let (sanitized, modified) = sanitize_filename("file?name.txt");
/// assert_eq!(sanitized, "file_name.txt");
/// assert!(modified);
///
/// let (sanitized, modified) = sanitize_filename("Season 1/Episode?.mp4");
/// assert_eq!(sanitized, "Season 1/Episode_.mp4");
/// assert!(modified);
/// ```
pub fn sanitize_filename(filename: &str) -> (String, bool) {
    // Process each path component separately to preserve directory structure
    let components: Vec<&str> = filename.split('/').collect();
    // "." and ".." are dropped like empty components: kept, they would let
    // a name escape the working directory
    let kept_count = components
        .iter()
        .filter(|c| !c.is_empty() && **c != "." && **c != "..")
        .count();

    // Dropped components (empty, "." or "..") count as a modification,
    // except for an empty input
    let mut was_modified = !filename.is_empty() && kept_count != components.len();

    let mut result = String::with_capacity(filename.len());
    let mut emitted = 0;

    for component in &components {
        if component.is_empty() || *component == "." || *component == ".." {
            continue;
        }

        if emitted > 0 {
            result.push('/');
        }
        emitted += 1;

        let (sanitized, modified) = sanitize_component(component);
        was_modified |= modified;
        result.push_str(&sanitized);
    }

    (result, was_modified)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_valid_filename() {
        let (result, modified) = sanitize_filename("normal_file-name.txt");
        assert_eq!(result, "normal_file-name.txt");
        assert!(!modified);
    }

    #[test]
    fn test_sanitize_valid_filename_with_path() {
        let (result, modified) = sanitize_filename("folder/subfolder/file.txt");
        assert_eq!(result, "folder/subfolder/file.txt");
        assert!(!modified);
    }

    #[test]
    fn test_sanitize_invalid_characters() {
        let (result, modified) = sanitize_filename("file?name:test<>.txt");
        assert_eq!(result, "file_name_test__.txt");
        assert!(modified);
    }

    #[test]
    fn test_sanitize_question_mark() {
        let (result, modified) = sanitize_filename("Episode?.mp4");
        assert_eq!(result, "Episode_.mp4");
        assert!(modified);
    }

    #[test]
    fn test_sanitize_with_path() {
        let (result, modified) = sanitize_filename("Season 1/Episode?.mp4");
        assert_eq!(result, "Season 1/Episode_.mp4");
        assert!(modified);
    }

    #[test]
    fn test_sanitize_multiple_invalid_in_path() {
        let (result, modified) = sanitize_filename("Folder:Name/File*Name?.txt");
        assert_eq!(result, "Folder_Name/File_Name_.txt");
        assert!(modified);
    }

    #[test]
    fn test_sanitize_windows_reserved_names() {
        let (result, modified) = sanitize_filename("CON.txt");
        assert_eq!(result, "CON_.txt");
        assert!(modified);

        let (result, modified) = sanitize_filename("con.txt");
        assert_eq!(result, "con_.txt");
        assert!(modified);

        let (result, modified) = sanitize_filename("PRN");
        assert_eq!(result, "PRN_");
        assert!(modified);

        let (result, modified) = sanitize_filename("aux.log");
        assert_eq!(result, "aux_.log");
        assert!(modified);

        let (result, modified) = sanitize_filename("COM1.dat");
        assert_eq!(result, "COM1_.dat");
        assert!(modified);

        let (result, modified) = sanitize_filename("LPT9.txt");
        assert_eq!(result, "LPT9_.txt");
        assert!(modified);
    }

    #[test]
    fn test_sanitize_reserved_in_path() {
        let (result, modified) = sanitize_filename("folder/CON.txt");
        assert_eq!(result, "folder/CON_.txt");
        assert!(modified);
    }

    #[test]
    fn test_sanitize_control_characters() {
        let (result, modified) = sanitize_filename("file\x00\x1fname.txt");
        assert_eq!(result, "file__name.txt");
        assert!(modified);

        let (result, modified) = sanitize_filename("test\x7Ffile.txt");
        assert_eq!(result, "test_file.txt");
        assert!(modified);
    }

    #[test]
    fn test_sanitize_backslash() {
        let (result, modified) = sanitize_filename("folder\\file.txt");
        assert_eq!(result, "folder_file.txt");
        assert!(modified);
    }

    #[test]
    fn test_sanitize_whitespace_edge_cases() {
        let (result, modified) = sanitize_filename(" leading.txt ");
        assert_eq!(result, "leading.txt");
        assert!(modified);

        let (result, modified) = sanitize_filename("folder/ spaces /file.txt");
        assert_eq!(result, "folder/spaces/file.txt");
        assert!(modified);
    }

    #[test]
    fn test_sanitize_drops_dot_segments() {
        // "." and ".." must not survive: kept, a server-controlled name
        // could escape the working directory.
        let (result, modified) = sanitize_filename("../../etc/passwd");
        assert_eq!(result, "etc/passwd");
        assert!(modified);

        let (result, modified) = sanitize_filename("a/./b/c.txt");
        assert_eq!(result, "a/b/c.txt");
        assert!(modified);

        let (result, modified) = sanitize_filename("a/b.txt");
        assert_eq!(result, "a/b.txt");
        assert!(!modified);
    }

    #[test]
    fn test_sanitize_trailing_dots() {
        let (result, modified) = sanitize_filename("file...");
        assert_eq!(result, "file");
        assert!(modified);

        let (result, modified) = sanitize_filename("folder./file.txt");
        assert_eq!(result, "folder/file.txt");
        assert!(modified);
    }

    #[test]
    fn test_sanitize_empty_components() {
        let (result, modified) = sanitize_filename("folder//file.txt");
        assert_eq!(result, "folder/file.txt");
        assert!(modified);

        let (result, modified) = sanitize_filename("/folder/file.txt");
        assert_eq!(result, "folder/file.txt");
        assert!(modified);

        let (result, modified) = sanitize_filename("folder/file.txt/");
        assert_eq!(result, "folder/file.txt");
        assert!(modified);
    }

    #[test]
    fn test_sanitize_all_invalid() {
        let (result, modified) = sanitize_filename("???");
        assert_eq!(result, "___");
        assert!(modified);
    }

    #[test]
    fn test_sanitize_unicode() {
        let (result, modified) = sanitize_filename("файл.txt");
        assert_eq!(result, "файл.txt");
        assert!(!modified);

        let (result, modified) = sanitize_filename("文件.txt");
        assert_eq!(result, "文件.txt");
        assert!(!modified);

        let (result, modified) = sanitize_filename("emoji😀.txt");
        assert_eq!(result, "emoji😀.txt");
        assert!(!modified);
    }

    #[test]
    fn test_sanitize_mixed_valid_invalid() {
        let (result, modified) =
            sanitize_filename("Red vs. Blue - Season 1/Episode 1: Why Are We Here?.mp4");
        assert_eq!(
            result,
            "Red vs. Blue - Season 1/Episode 1_ Why Are We Here_.mp4"
        );
        assert!(modified);
    }

    #[test]
    fn test_sanitize_preserves_extension() {
        let (result, modified) = sanitize_filename("file:name.tar.gz");
        assert_eq!(result, "file_name.tar.gz");
        assert!(modified);
    }
}
