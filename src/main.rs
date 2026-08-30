//! # ia-get
//!
//! A command-line tool for downloading files from the Internet Archive.
//!
//! This tool takes an archive.org details URL and downloads all associated files,
//! with support for resumable downloads and MD5 hash verification.

use clap::Parser;
use colored::*;
use ia_get::archive_metadata::{parse_xml_files, save_xml_metadata, XmlFile, XmlFiles};
use ia_get::constants::USER_AGENT;
use ia_get::downloader::{self, DownloadTask};
use ia_get::utils::{
    create_spinner, finish_spinner, format_size, print_downloaded_line, print_file_banner,
    sanitize_filename, validate_archive_url, with_cookie,
};
use ia_get::{IaGetError, Result};
use reqwest::header::HeaderValue;
use reqwest::{Client, Url};
use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Extended timeout for large file downloads (10 minutes for connection)
const CONNECTION_TIMEOUT_SECS: u64 = 600;

/// Idle timeout for reading the response body. Applies to each read and resets
/// after a successful one, so a stalled mid-transfer becomes a retryable
/// mid-stream error (resumed later) instead of an infinite hang.
const READ_TIMEOUT_SECS: u64 = 300;

/// Checks if a URL is accessible by sending a HEAD request
async fn is_url_accessible(
    url: &Url,
    client: &Client,
    cookie_header: Option<&HeaderValue>,
) -> Result<()> {
    let request = with_cookie(client.head(url.clone()), cookie_header);

    let response = request
        .timeout(std::time::Duration::from_secs(60))
        .send()
        .await?;

    response.error_for_status()?;
    Ok(())
}

/// Converts a details URL to the corresponding XML files list URL
///
/// Takes an archive.org details URL and converts it to the XML metadata URL
/// by replacing "details" with "download" and appending "_files.xml"
///
/// # Arguments
/// * `original_url` - The archive.org details URL
///
/// # Returns
/// The corresponding XML files list URL
fn get_xml_url(original_url: &str) -> String {
    // Remove trailing slash if present to get a consistent base for identifier extraction
    let trimmed_url = original_url.trim_end_matches('/');

    // The identifier is the last segment of the trimmed URL
    // This expect is considered safe because get_xml_url is only called after
    // validate_archive_url has confirmed the URL structure.
    let identifier = trimmed_url
        .rsplit('/')
        .next() // Changed from split().last() to address clippy warning
        .expect("Validated URL should have a valid identifier segment after validation");

    // The base URL for download is "https://archive.org/download/{identifier}"
    let download_url_base = format!("https://archive.org/download/{}", identifier);

    // The XML URL is "{download_url_base}/{identifier}_files.xml"
    format!("{}/{}_files.xml", download_url_base, identifier)
}

/// Percent-encodes every path segment of a file name, preserving the `/`
/// separators, so that URL-special characters in the name (`?`, `#`, `%`,
/// spaces, non-ASCII, ...) cannot be misread as a query string or fragment
/// by `Url::join`.
fn encode_download_path(name: &str) -> String {
    let mut out = String::new();
    for (i, segment) in name.split('/').enumerate() {
        if i > 0 {
            out.push('/');
        }
        for ch in segment.chars() {
            match ch {
                'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => out.push(ch),
                _ => {
                    let mut buf = [0u8; 4];
                    for b in ch.encode_utf8(&mut buf).as_bytes() {
                        out.push_str(&format!("%{b:02X}"));
                    }
                }
            }
        }
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NetscapeCookie {
    domain: String,
    include_subdomains: bool,
    path: String,
    secure: bool,
    expires: Option<u64>,
    name: String,
    value: String,
}

/// Builds an HTTP Cookie header value from a raw cookie string or cookies.txt path.
fn cookie_header_from_input(input: &str, url: &Url) -> Result<String> {
    if Path::new(input).is_file() {
        let cookie_file = fs::read_to_string(input)?;
        cookie_header_from_netscape_file(&cookie_file, url)
    } else {
        Ok(input.trim().to_string())
    }
}

fn parse_netscape_cookie(line: &str) -> Option<NetscapeCookie> {
    let line = line.trim();
    let line = line.strip_prefix("#HttpOnly_").unwrap_or(line);

    if line.is_empty() || line.starts_with('#') {
        return None;
    }

    let fields = line.split('\t').collect::<Vec<_>>();
    if fields.len() < 7 {
        return None;
    }

    let expires = match fields[4].parse::<u64>().unwrap_or(0) {
        0 => None,
        value => Some(value),
    };

    Some(NetscapeCookie {
        domain: fields[0].trim_start_matches('.').to_ascii_lowercase(),
        include_subdomains: fields[1].eq_ignore_ascii_case("TRUE"),
        path: fields[2].to_string(),
        secure: fields[3].eq_ignore_ascii_case("TRUE"),
        expires,
        name: fields[5].to_string(),
        value: fields[6].to_string(),
    })
}

fn cookie_domain_matches(cookie: &NetscapeCookie, url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };

    let host = host.to_ascii_lowercase();
    host == cookie.domain
        || (cookie.include_subdomains && host.ends_with(&format!(".{}", cookie.domain)))
}

fn cookie_path_matches(cookie: &NetscapeCookie, url: &Url) -> bool {
    let cookie_path = if cookie.path.is_empty() {
        "/"
    } else {
        &cookie.path
    };
    let request_path = url.path();

    request_path == cookie_path
        || request_path
            .strip_prefix(cookie_path)
            .is_some_and(|remainder| cookie_path.ends_with('/') || remainder.starts_with('/'))
}

fn cookie_applies_to_url(cookie: &NetscapeCookie, url: &Url, now: u64) -> bool {
    if let Some(expires) = cookie.expires {
        if expires <= now {
            return false;
        }
    }

    if cookie.secure && url.scheme() != "https" {
        return false;
    }

    cookie_domain_matches(cookie, url) && cookie_path_matches(cookie, url)
}

/// Parses Netscape cookies.txt content into an HTTP Cookie header value.
fn cookie_header_from_netscape_file(content: &str, url: &Url) -> Result<String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| IaGetError::FileSystem(e.to_string()))?
        .as_secs();

    let cookies = content
        .lines()
        .filter_map(parse_netscape_cookie)
        .filter(|cookie| cookie_applies_to_url(cookie, url, now))
        .map(|cookie| format!("{}={}", cookie.name, cookie.value))
        .collect::<Vec<_>>();

    Ok(cookies.join("; "))
}

fn cookie_header_value(cookie_input: Option<&str>, url: &Url) -> Result<Option<HeaderValue>> {
    let Some(cookie_input) = cookie_input else {
        return Ok(None);
    };

    let cookie_header = cookie_header_from_input(cookie_input, url)?;
    if cookie_header.is_empty() {
        return Ok(None);
    }

    let value = HeaderValue::from_str(&cookie_header)
        .map_err(|e| IaGetError::Network(format!("Invalid cookie header: {e}")))?;
    Ok(Some(value))
}

/// XML metadata response: the parsed file list plus the raw data needed to
/// persist the `_files.xml` document locally.
#[derive(Debug)]
struct XmlMetadata {
    files: XmlFiles,
    base_url: Url,
    cookie_header: Option<HeaderValue>,
    content: String,
    last_modified: Option<SystemTime>,
}

/// Fetches and parses XML metadata from archive.org
///
/// Combines XML URL generation, accessibility check, download, and parsing
/// into a single operation with integrated error handling.
///
/// # Arguments
/// * `details_url` - The original archive.org details URL
/// * `client` - HTTP client for requests
/// * `spinner` - Progress spinner to update during processing
///
/// # Returns
/// The `XmlMetadata` with the parsed files, base URL, cookie header, raw
/// XML content and the server's `Last-Modified` time
async fn fetch_xml_metadata(
    details_url: &str,
    client: &Client,
    spinner: &indicatif::ProgressBar,
    cookie_input: Option<&str>,
) -> Result<XmlMetadata> {
    // Generate XML URL
    let xml_url = get_xml_url(details_url);
    spinner.set_message(format!(
        "{} Accessing XML metadata: {}",
        "⚙".blue(),
        xml_url.bold()
    ));
    fetch_and_parse_xml(&xml_url, client, spinner, cookie_input).await
}

/// Fetches, downloads and parses a `_files.xml` document from an explicit URL.
///
/// Split out of `fetch_xml_metadata` so tests can point it at a local mock
/// server instead of the fixed archive.org download URL.
async fn fetch_and_parse_xml(
    xml_url: &str,
    client: &Client,
    spinner: &indicatif::ProgressBar,
    cookie_input: Option<&str>,
) -> Result<XmlMetadata> {
    // Parse base URL and fetch XML content
    let base_url = Url::parse(xml_url)?;
    let cookie_header = cookie_header_value(cookie_input, &base_url)?;

    // Check XML URL accessibility
    if let Err(e) = is_url_accessible(&base_url, client, cookie_header.as_ref()).await {
        spinner.finish_with_message(format!(
            "{} XML metadata not accessible: {}",
            "✘".red().bold(),
            xml_url.bold()
        ));
        return Err(e); // Propagate the error
    }

    spinner.set_message(format!(
        "{} {}",
        "⚙".blue(),
        "Parsing archive metadata...".bold()
    ));

    let request = with_cookie(client.get(base_url.clone()), cookie_header.as_ref());

    // The HEAD check above can pass while the GET still fails (throttling,
    // transient edge errors): surface it as a network error instead of
    // feeding an error page into the XML parser.
    let response = request.send().await?.error_for_status()?;
    let last_modified = downloader::parse_last_modified(response.headers());
    let xml_content = response.text().await?;

    // Parse XML content with improved error handling
    let files = parse_xml_files(&xml_content)?;

    Ok(XmlMetadata {
        files,
        base_url,
        cookie_header,
        content: xml_content,
        last_modified,
    })
}

/// The `_files.xml` entry name: the last path segment of the XML URL
/// (which always ends in "<identifier>_files.xml", see get_xml_url)
fn xml_file_name_of(url: &Url) -> &str {
    url.path()
        .rsplit('/')
        .next()
        .expect("XML URL should have a file name segment")
}

/// Persists the freshly fetched `_files.xml` (overwriting any previous copy)
/// and prints its "#1" file block.
///
/// Returns the total file count: the archive's files plus the saved
/// `_files.xml`, which archive.org lists as the first file, so the
/// downloaded files are numbered from #2. If the metadata lacks its own
/// entry, the saved copy is still counted.
fn save_and_announce_xml(
    files: &XmlFiles,
    base_url: &Url,
    content: &str,
    last_modified: Option<SystemTime>,
) -> Result<usize> {
    let xml_file_name = xml_file_name_of(base_url);
    save_xml_metadata(Path::new(xml_file_name), content, last_modified)?;

    let total_files =
        files.files.len() + usize::from(!files.files.iter().any(|f| f.name == xml_file_name));
    println!(" ");
    print_file_banner(xml_file_name, 1, total_files);
    // The file never crossed the network, so its line carries no time/rate
    print_downloaded_line(&"╰╼".cyan().dimmed(), content.len() as u64, None);

    Ok(total_files)
}

/// Filters out the archive's self-referencing `_files.xml` entry, whose
/// checksum, mtime and size are unreliable, leaving the files to download.
fn files_to_download(files: Vec<XmlFile>, xml_file_name: &str) -> Vec<XmlFile> {
    files
        .into_iter()
        .filter(|file| file.name != xml_file_name)
        .collect()
}

/// Converts the parsed metadata into download tasks: builds each file's
/// absolute URL and its sanitized local path, warning about every rename.
///
/// Returns the tasks and how many filenames were sanitized. A failed URL
/// join aborts the run: silently keeping the base URL would download the
/// metadata file under the file's name.
fn build_download_tasks(files: Vec<XmlFile>, base_url: &Url) -> Result<(Vec<DownloadTask>, usize)> {
    let mut sanitized_count = 0;
    let tasks = files
        .into_iter()
        .map(|file| {
            // Percent-encode the name first so '?' / '#' / '%' characters in
            // it cannot split the URL into query or fragment components.
            let encoded_name = encode_download_path(&file.name);
            let absolute_url = base_url.join(&encoded_name)?;

            // Sanitize filename for filesystem compatibility
            let (sanitized_name, was_modified) = sanitize_filename(&file.name);

            // Warn user if filename was modified
            if was_modified {
                println!(
                    "{} {} {} → {}",
                    "⚠".yellow().bold(),
                    "Sanitized:".yellow(),
                    file.name.dimmed(),
                    sanitized_name.bold()
                );
                sanitized_count += 1;
            }

            Ok(DownloadTask {
                url: absolute_url.to_string(),
                file_path: sanitized_name,
                expected_md5: file.md5,
                expected_size: file.size,
                expected_mtime: file.mtime,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok((tasks, sanitized_count))
}

/// Return formatted file rows for `--list` output.
fn list_file_rows(files: &XmlFiles) -> Vec<String> {
    files
        .files
        .iter()
        .map(|file| {
            let size = file
                .size
                .map(format_size)
                .unwrap_or_else(|| "unknown".to_string());
            format!("{size:>9} {}", file.name)
        })
        .collect()
}

/// Return a summary for `--list` output.
fn list_summary(files: &XmlFiles) -> String {
    let total_known_size: u64 = files.files.iter().filter_map(|file| file.size).sum();
    let unknown_size_count = files
        .files
        .iter()
        .filter(|file| file.size.is_none())
        .count();
    let file_label = if files.files.len() == 1 {
        "file"
    } else {
        "files"
    };

    if unknown_size_count == 0 {
        format!(
            "{} {file_label}, {} total",
            files.files.len(),
            format_size(total_known_size)
        )
    } else {
        let unknown_label = if unknown_size_count == 1 {
            "unknown size"
        } else {
            "unknown sizes"
        };
        format!(
            "{} {file_label}, {} total known size, {} {unknown_label}",
            files.files.len(),
            format_size(total_known_size),
            unknown_size_count
        )
    }
}

/// Lists parsed filenames from XML metadata when --list/-l is used
fn list_files(files: &XmlFiles, spinner: &indicatif::ProgressBar) {
    finish_spinner(
        spinner,
        &format!(
            "{} Archive has {}",
            "✔".green().bold(),
            list_summary(files).bold()
        ),
    );
    for row in list_file_rows(files) {
        println!("{row}");
    }
}

/// Command-line interface for ia-get
#[derive(Parser)]
#[command(name = "ia-get")]
#[command(about = "A command-line tool for downloading files from the Internet Archive")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(author = env!("CARGO_PKG_AUTHORS"))]
struct Cli {
    /// URL to an archive.org details page
    url: String,
    /// List files parsed from archive metadata XML and exit
    #[arg(short = 'l', long = "list")]
    list: bool,
    /// Cookie header or Netscape cookies.txt file for authenticated downloads
    #[arg(short = 'b', long = "cookies", value_name = "COOKIES")]
    cookies: Option<String>,
    /// Stop at the first failed file instead of continuing with the rest
    #[arg(long)]
    stop_on_error: bool,
}

/// Main application entry point
///
/// Parses command line arguments, validates the archive.org URL, checks URL accessibility,
/// downloads XML metadata, and initiates file downloads with built-in signal handling.
#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Extended timeouts for large file downloads: a long connection timeout,
    // plus a per-read idle timeout that resets after each successful read,
    // so a stalled mid-transfer becomes a retryable error instead of a hang
    let client = Client::builder()
        .user_agent(USER_AGENT)
        .connect_timeout(Duration::from_secs(CONNECTION_TIMEOUT_SECS))
        .read_timeout(Duration::from_secs(READ_TIMEOUT_SECS))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(1)
        .tcp_keepalive(Duration::from_secs(60))
        .build()?;

    // Start a single spinner for the entire initialization process
    let spinner = create_spinner(&format!("Processing archive.org URL: {}", cli.url.bold()));

    // Validate URL format using consolidated function
    if let Err(e) = validate_archive_url(&cli.url) {
        spinner.finish_with_message(format!("{} {}", "✘".red().bold(), e));
        return Err(e.into());
    }

    let details_url = Url::parse(&cli.url)?;
    let cookie_header = cookie_header_value(cli.cookies.as_deref(), &details_url)?;

    // Check URL accessibility
    if let Err(e) = is_url_accessible(&details_url, &client, cookie_header.as_ref()).await {
        spinner.finish_with_message(format!(
            "{} Archive.org URL not accessible: {}",
            "✘".red().bold(),
            cli.url.bold()
        ));
        return Err(e.into()); // Propagate error
    }

    // Fetch and parse XML metadata in one operation
    let XmlMetadata {
        files,
        base_url,
        cookie_header,
        content,
        last_modified,
    } = fetch_xml_metadata(&cli.url, &client, &spinner, cli.cookies.as_deref()).await?;

    // Persist the freshly fetched _files.xml (overwriting any previous copy)
    // with the server's Last-Modified time, and announce it as file #1
    let total_files = save_and_announce_xml(&files, &base_url, &content, last_modified)?;

    // If requested, list parsed filenames and exit
    if cli.list {
        list_files(&files, &spinner);
        return Ok(());
    }

    let files = files_to_download(files.files, xml_file_name_of(&base_url));

    // Successfully finished initialization; separate the banner from the
    // saved-metadata block above.
    println!();
    finish_spinner(
        &spinner,
        &format!(
            "{} {} to download {} files from archive.org {}",
            "✔".green().bold(),
            "Ready".bold(),
            files.len().to_string().bold(),
            "★".yellow()
        ),
    );

    // Prepare download data for batch processing
    let (download_tasks, sanitized_count) = build_download_tasks(files, &base_url)?;

    // Show summary if any files were sanitized
    if sanitized_count > 0 {
        println!(
            "\n{} {} {} file{} for filesystem compatibility",
            "✓".green().bold(),
            "Sanitized".bold(),
            sanitized_count.to_string().bold(),
            if sanitized_count == 1 { "" } else { "s" }
        );
    }

    // Download all files with integrated signal handling. File numbering
    // starts at #2: #1 is the _files.xml saved above.
    downloader::download_files(
        &client,
        download_tasks,
        total_files,
        2,
        cookie_header.as_ref(),
        cli.stop_on_error,
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ia_get::utils::validate_archive_url;

    #[test]
    fn check_valid_pattern() {
        assert!(validate_archive_url("https://archive.org/details/Valid-Pattern").is_ok());
        assert!(validate_archive_url("https://archive.org/details/Valid-Pattern/").is_ok());
        assert!(validate_archive_url("https://archive.org/details/test123").is_ok());
        assert!(validate_archive_url("https://archive.org/details/test123/").is_ok());
        assert!(validate_archive_url("https://archive.org/details/test_file-name.data").is_ok());
        assert!(validate_archive_url("https://archive.org/details/test_file-name.data/").is_ok());
        assert!(validate_archive_url("https://archive.org/details/user@domain").is_ok());
        assert!(validate_archive_url("https://archive.org/details/user@domain/").is_ok());
    }

    #[test]
    fn check_invalid_pattern() {
        assert!(validate_archive_url("https://archive.org/details/Invalid-Pattern-*").is_err());
        assert!(validate_archive_url("https://archive.org/details/").is_err()); // This should still be an error (empty identifier)
        assert!(validate_archive_url("https://example.com/details/test").is_err());
        assert!(validate_archive_url("http://archive.org/details/test").is_err());
        assert!(validate_archive_url("https://archive.org/details/test/extra").is_err());
        assert!(validate_archive_url("https://archive.org/details/test//").is_err());
        // Multiple trailing slashes
    }

    #[test]
    fn check_get_xml_url() {
        assert_eq!(
            get_xml_url("https://archive.org/details/item1"),
            "https://archive.org/download/item1/item1_files.xml"
        );
        assert_eq!(
            get_xml_url("https://archive.org/details/item1/"), // With trailing slash
            "https://archive.org/download/item1/item1_files.xml"
        );
        assert_eq!(
            get_xml_url("https://archive.org/details/another-item_v2.0"),
            "https://archive.org/download/another-item_v2.0/another-item_v2.0_files.xml"
        );
        assert_eq!(
            get_xml_url("https://archive.org/details/another-item_v2.0/"), // With trailing slash
            "https://archive.org/download/another-item_v2.0/another-item_v2.0_files.xml"
        );
    }

    #[test]
    fn encode_download_path_escapes_url_specials() {
        // '#' starts a fragment and '?' a query — both must be escaped.
        assert_eq!(encode_download_path("clip#1.mp4"), "clip%231.mp4");
        assert_eq!(encode_download_path("a?b.mp4"), "a%3Fb.mp4");
        // A literal '%' in the name must be escaped so it is not read as an
        // existing escape sequence.
        assert_eq!(encode_download_path("100%25off.mp4"), "100%2525off.mp4");
        // Spaces and non-ASCII are encoded; '/' separators are preserved.
        assert_eq!(
            encode_download_path("sub dir/файл.mp4"),
            "sub%20dir/%D1%84%D0%B0%D0%B9%D0%BB.mp4"
        );
        // Unreserved characters pass through unchanged.
        assert_eq!(
            encode_download_path("plain-file_v2.0.tar.gz"),
            "plain-file_v2.0.tar.gz"
        );
    }

    fn cookie_test_url(path: &str) -> Url {
        Url::parse(&format!("https://archive.org{path}")).unwrap()
    }

    #[test]
    fn cookie_header_accepts_raw_cookie_string() {
        assert_eq!(
            cookie_header_from_input(
                "logged-in-user=yes; logged-in-sig=abc123",
                &cookie_test_url("/download/item/item_files.xml"),
            )
            .unwrap(),
            "logged-in-user=yes; logged-in-sig=abc123"
        );
    }

    #[test]
    fn cookie_header_parses_netscape_cookie_file_content() {
        let cookies = "# Netscape HTTP Cookie File\n\
.archive.org\tTRUE\t/\tFALSE\t2145916800\tlogged-in-user\tyes\n\
archive.org\tFALSE\t/\tTRUE\t2145916800\tlogged-in-sig\tabc123\n";

        assert_eq!(
            cookie_header_from_netscape_file(
                cookies,
                &cookie_test_url("/download/item/item_files.xml")
            )
            .unwrap(),
            "logged-in-user=yes; logged-in-sig=abc123"
        );
    }

    #[test]
    fn cookie_header_respects_domain_and_path_scoping() {
        let cookies = "# Netscape HTTP Cookie File\n\
.archive.org\tTRUE\t/download\tFALSE\t2145916800\tdownload-root\tyes\n\
archive.org\tFALSE\t/account\tFALSE\t2145916800\taccount-only\tnope\n\
example.com\tFALSE\t/download\tFALSE\t2145916800\twrong-domain\tnope\n\
archive.org\tFALSE\t/download/private\tFALSE\t2145916800\tprivate-only\tsecret\n";

        assert_eq!(
            cookie_header_from_netscape_file(cookies, &cookie_test_url("/download/item/file.zip"))
                .unwrap(),
            "download-root=yes"
        );

        assert_eq!(
            cookie_header_from_netscape_file(
                cookies,
                &cookie_test_url("/download/private/file.zip")
            )
            .unwrap(),
            "download-root=yes; private-only=secret"
        );
    }

    #[test]
    fn cookie_header_ignores_expired_netscape_cookies() {
        let cookies = "archive.org\tFALSE\t/\tFALSE\t1\told\tvalue\n\
archive.org\tFALSE\t/\tFALSE\t2145916800\tcurrent\tvalue\n";

        assert_eq!(
            cookie_header_from_netscape_file(
                cookies,
                &cookie_test_url("/download/item/item_files.xml")
            )
            .unwrap(),
            "current=value"
        );
    }

    fn xml_file(name: &str, size: Option<u64>) -> ia_get::archive_metadata::XmlFile {
        ia_get::archive_metadata::XmlFile {
            name: name.to_string(),
            source: None,
            mtime: None,
            size,
            format: None,
            rotation: None,
            md5: None,
            crc32: None,
            sha1: None,
            btih: None,
            summation: None,
            original: None,
        }
    }

    #[test]
    fn list_file_rows_format_sizes_and_unknown_entries() {
        let files = XmlFiles {
            files: vec![
                xml_file("cover.jpg", Some(12_345)),
                xml_file("metadata.xml", None),
            ],
        };

        assert_eq!(
            list_file_rows(&files),
            vec![
                "  12.06KB cover.jpg".to_string(),
                "  unknown metadata.xml".to_string(),
            ]
        );
    }

    #[test]
    fn list_summary_reports_total_known_size_and_unknown_count() {
        let files = XmlFiles {
            files: vec![
                xml_file("disk1.zip", Some(1_048_576)),
                xml_file("disk2.zip", Some(2_097_152)),
                xml_file("notes.txt", None),
            ],
        };

        assert_eq!(
            list_summary(&files),
            "3 files, 3.00MB total known size, 1 unknown size"
        );
    }

    #[test]
    fn files_to_download_excludes_xml_self_reference() {
        let files = vec![
            xml_file("item1_files.xml", Some(123)),
            xml_file("scan.jpg", Some(456)),
        ];

        let result = files_to_download(files, "item1_files.xml");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "scan.jpg");
    }

    #[test]
    fn files_to_download_keeps_all_when_no_self_reference() {
        let files = vec![xml_file("scan.jpg", Some(456)), xml_file("notes.txt", None)];

        let result = files_to_download(files, "item1_files.xml");

        assert_eq!(result.len(), 2);
    }

    /// Minimal single-purpose mock: HEAD always succeeds, GET returns the
    /// given status and body. Used to drive `fetch_and_parse_xml` against a
    /// local URL instead of archive.org.
    fn start_xml_mock(get_status: u16, get_body: &str) -> String {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind mock server");
        let port = listener.local_addr().unwrap().port();
        let body = get_body.to_string();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                handle_xml_mock_connection(stream, get_status, &body);
            }
        });
        format!("http://127.0.0.1:{port}")
    }

    fn handle_xml_mock_connection(
        mut stream: std::net::TcpStream,
        get_status: u16,
        get_body: &str,
    ) {
        use std::io::{Read, Write};

        let mut buf = [0u8; 1024];
        let mut request = Vec::new();
        loop {
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    request.extend_from_slice(&buf[..n]);
                    if request.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => return,
            }
        }

        let head = String::from_utf8_lossy(&request);
        let method = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().next())
            .unwrap_or("GET");

        let (status, reason, body) = if method == "HEAD" {
            (200, "OK", "")
        } else {
            (get_status, "Error", get_body)
        };

        let response = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes());
    }

    #[tokio::test]
    async fn xml_metadata_http_error_is_a_network_error() {
        // The HEAD check passes; the GET must fail with a status error.
        let base = start_xml_mock(500, "<html><body>nginx error page</body></html>");
        let url = format!("{base}/download/item1/item1_files.xml");
        let client = Client::new();
        let spinner = create_spinner("mock");

        let err = fetch_and_parse_xml(&url, &client, &spinner, None)
            .await
            .expect_err("an HTTP error on the metadata GET must fail the fetch");

        match err {
            ia_get::IaGetError::Network(detail) => {
                assert!(
                    detail.contains("500"),
                    "expected the status code in the error, got: {detail}"
                );
            }
            other => panic!("expected a Network error, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn xml_metadata_success_parses_files() {
        let xml = "<files><file name=\"item1_files.xml\" source=\"original\"><size>23</size></file><file name=\"scan.jpg\" source=\"original\"><size>456</size></file></files>";
        let base = start_xml_mock(200, xml);
        let url = format!("{base}/download/item1/item1_files.xml");
        let client = Client::new();
        let spinner = create_spinner("mock");

        let meta = fetch_and_parse_xml(&url, &client, &spinner, None)
            .await
            .expect("metadata fetch should succeed");

        assert_eq!(meta.files.files.len(), 2);
        assert_eq!(meta.files.files[1].name, "scan.jpg");
        assert_eq!(meta.content, xml);
    }

    #[test]
    fn file_url_join_must_not_silently_fall_back() {
        // Regression: a failed join used to keep the XML metadata URL, which
        // would download _files.xml under the file's name. Encoded names must
        // always join cleanly against the metadata base URL.
        let base = Url::parse("https://archive.org/download/item1/item1_files.xml").unwrap();
        let joined = base
            .join(&encode_download_path("Season 1/clip#1.mp4"))
            .expect("encoded name must join against the base URL");
        assert_eq!(
            joined.as_str(),
            "https://archive.org/download/item1/Season%201/clip%231.mp4"
        );
    }
}
