//! # ia-get
//!
//! A command-line tool for downloading files from the Internet Archive.
//!
//! This tool takes an archive.org details URL and downloads all associated files,
//! with support for resumable downloads and MD5 hash verification.

use clap::Parser;
use colored::*;
use ia_get::archive_metadata::{
    encode_download_path, fetch_xml_metadata, is_url_accessible, save_xml_metadata,
    xml_file_name_of, XmlFile, XmlFiles, XmlMetadata,
};
use ia_get::constants::USER_AGENT;
use ia_get::cookie::cookie_header_value;
use ia_get::downloader::{self, DownloadTask};
use ia_get::utils::{
    create_spinner, finish_spinner, format_size, last_glyph, print_downloaded_line,
    print_file_banner, sanitize_filename, validate_archive_url,
};
use ia_get::Result;
use indicatif::ProgressBar;
use reqwest::{Client, Url};
use std::path::Path;
use std::time::{Duration, SystemTime};

/// Extended timeout for large file downloads (10 minutes for connection)
const CONNECTION_TIMEOUT_SECS: u64 = 600;

/// Idle timeout for reading the response body. Applies to each read and resets
/// after a successful one, so a stalled mid-transfer becomes a retryable
/// mid-stream error (resumed later) instead of an infinite hang.
const READ_TIMEOUT_SECS: u64 = 300;

/// Idle timeout for a pooled connection
const POOL_IDLE_TIMEOUT_SECS: u64 = 90;

/// Maximum number of idle connections kept per host
const POOL_MAX_IDLE_PER_HOST: usize = 1;

/// Interval between TCP keepalive probes
const TCP_KEEPALIVE_SECS: u64 = 60;

/// Number assigned to the saved `_files.xml` in the per-file listing; the
/// archive's files are numbered right after it.
const XML_FILE_NUMBER: usize = 1;

/// HTTP client for the download session: an extended connection timeout,
/// plus a per-read idle timeout that resets after each successful read, so
/// a stalled mid-transfer becomes a retryable error instead of a hang.
fn build_client() -> Result<Client> {
    Ok(Client::builder()
        .user_agent(USER_AGENT)
        .connect_timeout(Duration::from_secs(CONNECTION_TIMEOUT_SECS))
        .read_timeout(Duration::from_secs(READ_TIMEOUT_SECS))
        .pool_idle_timeout(Duration::from_secs(POOL_IDLE_TIMEOUT_SECS))
        .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
        .tcp_keepalive(Duration::from_secs(TCP_KEEPALIVE_SECS))
        .build()?)
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

    // The archive lists its own _files.xml as a file: count it once, either
    // from the metadata or from the saved copy above.
    let already_listed = files.files.iter().any(|f| f.name == xml_file_name);
    let total_files = files.files.len() + usize::from(!already_listed);
    println!(" ");
    print_file_banner(xml_file_name, XML_FILE_NUMBER, total_files);
    // The file never crossed the network, so its line carries no time/rate
    print_downloaded_line(last_glyph(), content.len() as u64, None);

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
fn list_files(files: &XmlFiles, spinner: &ProgressBar) {
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

    let client = build_client()?;

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

    // Download all files with integrated signal handling; numbering starts
    // right after the _files.xml saved above.
    downloader::download_files(
        &client,
        download_tasks,
        total_files,
        XML_FILE_NUMBER + 1,
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

    fn xml_file(name: &str, size: Option<u64>) -> XmlFile {
        XmlFile {
            name: name.to_string(),
            size,
            ..Default::default()
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
}
