//! # ia-get
//!
//! A command-line tool for downloading files from the Internet Archive.
//!
//! This tool takes an archive.org details URL and downloads all associated files,
//! with support for resumable downloads and MD5 hash verification.

use clap::Parser;
use colored::*;
use ia_get::archive_metadata::{
    XmlFiles, XmlMetadata, fetch_and_parse_xml, get_xml_url, list_file_rows, list_summary,
    save_xml_metadata, validate_archive_url, xml_file_name_of,
};
use ia_get::cookie::cookie_header_value;
use ia_get::display::{
    create_spinner, finish_spinner, last_glyph, print_downloaded_line, print_file_banner,
};
use ia_get::downloader;
use ia_get::plan::{files_to_download, plan_download_tasks};
use ia_get::{IaGetError, Result};
use indicatif::ProgressBar;
use reqwest::{Client, Url};
use std::path::Path;
use std::time::{Duration, SystemTime};

/// Timeout for establishing a single TCP+TLS connection. Stalled or
/// unreachable hosts must fail fast enough for the retry loop to kick in;
/// slow transfers mid-body are covered by READ_TIMEOUT_SECS instead.
const CONNECTION_TIMEOUT_SECS: u64 = 60;

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

/// User agent string for HTTP requests: tool name and version, as archive.org
/// asks clients to identify themselves with
const USER_AGENT: &str = concat!("ia-get/", env!("CARGO_PKG_VERSION"));

/// Number assigned to the saved `_files.xml` in the per-file listing; the
/// archive's files are numbered right after it.
const XML_FILE_NUMBER: usize = 1;

/// HTTP client for the download session: a bounded connection timeout,
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

/// Finishes `spinner` with a red ✘ and the error's message when it is still
/// running, so every failure path leaves a context-rich line on screen.
///
/// `spinner` may already be finished (a deeper failure can finish it on its
/// own before the error reaches here); finishing it again would just repaint
/// the same line, so the call is skipped in that case.
fn report_spinner_error(spinner: &ProgressBar, error: &IaGetError) {
    if !spinner.is_finished() {
        spinner.finish_with_message(format!("{} {}", "✘".red().bold(), error));
    }
}

/// Runs one initialization step of `run`, reporting its failure on the
/// spinner before the error propagates: on screen the user sees the
/// context-rich error line, and `main` only has to decide the exit code.
fn init_step<T>(spinner: &ProgressBar, step: Result<T>) -> Result<T> {
    match step {
        Ok(value) => Ok(value),
        Err(error) => {
            report_spinner_error(spinner, &error);
            Err(error)
        }
    }
}

/// Persists the freshly fetched `_files.xml` (overwriting any previous copy)
/// and prints its "#1" file block.
///
/// `total_files` is the "#1 of N" denominator, computed by the caller from
/// the planned tasks plus this saved copy.
fn save_and_announce_xml(
    base_url: &Url,
    content: &str,
    last_modified: Option<SystemTime>,
    total_files: usize,
) -> Result<()> {
    let xml_file_name = xml_file_name_of(base_url);
    save_xml_metadata(Path::new(xml_file_name), content, last_modified)?;

    println!(" ");
    print_file_banner(xml_file_name, XML_FILE_NUMBER, total_files);
    // The file never crossed the network, so its line carries no time/rate
    print_downloaded_line(last_glyph(), content.len() as u64, None);

    Ok(())
}

/// Lists parsed filenames from XML metadata when --list/-l is used
fn list_files(files: &XmlFiles, spinner: &ProgressBar, xml_file_name: &str) {
    finish_spinner(
        spinner,
        &format!(
            "{} Archive has {}",
            "✔".green().bold(),
            list_summary(files).bold()
        ),
    );
    for row in list_file_rows(files, xml_file_name) {
        println!("{row}");
    }
    // The archive's own _files.xml entry counts in the list above but not
    // in the download plan (it is saved locally as file #1): bridge the
    // two numbers so they do not look inconsistent.
    if files.files.iter().any(|file| file.name == xml_file_name) {
        let remaining = files.files.len() - 1;
        println!(
            "\n{} {} {}",
            "ⓘ".blue().bold(),
            "Note:".blue(),
            format!(
                "{} is the archive's own metadata (saved locally as file #1): a download fetches the remaining {} file{}",
                xml_file_name,
                remaining,
                if remaining == 1 { "" } else { "s" }
            )
            .dimmed()
        );
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
/// Parses the command line arguments and hands off to [`run`]. `run`
/// finishes its spinner with a context-rich message on every failure path,
/// so the error is already on screen; `main` only maps the result to a
/// process exit code, without letting the runtime print the error a second
/// time.
#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if run(&cli).await.is_err() {
        // The failure was already printed by run's spinner; just exit
        // non-zero so a shell or wrapping script sees it.
        std::process::exit(1);
    }
}

/// Validate the URL, fetch and parse the archive metadata, and download
/// the files.
///
/// A single spinner is shown for the whole initialization; every failure
/// path finishes it with a human-readable message before the error is
/// returned, so the caller only has to decide on the exit code.
async fn run(cli: &Cli) -> Result<()> {
    // Start a single spinner for the entire initialization process
    let spinner = create_spinner(&format!("Processing archive.org URL: {}", cli.url.bold()));

    let client = init_step(&spinner, build_client())?;

    // Validate URL format using consolidated function
    init_step(&spinner, validate_archive_url(&cli.url))?;

    // The cookie header is computed once against the download URL — the
    // scope every metadata and file request uses — and reused for the
    // metadata fetch and the file downloads, so a path-scoped cookie
    // applies consistently across the run.
    let xml_url = init_step(&spinner, get_xml_url(&cli.url))?;
    let cookie_header = init_step(
        &spinner,
        cookie_header_value(cli.cookies.as_deref(), &xml_url),
    )?;

    // The spinner switches from "processing the URL" to the parsing stage
    // before the metadata is fetched: any failure (the accessibility
    // pre-check, a failed GET, an unparseable document) is reported by
    // init_step with the full error detail.
    spinner.set_message(format!(
        "{} {}",
        "⚙".blue(),
        "Parsing archive metadata...".bold()
    ));

    let XmlMetadata {
        files,
        base_url,
        content,
        last_modified,
    } = init_step(
        &spinner,
        fetch_and_parse_xml(&xml_url, &client, cookie_header.as_ref()).await,
    )?;

    // If requested, list parsed filenames and exit: a read-only preview,
    // nothing is written to the working directory
    if cli.list {
        list_files(&files, &spinner, xml_file_name_of(&base_url));
        return Ok(());
    }

    // Convert the metadata into the download plan before anything is
    // announced, so the counts below only include the tasks that will
    // actually run (entries skipped for empty names or path collisions
    // never appear in the numbering).
    let plan = init_step(
        &spinner,
        plan_download_tasks(
            files_to_download(files.files, xml_file_name_of(&base_url)),
            &base_url,
        ),
    )?;

    // The saved _files.xml occupies file #1; the planned tasks follow
    // right after it.
    let total_files = plan.tasks.len() + 1;

    // Persist the freshly fetched _files.xml (overwriting any previous copy)
    // with the server's Last-Modified time, and announce it as file #1
    init_step(
        &spinner,
        save_and_announce_xml(&base_url, &content, last_modified, total_files),
    )?;

    // Successfully finished initialization; separate the banner from the
    // saved-metadata block above.
    println!();
    finish_spinner(
        &spinner,
        &format!(
            "{} {} to download {} files from archive.org {}",
            "✔".green().bold(),
            "Ready".bold(),
            plan.tasks.len().to_string().bold(),
            "★".yellow()
        ),
    );

    // The plan's warnings (sanitized, collided and skipped entries)
    // print after the Ready line.
    for warning in &plan.warnings {
        println!("{warning}");
    }

    // Show summary if any files were sanitized
    if plan.sanitized_count > 0 {
        println!(
            "\n{} {} {} file{} for filesystem compatibility",
            "✓".green().bold(),
            "Sanitized".bold(),
            plan.sanitized_count.to_string().bold(),
            if plan.sanitized_count == 1 { "" } else { "s" }
        );
    }

    // Download all files with integrated signal handling; numbering starts
    // right after the _files.xml saved above.
    downloader::download_files(
        &client,
        plan.tasks,
        total_files,
        XML_FILE_NUMBER + 1,
        cookie_header.as_ref(),
        cli.stop_on_error,
    )
    .await?;

    Ok(())
}
