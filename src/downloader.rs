//! Module for handling file downloads, verification, and related operations.

use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use colored::*;
use reqwest::header::{HeaderMap, HeaderValue, LAST_MODIFIED, RETRY_AFTER};
use reqwest::{Client, StatusCode};

use crate::error::IaGetError; // Import IaGetError for explicit error conversion
use crate::utils::{create_progress_bar, format_duration, format_size, format_transfer_rate};
use crate::Result; // Import utility functions

/// Buffer size for file operations (8KB)
const BUFFER_SIZE: usize = 8192;

/// File size threshold for showing hash progress bar (2MB)
const LARGE_FILE_THRESHOLD: u64 = 2 * 1024 * 1024;

/// Maximum number of retry attempts for a single failing request
const MAX_RETRIES: u32 = 10;

/// Initial delay between retries in milliseconds (doubles with each retry)
const INITIAL_RETRY_DELAY_MS: u64 = 5_000;

/// Upper bound for the exponential backoff delay in milliseconds
const MAX_RETRY_DELAY_MS: u64 = 60_000;

/// Upper bound (in seconds) for a server-provided Retry-After value
const MAX_RETRY_AFTER_SECS: u64 = 900;

/// Maximum full re-download attempts for a file that fails size/hash verification
const MAX_DOWNLOAD_ATTEMPTS: u32 = 3;

// Numerical constants for the linear congruential generator used to add jitter
const LCG_MULTIPLIER: u64 = 6364136223846793005;
const LCG_INCREMENT: u64 = 1442695040888963407;

/// Sets up signal handling for graceful shutdown on Ctrl+C
///
/// Returns an Arc<AtomicBool> that can be checked to see if the process
/// should stop. When Ctrl+C is pressed, this will be set to false.
fn setup_signal_handler() -> Arc<AtomicBool> {
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        println!(
            "\n{} Received Ctrl+C, finishing current operation...",
            "✘".red().bold()
        );
    })
    .expect("Error setting Ctrl+C handler");

    running
}

/// Calculates the MD5 hash of a file
fn calculate_md5(file_path: &str, running: &Arc<AtomicBool>) -> Result<String> {
    let file = File::open(file_path)?;
    let file_size = file.metadata()?.len();
    let is_large_file = file_size > LARGE_FILE_THRESHOLD;

    let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);
    let mut context = md5::Context::new();
    let mut buffer = [0; BUFFER_SIZE];

    let pb = if is_large_file {
        Some(create_progress_bar(
            file_size,
            &format!("{} {}    ", "╰╼".cyan().dimmed(), "Verifying".white()),
            Some("blue/blue"),
            false,
        ))
    } else {
        None
    };

    let mut bytes_processed: u64 = 0;

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        if !running.load(Ordering::SeqCst) {
            if let Some(ref progress_bar) = pb {
                progress_bar.finish_and_clear();
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "Hash calculation interrupted by signal",
            )
            .into());
        }

        context.consume(&buffer[..bytes_read]);

        if let Some(ref progress_bar) = pb {
            bytes_processed += bytes_read as u64;
            progress_bar.set_position(bytes_processed);
        }
    }

    if let Some(progress_bar) = pb.as_ref() {
        progress_bar.finish_and_clear();
    }

    let hash = context.finalize();
    Ok(format!("{:x}", hash))
}

/// Check if an existing file has the correct hash
fn check_existing_file(
    file_path: &str,
    expected_md5: Option<&str>,
    running: &Arc<AtomicBool>,
) -> Result<Option<bool>> {
    if !Path::new(file_path).exists() {
        return Ok(None);
    }

    if expected_md5.is_none() {
        return Ok(Some(true));
    }

    let local_md5 = match calculate_md5(file_path, running) {
        Ok(hash) => hash,
        Err(e) => {
            if e.to_string().contains("interrupted by signal") {
                return Err(e);
            }
            println!(
                "{} {} to calculate MD5 hash: {}",
                "╰╼".cyan().dimmed(),
                "Failed".red().bold(),
                e
            );
            return Ok(Some(false));
        }
    };

    Ok(Some(local_md5 == expected_md5.unwrap()))
}

/// Ensure parent directories exist for a file
fn ensure_parent_directories(file_path: &str) -> Result<()> {
    if let Some(path) = Path::new(file_path).parent() {
        if path.file_name().is_some() && !path.exists() {
            fs::create_dir_all(path)?;
        }
    }
    Ok(())
}

/// Prepare a file for download
fn prepare_file_for_download(file_path: &str) -> Result<File> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(file_path)?;

    // Seek to the end of the file for resume capability
    file.seek(SeekFrom::End(0))?;

    Ok(file)
}

/// Applies +/-20% jitter to a delay so that many clients do not retry in sync.
///
/// Uses a linear congruential generator seeded from the current time instead
/// of pulling in a `rand` dependency.
fn jitter_ms(value_ms: u64) -> u64 {
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E37_79B9_7F4A_7C15);
    let x = seed
        .wrapping_mul(LCG_MULTIPLIER)
        .wrapping_add(LCG_INCREMENT);
    let factor = 80u64 + (x >> 33) % 41; // 80..=120 percent
    value_ms.saturating_mul(factor) / 100
}

/// Exponential backoff delay for a given 1-based retry attempt.
///
/// Starts at `INITIAL_RETRY_DELAY_MS`, doubles per attempt, is capped at
/// `MAX_RETRY_DELAY_MS`, and then has +/-20% jitter applied.
fn backoff_delay(attempt: u32) -> Duration {
    let exp = attempt.saturating_sub(1).min(20);
    let base_ms = INITIAL_RETRY_DELAY_MS.saturating_mul(1u64 << exp);
    let capped_ms = base_ms.min(MAX_RETRY_DELAY_MS);
    Duration::from_millis(jitter_ms(capped_ms))
}

/// Parses a Retry-After header value given as an integer number of seconds.
///
/// HTTP-date forms are not supported and yield `None`. The result is capped
/// at `MAX_RETRY_AFTER_SECS`.
fn parse_retry_after(value: &str) -> Option<u64> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .map(|secs| secs.min(MAX_RETRY_AFTER_SECS))
}

/// Returns true for HTTP statuses that are worth retrying (transient server
/// or client throttling problems). Other 4xx/5xx statuses are fatal.
fn is_retryable_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 425 | 429 | 500 | 502 | 503 | 504)
}

/// Parses the server-provided `Last-Modified` header into a `SystemTime`.
///
/// `reqwest::Response` has no typed accessor for this header, so the raw
/// HTTP-date is parsed with `httpdate`.
pub fn parse_last_modified(headers: &HeaderMap) -> Option<SystemTime> {
    headers
        .get(LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| httpdate::parse_http_date(s).ok())
}

/// Converts the `<mtime>` value from `_files.xml` (unix seconds) into a
/// `SystemTime`, or `None` when absent or out of representable range.
fn mtime_from_xml(mtime: Option<u64>) -> Option<SystemTime> {
    mtime.and_then(|secs| UNIX_EPOCH.checked_add(Duration::from_secs(secs)))
}

/// Sets the file's last-modified time to `target` when the current time
/// differs at second granularity.
///
/// A failure to set the time is not fatal: it prints a warning and returns
/// `false` so the batch can continue.
pub fn sync_file_mtime(file_path: impl AsRef<Path>, target: SystemTime) -> bool {
    let target_secs = target
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let current_secs = fs::metadata(&file_path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs());

    if current_secs == Some(target_secs) {
        return false;
    }

    let file_time =
        filetime::FileTime::from_unix_time(i64::try_from(target_secs).unwrap_or(i64::MAX), 0);

    if let Err(e) = filetime::set_file_mtime(&file_path, file_time) {
        println!(
            "{} {}      {}",
            "⚠".yellow().bold(),
            "Could not set last modified time".yellow(),
            e.to_string().dimmed()
        );
        return false;
    }

    true
}

/// Records a failed attempt, prints the retry notice and waits.
///
/// Returns an error once `MAX_RETRIES` has been exhausted.
async fn handle_retry(
    retry_count: &mut u32,
    kind: &str,
    detail: &str,
    retry_after_secs: Option<u64>,
    retry_delay: fn(u32) -> Duration,
) -> Result<()> {
    *retry_count += 1;

    if *retry_count > MAX_RETRIES {
        println!(
            "{} {}      {} Maximum retries ({}) exceeded",
            "├╼".cyan().dimmed(),
            "Failed".red().bold(),
            "✘".red().bold(),
            MAX_RETRIES
        );
        return Err(IaGetError::Network(format!(
            "{}: {} (maximum retries {} exceeded)",
            kind, detail, MAX_RETRIES
        )));
    }

    let delay = retry_after_secs
        .map(Duration::from_secs)
        .unwrap_or_else(|| retry_delay(*retry_count));

    println!(
        "{} {}      {} {} (attempt {}/{}): {}",
        "├╼".cyan().dimmed(),
        "Retry".yellow().bold(),
        "⟳".yellow().bold(),
        kind,
        retry_count,
        MAX_RETRIES,
        detail
    );
    println!(
        "{} {}      Waiting {:.1}s before retry{}",
        "├╼".cyan().dimmed(),
        "Wait".white(),
        delay.as_secs_f64(),
        if retry_after_secs.is_some() {
            " (server requested)"
        } else {
            ""
        }
    );

    tokio::time::sleep(delay).await;
    Ok(())
}

/// Download file content with progress reporting and automatic retry on failure.
async fn download_file_content(
    client: &Client,
    url: &str,
    file: &mut File,
    running: &Arc<AtomicBool>,
    cookie_header: Option<&str>,
    expected_size: Option<u64>,
) -> Result<(u64, Option<SystemTime>)> {
    download_file_content_with_delay(
        client,
        url,
        file,
        running,
        cookie_header,
        expected_size,
        backoff_delay,
    )
    .await
}

/// Download file content, using the supplied function to compute retry delays.
///
/// `download_file_content` is a thin wrapper over this function so tests can
/// substitute near-instant delays.
///
/// Only a successful (2xx) response body is ever written to `file`. Error
/// pages are discarded, empty and truncated bodies are retried, and a 200
/// response to a ranged request resets the file instead of appending to it.
///
/// Returns the total file size and, when the server sent one on the final
/// successful response, the parsed `Last-Modified` header value.
async fn download_file_content_with_delay(
    client: &Client,
    url: &str,
    file: &mut File,
    running: &Arc<AtomicBool>,
    cookie_header: Option<&str>,
    expected_size: Option<u64>,
    retry_delay: fn(u32) -> Duration,
) -> Result<(u64, Option<SystemTime>)> {
    let mut retry_count = 0;

    loop {
        // Re-check file size at start of each attempt (in case of retry)
        let current_file_size = file.metadata()?.len();
        let resuming = current_file_size > 0;
        let mut download_action = if resuming {
            format!("{} {}     ", "╰╼".cyan().dimmed(), "Resuming".white())
        } else {
            format!("{} {}  ", "╰╼".cyan().dimmed(), "Downloading".white())
        };

        let mut headers = HeaderMap::new();
        if let Some(cookie_header) = cookie_header {
            headers.insert(
                reqwest::header::COOKIE,
                HeaderValue::from_str(cookie_header).map_err(|e| {
                    IaGetError::Network(format!("Invalid cookie header value: {}", e))
                })?,
            );
        }
        if resuming {
            // Use IaGetError::Network for header parsing errors
            headers.insert(
                reqwest::header::RANGE,
                HeaderValue::from_str(&format!("bytes={}-", current_file_size)).map_err(|e| {
                    IaGetError::Network(format!("Invalid range header value: {}", e))
                })?,
            );
        }

        let mut request = client.get(url);
        if !headers.is_empty() {
            request = request.headers(headers);
        }

        let mut response = match request.send().await {
            Ok(resp) => resp,
            Err(e) => {
                // Request failed before we even got a response
                handle_retry(
                    &mut retry_count,
                    "Connection error",
                    &e.to_string(),
                    None,
                    retry_delay,
                )
                .await?;
                continue;
            }
        };

        let status = response.status();
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_retry_after);
        // Captured on every successful response so the value returned to the
        // caller always comes from the attempt that completed the download.
        let last_modified = parse_last_modified(response.headers());

        if !status.is_success() {
            // Error pages must never be written to the file; drop the body.
            drop(response);

            if resuming && status == StatusCode::RANGE_NOT_SATISFIABLE {
                // The server rejects our offset: the local prefix is not valid,
                // so the caller must re-download from scratch.
                return Err(IaGetError::RangeNotSatisfiable);
            }

            if is_retryable_status(status) {
                let reason = status
                    .canonical_reason()
                    .unwrap_or("unknown status")
                    .to_string();
                handle_retry(
                    &mut retry_count,
                    &format!("HTTP {status}"),
                    &reason,
                    retry_after,
                    retry_delay,
                )
                .await?;
                continue;
            }

            return Err(IaGetError::Network(format!(
                "Server responded with HTTP {} {}",
                status,
                status.canonical_reason().unwrap_or("unknown status")
            )));
        }

        // The server ignored the Range header and is sending the full body:
        // the local prefix is untrusted, so reset the file before streaming.
        if resuming && status == StatusCode::OK {
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            download_action = format!("{} {}  ", "╰╼".cyan().dimmed(), "Downloading".white());
        }
        let base_size = file.metadata()?.len();

        let content_length = response.content_length().unwrap_or(0);
        let total_expected_size = base_size + content_length;

        let pb = create_progress_bar(
            total_expected_size,
            &download_action,
            Some("green/green"),
            true,
        );

        // Set initial progress to current file size for resumed downloads
        pb.set_position(base_size);

        let start_time = std::time::Instant::now();
        let mut total_bytes: u64 = base_size;
        let mut downloaded_bytes: u64 = 0;

        // Attempt the download
        let download_result: Result<()> = async {
            while let Some(chunk_result) = response.chunk().await.transpose() {
                if !running.load(Ordering::SeqCst) {
                    pb.finish_and_clear();
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "Download interrupted during file transfer",
                    )
                    .into());
                }

                let chunk = chunk_result?;
                file.write_all(&chunk)?;
                downloaded_bytes += chunk.len() as u64;
                total_bytes += chunk.len() as u64;
                pb.set_position(total_bytes);
            }
            Ok(())
        }
        .await;

        match download_result {
            Ok(_) => {
                // Ensure data is written to disk
                file.flush()?;

                // A 2xx body with zero bytes is a server malfunction (unless
                // the server explicitly announced a zero-byte file).
                if downloaded_bytes == 0 && base_size == 0 && expected_size != Some(0) {
                    pb.finish_and_clear();
                    handle_retry(
                        &mut retry_count,
                        "Empty response",
                        "server returned no data",
                        retry_after,
                        retry_delay,
                    )
                    .await?;
                    file.seek(SeekFrom::End(0))?;
                    continue;
                }

                // The body ended before the announced size: the transfer was
                // truncated, so resume from where we stopped.
                if let Some(expected) = expected_size {
                    if total_bytes < expected {
                        pb.finish_and_clear();
                        handle_retry(
                            &mut retry_count,
                            "Incomplete body",
                            &format!("received {} of {} bytes", total_bytes, expected),
                            None,
                            retry_delay,
                        )
                        .await?;
                        file.seek(SeekFrom::End(0))?;
                        continue;
                    }
                }

                let elapsed = start_time.elapsed();
                let elapsed_secs = elapsed.as_secs_f64();
                let transfer_rate_val = if elapsed_secs > 0.0 {
                    downloaded_bytes as f64 / elapsed_secs
                } else {
                    0.0
                };

                let (rate, unit) = format_transfer_rate(transfer_rate_val);

                pb.finish_and_clear();
                println!(
                    "{} {}   {} {} in {} ({:.2} {}/s)",
                    "├╼".cyan().dimmed(),
                    "Downloaded".white(),
                    "↓".green().bold(),
                    format_size(downloaded_bytes).bold(),
                    format_duration(elapsed).bold(),
                    rate,
                    unit
                );

                return Ok((total_bytes, last_modified));
            }
            Err(e) => {
                pb.finish_and_clear();

                // Check if this is a user interruption
                if e.to_string().contains("interrupted") {
                    return Err(e);
                }

                // Mid-stream failure: keep the partial data and resume later.
                handle_retry(
                    &mut retry_count,
                    "Download error",
                    &e.to_string(),
                    None,
                    retry_delay,
                )
                .await?;
                file.flush()?;
                file.seek(SeekFrom::End(0))?;
            }
        }
    }
}

/// Verify a downloaded file's size and hash against expected values
fn verify_downloaded_file(
    file_path: &str,
    expected_md5: Option<&str>,
    expected_size: Option<u64>,
    running: &Arc<AtomicBool>,
) -> Result<bool> {
    if let Some(expected_size) = expected_size {
        let local_size = fs::metadata(file_path)?.len();
        if local_size != expected_size {
            println!(
                "{} {}         {} {} (expected {})",
                "╰╼".cyan().dimmed(),
                "Size".white(),
                "✘".red().bold(),
                format_size(local_size).red(),
                format_size(expected_size).dimmed()
            );
            return Ok(false);
        }
    }

    if expected_md5.is_none() {
        println!(
            "{} {}",
            "-".dimmed(),
            "No MD5 hash provided for verification.".dimmed()
        );
        return Ok(true); // No hash to check against, consider it verified
    }
    let expected_md5_str = expected_md5.unwrap();
    let local_md5 = calculate_md5(file_path, running)?;
    if local_md5 == expected_md5_str {
        println!(
            "{} {}         {} {}",
            "╰╼".cyan().dimmed(),
            "Hash".white(),
            "✔".green().bold(),
            format!("({})", local_md5).dimmed()
        );
        Ok(true)
    } else {
        println!(
            "{} {}         {} ({}) Expected ({})",
            "╰╼".cyan().dimmed(),
            "Hash".white(),
            "✘".red().bold(),
            local_md5.red(),
            expected_md5_str.dimmed()
        );
        Ok(false)
    }
}

/// Download multiple files with shared signal handling
///
/// This function sets up signal handling once for the entire download session
/// and allows for graceful interruption between files.
///
/// Each file is streamed to a `<filename>.part` file and only renamed to its
/// final name once verification passes, so a failed download never corrupts
/// the final file. By default the batch continues after a failed file and
/// returns `IaGetError::BatchFailed` at the end; pass `stop_on_error` to
/// abort at the first failure instead.
pub async fn download_files<I>(
    client: &Client,
    files: I,
    total_files: usize,
    cookie_header: Option<&str>,
    stop_on_error: bool,
) -> Result<()>
where
    I: IntoIterator<Item = (String, String, Option<String>, Option<u64>, Option<u64>)>, // (url, filename, md5, size, mtime)
{
    // Set up signal handling for the entire download session
    let running = setup_signal_handler();

    download_files_with_signal(
        client,
        files,
        total_files,
        cookie_header,
        stop_on_error,
        &running,
    )
    .await
}

/// Batch download logic with an externally provided signal flag.
///
/// Split out from `download_files` so tests can drive the batch loop without
/// registering a second (and panicking) Ctrl+C handler.
async fn download_files_with_signal<I>(
    client: &Client,
    files: I,
    total_files: usize,
    cookie_header: Option<&str>,
    stop_on_error: bool,
    running: &Arc<AtomicBool>,
) -> Result<()>
where
    I: IntoIterator<Item = (String, String, Option<String>, Option<u64>, Option<u64>)>, // (url, filename, md5, size, mtime)
{
    let mut failed_files: Vec<(String, String)> = Vec::new();

    for (index, (url, file_path, expected_md5, expected_size, expected_mtime)) in
        files.into_iter().enumerate()
    {
        // Check if we should stop due to signal
        if !running.load(Ordering::SeqCst) {
            println!(
                "\n{} Download interrupted. Run the command again to resume remaining files.",
                "✘".red().bold()
            );
            break;
        }

        println!(" ");
        println!(
            "{}  {}     {}",
            "▣".bright_cyan().bold(),
            "Filename".white(),
            file_path.bold()
        );
        println!(
            "{} {}        {} {} of {}",
            "├╼".cyan().dimmed(),
            "Count".white(),
            "#".blue().bold(),
            (index + 1).to_string().bold(),
            total_files.to_string().bold()
        );

        let part_path = format!("{}.part", file_path);

        match check_existing_file(&file_path, expected_md5.as_deref(), running)? {
            Some(true) => {
                // Final file is already valid; clean up any stale .part file
                let _ = fs::remove_file(&part_path);
                // No request is made for a verified file, so only the XML
                // mtime is available here.
                if let Some(target) = mtime_from_xml(expected_mtime) {
                    sync_file_mtime(&file_path, target);
                }
                println!(
                    "{} {}   {}",
                    "╰╼".cyan().dimmed(),
                    "Downloaded".white(),
                    "✔".green().bold()
                );
                continue;
            }
            Some(false) => {
                println!(
                    "{} {}      {} the existing file failed verification, re-downloading",
                    "├╼".cyan().dimmed(),
                    "Partial".white(),
                    "▲".yellow().bold()
                );
                fs::remove_file(&file_path)?;
                let _ = fs::remove_file(&part_path);
            }
            None => {}
        }

        let mut downloaded = false;
        let mut downloaded_mtime: Option<SystemTime> = None;
        let mut range_rejected = false;
        let mut last_error = String::new();

        ensure_parent_directories(&part_path)?;
        let mut file = prepare_file_for_download(&part_path)?;

        for attempt in 1..=MAX_DOWNLOAD_ATTEMPTS {
            if attempt > 1 {
                drop(file);
                let _ = fs::remove_file(&part_path);
                file = prepare_file_for_download(&part_path)?;
                println!(
                    "{} {}      {} Re-downloading from scratch (attempt {}/{})",
                    "├╼".cyan().dimmed(),
                    "Retry".yellow().bold(),
                    "⟳".yellow().bold(),
                    attempt,
                    MAX_DOWNLOAD_ATTEMPTS
                );
            }

            match download_file_content(
                client,
                &url,
                &mut file,
                running,
                cookie_header,
                expected_size,
            )
            .await
            {
                Ok((_, server_mtime)) => {
                    if verify_downloaded_file(
                        &part_path,
                        expected_md5.as_deref(),
                        expected_size,
                        running,
                    )? {
                        downloaded_mtime = server_mtime;
                        downloaded = true;
                        break;
                    }
                    last_error = format!("file failed verification after {} attempt(s)", attempt);
                }
                Err(e) => {
                    if e.to_string().contains("interrupted") {
                        return Err(e);
                    }
                    last_error = e.to_string();
                    match e {
                        // The server rejected our offset: the partial file is
                        // not a valid prefix, so re-download from scratch.
                        IaGetError::RangeNotSatisfiable => range_rejected = true,
                        _ => break,
                    }
                }
            }
        }

        // Close the handle before renaming (Windows refuses to rename open files)
        drop(file);

        if downloaded {
            if Path::new(&file_path).exists() {
                fs::remove_file(&file_path)?;
            }
            fs::rename(&part_path, &file_path)?;
            // Prefer the server's Last-Modified header, fall back to the
            // mtime from _files.xml, and leave the time untouched if both
            // are absent.
            if let Some(target) = downloaded_mtime.or(mtime_from_xml(expected_mtime)) {
                sync_file_mtime(&file_path, target);
            }
            println!(
                "{} {}   {}",
                "╰╼".cyan().dimmed(),
                "Downloaded".white(),
                "✔".green().bold()
            );
        } else {
            if range_rejected {
                // The server rejected the resume offset; the .part file is not
                // a valid prefix, so discard it for the next run.
                let _ = fs::remove_file(&part_path);
            }
            failed_files.push((file_path.clone(), last_error));
            if stop_on_error {
                return Err(IaGetError::BatchFailed {
                    count: failed_files.len(),
                    total: total_files,
                    details: batch_failure_details(&failed_files),
                });
            }
        }
    }

    if !failed_files.is_empty() {
        println!(" ");
        println!(
            "{} {} {} file(s) could not be downloaded:",
            "✘".red().bold(),
            "Failed".red().bold(),
            failed_files.len()
        );
        for (path, reason) in &failed_files {
            println!("  {} {}", path.bold(), reason.dimmed());
        }
        return Err(IaGetError::BatchFailed {
            count: failed_files.len(),
            total: total_files,
            details: batch_failure_details(&failed_files),
        });
    }

    Ok(())
}

/// Formats the failure list for `IaGetError::BatchFailed` details
fn batch_failure_details(failed: &[(String, String)]) -> String {
    failed
        .iter()
        .map(|(path, reason)| format!("{}: {}", path, reason))
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::net::{TcpListener, TcpStream};
    use std::sync::Mutex;
    use std::thread;
    use std::time::Instant;

    /// Body variant for mock server responses
    #[derive(Clone)]
    enum MockBody {
        /// Send the whole body
        Full(Vec<u8>),
        /// Announce `announced_len` bytes via Content-Length, send only
        /// `partial` bytes, then close the connection mid-body
        Truncated {
            announced_len: u64,
            partial: Vec<u8>,
        },
    }

    #[derive(Clone)]
    struct MockResponse {
        status: u16,
        body: MockBody,
        extra_headers: Vec<(String, String)>,
    }

    impl MockResponse {
        fn new(status: u16, body: MockBody) -> Self {
            Self {
                status,
                body,
                extra_headers: vec![],
            }
        }

        fn with_header(mut self, name: &str, value: &str) -> Self {
            self.extra_headers
                .push((name.to_string(), value.to_string()));
            self
        }
    }

    struct ServerState {
        scripts: HashMap<String, VecDeque<MockResponse>>,
        fallback: MockResponse,
        request_count: usize,
        ranges: Vec<Option<u64>>,
    }

    struct MockServer {
        base_url: String,
        state: Arc<Mutex<ServerState>>,
    }

    impl MockServer {
        fn url(&self, path: &str) -> String {
            format!("{}{}", self.base_url, path)
        }

        fn request_count(&self) -> usize {
            self.state.lock().unwrap().request_count
        }

        fn ranges(&self) -> Vec<Option<u64>> {
            self.state.lock().unwrap().ranges.clone()
        }
    }

    fn handle_mock_connection(mut stream: TcpStream, state: &Arc<Mutex<ServerState>>) {
        // Read the request head
        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => return,
            }
        }

        let head = String::from_utf8_lossy(&buf).into_owned();
        let path = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or("/")
            .to_string();
        let range = head
            .lines()
            .find(|line| line.to_ascii_lowercase().starts_with("range:"))
            .and_then(|line| {
                line.split_once(':')?
                    .1
                    .trim()
                    .strip_prefix("bytes=")?
                    .split('-')
                    .next()?
                    .trim()
                    .parse::<u64>()
                    .ok()
            });

        let response = {
            let mut st = state.lock().unwrap();
            st.request_count += 1;
            st.ranges.push(range);
            st.scripts
                .get_mut(&path)
                .and_then(|q| q.pop_front())
                .unwrap_or_else(|| st.fallback.clone())
        };

        let (status, announced_len, body_bytes) = match &response.body {
            MockBody::Full(data) => (response.status, data.len() as u64, data.clone()),
            MockBody::Truncated {
                announced_len,
                partial,
            } => (response.status, *announced_len, partial.clone()),
        };

        let reason = StatusCode::from_u16(status)
            .ok()
            .and_then(|s| s.canonical_reason())
            .unwrap_or("Unknown")
            .to_string();
        let mut out = format!("HTTP/1.1 {} {}\r\n", status, reason);
        out.push_str(&format!("Content-Length: {}\r\n", announced_len));
        for (name, value) in &response.extra_headers {
            out.push_str(&format!("{}: {}\r\n", name, value));
        }
        out.push_str("Connection: close\r\n\r\n");

        let _ = stream.write_all(out.as_bytes());
        let _ = stream.write_all(&body_bytes);
        // The stream is dropped here: for Truncated bodies this closes the
        // connection before the announced Content-Length has been delivered.
    }

    /// Starts a mock HTTP server on 127.0.0.1 that serves scripted responses
    /// per path (falling back to `fallback` once the script is exhausted).
    fn start_mock_server(
        scripts: HashMap<String, VecDeque<MockResponse>>,
        fallback: MockResponse,
    ) -> MockServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind mock server");
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(Mutex::new(ServerState {
            scripts,
            fallback,
            request_count: 0,
            ranges: Vec::new(),
        }));
        let handler_state = state.clone();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                handle_mock_connection(stream, &handler_state);
            }
        });

        MockServer {
            base_url: format!("http://127.0.0.1:{}", port),
            state,
        }
    }

    /// Near-instant retry delay so tests do not wait for real backoff
    fn fast_retry(_attempt: u32) -> Duration {
        Duration::from_millis(1)
    }

    fn temp_dir_for(test_name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "ia-get-test-{}-{}-{}",
            std::process::id(),
            test_name,
            nanos
        ));
        fs::create_dir_all(&dir).expect("failed to create temp dir");
        dir
    }

    fn test_running() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(true))
    }

    /// Reads a file's last-modified time as unix seconds, if available.
    fn mtime_of(path: &Path) -> Option<u64> {
        fs::metadata(path)
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
    }

    /// Runs a single download against the mock server and returns the bytes
    /// reported as downloaded along with the captured `Last-Modified` time.
    async fn run_download(
        url: &str,
        part_path: &str,
        expected_size: Option<u64>,
    ) -> Result<(u64, Option<SystemTime>)> {
        let client = Client::new();
        let mut file = prepare_file_for_download(part_path)?;
        let result = download_file_content_with_delay(
            &client,
            url,
            &mut file,
            &test_running(),
            None,
            expected_size,
            fast_retry,
        )
        .await;
        drop(file);
        result
    }

    #[tokio::test]
    async fn http_500_body_is_not_written_to_file() {
        let content = b"0123456789abcdef";
        let nginx_error =
            b"<html><head><title>500 Internal Server Error</title></head><body>nginx</body></html>";

        let mut scripts = HashMap::new();
        scripts.insert(
            "/file.bin".to_string(),
            VecDeque::from(vec![
                MockResponse::new(500, MockBody::Full(nginx_error.to_vec())),
                MockResponse::new(500, MockBody::Full(nginx_error.to_vec())),
                MockResponse::new(200, MockBody::Full(content.to_vec())),
            ]),
        );
        let server = start_mock_server(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("http_500");
        let part = dir.join("file.bin.part");
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(content.len() as u64),
        )
        .await;

        assert!(result.is_ok(), "expected success, got {:?}", result.err());
        let data = fs::read(&part).unwrap();
        assert_eq!(data, content, "error body must not be written to the file");
        assert_eq!(server.request_count(), 3);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn empty_response_retries_then_fails() {
        let mut scripts = HashMap::new();
        scripts.insert(
            "/file.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(200, MockBody::Full(vec![]))]),
        );
        // Fallback also returns an empty 200 so every retry sees the same
        let server = start_mock_server(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("empty_response");
        let part = dir.join("file.bin.part");
        let result = run_download(&server.url("/file.bin"), part.to_str().unwrap(), Some(10)).await;

        assert!(
            matches!(result, Err(IaGetError::Network(_))),
            "expected a Network error, got {:?}",
            result.ok()
        );
        assert_eq!(fs::metadata(&part).unwrap().len(), 0);
        assert_eq!(server.request_count(), 1 + MAX_RETRIES as usize);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn resume_after_mid_stream_disconnect() {
        let full = b"01234567890123456789"; // 20 bytes

        let mut scripts = HashMap::new();
        scripts.insert(
            "/file.bin".to_string(),
            VecDeque::from(vec![
                MockResponse::new(
                    200,
                    MockBody::Truncated {
                        announced_len: full.len() as u64,
                        partial: full[..8].to_vec(),
                    },
                ),
                MockResponse::new(206, MockBody::Full(full[8..].to_vec())),
            ]),
        );
        let server = start_mock_server(scripts, MockResponse::new(206, MockBody::Full(vec![])));

        let dir = temp_dir_for("mid_stream");
        let part = dir.join("file.bin.part");
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(full.len() as u64),
        )
        .await;

        let (bytes, _) = result.expect("download should succeed");
        assert_eq!(bytes, full.len() as u64);
        assert_eq!(fs::read(&part).unwrap(), full);
        assert_eq!(server.ranges(), vec![None, Some(8)]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn full_200_response_resets_untrusted_prefix() {
        let full = b"0123456789"; // 10 bytes

        let mut scripts = HashMap::new();
        scripts.insert(
            "/file.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(200, MockBody::Full(full.to_vec()))]),
        );
        let server = start_mock_server(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("range_ignored");
        let part = dir.join("file.bin.part");
        fs::write(&part, b"XXXXXX").unwrap(); // 6-byte "partial" file
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(full.len() as u64),
        )
        .await;

        let (bytes, _) = result.expect("download should succeed");
        assert_eq!(bytes, full.len() as u64);
        assert_eq!(
            fs::read(&part).unwrap(),
            full,
            "200 response must replace the local prefix, not append to it"
        );
        assert_eq!(server.ranges(), vec![Some(6)]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn http_416_yields_range_not_satisfiable() {
        let mut scripts = HashMap::new();
        scripts.insert(
            "/file.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(
                416,
                MockBody::Full(b"Range not satisfiable".to_vec()),
            )]),
        );
        let server = start_mock_server(scripts, MockResponse::new(416, MockBody::Full(vec![])));

        let dir = temp_dir_for("http_416");
        let part = dir.join("file.bin.part");
        fs::write(&part, b"XXXX").unwrap();
        let result =
            run_download(&server.url("/file.bin"), part.to_str().unwrap(), Some(100)).await;

        assert!(
            matches!(result, Err(IaGetError::RangeNotSatisfiable)),
            "expected RangeNotSatisfiable, got {:?}",
            result
        );
        assert_eq!(server.request_count(), 1, "416 must not be retried");
        assert_eq!(server.ranges(), vec![Some(4)]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn http_404_fails_immediately() {
        let mut scripts = HashMap::new();
        scripts.insert(
            "/file.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(
                404,
                MockBody::Full(b"not found".to_vec()),
            )]),
        );
        let server = start_mock_server(scripts, MockResponse::new(404, MockBody::Full(vec![])));

        let dir = temp_dir_for("http_404");
        let part = dir.join("file.bin.part");
        let result = run_download(&server.url("/file.bin"), part.to_str().unwrap(), Some(10)).await;

        assert!(
            matches!(result, Err(IaGetError::Network(_))),
            "expected a Network error, got {:?}",
            result.ok()
        );
        assert_eq!(server.request_count(), 1, "404 must not be retried");
        assert_eq!(
            fs::metadata(&part).unwrap().len(),
            0,
            "error body must not be written"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn retry_after_header_is_respected() {
        let content = b"retry-after-content";

        let mut scripts = HashMap::new();
        scripts.insert(
            "/file.bin".to_string(),
            VecDeque::from(vec![
                MockResponse::new(429, MockBody::Full(vec![])).with_header("Retry-After", "1"),
                MockResponse::new(200, MockBody::Full(content.to_vec())),
            ]),
        );
        let server = start_mock_server(scripts, MockResponse::new(429, MockBody::Full(vec![])));

        let dir = temp_dir_for("retry_after");
        let part = dir.join("file.bin.part");
        let start = Instant::now();
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(content.len() as u64),
        )
        .await;

        assert!(result.is_ok(), "expected success, got {:?}", result.err());
        assert!(
            start.elapsed() >= Duration::from_secs(1),
            "Retry-After: 1 was not honored (elapsed {:?})",
            start.elapsed()
        );
        assert_eq!(fs::read(&part).unwrap(), content);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn batch_continues_after_file_failure() {
        let ok_content = b"ok-content-123";
        let ok_md5 = format!("{:x}", md5::compute(ok_content));

        let mut scripts = HashMap::new();
        scripts.insert(
            "/missing.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(
                404,
                MockBody::Full(b"gone".to_vec()),
            )]),
        );
        scripts.insert(
            "/ok.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(
                200,
                MockBody::Full(ok_content.to_vec()),
            )]),
        );
        let server = start_mock_server(scripts, MockResponse::new(404, MockBody::Full(vec![])));

        let dir = temp_dir_for("batch_continue");
        let missing_path = dir.join("missing.bin").to_str().unwrap().to_string();
        let ok_path = dir.join("ok.bin").to_str().unwrap().to_string();
        let files = vec![
            (
                server.url("/missing.bin"),
                missing_path,
                None,
                Some(5),
                None,
            ),
            (
                server.url("/ok.bin"),
                ok_path,
                Some(ok_md5),
                Some(ok_content.len() as u64),
                None,
            ),
        ];
        let client = Client::new();
        let running = test_running();

        let err = download_files_with_signal(&client, files, 2, None, false, &running)
            .await
            .unwrap_err();
        match err {
            IaGetError::BatchFailed { count, total, .. } => {
                assert_eq!((count, total), (1, 2));
            }
            other => panic!("expected BatchFailed, got {:?}", other),
        }
        assert_eq!(fs::read(dir.join("ok.bin")).unwrap(), ok_content);
        assert!(!dir.join("missing.bin").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn stop_on_error_aborts_batch() {
        let ok_content = b"ok-content-123";
        let ok_md5 = format!("{:x}", md5::compute(ok_content));

        let mut scripts = HashMap::new();
        scripts.insert(
            "/missing.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(
                404,
                MockBody::Full(b"gone".to_vec()),
            )]),
        );
        scripts.insert(
            "/ok.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(
                200,
                MockBody::Full(ok_content.to_vec()),
            )]),
        );
        let server = start_mock_server(scripts, MockResponse::new(404, MockBody::Full(vec![])));

        let dir = temp_dir_for("stop_on_error");
        let missing_path = dir.join("missing.bin").to_str().unwrap().to_string();
        let ok_path = dir.join("ok.bin").to_str().unwrap().to_string();
        let files = vec![
            (
                server.url("/missing.bin"),
                missing_path,
                None,
                Some(5),
                None,
            ),
            (
                server.url("/ok.bin"),
                ok_path,
                Some(ok_md5),
                Some(ok_content.len() as u64),
                None,
            ),
        ];
        let client = Client::new();
        let running = test_running();

        let err = download_files_with_signal(&client, files, 2, None, true, &running)
            .await
            .unwrap_err();
        assert!(
            matches!(err, IaGetError::BatchFailed { count: 1, .. }),
            "expected BatchFailed with one file, got {:?}",
            err
        );
        assert_eq!(
            server.request_count(),
            1,
            "second file must never be requested"
        );
        assert!(!dir.join("ok.bin").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn hash_mismatch_triggers_redownload() {
        let correct = b"correct-content-001";
        let corrupt = b"corrupted-content-001"; // same length so the size guard does not fire
        let md5 = format!("{:x}", md5::compute(correct));

        let mut scripts = HashMap::new();
        scripts.insert(
            "/file.bin".to_string(),
            VecDeque::from(vec![
                MockResponse::new(200, MockBody::Full(corrupt.to_vec())),
                MockResponse::new(200, MockBody::Full(correct.to_vec())),
            ]),
        );
        let server = start_mock_server(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("hash_mismatch");
        let file_path = dir.join("file.bin").to_str().unwrap().to_string();
        let files = vec![(
            server.url("/file.bin"),
            file_path,
            Some(md5),
            Some(correct.len() as u64),
            None,
        )];
        let client = Client::new();
        let running = test_running();

        download_files_with_signal(&client, files, 1, None, false, &running)
            .await
            .expect("batch should succeed after re-download");

        assert_eq!(
            server.request_count(),
            2,
            "mismatch must trigger exactly one re-download"
        );
        assert_eq!(fs::read(dir.join("file.bin")).unwrap(), correct);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn mtime_from_xml_converts_and_rejects_overflow() {
        assert_eq!(mtime_from_xml(None), None);
        assert_eq!(
            mtime_from_xml(Some(1_735_965_174)),
            Some(UNIX_EPOCH + Duration::from_secs(1_735_965_174))
        );
        assert_eq!(mtime_from_xml(Some(u64::MAX)), None);
    }

    #[tokio::test]
    async fn last_modified_header_is_captured() {
        let content = b"0123456789abcdef";

        let mut scripts = HashMap::new();
        scripts.insert(
            "/file.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(
                200,
                MockBody::Full(content.to_vec()),
            )
            .with_header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")]),
        );
        let server = start_mock_server(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("last_modified");
        let part = dir.join("file.bin.part");
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(content.len() as u64),
        )
        .await;

        let (bytes, mtime) = result.expect("download should succeed");
        assert_eq!(bytes, content.len() as u64);
        assert_eq!(
            mtime,
            httpdate::parse_http_date("Wed, 21 Oct 2015 07:28:00 GMT").ok()
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn downloaded_file_gets_xml_mtime_when_server_sends_none() {
        let content = b"xml-mtime-content";
        let md5 = format!("{:x}", md5::compute(content));
        let xml_mtime = 1_545_586_142;

        let mut scripts = HashMap::new();
        scripts.insert(
            "/file.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(
                200,
                MockBody::Full(content.to_vec()),
            )]),
        );
        let server = start_mock_server(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("xml_mtime");
        let file_path = dir.join("file.bin").to_str().unwrap().to_string();
        let files = vec![(
            server.url("/file.bin"),
            file_path,
            Some(md5),
            Some(content.len() as u64),
            Some(xml_mtime),
        )];
        let client = Client::new();
        let running = test_running();

        download_files_with_signal(&client, files, 1, None, false, &running)
            .await
            .expect("batch should succeed");

        assert_eq!(
            mtime_of(&dir.join("file.bin")),
            Some(xml_mtime),
            "file mtime must be set from _files.xml when the server sent no Last-Modified"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn server_last_modified_wins_over_xml_mtime() {
        let content = b"server-mtime-content";
        let md5 = format!("{:x}", md5::compute(content));
        let xml_mtime = 1_545_586_142;
        let header_mtime =
            httpdate::parse_http_date("Wed, 21 Oct 2015 07:28:00 GMT").expect("valid date");
        let expected = header_mtime
            .duration_since(UNIX_EPOCH)
            .expect("valid date")
            .as_secs();

        let mut scripts = HashMap::new();
        scripts.insert(
            "/file.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(
                200,
                MockBody::Full(content.to_vec()),
            )
            .with_header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")]),
        );
        let server = start_mock_server(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("server_mtime");
        let file_path = dir.join("file.bin").to_str().unwrap().to_string();
        let files = vec![(
            server.url("/file.bin"),
            file_path,
            Some(md5),
            Some(content.len() as u64),
            Some(xml_mtime),
        )];
        let client = Client::new();
        let running = test_running();

        download_files_with_signal(&client, files, 1, None, false, &running)
            .await
            .expect("batch should succeed");

        assert_eq!(
            mtime_of(&dir.join("file.bin")),
            Some(expected),
            "server Last-Modified must take precedence over the _files.xml mtime"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn verified_existing_file_gets_xml_mtime_without_download() {
        let content = b"already-verified-content";
        let xml_mtime = 1_545_586_142;

        let server = start_mock_server(
            HashMap::new(),
            MockResponse::new(200, MockBody::Full(vec![])),
        );

        let dir = temp_dir_for("verified_mtime");
        let file_path = dir.join("file.bin");
        fs::write(&file_path, content).expect("failed to write test file");
        let files = vec![(
            server.url("/file.bin"),
            file_path.to_str().unwrap().to_string(),
            None,
            Some(content.len() as u64),
            Some(xml_mtime),
        )];
        let client = Client::new();
        let running = test_running();

        download_files_with_signal(&client, files, 1, None, false, &running)
            .await
            .expect("batch should succeed");

        assert_eq!(
            server.request_count(),
            0,
            "verified file must not be re-downloaded"
        );
        assert_eq!(
            mtime_of(&file_path),
            Some(xml_mtime),
            "already-verified file must still get the _files.xml mtime"
        );
        assert_eq!(fs::read(&file_path).unwrap(), content);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn file_mtime_untouched_without_mtime_sources() {
        let content = b"no-mtime-content";
        let md5 = format!("{:x}", md5::compute(content));

        let mut scripts = HashMap::new();
        scripts.insert(
            "/file.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(
                200,
                MockBody::Full(content.to_vec()),
            )]),
        );
        let server = start_mock_server(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("no_mtime");
        let file_path = dir.join("file.bin").to_str().unwrap().to_string();
        let files = vec![(
            server.url("/file.bin"),
            file_path,
            Some(md5),
            Some(content.len() as u64),
            None,
        )];
        let client = Client::new();
        let running = test_running();

        download_files_with_signal(&client, files, 1, None, false, &running)
            .await
            .expect("batch should succeed");

        let secs = mtime_of(&dir.join("file.bin")).expect("file must exist");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(
            now.saturating_sub(secs) < 60,
            "mtime must stay at the download time when no source is available (age {}s)",
            now.saturating_sub(secs)
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn parse_retry_after_seconds() {
        assert_eq!(parse_retry_after("30"), Some(30));
        assert_eq!(parse_retry_after("  5 "), Some(5));
        assert_eq!(parse_retry_after("0"), Some(0));
        assert_eq!(parse_retry_after("999999"), Some(MAX_RETRY_AFTER_SECS));
        assert_eq!(parse_retry_after(""), None);
        assert_eq!(parse_retry_after("next tuesday"), None);
        assert_eq!(parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"), None);
    }

    #[test]
    fn backoff_delay_stays_within_bounds() {
        let d1 = backoff_delay(1);
        assert!(
            (4000..=6000).contains(&d1.as_millis()),
            "attempt 1: {:?}",
            d1
        );
        let d2 = backoff_delay(2);
        assert!(
            (8000..=12000).contains(&d2.as_millis()),
            "attempt 2: {:?}",
            d2
        );
        let d60 = backoff_delay(60);
        assert!(
            (48000..=72000).contains(&d60.as_millis()),
            "attempt 60 must be capped at 60s ±20%: {:?}",
            d60
        );
    }

    #[test]
    fn is_retryable_status_covers_server_errors() {
        for code in [408u16, 425, 429, 500, 502, 503, 504] {
            assert!(
                is_retryable_status(StatusCode::from_u16(code).unwrap()),
                "{} must be retryable",
                code
            );
        }
        for code in [200u16, 206, 301, 400, 401, 403, 404, 416] {
            assert!(
                !is_retryable_status(StatusCode::from_u16(code).unwrap()),
                "{} must not be retryable",
                code
            );
        }
    }
}
