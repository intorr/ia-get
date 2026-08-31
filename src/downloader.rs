//! Module for handling file downloads, verification, and related operations.

use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use colored::*;
use indicatif::ProgressBar;
use reqwest::header::{HeaderMap, HeaderValue, LAST_MODIFIED, RETRY_AFTER};
use reqwest::{Client, Response, StatusCode};

use crate::error::IaGetError; // Import IaGetError for explicit error conversion
use crate::utils::{
    create_progress_bar, format_size, print_downloaded_line, print_file_banner, with_cookie,
};
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

/// Finishes and clears the progress bar, if one was created
fn finish_progress_bar(pb: &Option<ProgressBar>) {
    if let Some(pb) = pb {
        pb.finish_and_clear();
    }
}

/// Calculates the MD5 hash of a file
fn calculate_md5(file_path: &str, running: &Arc<AtomicBool>) -> Result<String> {
    let file = File::open(file_path)?;
    let file_size = file.metadata()?.len();
    let is_large_file = file_size > LARGE_FILE_THRESHOLD;

    let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);
    let mut context = md5::Context::new();
    let mut buffer = [0; BUFFER_SIZE];

    let pb = is_large_file.then(|| {
        create_progress_bar(
            file_size,
            &format!("{} {}    ", "╰╼".cyan().dimmed(), "Verifying".white()),
            Some("blue/blue"),
            false,
        )
    });

    let mut bytes_processed: u64 = 0;

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        if !running.load(Ordering::SeqCst) {
            finish_progress_bar(&pb);
            return Err(IaGetError::Interrupted);
        }

        context.consume(&buffer[..bytes_read]);

        if let Some(ref progress_bar) = pb {
            bytes_processed += bytes_read as u64;
            progress_bar.set_position(bytes_processed);
        }
    }

    finish_progress_bar(&pb);

    let hash = context.finalize();
    Ok(format!("{:x}", hash))
}

/// Outcome of checking whether an already-downloaded file is still valid
enum ExistingFileStatus {
    /// The file does not exist and must be downloaded
    Missing,
    /// The file exists and passed verification; `md5` is the verified hash
    /// when archive.org provided one to compare against
    Verified { md5: Option<String> },
    /// The file exists but failed verification
    Invalid,
}

/// Size guard shared by the pre-download check and the post-download
/// verification.
///
/// Returns `Some((actual, expected))` when the local file's size differs
/// from the metadata, `None` when the sizes match or no size is known.
fn size_mismatch(file_path: &str, expected_size: Option<u64>) -> Result<Option<(u64, u64)>> {
    let Some(expected) = expected_size else {
        return Ok(None);
    };

    let actual = fs::metadata(file_path)?.len();
    if actual == expected {
        Ok(None)
    } else {
        Ok(Some((actual, expected)))
    }
}

/// Check if an existing file has the correct hash
fn check_existing_file(
    file_path: &str,
    expected_md5: Option<&str>,
    expected_size: Option<u64>,
    running: &Arc<AtomicBool>,
) -> Result<ExistingFileStatus> {
    if !Path::new(file_path).exists() {
        return Ok(ExistingFileStatus::Missing);
    }

    let Some(expected_md5) = expected_md5 else {
        // No hash to compare against: still reject a file whose size does not
        // match the metadata, so a stale or truncated copy is re-downloaded.
        if size_mismatch(file_path, expected_size)?.is_some() {
            return Ok(ExistingFileStatus::Invalid);
        }
        return Ok(ExistingFileStatus::Verified { md5: None });
    };

    let local_md5 = match calculate_md5(file_path, running) {
        Ok(hash) => hash,
        Err(e) => {
            if matches!(e, IaGetError::Interrupted) {
                return Err(e);
            }
            println!(
                "{} {} to calculate MD5 hash: {}",
                "╰╼".cyan().dimmed(),
                "Failed".red().bold(),
                e
            );
            return Ok(ExistingFileStatus::Invalid);
        }
    };

    if local_md5 == expected_md5 {
        Ok(ExistingFileStatus::Verified {
            md5: Some(local_md5),
        })
    } else {
        Ok(ExistingFileStatus::Invalid)
    }
}

/// Print the line shown once a file has been verified, using the same
/// format for freshly downloaded and already-verified files
fn print_verified_hash(md5: Option<&str>) {
    match md5 {
        Some(hash) => println!(
            "{} {}         {} {}",
            "╰╼".cyan().dimmed(),
            "Hash".white(),
            "✔".green().bold(),
            format!("({})", hash).dimmed()
        ),
        None => println!(
            "{} {}",
            "-".dimmed(),
            "No MD5 hash provided for verification.".dimmed()
        ),
    }
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

/// Tracks how many times a file transfer has been retried and how long to
/// wait before each retry, so retry call sites stay short.
struct RetryTracker {
    count: u32,
    delay: fn(u32) -> Duration,
}

impl RetryTracker {
    fn new(delay: fn(u32) -> Duration) -> Self {
        Self { count: 0, delay }
    }

    /// Records a failed attempt, prints the retry notice and waits.
    ///
    /// Returns an error once `MAX_RETRIES` has been exhausted.
    async fn record(
        &mut self,
        kind: &str,
        detail: &str,
        retry_after_secs: Option<u64>,
    ) -> Result<()> {
        self.count += 1;

        if self.count > MAX_RETRIES {
            println!(
                "{} {}       {} Maximum retries ({}) exceeded",
                "├╼".cyan().dimmed(),
                "Failed".red().bold(),
                "✘".red().bold(),
                MAX_RETRIES
            );
            return Err(IaGetError::Network(format!(
                "{kind}: {detail} (maximum retries {MAX_RETRIES} exceeded)"
            )));
        }

        let delay = retry_after_secs
            .map(Duration::from_secs)
            .unwrap_or_else(|| (self.delay)(self.count));

        println!(
            "{} {}        {} {} (attempt {}/{}): {}",
            "├╼".cyan().dimmed(),
            "Retry".yellow().bold(),
            "⟳".yellow().bold(),
            kind,
            self.count,
            MAX_RETRIES,
            detail
        );
        println!(
            "{} {}         Waiting {:.1}s before retry{}",
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
}

/// Streams the response body into `file`, updating the progress bar as data
/// arrives. Returns the number of new bytes written. A user interruption
/// aborts with `IaGetError::Interrupted`; partial data stays in the file so
/// the next attempt can resume.
async fn stream_response_body(
    response: &mut Response,
    file: &mut File,
    base_size: u64,
    pb: &ProgressBar,
    running: &Arc<AtomicBool>,
) -> Result<u64> {
    let mut downloaded_bytes: u64 = 0;

    while let Some(chunk_result) = response.chunk().await.transpose() {
        if !running.load(Ordering::SeqCst) {
            pb.finish_and_clear();
            return Err(IaGetError::Interrupted);
        }

        let chunk = chunk_result?;
        file.write_all(&chunk)?;
        downloaded_bytes += chunk.len() as u64;
        pb.set_position(base_size + downloaded_bytes);
    }

    Ok(downloaded_bytes)
}

/// A transfer's body could not be used: clear the progress bar, record the
/// retry, and leave the file positioned at its end so the next attempt
/// resumes from where this one stopped.
async fn retry_open_file(
    file: &mut File,
    pb: &ProgressBar,
    retry: &mut RetryTracker,
    kind: &str,
    detail: &str,
    retry_after_secs: Option<u64>,
) -> Result<()> {
    pb.finish_and_clear();
    retry.record(kind, detail, retry_after_secs).await?;
    file.flush()?;
    file.seek(SeekFrom::End(0))?;
    Ok(())
}

/// Progress bar label for a download attempt: "Resuming" when a Range
/// request continues an existing `.part` file, "Downloading" otherwise.
fn download_action_label(resuming: bool) -> String {
    if resuming {
        format!("{} {}     ", "╰╼".cyan().dimmed(), "Resuming".white())
    } else {
        format!("{} {}  ", "╰╼".cyan().dimmed(), "Downloading".white())
    }
}

/// Download file content with progress reporting and automatic retry on failure.
///
/// `retry_delay` computes the delay for a given 1-based retry attempt; tests
/// substitute near-instant delays for `backoff_delay`.
///
/// Only a successful (2xx) response body is ever written to `file`. Error
/// pages are discarded, empty and truncated bodies are retried, and a 200
/// response to a ranged request resets the file instead of appending to it.
///
/// Returns, when the server sent one on the final successful response, the
/// parsed `Last-Modified` header value.
async fn download_file_content(
    client: &Client,
    url: &str,
    file: &mut File,
    running: &Arc<AtomicBool>,
    cookie_header: Option<&HeaderValue>,
    expected_size: Option<u64>,
    retry_delay: fn(u32) -> Duration,
) -> Result<Option<SystemTime>> {
    let mut retry = RetryTracker::new(retry_delay);

    loop {
        // Re-check file size at start of each attempt (in case of retry)
        let current_file_size = file.metadata()?.len();
        let resuming = current_file_size > 0;
        let mut download_action = download_action_label(resuming);

        let mut request = with_cookie(client.get(url), cookie_header);
        if resuming {
            request = request.header(
                reqwest::header::RANGE,
                HeaderValue::from_str(&format!("bytes={}-", current_file_size)).map_err(|e| {
                    IaGetError::Network(format!("Invalid range header value: {}", e))
                })?,
            );
        }

        let mut response = match request.send().await {
            Ok(resp) => resp,
            Err(e) => {
                // Request failed before we even got a response
                retry
                    .record("Connection error", &e.to_string(), None)
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
                retry
                    .record(&format!("HTTP {status}"), &reason, retry_after)
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
            download_action = download_action_label(false);
        }
        let base_size = file.metadata()?.len();

        let pb = create_progress_bar(
            base_size + response.content_length().unwrap_or(0),
            &download_action,
            Some("green/green"),
            true,
        );
        // Set initial progress to current file size for resumed downloads
        pb.set_position(base_size);

        let start_time = Instant::now();
        match stream_response_body(&mut response, file, base_size, &pb, running).await {
            Ok(downloaded_bytes) => {
                let total_bytes = base_size + downloaded_bytes;
                // Ensure data is written to disk
                file.flush()?;

                // A 2xx body with zero bytes is a server malfunction (unless
                // the server explicitly announced a zero-byte file).
                if downloaded_bytes == 0 && base_size == 0 && expected_size != Some(0) {
                    retry_open_file(
                        file,
                        &pb,
                        &mut retry,
                        "Empty response",
                        "server returned no data",
                        retry_after,
                    )
                    .await?;
                    continue;
                }

                // The body ended before the announced size: the transfer was
                // truncated, so resume from where we stopped.
                if let Some(expected) = expected_size {
                    if total_bytes < expected {
                        retry_open_file(
                            file,
                            &pb,
                            &mut retry,
                            "Incomplete body",
                            &format!("received {total_bytes} of {expected} bytes"),
                            None,
                        )
                        .await?;
                        continue;
                    }
                }

                pb.finish_and_clear();
                print_downloaded_line(
                    "├╼".cyan().dimmed(),
                    downloaded_bytes,
                    Some(start_time.elapsed()),
                );

                return Ok(last_modified);
            }
            Err(e) => {
                // A user interruption aborts the run, other errors retry
                if matches!(e, IaGetError::Interrupted) {
                    pb.finish_and_clear();
                    return Err(e);
                }

                // Mid-stream failure: keep the partial data and resume later.
                retry_open_file(
                    file,
                    &pb,
                    &mut retry,
                    "Download error",
                    &e.to_string(),
                    None,
                )
                .await?;
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
    if let Some((actual_size, expected_size)) = size_mismatch(file_path, expected_size)? {
        println!(
            "{} {}         {} {} (expected {})",
            "╰╼".cyan().dimmed(),
            "Size".white(),
            "✘".red().bold(),
            format_size(actual_size).red(),
            format_size(expected_size).dimmed()
        );
        return Ok(false);
    }

    if expected_md5.is_none() {
        print_verified_hash(None);
        return Ok(true); // No hash to check against, consider it verified
    }
    let expected_md5_str = expected_md5.unwrap();
    let local_md5 = calculate_md5(file_path, running)?;
    if local_md5 == expected_md5_str {
        print_verified_hash(Some(&local_md5));
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

/// A single file to download, plus the archive metadata used to verify it
#[derive(Debug)]
pub struct DownloadTask {
    /// URL the file is downloaded from
    pub url: String,
    /// Path of the final file on disk (may include subdirectories)
    pub file_path: String,
    /// MD5 hash from the archive metadata, if present
    pub expected_md5: Option<String>,
    /// Expected size in bytes, if known
    pub expected_size: Option<u64>,
    /// Unix mtime from the archive metadata, if present
    pub expected_mtime: Option<u64>,
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
///
/// Files are numbered starting at `file_number_start` (1-based) out of
/// `total_files`, so files handled before the batch (for example the
/// locally saved `_files.xml` as file #1) can be counted into the numbering.
pub async fn download_files<I>(
    client: &Client,
    files: I,
    total_files: usize,
    file_number_start: usize,
    cookie_header: Option<&HeaderValue>,
    stop_on_error: bool,
) -> Result<()>
where
    I: IntoIterator<Item = DownloadTask>,
{
    // Set up signal handling for the entire download session
    let running = setup_signal_handler();

    download_files_with_signal(
        client,
        files,
        total_files,
        file_number_start,
        cookie_header,
        stop_on_error,
        &running,
    )
    .await
}

/// Outcome of processing a single file of the batch
enum FileOutcome {
    /// The file is now valid: already verified, or freshly downloaded
    Succeeded,
    /// The file could not be downloaded; holds the reason for the failure report
    Failed(String),
}

/// Outcome of checking the existing copy of a file before downloading
enum ExistingFileHandling {
    /// The existing copy is valid; nothing left to do
    Done,
    /// No valid copy on disk; the file must be (re)downloaded
    Download,
    /// The stale copy could not be removed; hold the failure reason
    Blocked(String),
}

/// Disposes of the existing (final) copy of a file before downloading it:
/// a verified copy finishes the job, a stale copy is removed so the
/// download starts from a clean slate, and any leftover `.part` file is
/// cleaned up either way.
fn handle_existing_file(
    file_path: &str,
    part_path: &str,
    expected_md5: Option<&str>,
    expected_size: Option<u64>,
    expected_mtime: Option<u64>,
    running: &Arc<AtomicBool>,
) -> Result<ExistingFileHandling> {
    match check_existing_file(file_path, expected_md5, expected_size, running)? {
        ExistingFileStatus::Verified { md5 } => {
            remove_part_file(part_path);
            // No request is made for a verified file, so only the XML
            // mtime is available here.
            if let Some(target) = mtime_from_xml(expected_mtime) {
                sync_file_mtime(file_path, target);
            }
            print_verified_hash(md5.as_deref());
            Ok(ExistingFileHandling::Done)
        }
        ExistingFileStatus::Invalid => {
            println!(
                "{} {}      {} the existing file failed verification, re-downloading",
                "├╼".cyan().dimmed(),
                "Partial".white(),
                "▲".yellow().bold()
            );
            if let Err(e) = fs::remove_file(file_path) {
                // A stale file that cannot be removed (locked, read-only)
                // is a per-file failure, not a batch-aborting one.
                remove_part_file(part_path);
                return Ok(ExistingFileHandling::Blocked(format!(
                    "stale file could not be removed: {e}"
                )));
            }
            remove_part_file(part_path);
            Ok(ExistingFileHandling::Download)
        }
        ExistingFileStatus::Missing => Ok(ExistingFileHandling::Download),
    }
}

/// Installs a verified `.part` file under its final name, then sets its
/// mtime: the server's Last-Modified header wins, the `_files.xml` mtime is
/// the fallback, and the time is left untouched when both are absent.
fn install_downloaded_file(
    file_path: &str,
    part_path: &str,
    server_mtime: Option<SystemTime>,
    xml_mtime: Option<u64>,
) -> Result<()> {
    if Path::new(file_path).exists() {
        fs::remove_file(file_path)?;
    }
    fs::rename(part_path, file_path)?;
    if let Some(target) = server_mtime.or(mtime_from_xml(xml_mtime)) {
        sync_file_mtime(file_path, target);
    }
    Ok(())
}

/// Processes one file of the batch: verifies an existing copy or downloads
/// (with verification) and renames the `.part` file to its final name.
///
/// Prints the file banner and status lines. Per-file problems (a stale file
/// that cannot be removed, a failed download) yield
/// `FileOutcome::Failed`; hard errors (I/O, user interruption) propagate.
async fn process_file(
    client: &Client,
    task: &DownloadTask,
    number: usize,
    total_files: usize,
    running: &Arc<AtomicBool>,
    cookie_header: Option<&HeaderValue>,
) -> Result<FileOutcome> {
    let url = &task.url;
    let file_path = &task.file_path;
    let expected_md5 = task.expected_md5.as_deref();
    let expected_size = task.expected_size;
    let expected_mtime = task.expected_mtime;

    println!(" ");
    print_file_banner(file_path, number, total_files);

    let part_path = format!("{}.part", file_path);

    match handle_existing_file(
        file_path,
        &part_path,
        expected_md5,
        expected_size,
        expected_mtime,
        running,
    )? {
        ExistingFileHandling::Done => return Ok(FileOutcome::Succeeded),
        ExistingFileHandling::Blocked(reason) => return Ok(FileOutcome::Failed(reason)),
        ExistingFileHandling::Download => {}
    }

    let outcome = run_download_attempts(
        client,
        url,
        &part_path,
        running,
        cookie_header,
        expected_md5,
        expected_size,
    )
    .await?;

    match outcome {
        DownloadOutcome::Verified { server_mtime } => {
            install_downloaded_file(file_path, &part_path, server_mtime, expected_mtime)?;
            Ok(FileOutcome::Succeeded)
        }
        DownloadOutcome::Failed {
            reason,
            discard_part,
        } => {
            if discard_part {
                // The server rejected the resume offset; the .part file is
                // not a valid prefix, so discard it for the next run.
                remove_part_file(&part_path);
            }
            Ok(FileOutcome::Failed(reason))
        }
    }
}

/// Best-effort removal of a leftover `.part` file: its absence is not an
/// error, and a locked file must not fail the processing of the file itself
fn remove_part_file(part_path: &str) {
    let _ = fs::remove_file(part_path);
}

/// Outcome of the re-download attempts for one file
enum DownloadOutcome {
    /// The file was downloaded and passed verification; `server_mtime` is
    /// the `Last-Modified` header of the final response, if the server sent one
    Verified { server_mtime: Option<SystemTime> },
    /// The file could not be downloaded; `reason` is reported in the batch
    /// summary, and `discard_part` says whether the `.part` file must not be
    /// kept for resuming
    Failed { reason: String, discard_part: bool },
}

/// Runs the download+verification attempts for one file, re-downloading from
/// scratch after a failed verification or a rejected resume offset.
async fn run_download_attempts(
    client: &Client,
    url: &str,
    part_path: &str,
    running: &Arc<AtomicBool>,
    cookie_header: Option<&HeaderValue>,
    expected_md5: Option<&str>,
    expected_size: Option<u64>,
) -> Result<DownloadOutcome> {
    ensure_parent_directories(part_path)?;
    let mut file = prepare_file_for_download(part_path)?;

    let mut last_reason = String::new();
    // True when the .part file does not hold a valid prefix and must not be
    // kept for resuming; reset whenever the file is re-created below
    let mut discard_part = false;
    let mut attempt = 0;

    let outcome = loop {
        attempt += 1;
        if attempt > MAX_DOWNLOAD_ATTEMPTS {
            break DownloadOutcome::Failed {
                reason: last_reason,
                discard_part,
            };
        }

        if attempt > 1 {
            // The .part file is re-created from scratch, so an earlier
            // range reject no longer applies to it
            discard_part = false;
            drop(file);
            remove_part_file(part_path);
            file = prepare_file_for_download(part_path)?;
            println!(
                "{} {}        {} Re-downloading from scratch (attempt {}/{})",
                "├╼".cyan().dimmed(),
                "Retry".yellow().bold(),
                "⟳".yellow().bold(),
                attempt,
                MAX_DOWNLOAD_ATTEMPTS
            );
        }

        match download_file_content(
            client,
            url,
            &mut file,
            running,
            cookie_header,
            expected_size,
            backoff_delay,
        )
        .await
        {
            Ok(server_mtime) => {
                if verify_downloaded_file(part_path, expected_md5, expected_size, running)? {
                    break DownloadOutcome::Verified { server_mtime };
                }
                last_reason = format!("file failed verification after {attempt} attempt(s)");
            }
            Err(e) if matches!(e, IaGetError::Interrupted) => return Err(e),
            // The server rejected our offset: the .part file is not a valid
            // prefix, so it must not be kept if the attempts end here
            Err(IaGetError::RangeNotSatisfiable) => {
                last_reason = IaGetError::RangeNotSatisfiable.to_string();
                discard_part = true;
            }
            // Any other failure is final; the partial .part file is still a
            // resumable prefix
            Err(e) => {
                break DownloadOutcome::Failed {
                    reason: e.to_string(),
                    discard_part,
                }
            }
        }
    };

    // Close the handle before renaming (Windows refuses to rename open files)
    drop(file);

    Ok(outcome)
}

/// Batch download logic with an externally provided signal flag.
///
/// Split out from `download_files` so tests can drive the batch loop without
/// registering a second (and panicking) Ctrl+C handler.
///
/// `file_number_start` is the 1-based number assigned to the first file of
/// the batch.
async fn download_files_with_signal<I>(
    client: &Client,
    files: I,
    total_files: usize,
    file_number_start: usize,
    cookie_header: Option<&HeaderValue>,
    stop_on_error: bool,
    running: &Arc<AtomicBool>,
) -> Result<()>
where
    I: IntoIterator<Item = DownloadTask>,
{
    let mut failed_files: Vec<(String, String)> = Vec::new();

    for (index, task) in files.into_iter().enumerate() {
        // Check if we should stop due to signal
        if !running.load(Ordering::SeqCst) {
            println!(
                "\n{} Download interrupted. Run the command again to resume remaining files.",
                "✘".red().bold()
            );
            break;
        }

        let outcome = process_file(
            client,
            &task,
            index + file_number_start,
            total_files,
            running,
            cookie_header,
        )
        .await?;

        if let FileOutcome::Failed(reason) = outcome {
            failed_files.push((task.file_path.clone(), reason));
            if stop_on_error {
                return Err(batch_failed(&failed_files, total_files));
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
        return Err(batch_failed(&failed_files, total_files));
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

/// Builds the terminal `IaGetError::BatchFailed` from the accumulated failures
fn batch_failed(failed_files: &[(String, String)], total: usize) -> IaGetError {
    IaGetError::BatchFailed {
        count: failed_files.len(),
        total,
        details: batch_failure_details(failed_files),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{mtime_of, temp_dir_for, MockBody, MockResponse, MockServer};
    use std::collections::{HashMap, VecDeque};
    use std::time::Instant;

    /// Near-instant retry delay so tests do not wait for real backoff
    fn fast_retry(_attempt: u32) -> Duration {
        Duration::from_millis(1)
    }

    fn test_running() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(true))
    }

    /// Builds a `DownloadTask` for the mock server
    fn task(
        url: String,
        file_path: String,
        md5: Option<String>,
        size: Option<u64>,
        mtime: Option<u64>,
    ) -> DownloadTask {
        DownloadTask {
            url,
            file_path,
            expected_md5: md5,
            expected_size: size,
            expected_mtime: mtime,
        }
    }

    /// Runs a single download against the mock server and returns the
    /// captured `Last-Modified` time.
    async fn run_download(
        url: &str,
        part_path: &str,
        expected_size: Option<u64>,
    ) -> Result<Option<SystemTime>> {
        let client = Client::new();
        let mut file = prepare_file_for_download(part_path)?;
        let result = download_file_content(
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
        let server = MockServer::start(scripts, MockResponse::new(200, MockBody::Full(vec![])));

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
        let server = MockServer::start(scripts, MockResponse::new(200, MockBody::Full(vec![])));

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
        let server = MockServer::start(scripts, MockResponse::new(206, MockBody::Full(vec![])));

        let dir = temp_dir_for("mid_stream");
        let part = dir.join("file.bin.part");
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(full.len() as u64),
        )
        .await;

        result.expect("download should succeed");
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
        let server = MockServer::start(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("range_ignored");
        let part = dir.join("file.bin.part");
        fs::write(&part, b"XXXXXX").unwrap(); // 6-byte "partial" file
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(full.len() as u64),
        )
        .await;

        result.expect("download should succeed");
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
        let server = MockServer::start(scripts, MockResponse::new(416, MockBody::Full(vec![])));

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
        let server = MockServer::start(scripts, MockResponse::new(404, MockBody::Full(vec![])));

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
        let server = MockServer::start(scripts, MockResponse::new(429, MockBody::Full(vec![])));

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
        let server = MockServer::start(scripts, MockResponse::new(404, MockBody::Full(vec![])));

        let dir = temp_dir_for("batch_continue");
        let missing_path = dir.join("missing.bin").to_str().unwrap().to_string();
        let ok_path = dir.join("ok.bin").to_str().unwrap().to_string();
        let files = vec![
            task(
                server.url("/missing.bin"),
                missing_path,
                None,
                Some(5),
                None,
            ),
            task(
                server.url("/ok.bin"),
                ok_path,
                Some(ok_md5),
                Some(ok_content.len() as u64),
                None,
            ),
        ];
        let client = Client::new();
        let running = test_running();

        let err = download_files_with_signal(&client, files, 2, 1, None, false, &running)
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
        let server = MockServer::start(scripts, MockResponse::new(404, MockBody::Full(vec![])));

        let dir = temp_dir_for("stop_on_error");
        let missing_path = dir.join("missing.bin").to_str().unwrap().to_string();
        let ok_path = dir.join("ok.bin").to_str().unwrap().to_string();
        let files = vec![
            task(
                server.url("/missing.bin"),
                missing_path,
                None,
                Some(5),
                None,
            ),
            task(
                server.url("/ok.bin"),
                ok_path,
                Some(ok_md5),
                Some(ok_content.len() as u64),
                None,
            ),
        ];
        let client = Client::new();
        let running = test_running();

        let err = download_files_with_signal(&client, files, 2, 1, None, true, &running)
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
        let server = MockServer::start(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("hash_mismatch");
        let file_path = dir.join("file.bin").to_str().unwrap().to_string();
        let files = vec![task(
            server.url("/file.bin"),
            file_path,
            Some(md5),
            Some(correct.len() as u64),
            None,
        )];
        let client = Client::new();
        let running = test_running();

        download_files_with_signal(&client, files, 1, 1, None, false, &running)
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
        let server = MockServer::start(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("last_modified");
        let part = dir.join("file.bin.part");
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(content.len() as u64),
        )
        .await;

        let mtime = result.expect("download should succeed");
        assert_eq!(fs::read(&part).unwrap(), content);
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
        let server = MockServer::start(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("xml_mtime");
        let file_path = dir.join("file.bin").to_str().unwrap().to_string();
        let files = vec![task(
            server.url("/file.bin"),
            file_path,
            Some(md5),
            Some(content.len() as u64),
            Some(xml_mtime),
        )];
        let client = Client::new();
        let running = test_running();

        download_files_with_signal(&client, files, 1, 1, None, false, &running)
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
        let server = MockServer::start(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("server_mtime");
        let file_path = dir.join("file.bin").to_str().unwrap().to_string();
        let files = vec![task(
            server.url("/file.bin"),
            file_path,
            Some(md5),
            Some(content.len() as u64),
            Some(xml_mtime),
        )];
        let client = Client::new();
        let running = test_running();

        download_files_with_signal(&client, files, 1, 1, None, false, &running)
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

        let server = MockServer::start(
            HashMap::new(),
            MockResponse::new(200, MockBody::Full(vec![])),
        );

        let dir = temp_dir_for("verified_mtime");
        let file_path = dir.join("file.bin");
        fs::write(&file_path, content).expect("failed to write test file");
        let files = vec![task(
            server.url("/file.bin"),
            file_path.to_str().unwrap().to_string(),
            None,
            Some(content.len() as u64),
            Some(xml_mtime),
        )];
        let client = Client::new();
        let running = test_running();

        download_files_with_signal(&client, files, 1, 1, None, false, &running)
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
        let server = MockServer::start(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("no_mtime");
        let file_path = dir.join("file.bin").to_str().unwrap().to_string();
        let files = vec![task(
            server.url("/file.bin"),
            file_path,
            Some(md5),
            Some(content.len() as u64),
            None,
        )];
        let client = Client::new();
        let running = test_running();

        download_files_with_signal(&client, files, 1, 1, None, false, &running)
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

    #[tokio::test]
    async fn existing_file_with_wrong_size_and_no_md5_is_redownloaded() {
        let content = b"fresh-content-001";

        let mut scripts = HashMap::new();
        scripts.insert(
            "/file.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(
                200,
                MockBody::Full(content.to_vec()),
            )]),
        );
        let server = MockServer::start(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("size_mismatch_skip");
        let file_path = dir.join("file.bin").to_str().unwrap().to_string();
        // A stale copy with the wrong size and no MD5 in the metadata: the
        // skip path must re-download instead of accepting it.
        fs::write(&file_path, b"stale").unwrap();
        let files = vec![task(
            server.url("/file.bin"),
            file_path,
            None,
            Some(content.len() as u64),
            None,
        )];
        let client = Client::new();
        let running = test_running();

        download_files_with_signal(&client, files, 1, 1, None, false, &running)
            .await
            .expect("batch should succeed after re-download");

        assert_eq!(
            server.request_count(),
            1,
            "size mismatch without MD5 must trigger a re-download"
        );
        assert_eq!(fs::read(dir.join("file.bin")).unwrap(), content);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn unremovable_stale_file_fails_only_that_file() {
        let ok_content = b"ok-content-123";
        let ok_md5 = format!("{:x}", md5::compute(ok_content));

        let mut scripts = HashMap::new();
        scripts.insert(
            "/ok.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(
                200,
                MockBody::Full(ok_content.to_vec()),
            )]),
        );
        let server = MockServer::start(scripts, MockResponse::new(404, MockBody::Full(vec![])));

        let dir = temp_dir_for("unremovable_stale");
        // A directory at the final path: both MD5 calculation and remove_file
        // fail on it on every platform, so it lands in the Invalid branch.
        let blocked = dir.join("blocked.bin");
        fs::create_dir(&blocked).unwrap();
        let blocked_path = blocked.to_str().unwrap().to_string();
        let ok_path = dir.join("ok.bin").to_str().unwrap().to_string();
        let files = vec![
            task(
                server.url("/blocked.bin"),
                blocked_path,
                Some("00000000000000000000000000000000".to_string()),
                None,
                None,
            ),
            task(
                server.url("/ok.bin"),
                ok_path,
                Some(ok_md5),
                Some(ok_content.len() as u64),
                None,
            ),
        ];
        let client = Client::new();
        let running = test_running();

        let err = download_files_with_signal(&client, files, 2, 1, None, false, &running)
            .await
            .unwrap_err();
        match err {
            IaGetError::BatchFailed {
                count,
                total,
                details,
            } => {
                assert_eq!((count, total), (1, 2));
                assert!(details.contains("blocked.bin"), "{details}");
            }
            other => panic!("expected BatchFailed with one file, got {:?}", other),
        }
        assert_eq!(fs::read(dir.join("ok.bin")).unwrap(), ok_content);
        assert!(
            blocked.exists(),
            "unremovable stale file must stay in place"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
