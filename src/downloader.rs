//! Module for handling file downloads, verification, and related operations.

use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use colored::*;
use digest::Digest;
use indicatif::ProgressBar;
use reqwest::header::{CONTENT_RANGE, HeaderMap, HeaderValue, LAST_MODIFIED, RETRY_AFTER};
use reqwest::{Client, Response, StatusCode};

use crate::Result;
use crate::error::{IaGetError, io_error_with_path}; // Import IaGetError for explicit error conversion
use crate::utils::{
    branch_glyph, create_progress_bar, ensure_not_symlink, format_size, last_glyph,
    print_downloaded_line, print_file_banner, with_cookie,
}; // Import utility functions

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

/// How often interruptible waits (retry backoff, a stalled body read) wake
/// up to check for a Ctrl+C, so a long server-requested wait (up to 15 min)
/// or a stalled transfer does not outlive the user's request to stop
const INTERRUPT_CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// Process-wide "should stop" flag, registered on first use.
static RUNNING_FLAG: OnceLock<Arc<AtomicBool>> = OnceLock::new();

/// Sets up signal handling for graceful shutdown on Ctrl+C
///
/// Returns an Arc<AtomicBool> that can be checked to see if the process
/// should stop. The first Ctrl+C sets it to false; a second one quits the
/// process immediately, so a long Retry-After wait can always be aborted.
///
/// Idempotent: repeated calls return the same flag; the handler is
/// registered only once (a second `ctrlc::set_handler` would panic).
fn setup_signal_handler() -> Arc<AtomicBool> {
    let running = RUNNING_FLAG.get_or_init(|| {
        let running = Arc::new(AtomicBool::new(true));
        let presses = Arc::new(AtomicU32::new(0));
        let r = running.clone();
        let p = presses.clone();

        ctrlc::set_handler(move || match handle_ctrl_c(&r, &p) {
            CtrlCAction::GracefulStop => {}
            CtrlCAction::QuitNow => std::process::exit(1),
        })
        .expect("Error setting Ctrl+C handler");

        running
    });
    running.clone()
}

/// What a Ctrl+C press must do
enum CtrlCAction {
    /// The batch was asked to stop gracefully
    GracefulStop,
    /// The process must terminate immediately
    QuitNow,
}

/// Ctrl+C handling: the first press asks the batch to stop gracefully, the
/// second one asks for an immediate quit. Kept apart from the handler
/// registration so the behaviour is testable without registering a second
/// (panicking) Ctrl+C handler.
fn handle_ctrl_c(running: &Arc<AtomicBool>, presses: &Arc<AtomicU32>) -> CtrlCAction {
    if presses.fetch_add(1, Ordering::SeqCst) == 0 {
        running.store(false, Ordering::SeqCst);
        println!(
            "\n{} Received Ctrl+C, finishing current operation...",
            "✘".red().bold()
        );
        CtrlCAction::GracefulStop
    } else {
        println!(
            "\n{} {} Quitting now.",
            "✘".red().bold(),
            "Ctrl+C".red().bold()
        );
        CtrlCAction::QuitNow
    }
}

/// Finishes and clears the progress bar, if one was created
fn finish_progress_bar(pb: &Option<ProgressBar>) {
    if let Some(pb) = pb {
        pb.finish_and_clear();
    }
}

/// Calculates the MD5 hash of a file
fn calculate_md5(file_path: &str, running: &Arc<AtomicBool>) -> Result<String> {
    let file = File::open(file_path).map_err(|e| io_error_with_path(file_path, e))?;
    let file_size = file
        .metadata()
        .map_err(|e| io_error_with_path(file_path, e))?
        .len();
    let is_large_file = file_size > LARGE_FILE_THRESHOLD;

    let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);
    let mut context = md5::Md5::new();
    let mut buffer = [0; BUFFER_SIZE];

    let pb = is_large_file.then(|| {
        create_progress_bar(
            file_size,
            &format!("{} {}    ", last_glyph(), "Verifying".white()),
            Some("blue/blue"),
            false,
        )
    });

    let mut bytes_processed: u64 = 0;

    loop {
        let bytes_read = reader
            .read(&mut buffer)
            .map_err(|e| io_error_with_path(file_path, e))?;
        if bytes_read == 0 {
            break;
        }

        if !running.load(Ordering::SeqCst) {
            finish_progress_bar(&pb);
            return Err(IaGetError::Interrupted);
        }

        context.update(&buffer[..bytes_read]);

        if let Some(ref progress_bar) = pb {
            bytes_processed += bytes_read as u64;
            progress_bar.set_position(bytes_processed);
        }
    }

    finish_progress_bar(&pb);

    let hash = context.finalize();
    Ok(hash.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// Outcome of checking whether an already-downloaded file is still valid
#[derive(Debug)]
enum ExistingFileStatus {
    /// The file does not exist and must be downloaded
    Missing,
    /// The file exists and passed verification; `md5` is the verified hash
    /// when archive.org provided one to compare against
    Verified { md5: Option<String> },
    /// The file exists but failed verification
    Invalid,
    /// The file could not be read (an I/O error): it is left untouched and
    /// the problem is reported for this file only, because a read failure
    /// proves nothing about the file's contents
    Unreadable(String),
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

    let actual = fs::metadata(file_path)
        .map_err(|e| io_error_with_path(file_path, e))?
        .len();
    if actual == expected {
        Ok(None)
    } else {
        Ok(Some((actual, expected)))
    }
}

/// Check if an existing file is still valid: the (cheap) size check runs
/// first so a stale or truncated copy never spends a full-file hash
/// before being rejected
fn check_existing_file(
    file_path: &str,
    expected_md5: Option<&str>,
    expected_size: Option<u64>,
    running: &Arc<AtomicBool>,
) -> Result<ExistingFileStatus> {
    if !Path::new(file_path).exists() {
        return Ok(ExistingFileStatus::Missing);
    }
    // A directory at the final path can neither be verified as a file nor
    // safely removed: report it as unreadable and leave it in place.
    if Path::new(file_path).is_dir() {
        return Ok(ExistingFileStatus::Unreadable(
            "a directory occupies the file path".to_string(),
        ));
    }

    let mismatch = match size_mismatch(file_path, expected_size) {
        Ok(mismatch) => mismatch,
        Err(e) => {
            return Ok(ExistingFileStatus::Unreadable(format!(
                "could not read file size: {e}"
            )));
        }
    };
    if mismatch.is_some() {
        return Ok(ExistingFileStatus::Invalid);
    }

    let Some(expected_md5) = expected_md5 else {
        // No hash to compare against and the size matches: verified
        return Ok(ExistingFileStatus::Verified { md5: None });
    };

    let local_md5 = match calculate_md5(file_path, running) {
        Ok(hash) => hash,
        Err(e) => {
            if matches!(e, IaGetError::Interrupted) {
                return Err(e);
            }
            // "Could not read" is not "corrupted": the file is kept as is
            // and the problem is reported for this file only.
            return Ok(ExistingFileStatus::Unreadable(format!(
                "could not verify existing file: {e}"
            )));
        }
    };

    // Hex digests compare case-insensitively: the metadata may carry an
    // uppercase or mixed-case MD5.
    if local_md5.eq_ignore_ascii_case(expected_md5) {
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
            last_glyph(),
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
    if let Some(path) = Path::new(file_path).parent()
        && path.file_name().is_some()
        && !path.exists()
    {
        fs::create_dir_all(path).map_err(|e| io_error_with_path(path, e))?;
    }
    Ok(())
}

/// Prepare a file for download
fn prepare_file_for_download(file_path: &str) -> Result<File> {
    // A pre-planted symlink at the .part path would be opened for writing:
    // every streamed byte would reach the link target instead of the file.
    ensure_not_symlink(Path::new(file_path))?;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(file_path)
        .map_err(|e| io_error_with_path(file_path, e))?;

    // Seek to the end of the file for resume capability
    file.seek(SeekFrom::End(0))
        .map_err(|e| io_error_with_path(file_path, e))?;

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

/// Parses the start offset of a `Content-Range` header of the form
/// `bytes <start>-<end>/<total>` (or `bytes <start>-<end>/*`), which a 206
/// response must carry.
///
/// Returns `Some(true)` when the offset equals `expected_start`,
/// `Some(false)` when it differs, and `None` when the header is absent or
/// malformed — in both `None` cases the caller treats the body as untrusted.
fn partial_content_offset(headers: &HeaderMap, expected_start: u64) -> Option<bool> {
    headers
        .get(CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            let range = value.strip_prefix("bytes ")?.split_once('/')?.0;
            let start = range.split_once('-')?.0;
            start
                .parse::<u64>()
                .ok()
                .map(|start| start == expected_start)
        })
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
    // A pre-1970 timestamp has no Unix representation; stamping the epoch
    // would fabricate a wrong mtime, so the time is left untouched instead.
    let Ok(target_duration) = target.duration_since(UNIX_EPOCH) else {
        return false;
    };
    let target_secs = target_duration.as_secs();

    let current_secs = fs::metadata(&file_path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs());

    if current_secs == Some(target_secs) {
        return false;
    }

    // u64 seconds only overflow i64 for dates around the year 292 million
    // AD; nothing representable exists there, so leave the time as is.
    let Ok(target_secs) = i64::try_from(target_secs) else {
        return false;
    };

    let file_time = filetime::FileTime::from_unix_time(target_secs, 0);

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
    /// Returns an error once `MAX_RETRIES` has been exhausted or the user
    /// interrupted the run while the wait was in progress.
    async fn record(
        &mut self,
        kind: &str,
        detail: &str,
        retry_after_secs: Option<u64>,
        running: &Arc<AtomicBool>,
    ) -> Result<()> {
        self.count += 1;

        if self.count > MAX_RETRIES {
            println!(
                "{} {}       {} Maximum retries ({}) exceeded",
                branch_glyph(),
                "Failed".red().bold(),
                "✘".red().bold(),
                MAX_RETRIES
            );
            return Err(IaGetError::Network {
                detail: format!("{kind}: {detail} (maximum retries {MAX_RETRIES} exceeded)"),
                source: None,
            });
        }

        let delay = retry_after_secs
            .map(Duration::from_secs)
            .unwrap_or_else(|| (self.delay)(self.count));

        println!(
            "{} {}        {} {} (attempt {}/{}): {}",
            branch_glyph(),
            "Retry".yellow().bold(),
            "⟳".yellow().bold(),
            kind,
            self.count,
            MAX_RETRIES,
            detail
        );
        println!(
            "{} {}         Waiting {:.1}s before retry{}",
            branch_glyph(),
            "Wait".white(),
            delay.as_secs_f64(),
            if retry_after_secs.is_some() {
                " (server requested)"
            } else {
                ""
            }
        );

        // Sleep in slices and check the flag at each slice boundary: a
        // Ctrl+C during a long wait (a server-requested Retry-After of up
        // to 15 min) must stop the retry loop without sleeping the whole
        // delay out.
        let mut remaining = delay;
        while remaining > Duration::ZERO {
            let slice = remaining.min(INTERRUPT_CHECK_INTERVAL);
            tokio::time::sleep(slice).await;
            remaining -= slice;
            if !running.load(Ordering::SeqCst) {
                return Err(IaGetError::Interrupted);
            }
        }
        Ok(())
    }
}

/// Resolves once the batch has been asked to stop, so a stalled body read
/// can be aborted at the next check instead of waiting for the next chunk
/// (or the read timeout).
async fn wait_for_stop(running: &Arc<AtomicBool>) {
    while running.load(Ordering::SeqCst) {
        tokio::time::sleep(INTERRUPT_CHECK_INTERVAL).await;
    }
}

/// Streams the response body into `file`, updating the progress bar as data
/// arrives. Returns the number of new bytes written. A user interruption
/// aborts with `IaGetError::Interrupted`; partial data stays in the file so
/// the next attempt can resume.
///
/// The stop flag is raced against every chunk read: a body that stalls
/// mid-transfer no longer forces a wait for the next chunk or the read
/// timeout after a Ctrl+C.
async fn stream_response_body(
    response: &mut Response,
    file: &mut File,
    base_size: u64,
    pb: &ProgressBar,
    running: &Arc<AtomicBool>,
) -> Result<u64> {
    let mut downloaded_bytes: u64 = 0;

    loop {
        tokio::select! {
            chunk = response.chunk() => match chunk {
                Ok(Some(chunk)) => {
                    file.write_all(&chunk)?;
                    downloaded_bytes += chunk.len() as u64;
                    pb.set_position(base_size + downloaded_bytes);
                }
                Ok(None) => break,
                Err(e) => return Err(e.into()),
            },
            _ = wait_for_stop(running) => {
                pb.finish_and_clear();
                return Err(IaGetError::Interrupted);
            }
        }
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
    running: &Arc<AtomicBool>,
) -> Result<()> {
    pb.finish_and_clear();
    retry
        .record(kind, detail, retry_after_secs, running)
        .await?;
    file.seek(SeekFrom::End(0))?;
    Ok(())
}

/// Progress bar label for a download attempt: "Resuming" when a Range
/// request continues an existing `.part` file, "Downloading" otherwise.
fn download_action_label(resuming: bool) -> String {
    if resuming {
        format!("{} {}     ", last_glyph(), "Resuming".white())
    } else {
        format!("{} {}  ", last_glyph(), "Downloading".white())
    }
}

/// True when the error is a disk-full / no-space condition that retrying
/// cannot fix (ENOSPC on Linux, ERROR_DISK_FULL on Windows).
fn is_disk_full_error(err: &IaGetError) -> bool {
    matches!(
        err,
        IaGetError::FileSystem { source: Some(src), .. }
            if src
                .downcast_ref::<std::io::Error>()
                .is_some_and(|io| io.kind() == std::io::ErrorKind::StorageFull)
    )
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
                    IaGetError::Network {
                        detail: format!("Invalid range header value: {e}"),
                        source: Some(Box::new(e)),
                    }
                })?,
            );
        }

        let mut response = match request.send().await {
            Ok(resp) => resp,
            Err(e) => {
                // Request failed before we even got a response
                retry
                    .record("Connection error", &e.to_string(), None, running)
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
                    .record(&format!("HTTP {status}"), &reason, retry_after, running)
                    .await?;
                continue;
            }

            return Err(IaGetError::Network {
                detail: format!(
                    "Server responded with HTTP {} {}",
                    status,
                    status.canonical_reason().unwrap_or("unknown status")
                ),
                source: None,
            });
        }

        // The server ignored the Range header and is sending the full body:
        // the local prefix is untrusted, so reset the file before streaming.
        if resuming && status == StatusCode::OK {
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            download_action = download_action_label(false);
        }
        // A 206 must continue exactly where our Range header started. A
        // missing or mismatched Content-Range offset means the body does
        // not continue the local prefix, so reset the file before streaming.
        if resuming
            && status == StatusCode::PARTIAL_CONTENT
            && partial_content_offset(response.headers(), current_file_size) != Some(true)
        {
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            download_action = download_action_label(false);
        }
        let base_size = file.metadata()?.len();

        // The bar's total: the server-announced length (the bytes this
        // response will send, added to the local prefix) when present,
        // else the metadata size, else the current size (an unknown-size
        // file has no usable total, matching the previous behaviour).
        let total = response
            .content_length()
            .map(|remaining| base_size + remaining)
            .or(expected_size)
            .unwrap_or(base_size);
        let pb = create_progress_bar(total, &download_action, Some("green/green"), true);
        // Set initial progress to current file size for resumed downloads
        pb.set_position(base_size);

        let start_time = Instant::now();
        match stream_response_body(&mut response, file, base_size, &pb, running).await {
            Ok(downloaded_bytes) => {
                let total_bytes = base_size + downloaded_bytes;

                // A 2xx body with zero bytes is a server malfunction only
                // when the metadata expects data: a zero-byte file with no
                // <size> is indistinguishable from a dropped body, so the
                // unknown-size case is trusted (an MD5, when present, still
                // verifies the result).
                if downloaded_bytes == 0
                    && base_size == 0
                    && expected_size.is_some_and(|expected| expected > 0)
                {
                    retry_open_file(
                        file,
                        &pb,
                        &mut retry,
                        "Empty response",
                        "server returned no data",
                        retry_after,
                        running,
                    )
                    .await?;
                    continue;
                }

                // The body ended before the announced size: the transfer was
                // truncated, so resume from where we stopped.
                if let Some(expected) = expected_size
                    && total_bytes < expected
                {
                    retry_open_file(
                        file,
                        &pb,
                        &mut retry,
                        "Incomplete body",
                        &format!("received {total_bytes} of {expected} bytes"),
                        None,
                        running,
                    )
                    .await?;
                    continue;
                }

                pb.finish_and_clear();
                print_downloaded_line(branch_glyph(), downloaded_bytes, Some(start_time.elapsed()));

                return Ok(last_modified);
            }
            Err(e) => {
                // A user interruption aborts the run, other errors retry
                if matches!(e, IaGetError::Interrupted) {
                    pb.finish_and_clear();
                    return Err(e);
                }

                // Disk full is not transient: retrying wastes minutes before
                // the same ENOSPC / ERROR_DISK_FULL recurs.
                if is_disk_full_error(&e) {
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
                    running,
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
            last_glyph(),
            "Size".white(),
            "✘".red().bold(),
            format_size(actual_size).red(),
            format_size(expected_size).dimmed()
        );
        return Ok(false);
    }

    let Some(expected_md5) = expected_md5 else {
        // No hash to check against, consider it verified
        print_verified_hash(None);
        return Ok(true);
    };

    let local_md5 = calculate_md5(file_path, running)?;
    if local_md5.eq_ignore_ascii_case(expected_md5) {
        print_verified_hash(Some(&local_md5));
        Ok(true)
    } else {
        println!(
            "{} {}         {} ({}) Expected ({})",
            last_glyph(),
            "Hash".white(),
            "✘".red().bold(),
            local_md5.red(),
            expected_md5.dimmed()
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
                branch_glyph(),
                "Partial".white(),
                "▲".yellow().bold()
            );
            if let Err(e) = fs::remove_file(file_path).map_err(|e| io_error_with_path(file_path, e))
            {
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
        // An unreadable file must neither be deleted nor re-downloaded:
        // the failure is reported for this file only.
        ExistingFileStatus::Unreadable(reason) => Ok(ExistingFileHandling::Blocked(reason)),
        ExistingFileStatus::Missing => Ok(ExistingFileHandling::Download),
    }
}

/// Installs a verified `.part` file under its final name, then sets its
/// mtime: the server's Last-Modified header wins, the `_files.xml` mtime is
/// the fallback, and the time is left untouched when both are absent.
///
/// `fs::rename` atomically replaces an existing destination on all
/// supported platforms, so no separate remove step (and the crash window
/// it opens) is needed. A symlink planted at the final name is refused,
/// mirroring the `.part` guard: the verified `.part` stays in place for
/// the next run.
fn install_downloaded_file(
    file_path: &str,
    part_path: &str,
    server_mtime: Option<SystemTime>,
    xml_mtime: Option<u64>,
) -> Result<()> {
    ensure_not_symlink(Path::new(file_path))?;
    fs::rename(part_path, file_path).map_err(|e| io_error_with_path(file_path, e))?;
    if let Some(target) = server_mtime.or(mtime_from_xml(xml_mtime)) {
        sync_file_mtime(file_path, target);
    }
    Ok(())
}

/// Processes one file of the batch: verifies an existing copy or downloads
/// (with verification) and renames the `.part` file to its final name.
///
/// Prints the file banner and status lines. Per-file problems (a stale file
/// that cannot be removed, a failed download, a `.part` that cannot be set
/// up or installed) yield `FileOutcome::Failed`; only a user interruption
/// propagates as a hard error.
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
            // A rename that fails (a locked destination) is a per-file
            // failure; the verified .part stays in place for the next run.
            if let Err(e) =
                install_downloaded_file(file_path, &part_path, server_mtime, expected_mtime)
            {
                return Ok(FileOutcome::Failed(format!("could not install file: {e}")));
            }
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
/// scratch after a failed verification or a rejected resume offset (a `.part`
/// that already holds the whole file is verified in place instead).
async fn run_download_attempts(
    client: &Client,
    url: &str,
    part_path: &str,
    running: &Arc<AtomicBool>,
    cookie_header: Option<&HeaderValue>,
    expected_md5: Option<&str>,
    expected_size: Option<u64>,
) -> Result<DownloadOutcome> {
    // Setup I/O problems (a locked or overly long path, a missing directory
    // that cannot be created) are per-file failures, not batch aborts.
    if let Err(e) = ensure_parent_directories(part_path) {
        return Ok(DownloadOutcome::Failed {
            reason: format!("could not create parent directories: {e}"),
            discard_part: false,
        });
    }
    let mut file = match prepare_file_for_download(part_path) {
        Ok(file) => file,
        Err(e) => {
            return Ok(DownloadOutcome::Failed {
                reason: format!("could not prepare .part file: {e}"),
                discard_part: false,
            });
        }
    };

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
            // range reject no longer applies to it. The stale handle is
            // closed when the assignment below shadows it.
            discard_part = false;
            remove_part_file(part_path);
            let new_file = prepare_file_for_download(part_path);
            file = match new_file {
                Ok(file) => file,
                Err(e) => {
                    break DownloadOutcome::Failed {
                        reason: format!("could not prepare .part file: {e}"),
                        discard_part: false,
                    };
                }
            };
            println!(
                "{} {}        {} Re-downloading from scratch (attempt {}/{})",
                branch_glyph(),
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
                match verify_downloaded_file(part_path, expected_md5, expected_size, running) {
                    Ok(true) => break DownloadOutcome::Verified { server_mtime },
                    Ok(false) => {
                        last_reason =
                            format!("file failed verification after {attempt} attempt(s)");
                    }
                    Err(e) if matches!(e, IaGetError::Interrupted) => return Err(e),
                    // An I/O error while reading the file back (e.g. a momentary
                    // AV lock) fails this file only; the complete .part is kept
                    // so the next run can verify it in place.
                    Err(e) => {
                        break DownloadOutcome::Failed {
                            reason: format!("could not verify downloaded file: {e}"),
                            discard_part: false,
                        };
                    }
                }
            }
            Err(e) if matches!(e, IaGetError::Interrupted) => return Err(e),
            // The server rejected our offset: the .part file is not a valid
            // prefix, so it must not be kept if the attempts end here. One
            // exception: a 416 for `bytes=N-` also fires when the part
            // already holds the whole file (a previous run was interrupted
            // after the body finished but before verification) — a part of
            // exactly the expected size that also hashes correctly is
            // verified in place instead of being discarded and re-downloaded.
            Err(IaGetError::RangeNotSatisfiable) => {
                let complete_part = expected_size.is_some_and(|expected| {
                    file.metadata().is_ok_and(|meta| meta.len() == expected)
                });
                if complete_part && expected_md5.is_some() {
                    println!(
                        "{} {}       {} the .part file is already complete, verifying it in place",
                        branch_glyph(),
                        "Resume".white(),
                        "↻".green().bold()
                    );
                    match verify_downloaded_file(part_path, expected_md5, expected_size, running) {
                        Ok(true) => break DownloadOutcome::Verified { server_mtime: None },
                        Ok(false) => {}
                        Err(e) if matches!(e, IaGetError::Interrupted) => return Err(e),
                        Err(e) => {
                            break DownloadOutcome::Failed {
                                reason: format!("could not verify complete .part file: {e}"),
                                discard_part: false,
                            };
                        }
                    }
                }
                last_reason = IaGetError::RangeNotSatisfiable.to_string();
                discard_part = true;
            }
            // Any other failure is final; the partial .part file is still a
            // resumable prefix
            Err(e) => {
                break DownloadOutcome::Failed {
                    reason: e.to_string(),
                    discard_part,
                };
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
    let tasks: Vec<DownloadTask> = files.into_iter().collect();
    let mut failed_files: Vec<(String, String)> = Vec::new();

    for (index, task) in tasks.iter().enumerate() {
        // Check if we should stop due to signal
        if !running.load(Ordering::SeqCst) {
            println!(
                "\n{} Download interrupted. Run the command again to resume remaining files.",
                "✘".red().bold()
            );
            // An interrupted batch is not a successful one: fail with a
            // non-zero exit exactly like an interrupt mid-file does.
            return Err(IaGetError::Interrupted);
        }

        let outcome = process_file(
            client,
            task,
            index + file_number_start,
            total_files,
            running,
            cookie_header,
        )
        .await?;

        if let FileOutcome::Failed(reason) = outcome {
            failed_files.push((task.file_path.clone(), reason));
            if stop_on_error {
                return Err(batch_failed(&failed_files, tasks.len()));
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
        return Err(batch_failed(&failed_files, tasks.len()));
    }

    Ok(())
}

/// Builds the terminal `IaGetError::BatchFailed` from the accumulated failures.
///
/// `total` is the number of files in this batch; files handled before the
/// batch (e.g. the locally saved `_files.xml`) are not counted.
fn batch_failed(failed_files: &[(String, String)], total: usize) -> IaGetError {
    IaGetError::BatchFailed {
        count: failed_files.len(),
        total,
        details: failed_files
            .iter()
            .map(|(path, reason)| format!("{path}: {reason}"))
            .collect::<Vec<_>>()
            .join("; "),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockBody, MockResponse, MockServer, mtime_of, temp_dir_for};
    use std::collections::{HashMap, VecDeque};
    use std::path::{Path, PathBuf};
    use std::time::Instant;

    /// Near-instant retry delay so tests do not wait for real backoff
    fn fast_retry(_attempt: u32) -> Duration {
        Duration::from_millis(1)
    }

    fn test_running() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(true))
    }

    /// The lowercase hex MD5 of `content`, in the form archive.org reports
    fn md5_hex(content: &[u8]) -> String {
        md5::Md5::digest(content)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
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

    /// A 200 response with an empty body — the fallback for most scripts
    fn ok_empty() -> MockResponse {
        MockResponse::new(200, MockBody::Full(vec![]))
    }

    /// Mock server serving "/file.bin" from `responses`, plus a fresh temp
    /// dir; the file under test is `dir.join("file.bin")`
    fn file_server(
        name: &str,
        responses: VecDeque<MockResponse>,
        fallback: MockResponse,
    ) -> (MockServer, PathBuf) {
        let mut scripts = HashMap::new();
        scripts.insert("/file.bin".to_string(), responses);
        let server = MockServer::start(scripts, fallback);
        (server, temp_dir_for(name))
    }

    /// A single-file batch task for "/file.bin" in `dir`
    fn file_task(
        server: &MockServer,
        dir: &Path,
        md5: Option<String>,
        size: Option<u64>,
        mtime: Option<u64>,
    ) -> DownloadTask {
        task(
            server.url("/file.bin"),
            dir.join("file.bin").to_str().unwrap().to_string(),
            md5,
            size,
            mtime,
        )
    }

    /// Runs a batch with a fresh client and a live "running" flag, mirroring
    /// the production call minus the Ctrl+C handler
    async fn run_batch(files: Vec<DownloadTask>, stop_on_error: bool) -> Result<()> {
        let total_files = files.len();
        download_files_with_signal(
            &Client::new(),
            files,
            total_files,
            1,
            None,
            stop_on_error,
            &test_running(),
        )
        .await
    }

    /// Two-file batch fixture: "/missing.bin" always 404s, "/ok.bin" serves
    /// `ok_content`; returns the server, temp dir, task list and content
    fn missing_and_ok_batch(name: &str) -> (MockServer, PathBuf, Vec<DownloadTask>, &'static [u8]) {
        let ok_content: &'static [u8] = b"ok-content-123";
        let ok_md5 = md5_hex(ok_content);

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

        let dir = temp_dir_for(name);
        let files = vec![
            task(
                server.url("/missing.bin"),
                dir.join("missing.bin").to_str().unwrap().to_string(),
                None,
                Some(5),
                None,
            ),
            task(
                server.url("/ok.bin"),
                dir.join("ok.bin").to_str().unwrap().to_string(),
                Some(ok_md5),
                Some(ok_content.len() as u64),
                None,
            ),
        ];
        (server, dir, files, ok_content)
    }

    /// Runs a single download against the mock server and returns the
    /// captured `Last-Modified` time.
    async fn run_download(
        url: &str,
        part_path: &str,
        expected_size: Option<u64>,
        running: &Arc<AtomicBool>,
    ) -> Result<Option<SystemTime>> {
        let client = Client::new();
        let mut file = prepare_file_for_download(part_path)?;
        let result = download_file_content(
            &client,
            url,
            &mut file,
            running,
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

        let (server, dir) = file_server(
            "http_500",
            VecDeque::from(vec![
                MockResponse::new(500, MockBody::Full(nginx_error.to_vec())),
                MockResponse::new(500, MockBody::Full(nginx_error.to_vec())),
                MockResponse::new(200, MockBody::Full(content.to_vec())),
            ]),
            ok_empty(),
        );
        let part = dir.join("file.bin.part");
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(content.len() as u64),
            &test_running(),
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
        // Fallback also returns an empty 200 so every retry sees the same
        let (server, dir) = file_server(
            "empty_response",
            VecDeque::from(vec![MockResponse::new(200, MockBody::Full(vec![]))]),
            ok_empty(),
        );
        let part = dir.join("file.bin.part");
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(10),
            &test_running(),
        )
        .await;

        assert!(
            matches!(result, Err(IaGetError::Network { .. })),
            "expected a Network error, got {:?}",
            result.ok()
        );
        assert_eq!(fs::metadata(&part).unwrap().len(), 0);
        assert_eq!(server.request_count(), 1 + MAX_RETRIES as usize);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn empty_response_with_unknown_size_is_accepted() {
        // A zero-byte file whose metadata carries no <size> must not loop on
        // "Empty response": with no size to compare against, the empty 200
        // is the only signal we have, so it is trusted (an MD5, when
        // present, still verifies the result).
        let (server, dir) = file_server(
            "empty_unknown_size",
            VecDeque::from(vec![MockResponse::new(200, MockBody::Full(vec![]))]),
            ok_empty(),
        );
        let part = dir.join("file.bin.part");
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            None,
            &test_running(),
        )
        .await;

        assert!(
            result.is_ok(),
            "an empty body with unknown size must succeed, got {:?}",
            result.err()
        );
        assert_eq!(
            server.request_count(),
            1,
            "a trusted empty body must not be retried"
        );
        assert_eq!(fs::metadata(&part).unwrap().len(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn resume_after_mid_stream_disconnect() {
        let full = b"01234567890123456789"; // 20 bytes

        let (server, dir) = file_server(
            "mid_stream",
            VecDeque::from(vec![
                MockResponse::new(
                    200,
                    MockBody::Truncated {
                        announced_len: full.len() as u64,
                        partial: full[..8].to_vec(),
                    },
                ),
                // Range-aware: the 206 tail and Content-Range are derived from
                // the client's Range request, so the resume offset is verified
                // against behaviour rather than script order.
                MockResponse::ranged(full.to_vec()),
            ]),
            MockResponse::new(206, MockBody::Full(vec![])),
        );
        let part = dir.join("file.bin.part");
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(full.len() as u64),
            &test_running(),
        )
        .await;

        result.expect("download should succeed");
        assert_eq!(fs::read(&part).unwrap(), full);
        assert_eq!(server.ranges(), vec![None, Some(8)]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn resume_sends_correct_offset_to_range_aware_server() {
        // The mock honors the Range header like a real origin: a wrong resume
        // offset would yield a mismatched 206 that resets the file rather than
        // a silently corrupted tail, so the exact offset the client requests
        // is what is verified here.
        let full = b"01234567890123456789"; // 20 bytes
        let prefix = &full[..8]; // a .part that already holds 8 bytes

        let (server, dir) = file_server(
            "range_aware_resume",
            VecDeque::from(vec![MockResponse::ranged(full.to_vec())]),
            ok_empty(),
        );
        let part = dir.join("file.bin.part");
        fs::write(&part, prefix).unwrap();

        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(full.len() as u64),
            &test_running(),
        )
        .await;

        result.expect("resume must succeed against a Range-aware server");
        assert_eq!(
            fs::read(&part).unwrap(),
            full,
            "the resume must append the correct tail, not a wrong offset"
        );
        assert_eq!(
            server.ranges(),
            vec![Some(8)],
            "the resume must request bytes=8-, exactly the .part size"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn partial_content_offset_mismatch_resets_file() {
        // A 206 whose Content-Range does not continue our prefix must not
        // be appended: the file is reset and re-downloaded from scratch.
        let full = b"0123456789"; // 10 bytes

        let (server, dir) = file_server(
            "cr_mismatch",
            VecDeque::from(vec![
                MockResponse::new(206, MockBody::Full(full.to_vec()))
                    .with_header("Content-Range", "bytes 0-9/10"),
            ]),
            ok_empty(),
        );
        let part = dir.join("file.bin.part");
        fs::write(&part, b"XXXX").unwrap(); // 4-byte "partial" file
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(full.len() as u64),
            &test_running(),
        )
        .await;

        result.expect("download should succeed");
        assert_eq!(
            fs::read(&part).unwrap(),
            full,
            "a mismatched 206 body must replace the local prefix, not append to it"
        );
        assert_eq!(server.ranges(), vec![Some(4)]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn partial_content_without_range_header_resets_file() {
        // A 206 without a Content-Range header is malformed (RFC 7233 makes
        // it mandatory): the body is untrusted, so the file is reset.
        let full = b"0123456789"; // 10 bytes

        let (server, dir) = file_server(
            "cr_missing",
            VecDeque::from(vec![MockResponse::new(206, MockBody::Full(full.to_vec()))]),
            ok_empty(),
        );
        let part = dir.join("file.bin.part");
        fs::write(&part, b"XXXX").unwrap(); // 4-byte "partial" file
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(full.len() as u64),
            &test_running(),
        )
        .await;

        result.expect("download should succeed");
        assert_eq!(
            fs::read(&part).unwrap(),
            full,
            "a 206 without Content-Range must replace the local prefix, not append to it"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_content_offset_parses_matching_and_mismatching_ranges() {
        fn headers(range: Option<&str>) -> HeaderMap {
            let mut headers = HeaderMap::new();
            if let Some(range) = range {
                headers.insert(
                    CONTENT_RANGE,
                    HeaderValue::from_str(range).expect("a static test string is a valid header"),
                );
            }
            headers
        }

        assert_eq!(
            partial_content_offset(&headers(Some("bytes 8-19/20")), 8),
            Some(true)
        );
        assert_eq!(
            partial_content_offset(&headers(Some("bytes 0-9/20")), 8),
            Some(false)
        );
        assert_eq!(
            partial_content_offset(&headers(Some("bytes 8-19/*")), 9),
            Some(false)
        );
        assert_eq!(partial_content_offset(&headers(None), 8), None);
        // A suffix-form range has no usable start offset: untrusted.
        assert_eq!(
            partial_content_offset(&headers(Some("bytes */20")), 8),
            None
        );
        // A range without a total is not what a 206 carries: untrusted.
        assert_eq!(
            partial_content_offset(&headers(Some("bytes 8-19")), 8),
            None
        );
    }

    #[tokio::test]
    async fn full_200_response_resets_untrusted_prefix() {
        let full = b"0123456789"; // 10 bytes

        let (server, dir) = file_server(
            "range_ignored",
            VecDeque::from(vec![MockResponse::new(200, MockBody::Full(full.to_vec()))]),
            ok_empty(),
        );
        let part = dir.join("file.bin.part");
        fs::write(&part, b"XXXXXX").unwrap(); // 6-byte "partial" file
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(full.len() as u64),
            &test_running(),
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
        let (server, dir) = file_server(
            "http_416",
            VecDeque::from(vec![MockResponse::new(
                416,
                MockBody::Full(b"Range not satisfiable".to_vec()),
            )]),
            MockResponse::new(416, MockBody::Full(vec![])),
        );
        let part = dir.join("file.bin.part");
        fs::write(&part, b"XXXX").unwrap();
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(100),
            &test_running(),
        )
        .await;

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
        let (server, dir) = file_server(
            "http_404",
            VecDeque::from(vec![MockResponse::new(
                404,
                MockBody::Full(b"not found".to_vec()),
            )]),
            MockResponse::new(404, MockBody::Full(vec![])),
        );
        let part = dir.join("file.bin.part");
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(10),
            &test_running(),
        )
        .await;

        assert!(
            matches!(result, Err(IaGetError::Network { .. })),
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

        let (server, dir) = file_server(
            "retry_after",
            VecDeque::from(vec![
                MockResponse::new(429, MockBody::Full(vec![])).with_header("Retry-After", "1"),
                MockResponse::new(200, MockBody::Full(content.to_vec())),
            ]),
            MockResponse::new(429, MockBody::Full(vec![])),
        );
        let part = dir.join("file.bin.part");
        let start = Instant::now();
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(content.len() as u64),
            &test_running(),
        )
        .await;

        assert!(result.is_ok(), "expected success, got {:?}", result.err());
        // A loaded CI host may deliver the 1s sleep late, but it cannot
        // deliver it *early*: the threshold only needs to rule out skipping
        // the wait.
        assert!(
            start.elapsed() >= Duration::from_millis(500),
            "Retry-After: 1 was not honored (elapsed {:?})",
            start.elapsed()
        );
        assert_eq!(fs::read(&part).unwrap(), content);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn batch_continues_after_file_failure() {
        let (_server, dir, files, ok_content) = missing_and_ok_batch("batch_continue");
        let err = run_batch(files, false).await.unwrap_err();
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
        let (server, dir, files, _ok_content) = missing_and_ok_batch("stop_on_error");
        let err = run_batch(files, true).await.unwrap_err();
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
    async fn batch_failed_total_counts_only_batch_files() {
        let (_server, dir, files, _ok_content) = missing_and_ok_batch("batch_failed_total");
        // The archive-wide total includes a file handled before the batch
        // (the saved _files.xml); the error must not count it.
        let batch_len = files.len();
        let err = download_files_with_signal(
            &Client::new(),
            files,
            batch_len + 1,
            1,
            None,
            false,
            &test_running(),
        )
        .await
        .unwrap_err();
        match err {
            IaGetError::BatchFailed { count, total, .. } => {
                assert_eq!(
                    (count, total),
                    (1, 2),
                    "total must be the batch size, not the archive-wide total"
                );
            }
            other => panic!("expected BatchFailed, got {:?}", other),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn hash_mismatch_triggers_redownload() {
        let correct = b"correct-content-001";
        let corrupt = b"corrupted-content-001"; // same length so the size guard does not fire
        let md5 = md5_hex(correct);

        let (server, dir) = file_server(
            "hash_mismatch",
            VecDeque::from(vec![
                MockResponse::new(200, MockBody::Full(corrupt.to_vec())),
                MockResponse::new(200, MockBody::Full(correct.to_vec())),
            ]),
            ok_empty(),
        );
        let files = vec![file_task(
            &server,
            &dir,
            Some(md5),
            Some(correct.len() as u64),
            None,
        )];

        run_batch(files, false)
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

    #[tokio::test]
    async fn md5_case_is_insensitive_on_download() {
        // archive.org reports lowercase MD5, but a metadata value in upper or
        // mixed case must still verify a correct file instead of failing it
        // into three re-downloads.
        let content = b"case-insensitive-md5";
        let md5_upper = md5_hex(content).to_ascii_uppercase();

        let (server, dir) = file_server(
            "md5_case_download",
            VecDeque::from(vec![MockResponse::new(
                200,
                MockBody::Full(content.to_vec()),
            )]),
            ok_empty(),
        );
        let files = vec![file_task(
            &server,
            &dir,
            Some(md5_upper),
            Some(content.len() as u64),
            None,
        )];

        run_batch(files, false)
            .await
            .expect("an uppercase MD5 must verify a correct file");

        assert_eq!(
            server.request_count(),
            1,
            "a correct file must not be re-downloaded"
        );
        assert_eq!(fs::read(dir.join("file.bin")).unwrap(), content);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn existing_file_md5_case_is_insensitive() {
        // A file already on disk verifies in place when the metadata MD5 is
        // uppercase: it must not be re-downloaded.
        let content = b"already-on-disk-md5";
        let md5_upper = md5_hex(content).to_ascii_uppercase();

        let (server, dir) = file_server("md5_case_existing", VecDeque::new(), ok_empty());
        fs::write(dir.join("file.bin"), content).unwrap();
        let files = vec![file_task(
            &server,
            &dir,
            Some(md5_upper),
            Some(content.len() as u64),
            None,
        )];

        run_batch(files, false)
            .await
            .expect("an uppercase MD5 must verify an existing file in place");

        assert_eq!(
            server.request_count(),
            0,
            "a verified existing file must not be re-downloaded"
        );
        assert_eq!(fs::read(dir.join("file.bin")).unwrap(), content);
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
            VecDeque::from(vec![
                MockResponse::new(200, MockBody::Full(content.to_vec()))
                    .with_header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
            ]),
        );
        let server = MockServer::start(scripts, MockResponse::new(200, MockBody::Full(vec![])));

        let dir = temp_dir_for("last_modified");
        let part = dir.join("file.bin.part");
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(content.len() as u64),
            &test_running(),
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
        let md5 = md5_hex(content);
        let xml_mtime = 1_545_586_142;

        let (server, dir) = file_server(
            "xml_mtime",
            VecDeque::from(vec![MockResponse::new(
                200,
                MockBody::Full(content.to_vec()),
            )]),
            ok_empty(),
        );
        let files = vec![file_task(
            &server,
            &dir,
            Some(md5),
            Some(content.len() as u64),
            Some(xml_mtime),
        )];

        run_batch(files, false).await.expect("batch should succeed");

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
        let md5 = md5_hex(content);
        let xml_mtime = 1_545_586_142;
        let header_mtime =
            httpdate::parse_http_date("Wed, 21 Oct 2015 07:28:00 GMT").expect("valid date");
        let expected = header_mtime
            .duration_since(UNIX_EPOCH)
            .expect("valid date")
            .as_secs();

        let (server, dir) = file_server(
            "server_mtime",
            VecDeque::from(vec![
                MockResponse::new(200, MockBody::Full(content.to_vec()))
                    .with_header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
            ]),
            ok_empty(),
        );
        let files = vec![file_task(
            &server,
            &dir,
            Some(md5),
            Some(content.len() as u64),
            Some(xml_mtime),
        )];

        run_batch(files, false).await.expect("batch should succeed");

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

        let (server, dir) = file_server("verified_mtime", VecDeque::new(), ok_empty());
        fs::write(dir.join("file.bin"), content).expect("failed to write test file");
        let files = vec![file_task(
            &server,
            &dir,
            None,
            Some(content.len() as u64),
            Some(xml_mtime),
        )];

        run_batch(files, false).await.expect("batch should succeed");

        assert_eq!(
            server.request_count(),
            0,
            "verified file must not be re-downloaded"
        );
        assert_eq!(
            mtime_of(&dir.join("file.bin")),
            Some(xml_mtime),
            "already-verified file must still get the _files.xml mtime"
        );
        assert_eq!(fs::read(dir.join("file.bin")).unwrap(), content);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn file_mtime_untouched_without_mtime_sources() {
        let content = b"no-mtime-content";
        let md5 = md5_hex(content);

        let (server, dir) = file_server(
            "no_mtime",
            VecDeque::from(vec![MockResponse::new(
                200,
                MockBody::Full(content.to_vec()),
            )]),
            ok_empty(),
        );
        let files = vec![file_task(
            &server,
            &dir,
            Some(md5),
            Some(content.len() as u64),
            None,
        )];

        run_batch(files, false).await.expect("batch should succeed");

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

        let (server, dir) = file_server(
            "size_mismatch_skip",
            VecDeque::from(vec![MockResponse::new(
                200,
                MockBody::Full(content.to_vec()),
            )]),
            ok_empty(),
        );
        // A stale copy with the wrong size and no MD5 in the metadata: the
        // skip path must re-download instead of accepting it.
        fs::write(dir.join("file.bin"), b"stale").unwrap();
        let files = vec![file_task(
            &server,
            &dir,
            None,
            Some(content.len() as u64),
            None,
        )];

        run_batch(files, false)
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
        let ok_md5 = md5_hex(ok_content);

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
        // A directory at the final path cannot be read on any platform, so
        // the check lands in the Unreadable branch: the file is kept and
        // the problem is reported for this file only.
        let blocked = dir.join("blocked.bin");
        fs::create_dir(&blocked).unwrap();
        let files = vec![
            task(
                server.url("/blocked.bin"),
                blocked.to_str().unwrap().to_string(),
                Some("00000000000000000000000000000000".to_string()),
                None,
                None,
            ),
            task(
                server.url("/ok.bin"),
                dir.join("ok.bin").to_str().unwrap().to_string(),
                Some(ok_md5),
                Some(ok_content.len() as u64),
                None,
            ),
        ];

        let err = run_batch(files, false).await.unwrap_err();
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

    #[test]
    fn size_mismatch_short_circuits_before_hashing() {
        // A copy whose size does not match the metadata must be rejected
        // as Invalid without spending a full-file hash.
        let dir = temp_dir_for("size_short_circuit");
        let path = dir.join("file.bin");
        fs::write(&path, b"short").unwrap(); // 5 bytes

        let expected = md5_hex(b"totally different content");
        let status = check_existing_file(
            path.to_str().unwrap(),
            Some(expected.as_str()),
            Some(100),
            &test_running(),
        )
        .unwrap();

        assert!(
            matches!(status, ExistingFileStatus::Invalid),
            "got {status:?}"
        );
        assert!(
            path.exists(),
            "the stale copy is removed by the caller, not by the check"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn unreadable_existing_file_is_reported_not_redownloaded() {
        // A directory at the file path must yield Unreadable (per-file
        // failure, the entry is kept), not Invalid, which would delete it
        // and re-download based on a read failure.
        let dir = temp_dir_for("unreadable_existing");
        let path = dir.join("dirfile.bin");
        fs::create_dir(&path).unwrap();

        let expected = md5_hex(b"x");
        let status = check_existing_file(
            path.to_str().unwrap(),
            Some(expected.as_str()),
            None,
            &test_running(),
        )
        .unwrap();

        match status {
            ExistingFileStatus::Unreadable(reason) => {
                assert!(!reason.is_empty(), "the reason must name the problem");
            }
            other => panic!("expected Unreadable, got {other:?}"),
        }
        assert!(path.exists(), "an unreadable file must not be deleted");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sync_file_mtime_skips_pre_epoch_times() {
        // A Last-Modified before 1970 has no Unix representation: the mtime
        // must be left untouched instead of being stamped to the epoch.
        let dir = temp_dir_for("mtime_pre_epoch");
        let path = dir.join("f.txt");
        fs::write(&path, "x").unwrap();

        let pre_epoch = UNIX_EPOCH - Duration::from_secs(1);
        assert!(!sync_file_mtime(&path, pre_epoch));

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let mtime = fs::metadata(&path).unwrap().modified().unwrap();
        assert!(
            mtime.duration_since(UNIX_EPOCH).unwrap().abs_diff(now) < Duration::from_secs(60),
            "the mtime must stay at the write time"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn uncreatable_part_file_fails_only_that_file() {
        // A .part path occupied by a directory cannot be opened on any
        // platform; the setup failure must fail that file only, letting the
        // rest of the batch proceed.
        let ok_content = b"ok-content-123";
        let ok_md5 = md5_hex(ok_content);

        let mut scripts = HashMap::new();
        scripts.insert(
            "/ok.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(
                200,
                MockBody::Full(ok_content.to_vec()),
            )]),
        );
        let server = MockServer::start(scripts, MockResponse::new(404, MockBody::Full(vec![])));

        let dir = temp_dir_for("uncreatable_part");
        fs::create_dir(dir.join("blocked.bin.part")).unwrap();
        let files = vec![
            task(
                server.url("/blocked.bin"),
                dir.join("blocked.bin").to_str().unwrap().to_string(),
                None,
                None,
                None,
            ),
            task(
                server.url("/ok.bin"),
                dir.join("ok.bin").to_str().unwrap().to_string(),
                Some(ok_md5),
                Some(ok_content.len() as u64),
                None,
            ),
        ];

        let err = run_batch(files, false).await.unwrap_err();
        match err {
            IaGetError::BatchFailed {
                count,
                total,
                details,
            } => {
                assert_eq!((count, total), (1, 2));
                assert!(
                    details.contains("could not prepare .part file"),
                    "{details}"
                );
            }
            other => panic!("expected BatchFailed with one file, got {:?}", other),
        }
        assert_eq!(fs::read(dir.join("ok.bin")).unwrap(), ok_content);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn complete_part_416_is_verified_in_place() {
        // A .part that already holds the whole file (a previous run was
        // interrupted after the body finished but before verification) must
        // be verified in place, not discarded and re-downloaded.
        let full = b"complete-part-content";
        let md5 = md5_hex(full);

        let (server, dir) = file_server(
            "complete_part_416",
            VecDeque::from(vec![MockResponse::new(416, MockBody::Full(vec![]))]),
            MockResponse::new(200, MockBody::Full(full.to_vec())),
        );
        let part = dir.join("file.bin.part");
        fs::write(&part, full).unwrap();
        let outcome = run_download_attempts(
            &Client::new(),
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            &test_running(),
            None,
            Some(&md5),
            Some(full.len() as u64),
        )
        .await
        .expect("a complete .part must verify in place");

        assert!(matches!(outcome, DownloadOutcome::Verified { .. }));
        assert_eq!(
            server.request_count(),
            1,
            "a complete .part must not trigger a re-download"
        );
        assert_eq!(server.ranges(), vec![Some(full.len() as u64)]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn complete_part_416_with_wrong_hash_is_redownloaded() {
        // A complete .part whose content does not match the expected hash is
        // still discarded and re-downloaded: size alone is not proof.
        let stale = b"stale-content-0001";
        let fresh = b"fresh-content-0001"; // same length, different content
        let md5 = md5_hex(fresh);

        let (server, dir) = file_server(
            "complete_part_416_stale",
            VecDeque::from(vec![MockResponse::new(416, MockBody::Full(vec![]))]),
            MockResponse::new(200, MockBody::Full(fresh.to_vec())),
        );
        let part = dir.join("file.bin.part");
        fs::write(&part, stale).unwrap();
        let outcome = run_download_attempts(
            &Client::new(),
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            &test_running(),
            None,
            Some(&md5),
            Some(fresh.len() as u64),
        )
        .await
        .expect("the re-download must succeed");

        assert!(matches!(outcome, DownloadOutcome::Verified { .. }));
        assert_eq!(
            server.request_count(),
            2,
            "a complete .part with a wrong hash must be re-downloaded"
        );
        assert_eq!(server.ranges(), vec![Some(stale.len() as u64), None]);
        assert_eq!(fs::read(&part).unwrap(), fresh);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn interrupt_between_files_is_an_error() {
        // An interrupt detected between files must not exit as a success.
        let (_server, dir, files, _) = missing_and_ok_batch("interrupt_between_files");
        let stopped = Arc::new(AtomicBool::new(false));
        let total = files.len();
        let err =
            download_files_with_signal(&Client::new(), files, total, 1, None, false, &stopped)
                .await
                .unwrap_err();
        assert!(
            matches!(err, IaGetError::Interrupted),
            "an interrupted batch must fail, got {err:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn second_ctrl_c_press_requests_immediate_quit() {
        let running = Arc::new(AtomicBool::new(true));
        let presses = Arc::new(AtomicU32::new(0));

        assert!(
            matches!(handle_ctrl_c(&running, &presses), CtrlCAction::GracefulStop),
            "the first press must stop the batch gracefully"
        );
        assert!(
            !running.load(Ordering::SeqCst),
            "the first press must flag the batch to stop"
        );

        assert!(
            matches!(handle_ctrl_c(&running, &presses), CtrlCAction::QuitNow),
            "the second press must ask the handler to exit the process"
        );
    }

    #[tokio::test]
    async fn interrupt_during_backoff_stops_retries() {
        // The server throttles forever; a Ctrl+C during the backoff wait must
        // end the retry loop instead of sending another request after it.
        let (server, dir) = file_server(
            "interrupt_backoff",
            VecDeque::from(vec![MockResponse::new(429, MockBody::Full(vec![]))]),
            MockResponse::new(429, MockBody::Full(vec![])),
        );
        let running = Arc::new(AtomicBool::new(true));
        let flag = running.clone();
        // Simulate the Ctrl+C right after the first 429 has been served (a
        // fixed timer would race the retry loop on fast machines): the
        // flag is then guaranteed to be observed inside a retry wait.
        let watcher = server.clone();
        tokio::spawn(async move {
            while watcher.request_count() < 1 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            flag.store(false, Ordering::SeqCst);
        });

        let part = dir.join("file.bin.part");
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(10),
            &running,
        )
        .await;

        assert!(
            matches!(result, Err(IaGetError::Interrupted)),
            "an interrupt during the backoff wait must abort the retries, got {result:?}"
        );
        assert!(
            (1..=3).contains(&server.request_count()),
            "at most one extra retry may follow the interrupt, got {}",
            server.request_count()
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn interrupt_during_stalled_body_stops_waiting_for_chunks() {
        // A connection that never delivers its body must not force a
        // Ctrl+C to wait out the read timeout: the stop flag is raced
        // against the chunk read.
        let (server, dir) = file_server(
            "stalled_body",
            VecDeque::from(vec![MockResponse::stalled(100)]),
            ok_empty(),
        );
        let running = Arc::new(AtomicBool::new(true));
        let flag = running.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            flag.store(false, Ordering::SeqCst);
        });

        let part = dir.join("file.bin.part");
        let start = Instant::now();
        let result = run_download(
            &server.url("/file.bin"),
            part.to_str().unwrap(),
            Some(10),
            &running,
        )
        .await;

        assert!(
            matches!(result, Err(IaGetError::Interrupted)),
            "a Ctrl+C during a stalled body must abort the download, got {:?}",
            result.ok()
        );
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the abort must land within one interrupt check, not the read timeout: {:?}",
            start.elapsed()
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_part_file_is_refused() {
        // A pre-planted symlink at the .part path must not be opened for
        // writing: the streamed bytes would reach the link target instead.
        let dir = temp_dir_for("symlink_part");
        let target = dir.join("target.bin");
        fs::write(&target, "do not touch").unwrap();
        let part = dir.join("file.bin.part");
        std::os::unix::fs::symlink(&target, &part).unwrap();

        let err = prepare_file_for_download(part.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("symlink"), "got: {err}");
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            "do not touch",
            "the link target must be left untouched"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn install_refuses_symlinked_final_file() {
        // A symlink planted at the final name must not shadow the verified
        // .part: the install fails and the .part is kept for the next run.
        let dir = temp_dir_for("symlink_install");
        let target = dir.join("target.bin");
        fs::write(&target, "do not touch").unwrap();
        let final_path = dir.join("file.bin");
        std::os::unix::fs::symlink(&target, &final_path).unwrap();
        let part = dir.join("file.bin.part");
        fs::write(&part, "fresh content").unwrap();

        let err = install_downloaded_file(
            final_path.to_str().unwrap(),
            part.to_str().unwrap(),
            None,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("symlink"), "got: {err}");
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            "do not touch",
            "the link target must be left untouched"
        );
        assert!(
            part.exists(),
            "the verified .part must be kept for the next run"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
