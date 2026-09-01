//! Module for handling file downloads, verification, and related operations.
//!
//! The pipeline is split into focused submodules — [`signal`] for Ctrl+C
//! handling, [`retry`] for backoff and retry tracking, [`verify`] for size
//! and MD5 checks, [`stream`] for streaming a response body, and [`mtime`]
//! for last-modified times — and this module orchestrates a batch of
//! [`DownloadTask`]s.

mod mtime;
mod retry;
mod signal;
mod stream;
mod verify;

use std::fs;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use colored::*;
use reqwest::Client;
use reqwest::header::HeaderValue;

use crate::Result;
use crate::display::{branch_glyph, print_file_banner};
use crate::downloader::mtime::mtime_from_xml;
use crate::downloader::retry::backoff_delay;
use crate::downloader::signal::setup_signal_handler;
use crate::downloader::stream::download_file_content;
use crate::downloader::verify::{
    ExistingFileStatus, check_existing_file, print_verified_hash, verify_downloaded_file,
};
use crate::error::{IaGetError, io_error_with_path};
use crate::utils::ensure_not_symlink;

// Re-export the items that form this module's public API.
pub use mtime::{parse_last_modified, sync_file_mtime};
#[cfg(test)]
pub(crate) use verify::digest_hex;

/// Maximum full re-download attempts for a file that fails size/hash verification
const MAX_DOWNLOAD_ATTEMPTS: u32 = 3;

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

/// Best-effort removal of a leftover `.part` file: its absence is not an
/// error, and a locked file must not fail the processing of the file itself
fn remove_part_file(part_path: &str) {
    let _ = fs::remove_file(part_path);
}

/// Re-creates the `.part` file from scratch after a failed verification:
/// the old contents are removed best-effort, then a fresh handle is opened.
/// The caller's previous handle is closed by the shadowing assignment.
fn reprepare_part_file(part_path: &str) -> Result<File> {
    remove_part_file(part_path);
    prepare_file_for_download(part_path)
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

/// Outcome of processing a single file of the batch
enum FileOutcome {
    /// The file is now valid: already verified, or freshly downloaded
    Succeeded,
    /// The file could not be downloaded; holds the reason for the failure report
    Failed(String),
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
            // range reject no longer applies to it.
            discard_part = false;
            file = match reprepare_part_file(part_path) {
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
pub async fn download_files(
    client: &Client,
    files: Vec<DownloadTask>,
    total_files: usize,
    file_number_start: usize,
    cookie_header: Option<&HeaderValue>,
    stop_on_error: bool,
) -> Result<()> {
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

/// Batch download logic with an externally provided signal flag.
///
/// Split out from `download_files` so tests can drive the batch loop without
/// registering a second (and panicking) Ctrl+C handler.
///
/// `file_number_start` is the 1-based number assigned to the first file of
/// the batch.
async fn download_files_with_signal(
    client: &Client,
    tasks: Vec<DownloadTask>,
    total_files: usize,
    file_number_start: usize,
    cookie_header: Option<&HeaderValue>,
    stop_on_error: bool,
    running: &Arc<AtomicBool>,
) -> Result<()> {
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
    use crate::downloader::retry::MAX_RETRIES;
    use crate::test_support::{
        MockBody, MockResponse, MockServer, TempDir, md5_hex, mtime_of, test_running,
    };
    use std::collections::{HashMap, VecDeque};
    use std::path::Path;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    /// Near-instant retry delay so tests do not wait for real backoff
    fn fast_retry(_attempt: u32) -> Duration {
        Duration::from_millis(1)
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
    ) -> (MockServer, TempDir) {
        let mut scripts = HashMap::new();
        scripts.insert("/file.bin".to_string(), responses);
        let server = MockServer::start(scripts, fallback);
        (server, TempDir::new(name))
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

    /// The good sibling file's content in the per-file failure tests:
    /// one file is blocked, this one must still download.
    const OK_CONTENT: &[u8] = b"ok-content-123";

    /// Mock serving the good sibling file ("/ok.bin" → `OK_CONTENT`, 404
    /// for anything else), plus a fresh temp dir
    fn ok_bin_server(name: &str) -> (MockServer, TempDir) {
        let mut scripts = HashMap::new();
        scripts.insert(
            "/ok.bin".to_string(),
            VecDeque::from(vec![MockResponse::new(
                200,
                MockBody::Full(OK_CONTENT.to_vec()),
            )]),
        );
        let server = MockServer::start(scripts, MockResponse::new(404, MockBody::Full(vec![])));
        (server, TempDir::new(name))
    }

    /// The download task for the "/ok.bin" sibling file in `dir`
    fn ok_bin_task(server: &MockServer, dir: &Path) -> DownloadTask {
        task(
            server.url("/ok.bin"),
            dir.join("ok.bin").to_str().unwrap().to_string(),
            Some(md5_hex(OK_CONTENT)),
            Some(OK_CONTENT.len() as u64),
            None,
        )
    }

    /// Two-file batch fixture: "/missing.bin" always 404s, "/ok.bin" serves
    /// `ok_content`; returns the server, temp dir, task list and content
    fn missing_and_ok_batch(name: &str) -> (MockServer, TempDir, Vec<DownloadTask>, &'static [u8]) {
        let (server, dir) = ok_bin_server(name);
        let missing = dir.join("missing.bin");
        let files = vec![
            task(
                server.url("/missing.bin"),
                missing.to_str().unwrap().to_string(),
                None,
                Some(5),
                None,
            ),
            ok_bin_task(&server, &dir),
        ];
        (server, dir, files, OK_CONTENT)
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
    }

    #[tokio::test]
    async fn batch_failed_total_counts_only_batch_files() {
        let (_server, _dir, files, _ok_content) = missing_and_ok_batch("batch_failed_total");
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

        let dir = TempDir::new("last_modified");
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
    }

    #[tokio::test]
    async fn unremovable_stale_file_fails_only_that_file() {
        let (server, dir) = ok_bin_server("unremovable_stale");
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
            ok_bin_task(&server, &dir),
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
        assert_eq!(fs::read(dir.join("ok.bin")).unwrap(), OK_CONTENT);
        assert!(
            blocked.exists(),
            "unremovable stale file must stay in place"
        );
    }

    #[tokio::test]
    async fn uncreatable_part_file_fails_only_that_file() {
        // A .part path occupied by a directory cannot be opened on any
        // platform; the setup failure must fail that file only, letting the
        // rest of the batch proceed.
        let (server, dir) = ok_bin_server("uncreatable_part");
        fs::create_dir(dir.join("blocked.bin.part")).unwrap();
        let files = vec![
            task(
                server.url("/blocked.bin"),
                dir.join("blocked.bin").to_str().unwrap().to_string(),
                None,
                None,
                None,
            ),
            ok_bin_task(&server, &dir),
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
        assert_eq!(fs::read(dir.join("ok.bin")).unwrap(), OK_CONTENT);
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
    }

    #[tokio::test]
    async fn interrupt_between_files_is_an_error() {
        // An interrupt detected between files must not exit as a success.
        let (_server, _dir, files, _) = missing_and_ok_batch("interrupt_between_files");
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
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_part_file_is_refused() {
        // A pre-planted symlink at the .part path must not be opened for
        // writing: the streamed bytes would reach the link target instead.
        let dir = TempDir::new("symlink_part");
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
    }

    #[cfg(unix)]
    #[test]
    fn install_refuses_symlinked_final_file() {
        // A symlink planted at the final name must not shadow the verified
        // .part: the install fails and the .part is kept for the next run.
        let dir = TempDir::new("symlink_install");
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
    }
}
