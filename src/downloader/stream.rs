//! Streaming an HTTP response body into a local file, with resume and
//! retry, plus the helpers that decide how a response's body is to be used.

use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use colored::*;
use indicatif::ProgressBar;
use reqwest::header::{CONTENT_RANGE, HeaderMap, HeaderValue, RANGE, RETRY_AFTER};
use reqwest::{Client, RequestBuilder, Response, StatusCode};

use crate::Result;
use crate::cookie::with_cookie;
use crate::display::{branch_glyph, create_progress_bar, last_glyph, print_downloaded_line};
use crate::downloader::mtime::parse_last_modified;
use crate::downloader::rate::RateLimiter;
use crate::downloader::retry::{INTERRUPT_CHECK_INTERVAL, RetryTracker, parse_retry_after};
use crate::error::IaGetError;
use crate::verbose;

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
    rate: &mut RateLimiter,
) -> Result<u64> {
    let mut downloaded_bytes: u64 = 0;

    loop {
        tokio::select! {
            chunk = response.chunk() => match chunk {
                Ok(Some(chunk)) => {
                    file.write_all(&chunk)?;
                    let chunk_len = chunk.len() as u64;
                    downloaded_bytes += chunk_len;
                    pb.set_position(base_size + downloaded_bytes);
                    // Pace the transfer to the configured rate, if any, so a
                    // limited download does not saturate the connection.
                    rate.pace(chunk_len).await;
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

/// Builds the next attempt's request: a plain GET for a fresh file, or a
/// `Range: bytes=<size>-` resume request when the local prefix is non-empty.
fn resume_request(
    client: &Client,
    url: &str,
    cookie_header: Option<&HeaderValue>,
    current_file_size: u64,
) -> Result<RequestBuilder> {
    let mut request = with_cookie(client.get(url), cookie_header);
    if current_file_size > 0 {
        request = request.header(
            RANGE,
            HeaderValue::from_str(&format!("bytes={current_file_size}-")).map_err(|e| {
                IaGetError::Network {
                    detail: format!("Invalid range header value: {e}"),
                    source: Some(Box::new(e)),
                }
            })?,
        );
    }
    Ok(request)
}

/// Handles a non-success response.
///
/// A 416 on a resume is fatal: the local prefix is not a valid one, so the
/// caller must re-download from scratch. A retryable status is recorded for
/// another attempt. Any other status is a fatal network error.
async fn handle_failed_status(
    status: StatusCode,
    resuming: bool,
    retry_after: Option<u64>,
    retry: &mut RetryTracker,
    running: &Arc<AtomicBool>,
) -> Result<()> {
    if resuming && status == StatusCode::RANGE_NOT_SATISFIABLE {
        return Err(IaGetError::RangeNotSatisfiable);
    }

    if !is_retryable_status(status) {
        return Err(IaGetError::Network {
            detail: format!(
                "Server responded with HTTP {} {}",
                status,
                status.canonical_reason().unwrap_or("unknown status")
            ),
            source: None,
        });
    }

    let reason = status
        .canonical_reason()
        .unwrap_or("unknown status")
        .to_string();
    retry
        .record(&format!("HTTP {status}"), &reason, retry_after, running)
        .await
}

/// Whether the local prefix may be appended to the server's successful
/// body: only a 206 whose `Content-Range` starts exactly where the file
/// ends. A full-body 200 replaces the file; any other status (e.g. 204)
/// carries no resumable body and the prefix is left in place.
fn prefix_trusted(status: StatusCode, headers: &HeaderMap, local_size: u64) -> bool {
    match status {
        StatusCode::OK => false,
        StatusCode::PARTIAL_CONTENT => partial_content_offset(headers, local_size) == Some(true),
        _ => true,
    }
}

/// The progress bar's total: the server-announced length (the bytes this
/// response will send, added to the local prefix) when present, else the
/// metadata size, else the current size (an unknown-size file has no usable
/// total, matching the previous behaviour).
fn progress_total(response: &Response, base_size: u64, expected_size: Option<u64>) -> u64 {
    response
        .content_length()
        .map(|remaining| base_size + remaining)
        .or(expected_size)
        .unwrap_or(base_size)
}

/// How to treat a streamed successful body
enum StreamedBody {
    /// The body is complete; the download finished
    Complete,
    /// The server returned no data at all; retry
    Empty,
    /// The body ended before the metadata size; resume where it stopped
    Incomplete { received: u64, expected: u64 },
}

/// Decides whether a streamed body is complete.
///
/// A zero-byte body is a server malfunction only when the metadata expects
/// data: a zero-byte file with no `<size>` is indistinguishable from a
/// dropped body, so the unknown-size case is trusted (an MD5, when present,
/// still verifies the result). A body shorter than the metadata size was
/// truncated.
fn assess_streamed_body(
    downloaded_bytes: u64,
    base_size: u64,
    expected_size: Option<u64>,
) -> StreamedBody {
    if downloaded_bytes == 0 && base_size == 0 && expected_size.is_some_and(|expected| expected > 0)
    {
        return StreamedBody::Empty;
    }
    if let Some(expected) = expected_size
        && base_size + downloaded_bytes < expected
    {
        return StreamedBody::Incomplete {
            received: base_size + downloaded_bytes,
            expected,
        };
    }
    StreamedBody::Complete
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
///
/// `rate_limit` (bytes/second, from `--limit-rate`) throttles the streamed
/// body when `Some(_ > 0)`; `None` or `Some(0)` leaves it unlimited.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn download_file_content(
    client: &Client,
    url: &str,
    file: &mut File,
    running: &Arc<AtomicBool>,
    cookie_header: Option<&HeaderValue>,
    expected_size: Option<u64>,
    retry_delay: fn(u32) -> Duration,
    rate_limit: Option<u64>,
) -> Result<Option<SystemTime>> {
    let mut retry = RetryTracker::new(retry_delay);
    let mut rate = RateLimiter::new(rate_limit);

    loop {
        // Re-check file size at start of each attempt (in case of retry)
        let current_file_size = file.metadata()?.len();
        let resuming = current_file_size > 0;
        let mut download_action = download_action_label(resuming);

        verbose::log(&match resuming {
            true => format!("GET {url} (Range: bytes={current_file_size}-)"),
            false => format!("GET {url}"),
        });

        let request = resume_request(client, url, cookie_header, current_file_size)?;
        let mut response = match request.send().await {
            Ok(response) => response,
            Err(e) => {
                verbose::log(&format!("  connection error: {e}"));
                // Request failed before we even got a response
                retry
                    .record("Connection error", &e.to_string(), None, running)
                    .await?;
                continue;
            }
        };

        let status = response.status();
        verbose::log(&format!("  -> HTTP {status}"));
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
            // Ok: the failure was recorded and another attempt follows;
            // Err: a fatal status returned instead.
            handle_failed_status(status, resuming, retry_after, &mut retry, running).await?;
            continue;
        }

        if resuming && !prefix_trusted(status, response.headers(), current_file_size) {
            // The server ignored the Range request and is sending the full
            // body (200), or the 206's Content-Range does not start where
            // the local file ends: in either case the local prefix is
            // untrusted, so the file is reset before the body is streamed.
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            download_action = download_action_label(false);
        }
        let base_size = file.metadata()?.len();

        let pb = create_progress_bar(
            progress_total(&response, base_size, expected_size),
            &download_action,
            "green/green",
            true,
        );
        // Set initial progress to current file size for resumed downloads
        pb.set_position(base_size);

        let start_time = Instant::now();
        match stream_response_body(&mut response, file, base_size, &pb, running, &mut rate).await {
            Ok(downloaded_bytes) => {
                match assess_streamed_body(downloaded_bytes, base_size, expected_size) {
                    StreamedBody::Complete => {
                        pb.finish_and_clear();
                        print_downloaded_line(
                            branch_glyph(),
                            downloaded_bytes,
                            Some(start_time.elapsed()),
                        );
                        return Ok(last_modified);
                    }
                    StreamedBody::Empty => {
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
                    }
                    StreamedBody::Incomplete { received, expected } => {
                        // The body ended before the announced size: the
                        // transfer was truncated, so resume from where it
                        // stopped.
                        retry_open_file(
                            file,
                            &pb,
                            &mut retry,
                            "Incomplete body",
                            &format!("received {received} of {expected} bytes"),
                            None,
                            running,
                        )
                        .await?;
                    }
                }
                continue;
            }
            Err(e) => {
                // A user interruption aborts the run; a disk-full condition
                // cannot be fixed by retrying (the same ENOSPC /
                // ERROR_DISK_FULL would recur).
                if matches!(e, IaGetError::Interrupted) || is_disk_full_error(&e) {
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

/// Returns true for HTTP statuses that are worth retrying (transient server
/// or client throttling problems). Other 4xx/5xx statuses are fatal.
fn is_retryable_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 425 | 429 | 500 | 502 | 503 | 504)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::downloader::retry::MAX_RETRIES;
    use crate::test_support::{
        MockBody, MockResponse, file_server, ok_empty, run_download, test_running,
    };
    use std::collections::VecDeque;
    use std::fs;
    use std::time::{Duration, Instant};

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
    async fn last_modified_header_is_captured() {
        let content = b"0123456789abcdef";

        let (server, dir) = file_server(
            "last_modified",
            VecDeque::from(vec![
                MockResponse::new(200, MockBody::Full(content.to_vec()))
                    .with_header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
            ]),
            MockResponse::new(200, MockBody::Full(vec![])),
        );
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
