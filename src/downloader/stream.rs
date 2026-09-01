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
use reqwest::{Client, Response, StatusCode};

use crate::Result;
use crate::downloader::mtime::parse_last_modified;
use crate::downloader::retry::{INTERRUPT_CHECK_INTERVAL, RetryTracker, parse_retry_after};
use crate::error::IaGetError;
use crate::utils::{
    branch_glyph, create_progress_bar, last_glyph, print_downloaded_line, with_cookie,
};

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
pub(crate) async fn download_file_content(
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
                RANGE,
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

        // The server ignored the Range request and is sending the full body
        // (200), or the 206's Content-Range does not start where the local
        // file ends: in either case the local prefix is untrusted, so the
        // file is reset before the body is streamed.
        let untrusted_prefix = match status {
            StatusCode::OK => true,
            StatusCode::PARTIAL_CONTENT => {
                partial_content_offset(response.headers(), current_file_size) != Some(true)
            }
            _ => false,
        };
        if resuming && untrusted_prefix {
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
        let pb = create_progress_bar(total, &download_action, "green/green", true);
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
