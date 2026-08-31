use crate::constants::XML_DEBUG_TRUNCATE_LEN;
use crate::cookie::cookie_header_value;
use crate::downloader::{parse_last_modified, sync_file_mtime};
use crate::utils::with_cookie;
use crate::{IaGetError, Result};
use colored::*;
use indicatif::ProgressBar;
use reqwest::header::HeaderValue;
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_xml_rs::from_str;
use std::path::Path;
use std::time::{Duration, SystemTime};

/// Root structure for parsing the XML files list from archive.org
/// The actual XML structure has a `files` root element containing multiple `file` elements
#[derive(Deserialize, Debug)]
pub struct XmlFiles {
    #[serde(rename = "file", default)]
    pub files: Vec<XmlFile>,
}

/// Represents a single file entry from the archive.org XML metadata
///
/// Archive.org XML structure has both attributes and nested elements:
/// ```xml
/// <file name="..." source="...">
///   <mtime>...</mtime>
///   <size>...</size>
///   <md5>...</md5>
///   ...
/// </file>
/// ```
#[allow(dead_code)]
#[derive(Deserialize, Debug, Default)]
pub struct XmlFile {
    #[serde(rename = "@name")]
    pub name: String,
    // Optional: a missing @source attribute must not break parsing of the
    // whole document.
    #[serde(rename = "@source", default)]
    pub source: Option<String>,
    pub mtime: Option<u64>,
    pub size: Option<u64>,
    pub format: Option<String>,
    pub rotation: Option<u32>,
    pub md5: Option<String>,
    pub crc32: Option<String>,
    pub sha1: Option<String>,
    pub btih: Option<String>,
    pub summation: Option<String>,
    pub original: Option<String>,
}

/// Extracts the 1-based line number from a `serde-xml-rs` error, whose
/// display carries the position as `Reader: line:column message`.
///
/// The layout is parser-specific: if `serde-xml-rs` ever changes its
/// display format, this yields `None` and the preview falls back to the
/// document head, so a dependency update degrades gracefully.
///
/// Returns `None` when the parser reported no position; the preview then
/// falls back to the document head.
fn parse_error_line(error: &str) -> Option<usize> {
    let rest = error.strip_prefix("Reader: ")?;
    let position = rest.split_once(' ')?.0;
    let line = position.split_once(':')?.0;
    line.parse().ok().filter(|line: &usize| *line >= 1)
}

/// Builds the numbered line preview shown when a parse fails: the few
/// lines around the reported error line (marked with `»`) when the
/// parser names one, otherwise the document head.
///
/// Each line is capped at `XML_DEBUG_TRUNCATE_LEN` characters so a
/// minified multi-megabyte line cannot blow up the error message.
fn error_context(xml_content: &str, line: Option<usize>) -> String {
    let lines: Vec<&str> = xml_content.lines().collect();
    let last_line = lines.len().max(1);
    let anchor = line.unwrap_or(1).min(last_line);
    let first = anchor.saturating_sub(3).max(1);
    let last = (first + 5).min(last_line);

    let mut out = String::new();
    for number in first..=last {
        let raw = lines.get(number - 1).copied().unwrap_or("").trim_end();
        let mut end = XML_DEBUG_TRUNCATE_LEN.min(raw.len());
        while end < raw.len() && !raw.is_char_boundary(end) {
            end -= 1;
        }
        let marker = if number == anchor { "»" } else { " " };
        out.push_str(&format!("{marker}{number:>6}  {}\n", &raw[..end]));
    }
    out
}

/// Parses XML content into XmlFiles structure with improved error context
///
/// # Arguments
/// * `xml_content` - Raw XML content string from archive.org
///
/// # Returns
/// * `Ok(XmlFiles)` if parsing succeeds
/// * `Err(IaGetError)` naming the failing line and showing the lines
///   around it, so a late entry in a large document is locatable
pub fn parse_xml_files(xml_content: &str) -> Result<XmlFiles> {
    from_str(xml_content).map_err(|e| {
        let message = e.to_string();
        IaGetError::XmlParsing(format!(
            "Failed to parse _files.xml metadata: {}. Context:\n{}",
            message,
            error_context(xml_content, parse_error_line(&message))
        ))
    })
}

/// Saves the raw `_files.xml` document to `path`, overwriting any existing
/// copy, and syncs its last-modified time with the server's
/// `Last-Modified` header when present.
///
/// The time is never taken from the document itself: its self-entry carries
/// unreliable metadata. Failing to set the time is not fatal, mirroring the
/// download batch.
pub fn save_xml_metadata(
    path: &Path,
    content: &str,
    last_modified: Option<SystemTime>,
) -> Result<()> {
    // Refuse to write through a pre-planted symlink named "<id>_files.xml":
    // fs::write would silently truncate whatever the link points at.
    if std::fs::symlink_metadata(path).is_ok_and(|existing| existing.file_type().is_symlink()) {
        return Err(IaGetError::FileSystem {
            detail: format!("refusing to overwrite a symlink: {}", path.display()),
            source: None,
        });
    }
    std::fs::write(path, content).map_err(|e| crate::error::io_error_with_path(path, e))?;

    if let Some(target) = last_modified {
        sync_file_mtime(path, target);
    }

    Ok(())
}

/// Timeout for the HEAD accessibility check
const HEAD_CHECK_TIMEOUT_SECS: u64 = 60;

/// Checks if a URL is accessible by sending a HEAD request
pub async fn is_url_accessible(
    url: &Url,
    client: &Client,
    cookie_header: Option<&HeaderValue>,
) -> Result<()> {
    let request = with_cookie(client.head(url.clone()), cookie_header);

    let response = request
        .timeout(Duration::from_secs(HEAD_CHECK_TIMEOUT_SECS))
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
pub fn get_xml_url(original_url: &str) -> String {
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

/// Percent-encodes every path segment of a file name, preserving internal `/`
/// separators, so that URL-special characters in the name (`?`, `#`, `%`,
/// spaces, non-ASCII, ...) cannot be misread as a query string or fragment
/// by `Url::join`. Leading `/` characters are dropped, so the result always
/// stays a relative reference: a name like `//host/x` would otherwise be
/// joined against a *different host*.
///
/// This is the RFC 3986 "unreserved" set, not form-URL-encoding: a space
/// must become `%20` (never `+`), which is why a small encoder is written
/// here instead of reusing `form_urlencoded::byte_serialize`.
pub fn encode_download_path(name: &str) -> String {
    let mut out = String::new();
    for segment in name.split('/') {
        // Dot segments would be silently resolved by Url::join (a "../"
        // name would escape the item directory); drop them up front so
        // the encoded reference matches what is actually requested.
        if segment == "." || segment == ".." {
            continue;
        }
        if !out.is_empty() {
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

/// The `_files.xml` entry name: the last path segment of the XML URL
/// (which always ends in "<identifier>_files.xml", see get_xml_url)
pub fn xml_file_name_of(url: &Url) -> &str {
    url.path()
        .rsplit('/')
        .next()
        .expect("XML URL should have a file name segment")
}

/// XML metadata response: the parsed file list plus the raw data needed to
/// persist the `_files.xml` document locally.
#[derive(Debug)]
pub struct XmlMetadata {
    /// The parsed `_files.xml` file list
    pub files: XmlFiles,
    /// The URL the metadata was fetched from (the download base for files)
    pub base_url: Url,
    /// The `Cookie` header for the metadata host, if any was supplied
    pub cookie_header: Option<HeaderValue>,
    /// The raw XML content, to persist locally
    pub content: String,
    /// The server's `Last-Modified` time, if the server sent one
    pub last_modified: Option<SystemTime>,
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
pub async fn fetch_xml_metadata(
    details_url: &str,
    client: &Client,
    spinner: &ProgressBar,
    cookie_input: Option<&str>,
) -> Result<XmlMetadata> {
    // Generate XML URL
    let xml_url = get_xml_url(details_url);
    spinner.set_message(format!(
        "{} Accessing XML metadata: {}",
        "⚙".blue(),
        xml_url.bold()
    ));
    // The header is scoped to the download URL: every file of the archive
    // lives under it, so this is the scope the file downloads reuse
    let download_url = Url::parse(&xml_url)?;
    let cookie_header = cookie_header_value(cookie_input, &download_url)?;
    fetch_and_parse_xml(&xml_url, client, spinner, cookie_header.as_ref()).await
}

/// Fetches, downloads and parses a `_files.xml` document from an explicit URL.
///
/// The `Cookie` header (if any) is precomputed by the caller against the
/// download URL, so the metadata fetch and the file downloads of one run
/// share a single header. Split out of `fetch_xml_metadata` so tests can
/// point it at a local mock server instead of the fixed archive.org download URL.
pub async fn fetch_and_parse_xml(
    xml_url: &str,
    client: &Client,
    spinner: &ProgressBar,
    cookie_header: Option<&HeaderValue>,
) -> Result<XmlMetadata> {
    // Parse base URL and fetch XML content
    let base_url = Url::parse(xml_url)?;

    // Check XML URL accessibility
    if let Err(e) = is_url_accessible(&base_url, client, cookie_header).await {
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

    let request = with_cookie(client.get(base_url.clone()), cookie_header);

    // The HEAD check above can pass while the GET still fails (throttling,
    // transient edge errors): surface it as a network error instead of
    // feeding an error page into the XML parser.
    let response = request.send().await?.error_for_status()?;
    let last_modified = parse_last_modified(response.headers());
    let xml_content = response.text().await?;

    // Parse XML content with improved error handling
    let files = parse_xml_files(&xml_content)?;

    Ok(XmlMetadata {
        files,
        base_url,
        cookie_header: cookie_header.cloned(),
        content: xml_content,
        last_modified,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockBody, MockResponse, MockServer, mtime_of, temp_dir_for};
    use crate::utils::create_spinner;
    use std::collections::{HashMap, VecDeque};
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn file_entry_without_source_attribute_parses() {
        // A missing @source must not break parsing of the whole document.
        let files = parse_xml_files("<files><file name=\"a.bin\"><size>3</size></file></files>")
            .expect("source attribute must be optional");
        assert_eq!(files.files.len(), 1);
        assert_eq!(files.files[0].name, "a.bin");
        assert!(files.files[0].source.is_none());
    }

    #[test]
    fn file_entry_with_source_attribute_parses() {
        let files = parse_xml_files(
            "<files><file name=\"a.bin\" source=\"original\"><size>3</size></file></files>",
        )
        .expect("valid metadata must parse");
        assert_eq!(files.files[0].source.as_deref(), Some("original"));
    }

    #[test]
    fn parse_error_line_reads_the_parser_position() {
        assert_eq!(
            parse_error_line("Reader: 12:34 Unexpected closing tag: a != b"),
            Some(12)
        );
        assert_eq!(parse_error_line("Reader: 1:44 EOF"), Some(1));
        assert_eq!(parse_error_line("no position here"), None);
    }

    #[test]
    fn xml_parse_error_shows_context_around_failing_line() {
        // 40 filler entries, then a stray closing tag on line 42: the
        // error must name that line and show its neighbourhood, not the
        // document head.
        let filler = (0..40)
            .map(|i| format!("  <file name=\"filler{i}\"/>"))
            .collect::<Vec<_>>()
            .join("\n");
        let xml = format!("<files>\n{filler}\n</strange>\n</files>");
        let msg = parse_xml_files(&xml).unwrap_err().to_string();

        assert!(msg.contains("42:"), "must name the failing line: {msg}");
        assert!(
            msg.contains("»    42"),
            "the failing line must be marked: {msg}"
        );
        assert!(
            msg.contains("strange"),
            "the context must include the failing line: {msg}"
        );
        assert!(
            !msg.contains("filler0"),
            "the context must not start at the document head: {msg}"
        );
    }

    #[test]
    fn error_context_without_position_shows_document_head() {
        let xml = format!("{}\n<files/>", "x".repeat(100));
        let ctx = error_context(&xml, None);
        assert!(ctx.contains(&"x".repeat(100)));
        assert!(ctx.contains("1"), "head context starts at line 1");
    }

    #[test]
    fn error_context_caps_very_long_lines() {
        // A minified document: one line far beyond the cap. The preview
        // must stay bounded and cut on a char boundary.
        let line = "字".repeat(3 * XML_DEBUG_TRUNCATE_LEN);
        let ctx = error_context(&line, Some(1));
        assert!(
            ctx.chars().count() <= XML_DEBUG_TRUNCATE_LEN + 16,
            "the preview must stay bounded, got {} chars",
            ctx.chars().count()
        );
    }

    #[test]
    fn error_context_clamps_anchor_past_the_end() {
        let xml = "<files>\n<file/>\n";
        let ctx = error_context(xml, Some(999));
        assert!(ctx.contains("2"), "the anchor must clamp to the last line");
        assert!(ctx.contains("»"), "the clamped line must be marked");
    }

    #[test]
    fn save_xml_metadata_writes_file_and_sets_mtime() {
        let dir = temp_dir_for("save_xml_sets_mtime");
        let path = dir.join("item1_files.xml");

        save_xml_metadata(
            &path,
            "<files><file name=\"item1_files.xml\"/></files>",
            Some(UNIX_EPOCH + Duration::from_secs(1_545_586_142)),
        )
        .unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("item1_files.xml"));
        assert_eq!(mtime_of(&path), Some(1_545_586_142));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_xml_metadata_overwrites_existing_file() {
        let dir = temp_dir_for("save_xml_overwrites");
        let path = dir.join("item1_files.xml");
        std::fs::write(&path, "stale content").unwrap();

        save_xml_metadata(&path, "<files/>", None).unwrap();

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "<files/>");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn save_xml_metadata_refuses_to_write_through_symlink() {
        // A symlink named "<id>_files.xml" must not have its target
        // silently truncated by the metadata save.
        let dir = temp_dir_for("save_xml_symlink");
        let target = dir.join("target.txt");
        std::fs::write(&target, "do not touch").unwrap();
        let link = dir.join("item1_files.xml");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let err = save_xml_metadata(&link, "<files/>", None).unwrap_err();
        assert!(err.to_string().contains("symlink"), "got: {err}");
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "do not touch",
            "the link target must be left untouched"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_xml_metadata_without_last_modified_keeps_current_time() {
        let dir = temp_dir_for("save_xml_no_mtime");
        let path = dir.join("item1_files.xml");

        save_xml_metadata(&path, "<files/>", None).unwrap();

        let now = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mtime = mtime_of(&path).expect("mtime should be readable");
        assert!(
            mtime.abs_diff(now) < 60,
            "mtime {mtime} should be within 60s of now {now}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_xml_url_converts_details_url() {
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

    #[test]
    fn encode_download_path_drops_dot_segments() {
        // A "../" entry must not escape the item directory once Url::join
        // removes dot segments; dropping them up front keeps the reference
        // relative and honest.
        assert_eq!(encode_download_path("../outside.mp4"), "outside.mp4");
        assert_eq!(encode_download_path("a/./b.mp4"), "a/b.mp4");
        assert_eq!(
            encode_download_path(".."),
            "",
            "a dot-segment-only name encodes to nothing and is skipped"
        );
    }

    #[test]
    fn encode_download_path_drops_leading_slashes() {
        // A leading '/' makes the reference path-absolute, and "//host/"
        // would be read as a different host — both must stay relative.
        assert_eq!(encode_download_path("/foo.mp4"), "foo.mp4");
        assert_eq!(
            encode_download_path("//evil.example/x.mp4"),
            "evil.example/x.mp4"
        );
        // Internal separators, including doubles, are preserved.
        assert_eq!(encode_download_path("a//b.mp4"), "a//b.mp4");
    }

    #[test]
    fn leading_slash_name_stays_under_item() {
        let base = Url::parse("https://archive.org/download/item1/item1_files.xml").unwrap();
        let joined = base
            .join(&encode_download_path("//evil.example/x.mp4"))
            .expect("encoded name must join against the base URL");
        assert_eq!(
            joined.as_str(),
            "https://archive.org/download/item1/evil.example/x.mp4"
        );
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

    #[tokio::test]
    async fn xml_metadata_http_error_is_a_network_error() {
        // The HEAD check passes; the GET must fail with a status error.
        let mut scripts = HashMap::new();
        scripts.insert(
            "/download/item1/item1_files.xml".to_string(),
            VecDeque::from(vec![
                MockResponse::new(200, MockBody::Full(vec![])), // HEAD check
                MockResponse::new(
                    500,
                    MockBody::Full(b"<html><body>nginx error page</body></html>".to_vec()),
                ),
            ]),
        );
        let server = MockServer::start(scripts, MockResponse::new(500, MockBody::Full(vec![])));
        let url = server.url("/download/item1/item1_files.xml");
        let client = Client::new();
        let spinner = create_spinner("mock");

        let err = fetch_and_parse_xml(&url, &client, &spinner, None)
            .await
            .expect_err("an HTTP error on the metadata GET must fail the fetch");

        match err {
            IaGetError::Network { detail, .. } => {
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
        let mut scripts = HashMap::new();
        scripts.insert(
            "/download/item1/item1_files.xml".to_string(),
            VecDeque::from(vec![
                MockResponse::new(200, MockBody::Full(vec![])), // HEAD check
                MockResponse::new(200, MockBody::Full(xml.as_bytes().to_vec())),
            ]),
        );
        let server = MockServer::start(scripts, MockResponse::new(200, MockBody::Full(vec![])));
        let url = server.url("/download/item1/item1_files.xml");
        let client = Client::new();
        let spinner = create_spinner("mock");

        let meta = fetch_and_parse_xml(&url, &client, &spinner, None)
            .await
            .expect("metadata fetch should succeed");

        assert_eq!(meta.files.files.len(), 2);
        assert_eq!(meta.files.files[1].name, "scan.jpg");
        assert_eq!(meta.content, xml);
    }

    #[tokio::test]
    async fn precomputed_cookie_header_reaches_the_xml_fetch() {
        let xml = "<files><file name=\"item1_files.xml\"/></files>";
        let mut scripts = HashMap::new();
        scripts.insert(
            "/download/item1/item1_files.xml".to_string(),
            VecDeque::from(vec![
                MockResponse::new(200, MockBody::Full(vec![])), // HEAD check
                MockResponse::new(200, MockBody::Full(xml.as_bytes().to_vec())),
            ]),
        );
        let server = MockServer::start(scripts, MockResponse::new(200, MockBody::Full(vec![])));
        let url = server.url("/download/item1/item1_files.xml");
        let client = Client::new();
        let spinner = create_spinner("mock");
        let header = HeaderValue::from_static("session=abc123");

        let meta = fetch_and_parse_xml(&url, &client, &spinner, Some(&header))
            .await
            .expect("metadata fetch should succeed");

        assert_eq!(meta.cookie_header.as_ref(), Some(&header));
        assert_eq!(
            server.cookies(),
            vec![
                Some("session=abc123".to_string()),
                Some("session=abc123".to_string()),
            ],
            "both the HEAD check and the GET must carry the precomputed header"
        );
    }
}
