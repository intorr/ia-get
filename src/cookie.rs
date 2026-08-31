//! Building the `Cookie` header from a raw string or a Netscape cookies.txt
//! file, so every authenticated request carries the right cookies.

use crate::{IaGetError, Result};
use colored::*;
use reqwest::header::HeaderValue;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

/// One entry of a Netscape cookies.txt file
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
///
/// The input is only treated as a file when it names an existing file that
/// either holds at least one recognizable Netscape cookie line or does not
/// look like a raw cookie pair (no `=`): a raw cookie string that merely
/// collides with a filename in the working directory is kept as a cookie
/// string instead of being silently swallowed as an empty cookies.txt.
///
/// A file that holds cookies but none of them apply to `url` (all expired,
/// or scoped to another domain/path) yields an empty header; a warning is
/// printed so an unauthenticated-looking 401/403 has an obvious cause.
pub fn cookie_header_from_input(input: &str, url: &Url) -> Result<String> {
    if Path::new(input).is_file() {
        let cookie_file = fs::read_to_string(input)?;
        if has_netscape_cookie_line(&cookie_file) || !input.contains('=') {
            let header = cookie_header_from_netscape_file(&cookie_file, url)?;
            if header.is_empty() {
                println!(
                    "{} {} {}",
                    "⚠".yellow().bold(),
                    "No applicable cookies found in".yellow(),
                    input.dimmed()
                );
            }
            return Ok(header);
        }
    }
    Ok(input.trim().to_string())
}

/// True when the content holds at least one recognizable Netscape cookies.txt line.
fn has_netscape_cookie_line(content: &str) -> bool {
    content
        .lines()
        .any(|line| parse_netscape_cookie(line).is_some())
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
pub fn cookie_header_from_netscape_file(content: &str, url: &Url) -> Result<String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| IaGetError::FileSystem {
            detail: e.to_string(),
            source: Some(Box::new(e)),
        })?
        .as_secs();

    let cookies = content
        .lines()
        .filter_map(parse_netscape_cookie)
        .filter(|cookie| cookie_applies_to_url(cookie, url, now))
        .map(|cookie| format!("{}={}", cookie.name, cookie.value))
        .collect::<Vec<_>>();

    Ok(cookies.join("; "))
}

/// Resolves the `--cookies` CLI input (raw string or cookies.txt path) into
/// a `Cookie` header value for requests to `url`, or `None` when no cookies
/// apply.
pub fn cookie_header_value(cookie_input: Option<&str>, url: &Url) -> Result<Option<HeaderValue>> {
    let Some(cookie_input) = cookie_input else {
        return Ok(None);
    };

    let cookie_header = cookie_header_from_input(cookie_input, url)?;
    if cookie_header.is_empty() {
        return Ok(None);
    }

    let value = HeaderValue::from_str(&cookie_header).map_err(|e| IaGetError::Network {
        detail: format!("Invalid cookie header: {e}"),
        source: Some(Box::new(e)),
    })?;
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn unique_temp_file(name: &str, content: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "{name}-{}-{}.txt",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&path, content).expect("failed to write temp cookie file");
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn cookie_header_file_with_cookie_lines_is_parsed() {
        // A real cookies.txt (even when the path itself contains '=') is
        // still parsed, not treated as a raw cookie string.
        let input = unique_temp_file(
            "ia-get-cookie-review=1",
            "archive.org\tFALSE\t/\tFALSE\t2145916800\tsession\tvalue\n",
        );

        let header = cookie_header_from_input(&input, &cookie_test_url("/download/item/f.xml"))
            .expect("cookies.txt must parse");
        assert_eq!(header, "session=value");

        let _ = fs::remove_file(&input);
    }

    #[test]
    fn cookie_header_file_with_only_expired_cookies_is_empty() {
        // A recognizable-but-expired cookie file must yield an empty header
        // (with a warning), not the file name or the stale cookie.
        let input = unique_temp_file(
            "ia-get-cookie-expired-only",
            "archive.org\tFALSE\t/\tFALSE\t1\told\tvalue\n",
        );

        let header = cookie_header_from_input(&input, &cookie_test_url("/download/item/f.xml"))
            .expect("an expired-cookies file must still parse");
        assert_eq!(header, "", "expired cookies must not reach the header");

        let _ = fs::remove_file(&input);
    }

    #[test]
    fn cookie_header_colliding_filename_is_kept_as_raw_string() {
        // A file that holds no recognizable cookie line must not swallow a
        // cookie-looking input: the raw cookie string wins.
        let input = unique_temp_file("ia-get-cookie-collide=1", "not a cookie file\n");

        let header = cookie_header_from_input(&input, &cookie_test_url("/download/item/f.xml"))
            .expect("input must be kept as a cookie string");
        assert_eq!(
            header,
            input.trim(),
            "the cookie-looking input must survive"
        );

        let _ = fs::remove_file(&input);
    }
}
