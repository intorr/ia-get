//! Shared helpers for unit tests, used by both the library and the binary.

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::StatusCode;

/// Creates a unique temp directory for a test.
///
/// Directories are not cleaned up, mirroring the existing test style:
/// leftovers under the OS temp dir are harmless.
pub fn temp_dir_for(test_name: &str) -> PathBuf {
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
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

/// Returns a file's mtime as unix seconds, if readable.
pub fn mtime_of(path: &Path) -> Option<u64> {
    std::fs::metadata(path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
}

/// Body variant for mock server responses
#[derive(Clone)]
pub enum MockBody {
    /// Send the whole body
    Full(Vec<u8>),
    /// Announce `announced_len` bytes via Content-Length, send only
    /// `partial` bytes, then close the connection mid-body
    Truncated {
        announced_len: u64,
        partial: Vec<u8>,
    },
}

/// A scripted response: status, body and extra headers
#[derive(Clone)]
pub struct MockResponse {
    status: u16,
    body: MockBody,
    extra_headers: Vec<(String, String)>,
}

impl MockResponse {
    pub fn new(status: u16, body: MockBody) -> Self {
        Self {
            status,
            body,
            extra_headers: vec![],
        }
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.extra_headers
            .push((name.to_string(), value.to_string()));
        self
    }
}

struct ServerState {
    scripts: HashMap<String, VecDeque<MockResponse>>,
    fallback: MockResponse,
    request_count: usize,
    methods: Vec<String>,
    ranges: Vec<Option<u64>>,
}

/// A mock HTTP server on 127.0.0.1 that serves scripted responses per path
/// (falling back to `fallback` once a path's script is exhausted).
///
/// Each request pops one response from the path's script queue; `HEAD`
/// requests get the response's status and headers but no body, as a real
/// server would send.
pub struct MockServer {
    base_url: String,
    state: Arc<Mutex<ServerState>>,
}

impl MockServer {
    /// Starts the server; the listener thread is detached and the server
    /// lives until the process exits
    pub fn start(scripts: HashMap<String, VecDeque<MockResponse>>, fallback: MockResponse) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind mock server");
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(Mutex::new(ServerState {
            scripts,
            fallback,
            request_count: 0,
            methods: Vec::new(),
            ranges: Vec::new(),
        }));
        let handler_state = state.clone();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                handle_connection(stream, &handler_state);
            }
        });

        Self {
            base_url: format!("http://127.0.0.1:{port}"),
            state,
        }
    }

    /// URL for the given absolute path (e.g. "/file.bin")
    pub fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    pub fn request_count(&self) -> usize {
        self.state.lock().unwrap().request_count
    }

    /// HTTP methods of the received requests, in order
    pub fn methods(&self) -> Vec<String> {
        self.state.lock().unwrap().methods.clone()
    }

    /// Parsed `Range` header start offsets, in order (`None` when absent)
    pub fn ranges(&self) -> Vec<Option<u64>> {
        self.state.lock().unwrap().ranges.clone()
    }
}

fn handle_connection(mut stream: TcpStream, state: &Arc<Mutex<ServerState>>) {
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
    let request_line = head.lines().next().unwrap_or("");
    let method = request_line
        .split_whitespace()
        .next()
        .unwrap_or("GET")
        .to_string();
    let path = request_line
        .split_whitespace()
        .nth(1)
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
        st.methods.push(method.clone());
        st.ranges.push(range);
        st.scripts
            .get_mut(&path)
            .and_then(|q| q.pop_front())
            .unwrap_or_else(|| st.fallback.clone())
    };

    // A HEAD response carries the status and headers but no body
    let (announced_len, body_bytes) = match (&response.body, method.eq_ignore_ascii_case("HEAD")) {
        (_, true) => (0, Vec::new()),
        (MockBody::Full(data), false) => (data.len() as u64, data.clone()),
        (
            MockBody::Truncated {
                announced_len,
                partial,
            },
            false,
        ) => (*announced_len, partial.clone()),
    };

    let reason = StatusCode::from_u16(response.status)
        .ok()
        .and_then(|s| s.canonical_reason())
        .unwrap_or("Unknown")
        .to_string();
    let mut out = format!("HTTP/1.1 {} {}\r\n", response.status, reason);
    out.push_str(&format!("Content-Length: {announced_len}\r\n"));
    for (name, value) in &response.extra_headers {
        out.push_str(&format!("{name}: {value}\r\n"));
    }
    out.push_str("Connection: close\r\n\r\n");

    let _ = stream.write_all(out.as_bytes());
    // The stream is dropped here: for Truncated bodies this closes the
    // connection before the announced Content-Length has been delivered.
    let _ = stream.write_all(&body_bytes);
}
