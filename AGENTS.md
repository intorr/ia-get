# AGENTS.md

Rust CLI for downloading files from archive.org with resume support, integrity checks, and directory preservation.

## Setup

```shell
nix develop              # Enter dev shell (preferred)
cargo build              # Build debug binary
cargo build --release    # Optimised build (stripped, LTO)
```

## Testing

```shell
cargo test               # Unit tests
cargo clippy             # Linting
cargo fmt --check        # Format check
```

Manual test URLs:
- `ia-get https://archive.org/details/deftributetozzap64`
- `ia-get https://archive.org/details/zzapp_64_issue_001_600dpi`

## Code Style

- Run `cargo fmt` and `cargo clippy` before committing
- Explicit error handling with `Result<T, E>`
- Use `thiserror` for custom error types
- Prefer idiomatic Rust patterns

## Architecture

```
src/
├── main.rs              # CLI entry point (clap) and orchestration
├── lib.rs               # Library exports
├── downloader.rs        # HTTP download with retry/resume, size + MD5 verification
├── archive_metadata.rs  # _files.xml fetch, XML parsing, local persistence
├── cookie.rs            # Cookie header from raw string or Netscape cookies.txt
├── utils.rs             # Helpers (filename sanitisation, progress bars, status lines)
├── error.rs             # Custom error types (thiserror)
├── constants.rs         # User agent, URL pattern, debug limits
└── test_support.rs      # Shared test helpers (in-memory HTTP mock server)
```

## Dependencies

- Keep minimal and well-justified
- Prefer crates with good Nix support
- Update `Cargo.lock` when adding dependencies
- TLS via `rustls` only (no openssl)

## Platform Support

Must build on: `x86_64-linux`, `aarch64-linux`, `x86_64-darwin`, `aarch64-darwin`, `x86_64-pc-windows-msvc`

Use `nix build` to verify cross-platform compatibility.

## Commit Guidelines

- Update README.md for new features or usage changes
- Include examples in help text
- Document Internet Archive API specifics or limitations
