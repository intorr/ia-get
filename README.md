<h1 align="center">
  <img src="assets/ia-get.png" width="256" height="256" alt="ia-get">
  <br />
  ia-get
</h1>

<p align="center"><b>File downloader for archive.org</b></p>
<p align="center">
<img alt="GitHub all releases" src="https://img.shields.io/github/downloads/wimpysworld/ia-get/total?logo=github&label=Downloads">
</p>

<p align="center">Made with 💝 by 🤖</p>

# Usage 📖

Simply pass the URL of an [archive.org](https://archive.org) details page you want to download and `ia-get` will automatically get the XML metadata and download all files to the current working directory.

```shell
ia-get https://archive.org/details/<identifier>
```

Pass cookies for archive.org items that require a logged-in session.
`--cookies`/`-b` accepts either a raw Cookie header string or a Netscape `cookies.txt` file exported from your browser.

```shell
ia-get --cookies cookies.txt https://archive.org/details/<identifier>
ia-get -b 'logged-in-user=...; logged-in-sig=...' https://archive.org/details/<identifier>
```

Preview the files first with `--list` or `-l`.
This lists the names and sizes reported by archive.org metadata without downloading anything.
If archive.org does not provide a size for an entry, `ia-get` reports it as `unknown` and excludes it from the total known size.

```shell
ia-get --list https://archive.org/details/<identifier>
ia-get -l https://archive.org/details/<identifier>
```

Archive.org's edge servers occasionally return error pages, empty responses, or drop connections mid-transfer. `ia-get` is built to ride out those outages:

- Server errors (timeouts, connection resets, HTTP 5xx/429) are retried with exponential backoff — 5s base, doubling per attempt up to a 60s cap, with ±20% jitter — and a `Retry-After` header is honored when present.
- Error response bodies are never written into your files, and an empty or truncated response is retried rather than saved.
- Each file is downloaded to a `<name>.part` temporary file and renamed to its final name only after the size (when known) and MD5 hash have been verified. A failed verification triggers a re-download from scratch, up to three attempts per file.
- If a file ultimately fails, the remaining files in the archive still download; `ia-get` exits with a non-zero status and prints a list of the failures. Use `--stop-on-error` to abort at the first failure instead.

```shell
ia-get --stop-on-error https://archive.org/details/<identifier>
```

## Why? 🤔💭

I wanted to download high-quality scans of [ZZap!64 magazine](https://en.wikipedia.org/wiki/Zzap!64) and some read-only memory from archive.org.
Archives of this type often include many large files, torrents are not always provided and when they are available they do not index all the available files in the archive.

Archive.org publishes XML documents for every page that indexes every file available.
So I co-authored `ia-get` to automate the download process.

### Features ✨

- 🔽 Reliably download files from the Internet Archive
- 🌳 Preserves the original directory structure
- 🔄 Automatically resumes partial or failed downloads
- 🔏 Hash checks to confirm file integrity
- 🕓 Preserves file modification times — taken from the server's `Last-Modified` header, or from the `<mtime>` entry in `_files.xml` when the header is absent; already-verified files get their times corrected on re-runs
- 🔁 Retries server errors (timeouts, HTTP 5xx/429, empty or truncated responses) with exponential backoff that honors `Retry-After`
- 🧩 Downloads to `<name>.part` files and renames them only after size and MD5 verification passes
- 🚦 A failed file no longer aborts the batch — the remaining files continue and `ia-get` exits non-zero, listing the failures
- 🌱 Can be run multiple times to update existing downloads
- 📊 Gets all the metadata for the archive
- 📦️ Available for **Linux** 🐧 **macOS** 🍏 and **Windows** 🪟

### Sharing is caring 🤝

You can use `ia-get` to download files from archive.org, including all the metadata and the `.torrent` file, if there is one.
You can the start seeding the torrent using a pristine copy of the archive, and a complete file set.

# Demo 🧑‍💻

<div align="center"><img alt="ia-get demo" src="assets/ia-get.gif" width="1024" /></div>

# Development 🏗️

Such as it is.

```shell
cargo build
```

## Unit Tests 🧪

You can run the built-in unit tests with:

```shell
cargo test
```

This will run tests that verify URL pattern validation and other core functionality.

## Manual Tests 🤞

I used these commands to test `ia-get` during development.

```shell
ia-get https://archive.org/details/deftributetozzap64
ia-get https://archive.org/details/zzapp_64_issue_001_600dpi
```

# A.I. Driven Development 🤖

This program is an experiment 🧪 In late 2023, it was initially co-authored using [Chatty Jeeps](https://ubuntu.social/@popey/111527182881051626).
When I started this project, I had no experience 👶 with [Rust](https://www.rust-lang.org/) and was curious to see if I could use AI tools to assist in developing a program in a language I do not know.

**As featured on [Linux Matters](https://linuxmatters.sh) podcast!** 🎙️ I am a presenter on Linux Matters and we discussed how the [initial version of the program](https://github.com/wimpysworld/ia-get/tree/5f2b356e7d841f2756780e2a101cf8be4041a7f6) was created using Chatty Jeeps (ChatGPT-4) in [Episode 16 - Blogging to the Fediverse](https://linuxmatters.sh/16/).

I discussed that process, its successes, and drawbacks. In a future episode, we will discuss the latest version of the project.

<div align="center">
  <a href="https://linuxmatters.sh" target="_blank"><img src="https://raw.githubusercontent.com/wimpysworld/nix-config/main/.github/screenshots/linuxmatters.png" alt="Linux Matters Podcast"/></a>
  <br />
  <em>Linux Matters Podcast</em>
</div>

Since that initial MVP, I used [Unfold.ai](https://unfoldai.io/) to add features and improve the code 🧑‍💻.
All commits from October 27, 2023, until the end of December 2023 that were AI co-authored have full details of the AI contribution in the commit messages.
Linux Matters listner [Daniel Dewberry](https://github.com/DanielDewberry) submitted a [*"peer review"* of ia-get](https://github.com/wimpysworld/ia-get/issues/7) in January 2024.
The project had little development activity until May 2025, when I incorporated the improvements Daniel had suggested.

I've picked up some Rust along the way, and some of the refactoring and redesign comes directly from my brain 🧠 and some assistance from GitHub CoPilot using Claude Sonnet 3.7 and Gemini Pro 2.5.
