//! Converting parsed archive metadata into a concrete download plan:
//! filtering out the archive's self-referencing `_files.xml` entry,
//! building each file's absolute URL and sanitized local path, and
//! detecting collisions that would cause one file to overwrite another.

use std::collections::HashMap;

use colored::*;
use reqwest::Url;

use crate::Result;
use crate::archive_metadata::{XmlFile, encode_download_path, xml_file_name_of};
use crate::downloader::DownloadTask;
use crate::utils::sanitize_filename;

/// Filters out the archive's self-referencing `_files.xml` entry, whose
/// checksum, mtime and size are unreliable, leaving the files to download.
pub fn files_to_download(files: Vec<XmlFile>, xml_file_name: &str) -> Vec<XmlFile> {
    files
        .into_iter()
        .filter(|file| file.name != xml_file_name)
        .collect()
}

/// Normalised key for local-path collision detection.
///
/// On case-insensitive filesystems (Windows, default macOS) "a.pdf" and
/// "A.pdf" are the same path, so the key is lowercased there; on
/// case-sensitive filesystems (Linux) distinct casing stays distinct.
fn local_path_key(path: &str) -> String {
    #[cfg(target_os = "linux")]
    {
        path.to_string()
    }
    #[cfg(not(target_os = "linux"))]
    {
        path.to_lowercase()
    }
}

/// A plan warning line: the shared "⚠ Label:" prefix (label and colon
/// styled together) followed by the styled details of the skipped,
/// renamed or colliding entry.
fn warning_line(label: &str, details: String) -> String {
    format!(
        "{} {}: {}",
        "⚠".yellow().bold(),
        format!("{label}:").yellow(),
        details
    )
}

/// The download plan: the tasks that will actually run, how many file
/// names were sanitized, and the warning lines (sanitized, collided and
/// skipped entries) that the caller prints.
pub struct DownloadPlan {
    pub tasks: Vec<DownloadTask>,
    pub sanitized_count: usize,
    pub warnings: Vec<String>,
}

/// Converts the parsed metadata into download tasks: builds each file's
/// absolute URL and its sanitized local path, collecting a warning line
/// for every rename, collision or skip.
///
/// Entries whose name encodes to an empty URL path (an empty name, or
/// slashes only) are skipped: joining `""` would resolve to the metadata
/// URL itself. Likewise, an entry whose sanitized local path collides with
/// an earlier entry's is skipped, so one file never overwrites another.
/// Collisions are compared case-insensitively on case-insensitive
/// filesystems (Windows, default macOS), where "a.pdf" and "A.pdf" are the
/// same path.
///
/// A failed URL join aborts the run: silently keeping the base URL would
/// download the metadata file under the file's name.
pub fn plan_download_tasks(files: Vec<XmlFile>, base_url: &Url) -> Result<DownloadPlan> {
    let mut sanitized_count = 0;
    let mut tasks: Vec<DownloadTask> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    // Normalised local path -> original name, so a collision can be
    // reported with both sides of the clash.
    let mut taken_paths: HashMap<String, String> = HashMap::new();

    // The locally saved "<id>_files.xml" occupies the item root too: an
    // entry that sanitizes to that name (differing only by case on
    // case-insensitive filesystems) would silently overwrite the metadata.
    let xml_file_name = xml_file_name_of(base_url);
    taken_paths.insert(local_path_key(xml_file_name), xml_file_name.to_string());

    for file in files {
        // Percent-encode the name first so '?' / '#' / '%' characters in
        // it cannot split the URL into query or fragment components.
        let encoded_name = encode_download_path(&file.name);

        if encoded_name.is_empty() {
            warnings.push(warning_line(
                "Skipped",
                format!("{} (empty name)", file.name.dimmed()),
            ));
            continue;
        }

        let absolute_url = base_url.join(&encoded_name)?;

        // Sanitize filename for filesystem compatibility
        let (sanitized_name, was_modified) = sanitize_filename(&file.name);

        // Collect a warning line if the filename was modified
        if was_modified {
            warnings.push(warning_line(
                "Sanitized",
                format!("{} → {}", file.name.dimmed(), sanitized_name.bold()),
            ));
            sanitized_count += 1;
        }

        // Two different remote names may sanitize to the same local path
        // (e.g. "file:1.mp4" and "file_1.mp4"); downloading both would
        // overwrite the earlier file, so keep the first entry and skip the
        // rest.
        // The key normalises case on case-insensitive filesystems, so
        // "Report.PDF" and "report.pdf" collide there but not on Linux.
        let path_key = local_path_key(&sanitized_name);
        if let Some(first_name) = taken_paths.get(&path_key) {
            warnings.push(warning_line(
                "Collision",
                format!(
                    "{} collides with {} at {} — the later entry is skipped",
                    file.name.dimmed(),
                    first_name.dimmed(),
                    sanitized_name.bold()
                ),
            ));
            continue;
        }
        taken_paths.insert(path_key, file.name);

        tasks.push(DownloadTask {
            url: absolute_url.to_string(),
            file_path: sanitized_name,
            expected_md5: file.md5,
            expected_size: file.size,
            expected_mtime: file.mtime,
        });
    }

    Ok(DownloadPlan {
        tasks,
        sanitized_count,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::xml_file;

    #[test]
    fn files_to_download_excludes_xml_self_reference() {
        let files = vec![
            xml_file("item1_files.xml", Some(123)),
            xml_file("scan.jpg", Some(456)),
        ];

        let result = files_to_download(files, "item1_files.xml");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "scan.jpg");
    }

    #[test]
    fn files_to_download_keeps_all_when_no_self_reference() {
        let files = vec![xml_file("scan.jpg", Some(456)), xml_file("notes.txt", None)];

        let result = files_to_download(files, "item1_files.xml");

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn plan_download_tasks_skips_entries_with_empty_names() {
        let base = Url::parse("https://archive.org/download/item1/item1_files.xml").unwrap();
        let files = vec![
            xml_file("", Some(10)),
            xml_file("//", None),
            xml_file("ok.bin", Some(5)),
        ];
        let plan = plan_download_tasks(files, &base).expect("plan must build");

        assert_eq!(plan.sanitized_count, 0);
        assert_eq!(
            plan.tasks.len(),
            1,
            "empty-name entries must be skipped, not joined to the base URL"
        );
        assert_eq!(plan.tasks[0].file_path, "ok.bin");
        assert_eq!(
            plan.tasks[0].url,
            "https://archive.org/download/item1/ok.bin"
        );
        assert_eq!(
            plan.warnings.len(),
            2,
            "each skipped entry must leave a warning line"
        );
    }

    #[test]
    fn plan_download_tasks_skips_sanitized_name_collisions() {
        let base = Url::parse("https://archive.org/download/item1/item1_files.xml").unwrap();
        // Both names sanitize to "file_1.mp4"; the later entry must not
        // overwrite the earlier file.
        let files = vec![
            xml_file("file:1.mp4", Some(1)),
            xml_file("file_1.mp4", Some(2)),
        ];
        let plan = plan_download_tasks(files, &base).expect("plan must build");

        assert_eq!(plan.sanitized_count, 1, "only the colon name is sanitized");
        assert_eq!(plan.tasks.len(), 1, "the colliding entry must be skipped");
        assert_eq!(
            plan.tasks[0].url,
            "https://archive.org/download/item1/file%3A1.mp4"
        );
        assert_eq!(plan.tasks[0].file_path, "file_1.mp4");
    }

    #[test]
    fn plan_download_tasks_case_collision_follows_filesystem() {
        // Names differing only by case: on case-insensitive filesystems
        // (Windows, default macOS) the later entry must be skipped, on
        // case-sensitive ones (Linux) both names are distinct paths.
        let base = Url::parse("https://archive.org/download/item1/item1_files.xml").unwrap();
        let files = vec![
            xml_file("Report.PDF", Some(1)),
            xml_file("report.pdf", Some(2)),
        ];
        let plan = plan_download_tasks(files, &base).expect("plan must build");

        #[cfg(target_os = "linux")]
        {
            assert_eq!(
                plan.tasks.len(),
                2,
                "differing case must stay two files on Linux"
            );
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(
                plan.tasks.len(),
                1,
                "the case-colliding entry must be skipped"
            );
            assert_eq!(plan.tasks[0].file_path, "Report.PDF");
        }
    }

    #[test]
    fn plan_download_tasks_protects_saved_xml_name() {
        // "<id>_files.xml" is saved locally as file #1: an entry that
        // sanitizes to the same name must not overwrite the metadata.
        let base = Url::parse("https://archive.org/download/item1/item1_files.xml").unwrap();
        let files = vec![
            xml_file("Item1_Files.XML", Some(1)),
            xml_file("scan.jpg", Some(2)),
        ];
        let plan = plan_download_tasks(files, &base).expect("plan must build");

        #[cfg(target_os = "linux")]
        assert_eq!(
            plan.tasks.len(),
            2,
            "differing case stays distinct on case-sensitive filesystems"
        );
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(
                plan.tasks.len(),
                1,
                "the xml-name-colliding entry must be skipped"
            );
            assert_eq!(plan.tasks[0].file_path, "scan.jpg");
        }
    }

    #[test]
    fn local_path_key_normalises_case_per_platform() {
        #[cfg(target_os = "linux")]
        assert_eq!(local_path_key("A/b.pdf"), "A/b.pdf");
        #[cfg(not(target_os = "linux"))]
        assert_eq!(local_path_key("A/b.PDF"), "a/b.pdf");
    }
}
