//! Converting parsed archive metadata into a concrete download plan:
//! selecting the files a URL and the name filters target, building each
//! file's absolute URL and sanitized local path (under the output
//! directory, when one was given), and detecting collisions that would
//! cause one file to overwrite another.

use std::collections::HashMap;

use colored::*;
use reqwest::Url;

use crate::Result;
use crate::archive_metadata::{ArchiveTarget, XmlFile, encode_download_path, xml_file_name_of};
use crate::downloader::DownloadTask;
use crate::error::IaGetError;
use crate::filename::sanitize_filename;

/// Filters out the archive's self-referencing `_files.xml` entry, whose
/// checksum, mtime and size are unreliable, leaving the files to download.
pub fn files_to_download(files: Vec<XmlFile>, xml_file_name: &str) -> Vec<XmlFile> {
    files
        .into_iter()
        .filter(|file| file.name != xml_file_name)
        .collect()
}

/// Narrows the archive's file list to the candidates of one run: for a
/// whole-item URL every file except the self-referencing `_files.xml`
/// entry (kept out by [`files_to_download`]); for a single-file URL,
/// exactly the entry the URL names, whatever it is.
///
/// A single-file URL naming a file the metadata does not list is an error,
/// not an empty plan: the user asked for that file specifically.
pub fn select_files(
    files: Vec<XmlFile>,
    xml_file_name: &str,
    target: &ArchiveTarget,
) -> Result<Vec<XmlFile>> {
    if let Some(file_path) = target.file_path.as_deref() {
        return files
            .into_iter()
            .find(|file| file.name == file_path)
            .map(|file| vec![file])
            .ok_or(IaGetError::FileNotFoundInArchive {
                identifier: target.identifier.clone(),
                path: file_path.to_string(),
            });
    }
    Ok(files_to_download(files, xml_file_name))
}

/// Joins the output directory ("", "out", "out/sub") onto an
/// archive-relative local path; with no directory the path is kept as is.
pub fn join_output_dir(output_dir: &str, path: &str) -> String {
    if output_dir.is_empty() {
        path.to_string()
    } else {
        format!("{output_dir}/{path}")
    }
}

/// Normalised key for local-path collision detection.
///
/// On case-insensitive filesystems (Windows, default macOS) "a.pdf" and
/// "A.pdf" are the same path, so the key is lowercased there; on
/// case-sensitive filesystems (Linux) distinct casing stays distinct.
/// A case-sensitive macOS volume is treated like the default: entries
/// differing only by case are still skipped — the conservative choice,
/// since a volume's case sensitivity is not detectable portably.
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

/// The directory components a local path implies: for "a/b/c.txt" the
/// ancestors are "a" and "a/b". A single-component path has none.
fn ancestor_components(path: &str) -> Vec<String> {
    let components: Vec<&str> = path.split('/').collect();
    (1..components.len())
        .map(|split_at| components[..split_at].join("/"))
        .collect()
}

/// The first planned entry that a new local path would clash with, if any:
/// the path itself (two files sanitizing to the same name), a planned file
/// where the new path needs a directory ("notes" then "notes/file.txt"),
/// or a planned directory where the new path would land a file (the reverse
/// order). Returns (the path the clash is reported at, the earlier
/// entry's original name).
fn clashing_entry(
    sanitized_name: &str,
    planned_files: &HashMap<String, String>,
    planned_dirs: &HashMap<String, String>,
) -> Option<(String, String)> {
    let path_key = local_path_key(sanitized_name);
    if let Some(first) = planned_files.get(&path_key) {
        return Some((sanitized_name.to_string(), first.clone()));
    }
    for ancestor in ancestor_components(sanitized_name) {
        if let Some(first) = planned_files.get(&local_path_key(&ancestor)) {
            return Some((ancestor, first.clone()));
        }
    }
    planned_dirs
        .get(&path_key)
        .map(|first| (sanitized_name.to_string(), first.clone()))
}

/// Registers a newly planned path: the file itself plus every ancestor
/// directory component it implies.
fn register_planned_path(
    sanitized_name: &str,
    original_name: &str,
    planned_files: &mut HashMap<String, String>,
    planned_dirs: &mut HashMap<String, String>,
) {
    planned_files.insert(local_path_key(sanitized_name), original_name.to_string());
    for ancestor in ancestor_components(sanitized_name) {
        planned_dirs
            .entry(local_path_key(&ancestor))
            .or_insert_with(|| original_name.to_string());
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
/// absolute URL and its sanitized local path (prefixed by `output_dir`
/// when one was given), collecting a warning line for every rename,
/// collision or skip.
///
/// A whole-item run reserves the locally saved `_files.xml` name, so an
/// entry that sanitizes to it never overwrites the metadata; a
/// single-file run reserves nothing.
///
/// Entries whose name encodes to an empty URL path (an empty name, or
/// slashes only) are skipped: joining `""` would resolve to the metadata
/// URL itself. Likewise, an entry whose sanitized local path collides with
/// an earlier entry's is skipped, so one file never overwrites another and
/// a file never lands where another entry needs a directory (or the
/// reverse). Collisions are compared case-insensitively on case-insensitive
/// filesystems (Windows, default macOS), where "a.pdf" and "A.pdf" are the
/// same path.
///
/// A failed URL join aborts the run: silently keeping the base URL would
/// download the metadata file under the file's name.
pub fn plan_download_tasks(
    files: Vec<XmlFile>,
    base_url: &Url,
    output_dir: &str,
    target: &ArchiveTarget,
) -> Result<DownloadPlan> {
    let mut sanitized_count = 0;
    let mut tasks: Vec<DownloadTask> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    // Normalised local path -> original name, so a collision can be
    // reported with both sides of the clash.
    let mut planned_files: HashMap<String, String> = HashMap::new();
    // Normalised directory component -> original name of the first entry
    // that needs it as a directory.
    let mut planned_dirs: HashMap<String, String> = HashMap::new();

    // The locally saved "<id>_files.xml" occupies the item root too: an
    // entry that sanitizes to that name (differing only by case on
    // case-insensitive filesystems) would silently overwrite the metadata.
    // A single-file run never saves the metadata document, so it reserves
    // nothing — the file itself may be the _files.xml entry.
    if target.file_path.is_none() {
        let xml_file_name = xml_file_name_of(base_url);
        planned_files.insert(local_path_key(xml_file_name), xml_file_name.to_string());
    }

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

        // Two entries may clash in three ways: the same local path (e.g.
        // "file:1.mp4" and "file_1.mp4" both sanitize to "file_1.mp4"), a
        // planned file where this entry needs a directory ("notes" then
        // "notes/file.txt"), or a planned directory where this entry would
        // land a file (the reverse order). Downloading both would always
        // lose one file, so keep the first entry and skip the rest.
        // The keys normalise case on case-insensitive filesystems, so
        // "Report.PDF" and "report.pdf" collide there but not on Linux.
        if let Some((at, first_name)) =
            clashing_entry(&sanitized_name, &planned_files, &planned_dirs)
        {
            warnings.push(warning_line(
                "Collision",
                format!(
                    "{} collides with {} at {} — the later entry is skipped",
                    file.name.dimmed(),
                    first_name.dimmed(),
                    at.bold()
                ),
            ));
            continue;
        }
        register_planned_path(
            &sanitized_name,
            &file.name,
            &mut planned_files,
            &mut planned_dirs,
        );

        tasks.push(DownloadTask {
            url: absolute_url.to_string(),
            // The output directory (when given) prefixes every local
            // path; the URLs are archive-absolute and stay unprefixed.
            file_path: join_output_dir(output_dir, &sanitized_name),
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

    /// The whole-item target of the "item1" fixture archives
    fn whole_item() -> ArchiveTarget {
        ArchiveTarget {
            identifier: "item1".to_string(),
            file_path: None,
        }
    }

    /// The single-file target of the "item1" fixture archives
    fn single_file(path: &str) -> ArchiveTarget {
        ArchiveTarget {
            identifier: "item1".to_string(),
            file_path: Some(path.to_string()),
        }
    }

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
        let plan = plan_download_tasks(files, &base, "", &whole_item()).expect("plan must build");

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
        let plan = plan_download_tasks(files, &base, "", &whole_item()).expect("plan must build");

        assert_eq!(plan.sanitized_count, 1, "only the colon name is sanitized");
        assert_eq!(plan.tasks.len(), 1, "the colliding entry must be skipped");
        assert_eq!(
            plan.tasks[0].url,
            "https://archive.org/download/item1/file%3A1.mp4"
        );
        assert_eq!(plan.tasks[0].file_path, "file_1.mp4");
    }

    #[test]
    fn plan_download_tasks_file_and_directory_name_clash() {
        let base = Url::parse("https://archive.org/download/item1/item1_files.xml").unwrap();

        // A planned file where a later entry needs a directory: "notes"
        // exists as a file, so "notes/file.txt" can never be created.
        let plan = plan_download_tasks(
            vec![
                xml_file("notes", Some(1)),
                xml_file("notes/file.txt", Some(2)),
            ],
            &base,
            "",
            &whole_item(),
        )
        .expect("plan must build");
        assert_eq!(
            plan.tasks.len(),
            1,
            "the entry under the planned file must be skipped"
        );
        assert_eq!(plan.tasks[0].file_path, "notes");
        assert!(
            plan.warnings.iter().any(|line| line.contains("collides")),
            "the clash must leave a warning line: {:?}",
            plan.warnings
        );

        // The reverse order: a planned directory where a later file must
        // land. "notes/file.txt" is kept, the bare "notes" is skipped.
        let plan = plan_download_tasks(
            vec![
                xml_file("notes/file.txt", Some(1)),
                xml_file("notes", Some(2)),
            ],
            &base,
            "",
            &whole_item(),
        )
        .expect("plan must build");
        assert_eq!(
            plan.tasks.len(),
            1,
            "the entry at the planned directory must be skipped"
        );
        assert_eq!(plan.tasks[0].file_path, "notes/file.txt");
    }

    #[test]
    fn plan_download_tasks_shared_directory_is_not_a_clash() {
        // Two files in the same directory share the ancestor component:
        // that is a plain directory, not a collision.
        let base = Url::parse("https://archive.org/download/item1/item1_files.xml").unwrap();
        let plan = plan_download_tasks(
            vec![xml_file("a/b.txt", Some(1)), xml_file("a/c.txt", Some(2))],
            &base,
            "",
            &whole_item(),
        )
        .expect("plan must build");
        assert_eq!(plan.tasks.len(), 2, "sibling files must both be planned");
        assert!(plan.warnings.is_empty(), "{:?}", plan.warnings);
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
        let plan = plan_download_tasks(files, &base, "", &whole_item()).expect("plan must build");

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
        let plan = plan_download_tasks(files, &base, "", &whole_item()).expect("plan must build");

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

    #[test]
    fn select_files_keeps_all_but_the_xml_entry_for_whole_items() {
        let files = vec![
            xml_file("item1_files.xml", Some(1)),
            xml_file("scan.jpg", Some(2)),
            xml_file("notes.txt", Some(3)),
        ];

        let selected =
            select_files(files, "item1_files.xml", &whole_item()).expect("selection must work");

        assert_eq!(
            selected.iter().map(|file| &file.name).collect::<Vec<_>>(),
            vec!["scan.jpg", "notes.txt"]
        );
    }

    #[test]
    fn select_files_picks_the_named_entry_for_single_file_urls() {
        // Even the _files.xml entry itself is selectable: the user asked
        // for it explicitly, so no self-reference filter applies.
        let files = vec![
            xml_file("item1_files.xml", Some(1)),
            xml_file("scan.jpg", Some(2)),
        ];

        let selected = select_files(files, "item1_files.xml", &single_file("item1_files.xml"))
            .expect("the named file must be found");

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].name, "item1_files.xml");
    }

    #[test]
    fn select_files_missing_name_is_an_error() {
        let err = select_files(
            vec![xml_file("scan.jpg", Some(2))],
            "item1_files.xml",
            &single_file("nope.bin"),
        )
        .expect_err("an unknown file must not yield an empty plan");

        assert!(
            matches!(err, IaGetError::FileNotFoundInArchive { .. }),
            "got: {err:?}"
        );
    }

    #[test]
    fn plan_download_tasks_prefixes_the_output_dir() {
        let base = Url::parse("https://archive.org/download/item1/item1_files.xml").unwrap();
        let files = vec![xml_file("a/b.txt", Some(1)), xml_file("c.txt", Some(2))];
        let plan =
            plan_download_tasks(files, &base, "out/item", &whole_item()).expect("plan must build");

        assert_eq!(plan.tasks[0].file_path, "out/item/a/b.txt");
        assert_eq!(plan.tasks[1].file_path, "out/item/c.txt");
        // The URLs are archive-absolute and unaffected by the output dir
        assert_eq!(
            plan.tasks[0].url,
            "https://archive.org/download/item1/a/b.txt"
        );
    }

    #[test]
    fn plan_download_tasks_single_file_run_does_not_reserve_the_xml_name() {
        // Requesting "<id>_files.xml" as the single file must be planned,
        // not skipped as a collision with the saved metadata.
        let base = Url::parse("https://archive.org/download/item1/item1_files.xml").unwrap();
        let plan = plan_download_tasks(
            vec![xml_file("item1_files.xml", Some(3))],
            &base,
            "",
            &single_file("item1_files.xml"),
        )
        .expect("plan must build");

        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(plan.tasks[0].file_path, "item1_files.xml");
        assert!(plan.warnings.is_empty(), "{:?}", plan.warnings);
    }

    #[test]
    fn join_output_dir_keeps_the_path_bare_without_a_directory() {
        assert_eq!(join_output_dir("", "a/b.txt"), "a/b.txt");
        assert_eq!(join_output_dir("out", "a/b.txt"), "out/a/b.txt");
        assert_eq!(join_output_dir("out/sub", "c.txt"), "out/sub/c.txt");
    }
}
