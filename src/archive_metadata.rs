use crate::constants::XML_DEBUG_TRUNCATE_LEN;
use crate::{downloader, IaGetError, Result};
use serde::Deserialize;
use serde_xml_rs::from_str;
use std::path::Path;
use std::time::SystemTime;

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
#[derive(Deserialize, Debug)]
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

/// Builds a truncated preview of XML content for error messages: at most
/// `XML_DEBUG_TRUNCATE_LEN` characters, suffixed with `...` when truncated.
fn content_preview(xml_content: &str) -> String {
    if xml_content.len() > XML_DEBUG_TRUNCATE_LEN {
        format!("{}...", &xml_content[..XML_DEBUG_TRUNCATE_LEN])
    } else {
        xml_content.to_string()
    }
}

/// Parses XML content into XmlFiles structure with improved error context
///
/// # Arguments
/// * `xml_content` - Raw XML content string from archive.org
///
/// # Returns
/// * `Ok(XmlFiles)` if parsing succeeds
/// * `Err(IaGetError)` with context if parsing fails
pub fn parse_xml_files(xml_content: &str) -> Result<XmlFiles> {
    from_str(xml_content).map_err(|e| {
        IaGetError::XmlParsing(format!(
            "Failed to parse _files.xml metadata: {}. Content preview: {}",
            e,
            content_preview(xml_content)
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
    std::fs::write(path, content)?;

    if let Some(target) = last_modified {
        downloader::sync_file_mtime(path, target);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{mtime_of, temp_dir_for};
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
    }

    #[test]
    fn save_xml_metadata_overwrites_existing_file() {
        let dir = temp_dir_for("save_xml_overwrites");
        let path = dir.join("item1_files.xml");
        std::fs::write(&path, "stale content").unwrap();

        save_xml_metadata(&path, "<files/>", None).unwrap();

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "<files/>");
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
    }
}
