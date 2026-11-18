use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

/// Replace preferences only after the complete new document has reached disk.
/// NamedTempFile::persist replaces existing files atomically on Windows and Unix.
pub fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("設定檔沒有上層目錄")?;
    std::fs::create_dir_all(parent).context("建立設定目錄失敗")?;
    let mut file = tempfile::NamedTempFile::new_in(parent).context("建立設定暫存檔失敗")?;
    serde_json::to_writer_pretty(file.as_file_mut(), value).context("寫入設定失敗")?;
    file.as_file().sync_all().context("同步設定失敗")?;
    file.persist(path).context("替換設定檔失敗")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::write_json;

    #[test]
    fn failed_serialization_preserves_preferences_and_existing_files_can_be_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("preferences.json");
        let original = br#"{"theme":"dark"}"#;
        std::fs::write(&path, original).unwrap();
        let invalid = std::collections::BTreeMap::from([(vec![1u8, 2], 1u8)]);
        assert!(write_json(&path, &invalid).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
        let updated = serde_json::json!({"theme": "light"});
        write_json(&path, &updated).unwrap();
        let stored: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(stored, updated);
    }
}
