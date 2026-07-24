use crate::persistence::Persistence;
use anyhow::{Context, Result, anyhow};
use serde_json::{Map, Value, json};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

const INDEX_PAGE_SIZE: usize = 1_000;

pub(super) async fn write_target_index(
    persistence: &Persistence,
    tenant_id: i64,
    bucket: &str,
    prefix: &str,
    source_repo: &str,
    revision: &str,
    directory: &Path,
) -> Result<PathBuf> {
    let path = directory.join("anvil-index.json");
    let mut output = tokio::fs::File::create(&path)
        .await
        .with_context(|| format!("create {}", path.display()))?;
    output.write_all(b"{\n  \"files\": {").await?;

    let mut cursor = None;
    let mut first = true;
    let mut total_files = 0_u64;
    let mut total_bytes = 0_u64;
    loop {
        let page = persistence
            .hf_list_stored_target_item_page(
                tenant_id,
                bucket,
                prefix,
                cursor.as_deref(),
                INDEX_PAGE_SIZE,
            )
            .await?;
        for item in page.items {
            if !first {
                output.write_all(b",").await?;
            }
            first = false;
            output.write_all(b"\n    ").await?;
            output.write_all(&serde_json::to_vec(&item.path)?).await?;
            output.write_all(b": ").await?;

            let mut metadata = Map::new();
            if let Some(size) = item.size {
                let size = u64::try_from(size)
                    .map_err(|_| anyhow!("stored HuggingFace item size must be non-negative"))?;
                total_bytes = total_bytes
                    .checked_add(size)
                    .ok_or_else(|| anyhow!("HuggingFace index total byte count overflow"))?;
                metadata.insert("size".to_string(), Value::from(size));
            }
            if let Some(etag) = item.etag {
                metadata.insert("etag".to_string(), Value::from(etag));
            }
            if let Some(finished_at) = item.finished_at {
                metadata.insert(
                    "last_modified".to_string(),
                    Value::from(finished_at.to_rfc3339()),
                );
            }
            output.write_all(&serde_json::to_vec(&metadata)?).await?;
            total_files = total_files
                .checked_add(1)
                .ok_or_else(|| anyhow!("HuggingFace index file count overflow"))?;
        }

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        if cursor
            .as_ref()
            .is_some_and(|current| current >= &next_cursor)
        {
            return Err(anyhow!("HuggingFace target index cursor did not advance"));
        }
        cursor = Some(next_cursor);
    }

    let metadata = json!({
        "source_repo": source_repo,
        "revision": revision,
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "total_files": total_files,
        "total_bytes": total_bytes,
    });
    output.write_all(b"\n  },\n  \"meta\": ").await?;
    output.write_all(&serde_json::to_vec(&metadata)?).await?;
    output.write_all(b"\n}\n").await?;
    output.flush().await?;
    output.sync_all().await?;
    Ok(path)
}
