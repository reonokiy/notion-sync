use anyhow::{Context, Result};
use std::collections::HashSet;

use log::info;

use crate::render::{render_page, BlobRef};
use crate::{AppState, DatabaseState};

pub async fn sync_page_by_id(state: &AppState, page_id: &str) -> Result<()> {
    let parent = state
        .notion
        .get_page_parent(page_id)
        .await
        .with_context(|| format!("failed to resolve parent for {page_id}"))?;

    let data_source_id = parent.data_source_id.as_deref();
    let database = if let Some(data_source_id) = data_source_id {
        state
            .databases
            .iter()
            .find(|db| db.data_sources.iter().any(|ds| ds.id == data_source_id))
    } else if let Some(database_id) = parent.database_id.as_deref() {
        state.databases.iter().find(|db| db.id == database_id)
    } else {
        None
    };

    let Some(database) = database else {
        info!("page {} parent is not configured, skipping", page_id);
        return Ok(());
    };

    sync_page(state, database, page_id).await
}

pub async fn sync_page(state: &AppState, database: &DatabaseState, page_id: &str) -> Result<()> {
    let metadata = state
        .notion
        .get_page_metadata(page_id)
        .await
        .with_context(|| format!("failed to fetch page metadata for {page_id}"))?;
    let blocks = state
        .notion
        .fetch_blocks(page_id, state.max_depth)
        .await
        .with_context(|| format!("failed to fetch blocks for {page_id}"))?;
    let rendered = render_page(&metadata, &blocks, &database.key_map);
    let page_path = format!("pages/{}.md", page_id);
    database
        .op
        .write(&page_path, rendered.markdown)
        .await
        .with_context(|| format!("failed to write markdown to {page_path}"))?;

    sync_blobs(state, database, &rendered.blobs).await?;
    info!("synced page {} into {}", page_id, database.id);
    Ok(())
}

async fn sync_blobs(
    state: &AppState,
    database: &DatabaseState,
    blobs: &[BlobRef],
) -> Result<()> {
    let mut seen = HashSet::new();
    for blob in blobs {
        if !seen.insert(blob.path.clone()) {
            continue;
        }
        let response = state.http.get(&blob.url).send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(anyhow::anyhow!(
                "failed to download blob {}: {}",
                blob.url,
                status
            ));
        }
        let bytes = response.bytes().await?;
        database
            .op
            .write(&blob.path, bytes.to_vec())
            .await
            .with_context(|| format!("failed to write blob {}", blob.path))?;
    }
    Ok(())
}
