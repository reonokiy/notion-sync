use anyhow::{Context, Result};
use std::collections::HashSet;

use tracing::info;

use crate::render::{render_blocks, BlobRef};
use crate::{AppState, DatabaseState};

pub async fn sync_page_by_id(state: &AppState, page_id: &str) -> Result<()> {
    let database_id = state
        .notion
        .get_page_parent_database_id(page_id)
        .await
        .with_context(|| format!("failed to resolve parent database for {page_id}"))?;
    let Some(database_id) = database_id else {
        info!("page {} is not under a database, skipping", page_id);
        return Ok(());
    };

    let database = state
        .databases
        .iter()
        .find(|db| db.id == database_id);
    let Some(database) = database else {
        info!("database {} not configured, skipping", database_id);
        return Ok(());
    };

    sync_page(state, database, page_id).await
}

pub async fn sync_page(state: &AppState, database: &DatabaseState, page_id: &str) -> Result<()> {
    let blocks = state
        .notion
        .fetch_blocks(page_id, state.max_depth)
        .await
        .with_context(|| format!("failed to fetch blocks for {page_id}"))?;
    let rendered = render_blocks(&blocks);
    let page_path = format!("pages/{}.md", page_id);
    database
        .op
        .write(&page_path, rendered.markdown)
        .await
        .with_context(|| format!("failed to write markdown to {page_path}"))?;

    sync_blobs(state, database, &rendered.blobs).await?;
    info!("synced page {} into {}", page_id, database.name);
    Ok(())
}

pub async fn sync_database(state: &AppState, database: &DatabaseState) -> Result<()> {
    let page_ids = state
        .notion
        .query_database_page_ids(&database.id)
        .await
        .with_context(|| format!("failed to query database {}", database.id))?;
    for page_id in page_ids {
        sync_page(state, database, &page_id).await?;
    }
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
