use anyhow::{anyhow, Result};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::Deserialize;
use serde_json::json;

const NOTION_VERSION: &str = "2025-09-03";

#[derive(Clone)]
pub struct NotionClient {
    client: reqwest::Client,
}

impl NotionClient {
    pub fn new(token: &str) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))?,
        );
        headers.insert("Notion-Version", HeaderValue::from_static(NOTION_VERSION));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;
        Ok(Self { client })
    }

    pub async fn fetch_blocks(&self, block_id: &str, depth: usize) -> Result<Vec<Block>> {
        let mut blocks = self.fetch_block_children(block_id).await?;
        if depth == 0 {
            return Ok(blocks);
        }

        let mut depths = vec![depth; blocks.len()];
        let mut index = 0usize;
        while index < blocks.len() {
            let remaining_depth = depths[index];
            if remaining_depth > 0 && blocks[index].has_children {
                let id = blocks[index].id.clone();
                let marker = Block::children_marker(&id);
                blocks.insert(index + 1, marker);
                depths.insert(index + 1, 0);

                let children = self.fetch_block_children(&id).await?;
                let child_depth = remaining_depth.saturating_sub(1);
                for (offset, child) in children.into_iter().enumerate() {
                    blocks.insert(index + 2 + offset, child);
                    depths.insert(index + 2 + offset, child_depth);
                }
            }
            index += 1;
        }

        Ok(blocks)
    }

    async fn fetch_block_children(&self, block_id: &str) -> Result<Vec<Block>> {
        let mut blocks = Vec::new();
        let mut cursor = None;

        loop {
            let url = format!(
                "https://api.notion.com/v1/blocks/{}/children?page_size=100{}",
                block_id,
                cursor
                    .as_ref()
                    .map(|value| format!("&start_cursor={}", value))
                    .unwrap_or_default()
            );
            let response = self.client.get(&url).send().await?;
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow!("Notion API error {status}: {body}"));
            }
            let data: BlocksResponse = response.json().await?;
            blocks.extend(data.results);
            if data.has_more {
                cursor = data.next_cursor;
            } else {
                break;
            }
        }

        Ok(blocks)
    }

    pub async fn query_database_page_ids(&self, database_id: &str) -> Result<Vec<String>> {
        let data_sources = self.fetch_database_data_sources(database_id).await?;
        let mut page_ids = Vec::new();
        for data_source in data_sources {
            let mut ids = self.query_data_source_page_ids(&data_source.id).await?;
            page_ids.append(&mut ids);
        }
        Ok(page_ids)
    }

    pub async fn query_data_source_page_ids(&self, data_source_id: &str) -> Result<Vec<String>> {
        let mut page_ids = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let url = format!("https://api.notion.com/v1/data_sources/{}/query", data_source_id);
            let mut body = json!({});
            if let Some(value) = cursor.as_ref() {
                body["start_cursor"] = json!(value);
            }
            let response = self.client.post(&url).json(&body).send().await?;
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow!("Notion API error {status}: {body}"));
            }
            let data: DataSourceQueryResponse = response.json().await?;
            page_ids.extend(data.results.into_iter().map(|page| page.id));
            if data.has_more {
                cursor = data.next_cursor;
            } else {
                break;
            }
        }

        Ok(page_ids)
    }

    pub async fn fetch_database_data_sources(
        &self,
        database_id: &str,
    ) -> Result<Vec<DataSourceInfo>> {
        let url = format!("https://api.notion.com/v1/databases/{}", database_id);
        let response = self.client.get(&url).send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Notion API error {status}: {body}"));
        }
        let data: DatabaseResponse = response.json().await?;
        Ok(data.data_sources)
    }

    pub async fn get_page_parent(&self, page_id: &str) -> Result<PageParent> {
        let url = format!("https://api.notion.com/v1/pages/{}", page_id);
        let response = self.client.get(&url).send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Notion API error {status}: {body}"));
        }
        let data: PageResponse = response.json().await?;
        Ok(PageParent {
            parent_type: data.parent.parent_type,
            database_id: data.parent.database_id,
            data_source_id: data.parent.data_source_id,
        })
    }
}

#[derive(Debug, Deserialize)]
struct BlocksResponse {
    results: Vec<Block>,
    next_cursor: Option<String>,
    has_more: bool,
}

#[derive(Debug, Deserialize)]
struct DataSourceQueryResponse {
    results: Vec<PageObject>,
    next_cursor: Option<String>,
    has_more: bool,
}

#[derive(Debug, Deserialize)]
struct PageObject {
    id: String,
}

#[derive(Debug, Deserialize)]
struct DatabaseResponse {
    data_sources: Vec<DataSourceInfo>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DataSourceInfo {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PageResponse {
    parent: Parent,
}

#[derive(Debug, Deserialize)]
struct Parent {
    #[serde(rename = "type")]
    parent_type: String,
    database_id: Option<String>,
    data_source_id: Option<String>,
}

#[derive(Debug)]
pub struct PageParent {
    pub parent_type: String,
    pub database_id: Option<String>,
    pub data_source_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Block {
    pub id: String,
    #[serde(rename = "type")]
    pub block_type: String,
    pub has_children: bool,
    pub paragraph: Option<RichTextContainer>,
    pub heading_1: Option<RichTextContainer>,
    pub heading_2: Option<RichTextContainer>,
    pub heading_3: Option<RichTextContainer>,
    pub bulleted_list_item: Option<RichTextContainer>,
    pub numbered_list_item: Option<RichTextContainer>,
    pub to_do: Option<ToDoContainer>,
    pub quote: Option<RichTextContainer>,
    pub code: Option<CodeContainer>,
    pub callout: Option<CalloutContainer>,
    pub divider: Option<EmptyContainer>,
    pub image: Option<ImageContainer>,
    pub bookmark: Option<BookmarkContainer>,
    pub toggle: Option<RichTextContainer>,
    pub equation: Option<EquationContainer>,
    pub child_page: Option<TitleContainer>,
    pub child_database: Option<TitleContainer>,
    pub table: Option<TableContainer>,
    pub table_row: Option<TableRowContainer>,
    pub file: Option<FileContainer>,
    pub pdf: Option<FileContainer>,
    pub video: Option<FileContainer>,
    pub audio: Option<FileContainer>,
    pub embed: Option<EmbedContainer>,
    pub link_to_page: Option<LinkToPageContainer>,
}

impl Block {
    pub fn children_marker(id: &str) -> Self {
        Self {
            id: format!("{}::children", id),
            block_type: "children".to_string(),
            has_children: false,
            paragraph: None,
            heading_1: None,
            heading_2: None,
            heading_3: None,
            bulleted_list_item: None,
            numbered_list_item: None,
            to_do: None,
            quote: None,
            code: None,
            callout: None,
            divider: None,
            image: None,
            bookmark: None,
            toggle: None,
            equation: None,
            child_page: None,
            child_database: None,
            table: None,
            table_row: None,
            file: None,
            pdf: None,
            video: None,
            audio: None,
            embed: None,
            link_to_page: None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct RichTextContainer {
    pub rich_text: Vec<RichText>,
}

#[derive(Debug, Deserialize)]
pub struct ToDoContainer {
    pub rich_text: Vec<RichText>,
    pub checked: bool,
}

#[derive(Debug, Deserialize)]
pub struct CodeContainer {
    pub rich_text: Vec<RichText>,
    pub language: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CalloutContainer {
    pub rich_text: Vec<RichText>,
}

#[derive(Debug, Deserialize)]
pub struct EquationContainer {
    pub expression: String,
}

#[derive(Debug, Deserialize)]
pub struct EmptyContainer {}

#[derive(Debug, Deserialize)]
pub struct BookmarkContainer {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct TitleContainer {
    pub title: String,
}

#[derive(Debug, Deserialize)]
pub struct TableContainer {
    pub table_width: usize,
    pub has_column_header: bool,
    pub has_row_header: bool,
}

#[derive(Debug, Deserialize)]
pub struct TableRowContainer {
    pub cells: Vec<Vec<RichText>>,
}

#[derive(Debug, Deserialize)]
pub struct ImageContainer {
    #[serde(default)]
    pub r#type: Option<String>,
    pub file: Option<FileObject>,
    pub external: Option<ExternalObject>,
}

#[derive(Debug, Deserialize)]
pub struct FileContainer {
    #[serde(default)]
    pub r#type: Option<String>,
    pub file: Option<FileObject>,
    pub external: Option<ExternalObject>,
    pub name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct EmbedContainer {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct LinkToPageContainer {
    #[serde(rename = "type")]
    pub link_type: String,
    pub page_id: Option<String>,
    pub database_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FileObject {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct ExternalObject {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct RichText {
    pub plain_text: String,
    #[serde(default)]
    pub annotations: Option<Annotations>,
    pub href: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Annotations {
    #[serde(default)]
    pub bold: bool,
    #[serde(default)]
    pub italic: bool,
    #[serde(default)]
    pub strikethrough: bool,
    #[serde(default)]
    pub underline: bool,
    #[serde(default)]
    pub code: bool,
}
