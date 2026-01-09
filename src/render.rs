use crate::notion::{Block, FileContainer, PageMetadata, PropertyValue, RichText, RichTextContainer};
use serde_yaml::{Mapping, Value as YamlValue};
use std::collections::{BTreeMap, HashSet};

pub struct Rendered {
    pub markdown: String,
    pub blobs: Vec<BlobRef>,
}

#[derive(Clone, Debug)]
pub struct BlobRef {
    pub path: String,
    pub url: String,
}

pub fn render_page(
    metadata: &PageMetadata,
    blocks: &[Block],
    key_map: &BTreeMap<String, String>,
    property_includes: Option<&HashSet<String>>,
) -> Rendered {
    let mut out = String::new();
    let mut numbering = 1usize;
    let mut table_state: Option<TableState> = None;
    let mut blobs: Vec<BlobRef> = Vec::new();

    let mut front_matter = Mapping::new();
    let mut notion_meta = Mapping::new();
    notion_meta.insert(
        YamlValue::String("page_id".to_string()),
        YamlValue::String(metadata.id.clone()),
    );
    if let Some(database_id) = metadata.parent.database_id.as_ref() {
        notion_meta.insert(
            YamlValue::String("database_id".to_string()),
            YamlValue::String(database_id.clone()),
        );
    }
    front_matter.insert(YamlValue::String("_notion".to_string()), YamlValue::Mapping(notion_meta));
    for (key, value) in &metadata.properties {
        if let Some(includes) = property_includes {
            if !includes.contains(key) {
                continue;
            }
        }
        let mapped_key = key_map.get(key).map(|v| v.as_str()).unwrap_or(key);
        if mapped_key.is_empty() {
            continue;
        }
        let yaml_value = match value {
            PropertyValue::Text(value) => YamlValue::String(value.clone()),
            PropertyValue::List(values) => YamlValue::Sequence(
                values
                    .iter()
                    .map(|item| YamlValue::String(item.clone()))
                    .collect(),
            ),
        };
        front_matter.insert(YamlValue::String(mapped_key.to_string()), yaml_value);
    }
    let yaml = serde_yaml::to_string(&front_matter).unwrap_or_default();
    let yaml = yaml.strip_prefix("---\n").unwrap_or(&yaml);
    out.push_str("---\n");
    out.push_str(yaml);
    if !yaml.ends_with('\n') {
        out.push('\n');
    }
    out.push_str("---\n\n");

    for block in blocks {
        if table_state.is_some()
            && block.block_type != "table_row"
            && block.block_type != "children"
        {
            flush_table(&mut out, table_state.take());
        }

        match block.block_type.as_str() {
            "paragraph" => {
                if let Some(text) = block.paragraph.as_ref().map(render_rich_text) {
                    out.push_str(&text);
                    out.push_str("\n\n");
                }
            }
            "heading_1" => {
                if let Some(text) = block.heading_1.as_ref().map(render_rich_text) {
                    out.push_str("# ");
                    out.push_str(&text);
                    out.push_str("\n\n");
                }
            }
            "heading_2" => {
                if let Some(text) = block.heading_2.as_ref().map(render_rich_text) {
                    out.push_str("## ");
                    out.push_str(&text);
                    out.push_str("\n\n");
                }
            }
            "heading_3" => {
                if let Some(text) = block.heading_3.as_ref().map(render_rich_text) {
                    out.push_str("### ");
                    out.push_str(&text);
                    out.push_str("\n\n");
                }
            }
            "bulleted_list_item" => {
                if let Some(text) = block.bulleted_list_item.as_ref().map(render_rich_text) {
                    out.push_str("- ");
                    out.push_str(&text);
                    out.push('\n');
                }
            }
            "numbered_list_item" => {
                if let Some(text) = block.numbered_list_item.as_ref().map(render_rich_text) {
                    out.push_str(&format!("{}. {}\n", numbering, text));
                    numbering += 1;
                }
            }
            "to_do" => {
                if let Some(todo) = block.to_do.as_ref() {
                    let mark = if todo.checked { "x" } else { " " };
                    out.push_str(&format!(
                        "- [{}] {}\n",
                        mark,
                        render_rich_text_vec(&todo.rich_text)
                    ));
                }
            }
            "quote" => {
                if let Some(text) = block.quote.as_ref().map(render_rich_text) {
                    out.push_str("> ");
                    out.push_str(&text);
                    out.push_str("\n\n");
                }
            }
            "code" => {
                if let Some(code) = block.code.as_ref() {
                    let lang = code.language.as_deref().unwrap_or("");
                    out.push_str(&format!("```{}\n", lang));
                    out.push_str(&render_rich_text_vec(&code.rich_text));
                    out.push_str("\n```\n\n");
                }
            }
            "callout" => {
                if let Some(callout) = block.callout.as_ref() {
                    let text = render_rich_text_vec(&callout.rich_text);
                    out.push_str("> [!NOTE]\n> ");
                    out.push_str(&text);
                    out.push_str("\n\n");
                }
            }
            "divider" => {
                out.push_str("---\n\n");
            }
            "image" => {
                if let Some(image) = block.image.as_ref()
                    && let Some(url) = image
                        .file
                        .as_ref()
                        .map(|file| file.url.clone())
                        .or_else(|| image.external.as_ref().map(|ext| ext.url.clone()))
                {
                    let blob_path = build_blob_path(&block.id, None, Some(&url));
                    blobs.push(BlobRef {
                        path: blob_path.clone(),
                        url,
                    });
                    out.push_str(&format!("![]({})\n\n", format_blob_link(&blob_path)));
                }
            }
            "bookmark" => {
                if let Some(bookmark) = block.bookmark.as_ref() {
                    out.push_str(&format!("[{}]({})\n\n", bookmark.url, bookmark.url));
                }
            }
            "toggle" => {
                if let Some(text) = block.toggle.as_ref().map(render_rich_text) {
                    out.push_str(&format!("> **Toggle:** {}\n\n", text));
                }
            }
            "equation" => {
                if let Some(eq) = block.equation.as_ref() {
                    out.push_str(&format!("$$\n{}\n$$\n\n", eq.expression));
                }
            }
            "child_page" => {
                if let Some(child) = block.child_page.as_ref() {
                    out.push_str(&format!("- [Page] {}\n\n", child.title));
                }
            }
            "child_database" => {
                if let Some(child) = block.child_database.as_ref() {
                    out.push_str(&format!("- [Database] {}\n\n", child.title));
                }
            }
            "table" => {
                if let Some(table) = block.table.as_ref() {
                    table_state = Some(TableState::new(
                        table.table_width,
                        table.has_column_header,
                        table.has_row_header,
                    ));
                }
            }
            "table_row" => {
                if let (Some(row), Some(state)) =
                    (block.table_row.as_ref(), table_state.as_mut())
                {
                    let cells = row
                        .cells
                        .iter()
                        .map(|cell| render_rich_text_vec(cell))
                        .collect::<Vec<_>>();
                    state.rows.push(cells);
                }
            }
            "file" => {
                if let Some(link) = render_file_link(block.file.as_ref(), &block.id) {
                    blobs.push(BlobRef {
                        path: link.path.clone(),
                        url: link.url,
                    });
                    out.push_str(&format!(
                        "[{}]({})\n\n",
                        link.label,
                        format_blob_link(&link.path)
                    ));
                }
            }
            "pdf" => {
                if let Some(link) = render_file_link(block.pdf.as_ref(), &block.id) {
                    blobs.push(BlobRef {
                        path: link.path.clone(),
                        url: link.url,
                    });
                    out.push_str(&format!(
                        "[{}]({})\n\n",
                        link.label,
                        format_blob_link(&link.path)
                    ));
                }
            }
            "video" => {
                if let Some(link) = render_file_link(block.video.as_ref(), &block.id) {
                    blobs.push(BlobRef {
                        path: link.path.clone(),
                        url: link.url,
                    });
                    out.push_str(&format!(
                        "[{}]({})\n\n",
                        link.label,
                        format_blob_link(&link.path)
                    ));
                }
            }
            "audio" => {
                if let Some(link) = render_file_link(block.audio.as_ref(), &block.id) {
                    blobs.push(BlobRef {
                        path: link.path.clone(),
                        url: link.url,
                    });
                    out.push_str(&format!(
                        "[{}]({})\n\n",
                        link.label,
                        format_blob_link(&link.path)
                    ));
                }
            }
            "embed" => {
                if let Some(embed) = block.embed.as_ref() {
                    out.push_str(&format!("[Embed]({})\n\n", embed.url));
                }
            }
            "link_to_page" => {
                if let Some(link) = block.link_to_page.as_ref() {
                    let target = link
                        .page_id
                        .as_deref()
                        .or(link.database_id.as_deref())
                        .unwrap_or("unknown");
                    out.push_str(&format!("[Link] {}\n\n", target));
                }
            }
            "children" => {
                out.push('\n');
                numbering = 1;
            }
            _ => {}
        }

        if block.block_type != "numbered_list_item" {
            numbering = 1;
        }
    }

    if table_state.is_some() {
        flush_table(&mut out, table_state.take());
    }

    Rendered {
        markdown: out,
        blobs,
    }
}

fn render_rich_text(container: &RichTextContainer) -> String {
    render_rich_text_vec(&container.rich_text)
}

fn render_rich_text_vec(rich_text: &[RichText]) -> String {
    rich_text
        .iter()
        .map(render_rich_text_item)
        .collect::<Vec<_>>()
        .join("")
}

fn render_rich_text_item(item: &RichText) -> String {
    let mut text = item.plain_text.clone();
    if let Some(annotations) = item.annotations.as_ref() {
        if annotations.code {
            text = format!("`{}`", text);
        } else {
            if annotations.bold {
                text = format!("**{}**", text);
            }
            if annotations.italic {
                text = format!("*{}*", text);
            }
            if annotations.strikethrough {
                text = format!("~~{}~~", text);
            }
            if annotations.underline {
                text = format!("<u>{}</u>", text);
            }
        }
    }

    if let Some(href) = item.href.as_ref() {
        text = format!("[{}]({})", text, href);
    }

    text
}

struct TableState {
    rows: Vec<Vec<String>>,
    width: usize,
    has_column_header: bool,
    has_row_header: bool,
}

impl TableState {
    fn new(width: usize, has_column_header: bool, has_row_header: bool) -> Self {
        Self {
            rows: Vec::new(),
            width,
            has_column_header,
            has_row_header,
        }
    }
}

struct FileLink {
    label: String,
    url: String,
    path: String,
}

fn render_file_link(container: Option<&FileContainer>, block_id: &str) -> Option<FileLink> {
    let container = container?;
    let url = container
        .file
        .as_ref()
        .map(|file| file.url.clone())
        .or_else(|| container.external.as_ref().map(|ext| ext.url.clone()))?;
    let label = container
        .name
        .clone()
        .unwrap_or_else(|| "File".to_string());
    let path = build_blob_path(block_id, container.name.as_deref(), Some(&url));
    Some(FileLink { label, url, path })
}

fn build_blob_path(block_id: &str, name: Option<&str>, url: Option<&str>) -> String {
    let ext = name
        .and_then(extract_extension_from_name)
        .or_else(|| url.and_then(extract_extension_from_url))
        .unwrap_or_else(|| "bin".to_string());
    format!("blobs/{}.{}", block_id, ext)
}

fn extract_extension_from_name(name: &str) -> Option<String> {
    let trimmed = name.trim();
    let (_, ext) = trimmed.rsplit_once('.')?;
    if ext.is_empty() {
        None
    } else {
        Some(ext.to_lowercase())
    }
}

fn extract_extension_from_url(url: &str) -> Option<String> {
    let without_query = url.split('?').next().unwrap_or(url);
    let without_fragment = without_query.split('#').next().unwrap_or(without_query);
    let filename = without_fragment.rsplit('/').next().unwrap_or(without_fragment);
    extract_extension_from_name(filename)
}

fn format_blob_link(path: &str) -> String {
    format!("../{}", path)
}

fn flush_table(out: &mut String, state: Option<TableState>) {
    let Some(state) = state else { return };
    if state.rows.is_empty() {
        return;
    }

    let width = state
        .rows
        .iter()
        .map(|row| row.len())
        .max()
        .unwrap_or(state.width)
        .max(state.width);

    let mut rows = state.rows;
    for row in rows.iter_mut() {
        while row.len() < width {
            row.push(String::new());
        }
    }

    let (header, body_start) = if state.has_column_header {
        (rows[0].clone(), 1)
    } else {
        (vec![String::new(); width], 0)
    };

    let header = header
        .into_iter()
        .map(|cell| if cell.is_empty() { " ".to_string() } else { cell })
        .collect::<Vec<_>>();

    out.push('|');
    out.push_str(&header.join(" | "));
    out.push_str(" |\n|");
    out.push_str(&vec!["---"; width].join(" | "));
    out.push_str(" |\n");

    for row in rows.into_iter().skip(body_start) {
        let mut row_cells = row;
        if state.has_row_header && !row_cells.is_empty() {
            row_cells[0] = format!("**{}**", row_cells[0]);
        }
        out.push('|');
        out.push_str(&row_cells.join(" | "));
        out.push_str(" |\n");
    }
    out.push('\n');
}
