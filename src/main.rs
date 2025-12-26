use anyhow::{Context, Result};
use axum::{routing::post, Router};
use tokio::net::TcpListener;
use log::info;
use logforth::append;
use log::LevelFilter;

const DEFAULT_MAX_DEPTH: usize = 3;

mod config;
mod notion;
mod render;
mod storage;
mod sync;
mod webhook;

use config::AppConfig;
use notion::{DataSourceInfo, NotionClient};
use storage::init_opendal;
use webhook::handle_webhook;

#[derive(Clone)]
pub struct AppState {
    pub notion: NotionClient,
    pub max_depth: usize,
    pub webhook_secret: Option<String>,
    pub webhook_max_age_seconds: u64,
    pub databases: Vec<DatabaseState>,
    pub http: reqwest::Client,
}

#[derive(Clone)]
pub struct DatabaseState {
    pub id: String,
    pub op: opendal::Operator,
    pub data_sources: Vec<DataSourceInfo>,
    pub key_map: std::collections::BTreeMap<String, String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let config = AppConfig::load()?;
    let notion = NotionClient::new(&config.notion.api_key)?;
    let http = reqwest::Client::new();

    let mut databases = Vec::new();
    for db in &config.database {
        let backend = db
            .storage
            .first()
            .ok_or_else(|| anyhow::anyhow!("database {} has no storage", db.id))?;
        let op = init_opendal(backend)?;
        let data_sources = notion.fetch_database_data_sources(&db.id).await?;
        databases.push(DatabaseState {
            id: db.id.clone(),
            op,
            data_sources,
            key_map: db.key_map.clone(),
        });
    }

    let state = AppState {
        notion,
        max_depth: DEFAULT_MAX_DEPTH,
        webhook_secret: config.webhook.secret,
        webhook_max_age_seconds: config.webhook.max_age_seconds,
        databases,
        http,
    };

    for database in &state.databases {
        sync::sync_database(&state, database).await?;
    }

    let app = Router::new()
        .route("/webhook", post(handle_webhook))
        .with_state(state);

    let listen_addr = format!("{}:{}", config.webhook.host, config.webhook.port);
    let listener = TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("failed to bind {}", listen_addr))?;
    info!("listening on {}", listen_addr);
    axum::serve(listener, app).await?;

    Ok(())
}

fn init_logging() {
    logforth::builder()
        .dispatch(|d| {
            d.filter(LevelFilter::Error)
                .append(append::Stderr::default())
        })
        .dispatch(|d| {
            d.filter(LevelFilter::Info)
                .append(append::Stdout::default())
        })
        .apply();
}
