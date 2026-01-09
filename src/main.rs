use anyhow::{Context, Result};
use axum::{routing::{get, post}, Router};
use tokio::net::TcpListener;
use log::info;
use logforth::append;
use logforth::layout::TextLayout;
use logforth::record::{Level, LevelFilter};
use std::collections::HashSet;

const DEFAULT_MAX_DEPTH: usize = 3;

mod config;
mod notion;
mod render;
mod scheduler;
mod storage;
mod sync;
mod webhook;

use config::AppConfig;
use notion::{DataSourceInfo, NotionClient};
use scheduler::spawn_periodic_sync;
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
    pub property_map: std::collections::BTreeMap<String, String>,
    pub property_includes: Option<HashSet<String>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    info!("logging initialized");

    let config = AppConfig::load()?;
    info!("configuration loaded");
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
        let property_map = if db.properties.map.is_empty() {
            db.key_map.clone()
        } else {
            db.properties.map.clone()
        };
        let property_includes = db
            .properties
            .filter
            .includes
            .as_ref()
            .map(|items| items.iter().cloned().collect());
        databases.push(DatabaseState {
            id: db.id.clone(),
            op,
            data_sources,
            property_map,
            property_includes,
        });
    }
    info!("databases initialized");

    let state = AppState {
        notion,
        max_depth: DEFAULT_MAX_DEPTH,
        webhook_secret: config.webhook.secret,
        webhook_max_age_seconds: config.webhook.max_age_seconds,
        databases,
        http,
    };

    spawn_periodic_sync(state.clone(), config.sync.interval_seconds);
    info!("periodic sync started");

    let app = Router::new()
        .route("/webhook", post(handle_webhook))
        .route("/health", get(health))
        .with_state(state);

    let listen_addr = format!("{}:{}", config.webhook.host, config.webhook.port);
    let listener = TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("failed to bind {}", listen_addr))?;
    info!("listening on {}", listen_addr);
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

fn init_logging() {
    logforth::starter_log::builder()
        .dispatch(|d| {
            d.filter(LevelFilter::MoreSevereEqual(Level::Error))
                .append(append::Stderr::default().with_layout(TextLayout::default()))
        })
        .dispatch(|d| {
            d.filter(LevelFilter::MoreSevereEqual(Level::Info))
                .append(append::Stdout::default().with_layout(TextLayout::default()))
        })
        .apply();
}
