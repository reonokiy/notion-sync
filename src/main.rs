use anyhow::{Context, Result};
use axum::{routing::{get, post}, Router};
use tokio::net::TcpListener;
use log::info;
use logforth::append;
use logforth::record::{Level, LevelFilter};

const DEFAULT_MAX_DEPTH: usize = 3;

mod config;
mod notion;
mod queue;
mod render;
mod storage;
mod sync;
mod webhook;

use config::AppConfig;
use notion::{DataSourceInfo, NotionClient};
use queue::{enqueue_initial_scan, init_queue, spawn_sync_worker};
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
    pub queue: queue::QueueHandle,
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
    info!("logging initialized");

    let config = AppConfig::load()?;
    info!("configuration loaded");
    let notion = NotionClient::new(&config.notion.api_key)?;
    let http = reqwest::Client::new();
    let (queue, worker) = init_queue(&config.queue)?;
    info!("queue initialized");

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
    info!("databases initialized");

    let state = AppState {
        notion,
        max_depth: DEFAULT_MAX_DEPTH,
        webhook_secret: config.webhook.secret,
        webhook_max_age_seconds: config.webhook.max_age_seconds,
        databases,
        http,
        queue: queue.clone(),
    };

    spawn_sync_worker(state.clone(), worker, queue.clone());
    info!("sync worker spawned");
    let initial_state = state.clone();
    tokio::spawn(async move {
        enqueue_initial_scan(&initial_state).await;
    });
    info!("initial scan enqueued");

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
                .append(append::Stderr::default())
        })
        .dispatch(|d| {
            d.filter(LevelFilter::MoreSevereEqual(Level::Info))
                .append(append::Stdout::default())
        })
        .apply();
}
