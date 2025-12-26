use anyhow::{Context, Result};
use axum::{routing::post, Router};
use tokio::net::TcpListener;
use tracing::info;

const DEFAULT_MAX_DEPTH: usize = 3;

mod config;
mod notion;
mod render;
mod storage;
mod sync;
mod webhook;

use config::AppConfig;
use notion::NotionClient;
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
    pub name: String,
    pub id: String,
    pub op: opendal::Operator,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = AppConfig::load()?;
    let notion = NotionClient::new(&config.notion.api_key)?;
    let http = reqwest::Client::new();

    let mut databases = Vec::new();
    for (name, db) in &config.database {
        let op = init_opendal(&db.backend)?;
        databases.push(DatabaseState {
            name: name.clone(),
            id: db.id.clone(),
            op,
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
