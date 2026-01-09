use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use log::{error, info};

use crate::{sync, AppState};

pub async fn handle_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let payload: Value = match serde_json::from_slice(&body) {
        Ok(payload) => payload,
        Err(err) => {
            error!("failed to parse webhook payload: {err}");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    if let Some(verification_token) = payload
        .get("verification_token")
        .and_then(|value| value.as_str())
    {
        info!("received notion verification token: {}", verification_token);
        return (StatusCode::OK, Json(json!({ "ok": true }))).into_response();
    }

    if let Some(secret) = state.webhook_secret.as_deref()
        && let Err(err) = verify_signature(&headers, &body, secret)
    {
        error!("webhook signature verification failed: {err}");
        return StatusCode::UNAUTHORIZED.into_response();
    }

    if let Some(event_time) = extract_event_time(&payload) {
        let now = OffsetDateTime::now_utc();
        let age = if now >= event_time {
            now - event_time
        } else {
            event_time - now
        };
        if age.as_seconds_f64() > state.webhook_max_age_seconds as f64 {
            info!("dropping stale webhook event: {}", event_time);
            return StatusCode::OK.into_response();
        }
    }

    if let Some(page_id) = extract_page_id(&payload) {
        spawn_sync(&state, move |state| async move {
            if let Err(err) = sync::sync_page_by_id(&state, &page_id).await {
                error!("page sync failed {}: {err}", page_id);
            }
        });
        return StatusCode::OK.into_response();
    }

    if let Some(data_source_id) = extract_data_source_id(&payload) {
        let database = state.databases.iter().find(|db| {
            db.data_sources.iter().any(|ds| ds.id == data_source_id)
        });
        let Some(database) = database else {
            info!("data source {} not configured, skipping", data_source_id);
            return StatusCode::OK.into_response();
        };
        let database = database.clone();
        spawn_sync(&state, move |state| async move {
            if let Err(err) = sync::scan_data_source(&state, &database, &data_source_id).await {
                error!("data source scan failed {}: {err}", data_source_id);
            }
        });
        return StatusCode::OK.into_response();
    }

    if let Some(database_id) = extract_database_id(&payload) {
        let database = state.databases.iter().find(|db| db.id == database_id);
        let Some(database) = database else {
            info!("database {} not configured, skipping", database_id);
            return StatusCode::OK.into_response();
        };
        let database = database.clone();
        spawn_sync(&state, move |state| async move {
            if let Err(err) = sync::scan_database(&state, &database).await {
                error!("database scan failed {}: {err}", database.id);
            }
        });
        return StatusCode::OK.into_response();
    }

    StatusCode::BAD_REQUEST.into_response()
}

fn extract_page_id(payload: &Value) -> Option<String> {
    if let Some(page_id) = payload.get("page_id").and_then(|v| v.as_str()) {
        return Some(page_id.to_string());
    }

    payload
        .get("data")
        .and_then(|data| data.get("id"))
        .and_then(|id| id.as_str())
        .map(|value| value.to_string())
}

fn spawn_sync<F, Fut>(state: &AppState, handler: F)
where
    F: FnOnce(AppState) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let state = state.clone();
    tokio::spawn(handler(state));
}

fn extract_database_id(payload: &Value) -> Option<String> {
    if let Some(database_id) = payload.get("database_id").and_then(|v| v.as_str()) {
        return Some(database_id.to_string());
    }

    payload
        .get("data")
        .and_then(|data| data.get("database_id"))
        .and_then(|id| id.as_str())
        .map(|value| value.to_string())
        .or_else(|| {
            payload
                .get("data")
                .and_then(|data| data.get("parent"))
                .and_then(|parent| parent.get("database_id"))
                .and_then(|id| id.as_str())
                .map(|value| value.to_string())
        })
}

fn extract_data_source_id(payload: &Value) -> Option<String> {
    if let Some(data_source_id) = payload.get("data_source_id").and_then(|v| v.as_str()) {
        return Some(data_source_id.to_string());
    }

    payload
        .get("data")
        .and_then(|data| data.get("data_source_id"))
        .and_then(|id| id.as_str())
        .map(|value| value.to_string())
        .or_else(|| {
            payload
                .get("data")
                .and_then(|data| data.get("parent"))
                .and_then(|parent| parent.get("data_source_id"))
                .and_then(|id| id.as_str())
                .map(|value| value.to_string())
        })
}

fn verify_signature(headers: &HeaderMap, body: &[u8], secret: &str) -> anyhow::Result<()> {
    let signature = headers
        .get("x-notion-signature")
        .ok_or_else(|| anyhow::anyhow!("missing X-Notion-Signature header"))?
        .to_str()?
        .trim()
        .to_string();

    let signature = signature
        .strip_prefix("sha256=")
        .unwrap_or(signature.as_str());
    let signature_bytes = hex::decode(signature)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
    mac.update(body);
    mac.verify_slice(&signature_bytes)
        .map_err(|_| anyhow::anyhow!("signature mismatch"))?;
    Ok(())
}

fn extract_event_time(payload: &Value) -> Option<OffsetDateTime> {
    let timestamp = payload.get("timestamp")?.as_str()?;
    OffsetDateTime::parse(timestamp, &Rfc3339).ok()
}
