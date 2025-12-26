use anyhow::{anyhow, Result};
use figment::{
    providers::{Env, Format, Serialized, Toml, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AppConfig {
    pub notion: NotionConfig,
    #[serde(default)]
    pub webhook: WebhookConfig,
    #[serde(default)]
    pub database: BTreeMap<String, DatabaseConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NotionConfig {
    pub api_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WebhookConfig {
    #[serde(default = "default_webhook_host")]
    pub host: String,
    #[serde(default = "default_webhook_port")]
    pub port: u16,
    #[serde(default)]
    pub secret: Option<String>,
    #[serde(default = "default_webhook_max_age_seconds")]
    pub max_age_seconds: u64,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            host: default_webhook_host(),
            port: default_webhook_port(),
            secret: None,
            max_age_seconds: default_webhook_max_age_seconds(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub id: String,
    #[serde(alias = "storage")]
    pub backend: BackendConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackendConfig {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(flatten)]
    pub settings: BTreeMap<String, Value>,
}

impl BackendConfig {
    pub fn settings_as_strings(&self) -> BTreeMap<String, String> {
        self.settings
            .iter()
            .filter_map(|(key, value)| value_to_string(value).map(|v| (key.clone(), v)))
            .collect()
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

impl Default for NotionConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
        }
    }
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let figment = Figment::from(Serialized::defaults(AppConfig::default()))
            .merge(Toml::file("config.toml"))
            .merge(Yaml::file("config.yaml"))
            .merge(Yaml::file("config.yml"))
            .merge(Env::raw().split("__").lowercase(true));
        let config: AppConfig = figment.extract()?;
        if config.notion.api_key.trim().is_empty() {
            return Err(anyhow!("notion.api_key is required"));
        }
        if config.database.is_empty() {
            return Err(anyhow!("at least one database entry is required"));
        }
        Ok(config)
    }
}

fn default_webhook_host() -> String {
    "0.0.0.0".to_string()
}

fn default_webhook_port() -> u16 {
    3000
}

fn default_webhook_max_age_seconds() -> u64 {
    300
}
