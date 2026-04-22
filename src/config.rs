use std::{
    env,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{fs, io::AsyncWriteExt};
use tracing::warn;
use uuid::Uuid;

use crate::parser::Node;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub web_port: u16,
    pub admin_token: String,
    pub subscription_token: String,
    pub update_interval_minutes: u64,
    pub ping_timeout_ms: u64,
    pub max_concurrent_pings: usize,
    #[serde(default = "default_max_concurrent_tunnels")]
    pub max_concurrent_tunnels: usize,
    #[serde(default = "default_network_check_interval_minutes")]
    pub network_check_interval_minutes: u64,
    #[serde(default = "default_white_url")]
    pub white_url: String,
    #[serde(default = "default_gray_url")]
    pub gray_url: String,
    pub subscription_urls: Vec<String>,
    pub last_update: Option<DateTime<Utc>>,
    pub nodes_total: usize,
    pub nodes_after_dedup: usize,
    pub nodes_after_ping: usize,
    #[serde(default)]
    pub nodes_after_tunnel: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublicConfig {
    pub web_port: u16,
    pub subscription_token: String,
    pub update_interval_minutes: u64,
    pub ping_timeout_ms: u64,
    pub max_concurrent_pings: usize,
    pub max_concurrent_tunnels: usize,
    pub network_check_interval_minutes: u64,
    pub white_url: String,
    pub gray_url: String,
    pub subscription_urls: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdateConfigRequest {
    pub subscription_urls: Vec<String>,
    pub update_interval_minutes: u64,
    pub ping_timeout_ms: u64,
    pub max_concurrent_pings: usize,
    #[serde(default = "default_max_concurrent_tunnels")]
    pub max_concurrent_tunnels: usize,
    #[serde(default = "default_network_check_interval_minutes")]
    pub network_check_interval_minutes: u64,
    #[serde(default = "default_white_url")]
    pub white_url: String,
    #[serde(default = "default_gray_url")]
    pub gray_url: String,
}

impl AppConfig {
    pub fn new_default() -> Self {
        Self {
            web_port: 8080,
            admin_token: Uuid::new_v4().to_string(),
            subscription_token: Uuid::new_v4().to_string(),
            update_interval_minutes: 60,
            ping_timeout_ms: 3000,
            max_concurrent_pings: 100,
            max_concurrent_tunnels: 20,
            network_check_interval_minutes: default_network_check_interval_minutes(),
            white_url: default_white_url(),
            gray_url: default_gray_url(),
            subscription_urls: Vec::new(),
            last_update: None,
            nodes_total: 0,
            nodes_after_dedup: 0,
            nodes_after_ping: 0,
            nodes_after_tunnel: 0,
        }
    }

    pub fn public_view(&self) -> PublicConfig {
        PublicConfig {
            web_port: self.web_port,
            subscription_token: self.subscription_token.clone(),
            update_interval_minutes: self.update_interval_minutes,
            ping_timeout_ms: self.ping_timeout_ms,
            max_concurrent_pings: self.max_concurrent_pings,
            max_concurrent_tunnels: self.max_concurrent_tunnels,
            network_check_interval_minutes: self.network_check_interval_minutes,
            white_url: self.white_url.clone(),
            gray_url: self.gray_url.clone(),
            subscription_urls: self.subscription_urls.clone(),
        }
    }
}

fn default_max_concurrent_tunnels() -> usize {
    20
}

fn default_network_check_interval_minutes() -> u64 {
    10
}

fn default_white_url() -> String {
    "https://www.gstatic.com/generate_204".to_string()
}

fn default_gray_url() -> String {
    "https://example.com".to_string()
}

pub fn resolve_config_path(cli_arg: Option<String>) -> PathBuf {
    if let Some(path) = cli_arg.filter(|value| !value.trim().is_empty()) {
        return PathBuf::from(path);
    }

    if let Ok(path) = env::var("CONFIG_PATH") {
        if !path.trim().is_empty() {
            return PathBuf::from(path);
        }
    }

    PathBuf::from("/opt/xray-sub-manager/config.json")
}

pub fn cache_path_for(config_path: &Path) -> PathBuf {
    let parent = config_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    parent.join("subscription.cache")
}

pub async fn load_or_init_config(path: &Path) -> Result<AppConfig> {
    if fs::try_exists(path)
        .await
        .context("failed to check config path")?
    {
        let content = fs::read_to_string(path)
            .await
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let config = serde_json::from_str::<AppConfig>(&content)
            .with_context(|| format!("failed to parse config file {}", path.display()))?;
        return Ok(config);
    }

    let config = AppConfig::new_default();
    save_config_atomic(path, &config).await?;
    Ok(config)
}

pub async fn load_nodes_cache(path: &Path) -> Result<Option<Vec<Node>>> {
    if !fs::try_exists(path)
        .await
        .context("failed to check subscription cache path")?
    {
        return Ok(None);
    }

    let content = fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read cache file {}", path.display()))?;
    match serde_json::from_str::<Vec<Node>>(&content) {
        Ok(nodes) => Ok(Some(nodes)),
        Err(error) => {
            warn!(path = %path.display(), error = %error, "ignoring incompatible subscription cache");
            Ok(None)
        }
    }
}

pub async fn save_nodes_cache(path: &Path, nodes: &[Node]) -> Result<()> {
    let content = serde_json::to_vec_pretty(nodes).context("failed to serialize nodes cache")?;
    atomic_write(path, &content).await
}

pub async fn save_config_atomic(path: &Path, config: &AppConfig) -> Result<()> {
    let content = serde_json::to_vec_pretty(config).context("failed to serialize config")?;
    atomic_write(path, &content).await
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    let parent = path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let temp_path = parent.join(format!(
        ".{}.tmp-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("config"),
        Uuid::new_v4()
    ));

    let mut file = fs::File::create(&temp_path)
        .await
        .with_context(|| format!("failed to create temp file {}", temp_path.display()))?;
    file.write_all(bytes)
        .await
        .with_context(|| format!("failed to write temp file {}", temp_path.display()))?;
    file.flush()
        .await
        .with_context(|| format!("failed to flush temp file {}", temp_path.display()))?;
    file.sync_all()
        .await
        .with_context(|| format!("failed to sync temp file {}", temp_path.display()))?;
    drop(file);

    fs::rename(&temp_path, path)
        .await
        .with_context(|| format!("failed to rename temp file into {}", path.display()))?;

    Ok(())
}
