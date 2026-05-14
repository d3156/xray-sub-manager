use std::{
    collections::{BTreeMap, HashSet},
    env,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{fs, io::AsyncWriteExt};
use tracing::warn;
use url::Url;
use uuid::Uuid;

use crate::parser::Node;

pub const APP_CONFIG_SCHEMA_VERSION: u32 = 1;
pub const SUBSCRIPTION_CACHE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub schema_version: u32,
    pub web_port: u16,
    pub admin_token: String,
    pub subscription_token: String,
    pub update_interval_minutes: u64,
    pub ping_timeout_ms: u64,
    pub max_concurrent_pings: usize,
    pub max_concurrent_tunnels: usize,
    pub network_check_interval_minutes: u64,
    pub white_url: String,
    pub gray_url: String,
    pub subscription_urls: Vec<String>,
    pub modems: Vec<ModemConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModemConfig {
    pub modem_tag: String,
    pub modem_interface: String,
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
    pub modems: Vec<ModemConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateConfigRequest {
    pub subscription_urls: Vec<String>,
    pub update_interval_minutes: u64,
    pub ping_timeout_ms: u64,
    pub max_concurrent_pings: usize,
    pub max_concurrent_tunnels: usize,
    pub network_check_interval_minutes: u64,
    pub white_url: String,
    pub gray_url: String,
    pub modems: Vec<ModemConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionCache {
    pub schema_version: u32,
    pub last_update: DateTime<Utc>,
    pub nodes_total: usize,
    pub nodes_after_dedup: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nodes_after_ping: Option<usize>,
    pub by_modem: BTreeMap<String, ModemCacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModemCacheEntry {
    pub modem_tag: String,
    pub modem_interface: String,
    pub last_update: DateTime<Utc>,
    pub nodes_after_ping: usize,
    pub nodes_after_tunnel: usize,
    pub nodes: Vec<Node>,
}

impl AppConfig {
    pub fn new_default() -> Self {
        Self {
            schema_version: APP_CONFIG_SCHEMA_VERSION,
            web_port: 8080,
            admin_token: Uuid::new_v4().to_string(),
            subscription_token: Uuid::new_v4().to_string(),
            update_interval_minutes: 60,
            ping_timeout_ms: 3000,
            max_concurrent_pings: 100,
            max_concurrent_tunnels: 20,
            network_check_interval_minutes: 10,
            white_url: default_white_url(),
            gray_url: default_gray_url(),
            subscription_urls: Vec::new(),
            modems: vec![ModemConfig {
                modem_tag: "default".to_string(),
                modem_interface: "wwan0".to_string(),
            }],
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
            modems: self.modems.clone(),
        }
    }

    pub fn sanitized(mut self) -> Self {
        self.sanitize();
        self
    }

    pub fn sanitize(&mut self) {
        self.white_url = self.white_url.trim().to_string();
        self.gray_url = self.gray_url.trim().to_string();
        self.subscription_urls =
            sanitize_subscription_urls(std::mem::take(&mut self.subscription_urls));
        for modem in &mut self.modems {
            modem.modem_tag = modem.modem_tag.trim().to_string();
            modem.modem_interface = modem.modem_interface.trim().to_string();
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != APP_CONFIG_SCHEMA_VERSION {
            bail!(
                "unsupported config schema_version {}, expected {}",
                self.schema_version,
                APP_CONFIG_SCHEMA_VERSION
            );
        }
        if self.update_interval_minutes == 0 {
            bail!("update_interval_minutes must be greater than zero");
        }
        if self.network_check_interval_minutes == 0 {
            bail!("network_check_interval_minutes must be greater than zero");
        }
        if self.network_check_interval_minutes >= self.update_interval_minutes {
            bail!("network_check_interval_minutes must be less than update_interval_minutes");
        }
        if self.ping_timeout_ms == 0 {
            bail!("ping_timeout_ms must be greater than zero");
        }
        if self.max_concurrent_pings == 0 {
            bail!("max_concurrent_pings must be greater than zero");
        }
        if self.max_concurrent_tunnels == 0 {
            bail!("max_concurrent_tunnels must be greater than zero");
        }

        validate_url(&self.white_url, "white_url")?;
        validate_url(&self.gray_url, "gray_url")?;
        validate_modems(&self.modems)
    }
}

impl UpdateConfigRequest {
    pub fn into_config(self, current: &AppConfig) -> AppConfig {
        AppConfig {
            schema_version: APP_CONFIG_SCHEMA_VERSION,
            web_port: current.web_port,
            admin_token: current.admin_token.clone(),
            subscription_token: current.subscription_token.clone(),
            update_interval_minutes: self.update_interval_minutes,
            ping_timeout_ms: self.ping_timeout_ms,
            max_concurrent_pings: self.max_concurrent_pings,
            max_concurrent_tunnels: self.max_concurrent_tunnels,
            network_check_interval_minutes: self.network_check_interval_minutes,
            white_url: self.white_url,
            gray_url: self.gray_url,
            subscription_urls: self.subscription_urls,
            modems: self.modems,
        }
        .sanitized()
    }
}

impl SubscriptionCache {
    pub fn empty(
        now: DateTime<Utc>,
        nodes_total: usize,
        nodes_after_dedup: usize,
        nodes_after_ping: usize,
    ) -> Self {
        Self {
            schema_version: SUBSCRIPTION_CACHE_SCHEMA_VERSION,
            last_update: now,
            nodes_total,
            nodes_after_dedup,
            nodes_after_ping: Some(nodes_after_ping),
            by_modem: BTreeMap::new(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != SUBSCRIPTION_CACHE_SCHEMA_VERSION {
            bail!(
                "unsupported cache schema_version {}, expected {}",
                self.schema_version,
                SUBSCRIPTION_CACHE_SCHEMA_VERSION
            );
        }

        for (tag, entry) in &self.by_modem {
            if tag != &entry.modem_tag {
                bail!("cache by_modem key {tag} does not match entry modem_tag");
            }
        }

        Ok(())
    }

    pub fn retain_configured_modems(&mut self, modems: &[ModemConfig]) {
        let configured_tags = modems
            .iter()
            .map(|modem| modem.modem_tag.as_str())
            .collect::<HashSet<_>>();
        self.by_modem
            .retain(|tag, _| configured_tags.contains(tag.as_str()));
    }
}

fn default_white_url() -> String {
    "https://www.gstatic.com/generate_204".to_string()
}

fn default_gray_url() -> String {
    "https://example.com".to_string()
}

fn sanitize_subscription_urls(urls: Vec<String>) -> Vec<String> {
    urls.into_iter()
        .map(|url| url.trim().to_string())
        .filter(|url| !url.is_empty())
        .collect()
}

fn validate_url(value: &str, field: &str) -> Result<()> {
    let url = Url::parse(value).with_context(|| format!("{field} must be a valid URL"))?;
    if url.scheme().is_empty() || url.host_str().is_none() {
        return Err(anyhow!("{field} must include scheme and host"));
    }
    Ok(())
}

pub fn validate_modems(modems: &[ModemConfig]) -> Result<()> {
    if modems.is_empty() {
        bail!("at least one modem must be configured");
    }

    let mut tags = HashSet::new();
    for modem in modems {
        if modem.modem_tag.is_empty() {
            bail!("modem_tag must not be empty");
        }
        if !modem
            .modem_tag
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
        {
            bail!(
                "modem_tag {} contains unsupported characters",
                modem.modem_tag
            );
        }
        if !tags.insert(modem.modem_tag.as_str()) {
            bail!("duplicate modem_tag {}", modem.modem_tag);
        }
        if modem.modem_interface.is_empty() {
            bail!("modem_interface for {} must not be empty", modem.modem_tag);
        }
    }

    Ok(())
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
        let mut config = serde_json::from_str::<AppConfig>(&content)
            .with_context(|| format!("failed to parse config file {}", path.display()))?;
        config.sanitize();
        config
            .validate()
            .with_context(|| format!("invalid config file {}", path.display()))?;
        return Ok(config);
    }

    let config = AppConfig::new_default();
    config.validate()?;
    save_config_atomic(path, &config).await?;
    Ok(config)
}

pub async fn load_subscription_cache(path: &Path) -> Result<Option<SubscriptionCache>> {
    if !fs::try_exists(path)
        .await
        .context("failed to check subscription cache path")?
    {
        return Ok(None);
    }

    let content = fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read cache file {}", path.display()))?;
    match serde_json::from_str::<SubscriptionCache>(&content) {
        Ok(cache) => match cache.validate() {
            Ok(()) => Ok(Some(cache)),
            Err(error) => {
                warn!(path = %path.display(), error = %error, "ignoring invalid subscription cache");
                Ok(None)
            }
        },
        Err(error) => {
            warn!(path = %path.display(), error = %error, "ignoring incompatible subscription cache");
            Ok(None)
        }
    }
}

pub async fn save_subscription_cache(path: &Path, cache: &SubscriptionCache) -> Result<()> {
    cache.validate()?;
    let content =
        serde_json::to_vec_pretty(cache).context("failed to serialize subscription cache")?;
    atomic_write(path, &content).await
}

pub async fn save_config_atomic(path: &Path, config: &AppConfig) -> Result<()> {
    config.validate()?;
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
