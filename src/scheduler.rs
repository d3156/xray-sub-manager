use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::Serialize;
use tokio::{
    process::Command,
    sync::{broadcast, mpsc, Mutex, RwLock},
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout, Duration},
};
use tracing::{error, info, warn};

use crate::{
    config::{self, AppConfig, ModemCacheEntry, ModemConfig, SubscriptionCache},
    dedup::dedup_nodes,
    fetcher::fetch_all,
    parser::{self, Node},
    pinger::{ping_nodes_via_interface, validate_interface_binding, ProgressCallback},
    renamer::rename_nodes,
    tunnel::probe_tunnels,
};

#[derive(Clone)]
pub struct SharedState {
    pub config_path: PathBuf,
    pub subscription_cache_path: PathBuf,
    pub config: Arc<RwLock<AppConfig>>,
    pub subscription_cache: Arc<RwLock<Option<SubscriptionCache>>>,
    pub health_state: Arc<RwLock<BTreeMap<String, ModemHealthState>>>,
    pub update_progress: Arc<RwLock<UpdateProgress>>,
    pub next_update: Arc<RwLock<Option<DateTime<Utc>>>>,
    pub is_updating: Arc<AtomicBool>,
    pub update_lock: Arc<Mutex<()>>,
    pub health_check_lock: Arc<Mutex<()>>,
    pub events_tx: broadcast::Sender<UpdateEvent>,
    pub http_client: reqwest::Client,
}

impl SharedState {
    pub fn new(
        config_path: PathBuf,
        subscription_cache_path: PathBuf,
        config: AppConfig,
        cached_subscription: Option<SubscriptionCache>,
        events_tx: broadcast::Sender<UpdateEvent>,
        http_client: reqwest::Client,
    ) -> Self {
        let mut cached_subscription = cached_subscription;
        if let Some(cache) = cached_subscription.as_mut() {
            cache.retain_configured_modems(&config.modems);
        }

        let next_update = cached_subscription
            .as_ref()
            .map(|cache| {
                cache.last_update + ChronoDuration::minutes(config.update_interval_minutes as i64)
            })
            .or_else(|| Some(Utc::now()));
        let next_health_check = Some(Utc::now());

        Self {
            config_path,
            subscription_cache_path,
            health_state: Arc::new(RwLock::new(initial_health_state(
                &config,
                next_health_check,
            ))),
            update_progress: Arc::new(RwLock::new(UpdateProgress::idle_for_config(&config))),
            config: Arc::new(RwLock::new(config)),
            subscription_cache: Arc::new(RwLock::new(cached_subscription)),
            next_update: Arc::new(RwLock::new(next_update)),
            is_updating: Arc::new(AtomicBool::new(false)),
            update_lock: Arc::new(Mutex::new(())),
            health_check_lock: Arc::new(Mutex::new(())),
            events_tx,
            http_client,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UpdateStage {
    Idle,
    Fetch,
    Dedup,
    Modems,
    Complete,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModemStage {
    Pending,
    Health,
    Ping,
    Tunnel,
    Complete,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
pub struct UpdateProgress {
    pub is_updating: bool,
    pub started_at: Option<DateTime<Utc>>,
    pub stage: UpdateStage,
    pub nodes_total: usize,
    pub nodes_after_dedup: usize,
    pub modems: BTreeMap<String, ModemProgress>,
}

impl UpdateProgress {
    fn idle_for_config(config: &AppConfig) -> Self {
        Self {
            is_updating: false,
            started_at: None,
            stage: UpdateStage::Idle,
            nodes_total: 0,
            nodes_after_dedup: 0,
            modems: initial_modem_progress(&config.modems),
        }
    }

    fn started_for_config(config: &AppConfig) -> Self {
        Self {
            is_updating: true,
            started_at: Some(Utc::now()),
            stage: UpdateStage::Fetch,
            nodes_total: 0,
            nodes_after_dedup: 0,
            modems: initial_modem_progress(&config.modems),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ModemProgress {
    pub modem_tag: String,
    pub modem_interface: String,
    pub stage: ModemStage,
    pub ping_done: usize,
    pub ping_total: usize,
    pub nodes_after_ping: usize,
    pub tunnel_done: usize,
    pub tunnel_total: usize,
    pub nodes_after_tunnel: usize,
    pub last_error: Option<String>,
}

impl ModemProgress {
    fn pending(modem: &ModemConfig) -> Self {
        Self {
            modem_tag: modem.modem_tag.clone(),
            modem_interface: modem.modem_interface.clone(),
            stage: ModemStage::Pending,
            ping_done: 0,
            ping_total: 0,
            nodes_after_ping: 0,
            tunnel_done: 0,
            tunnel_total: 0,
            nodes_after_tunnel: 0,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ModemHealthState {
    pub modem_tag: String,
    pub modem_interface: String,
    pub last_check: Option<DateTime<Utc>>,
    pub next_check: Option<DateTime<Utc>>,
    pub check_in_progress: bool,
    pub white_url_ok: Option<bool>,
    pub gray_url_ok: Option<bool>,
    pub whitelist_mode: bool,
    pub modem_online: Option<bool>,
    pub last_error: Option<String>,
}

impl ModemHealthState {
    fn pending(modem: &ModemConfig, next_check: Option<DateTime<Utc>>) -> Self {
        Self {
            modem_tag: modem.modem_tag.clone(),
            modem_interface: modem.modem_interface.clone(),
            last_check: None,
            next_check,
            check_in_progress: false,
            white_url_ok: None,
            gray_url_ok: None,
            whitelist_mode: false,
            modem_online: None,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct UpdateEvent {
    pub event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modem_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modem_interface: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub done: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodes_total: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodes_after_dedup: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodes_after_ping: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodes_after_tunnel: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub white_url_ok: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gray_url_ok: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub whitelist_mode: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modem_online: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_check_in_progress: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_check: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_check: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl UpdateEvent {
    fn new(event: impl Into<String>) -> Self {
        Self {
            event: event.into(),
            stage: None,
            modem_tag: None,
            modem_interface: None,
            done: None,
            total: None,
            nodes_total: None,
            nodes_after_dedup: None,
            nodes_after_ping: None,
            nodes_after_tunnel: None,
            white_url_ok: None,
            gray_url_ok: None,
            whitelist_mode: None,
            modem_online: None,
            health_check_in_progress: None,
            last_check: None,
            next_check: None,
            message: None,
        }
    }

    fn update_started() -> Self {
        let mut event = Self::new("update_started");
        event.stage = Some("fetch".to_string());
        event.message = Some("Update started".to_string());
        event
    }

    fn update_failed(message: String) -> Self {
        let mut event = Self::new("update_failed");
        event.stage = Some("failed".to_string());
        event.message = Some(message);
        event
    }

    fn modem_event(event_name: &str, modem: &ModemConfig) -> Self {
        let mut event = Self::new(event_name);
        event.modem_tag = Some(modem.modem_tag.clone());
        event.modem_interface = Some(modem.modem_interface.clone());
        event
    }

    fn health_event(event_name: &str, state: &ModemHealthState) -> Self {
        let mut event = Self::new(event_name);
        event.modem_tag = Some(state.modem_tag.clone());
        event.modem_interface = Some(state.modem_interface.clone());
        event.health_check_in_progress = Some(state.check_in_progress);
        event.white_url_ok = state.white_url_ok;
        event.gray_url_ok = state.gray_url_ok;
        event.whitelist_mode = Some(state.whitelist_mode);
        event.modem_online = state.modem_online;
        event.last_check = state.last_check;
        event.next_check = state.next_check;
        event.message = state.last_error.clone();
        event
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StatsSnapshot {
    pub last_update: Option<DateTime<Utc>>,
    pub next_update: Option<DateTime<Utc>>,
    pub is_updating: bool,
    pub stage: UpdateStage,
    pub nodes_total: usize,
    pub nodes_after_dedup: usize,
    pub modems: Vec<ModemStats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ModemStats {
    pub modem_tag: String,
    pub modem_interface: String,
    pub stage: ModemStage,
    pub last_update: Option<DateTime<Utc>>,
    pub ping_done: usize,
    pub ping_total: usize,
    pub nodes_after_ping: usize,
    pub tunnel_done: usize,
    pub tunnel_total: usize,
    pub nodes_after_tunnel: usize,
    pub cached_nodes: usize,
    pub health_check_in_progress: bool,
    pub white_url_ok: Option<bool>,
    pub gray_url_ok: Option<bool>,
    pub whitelist_mode: bool,
    pub modem_online: Option<bool>,
    pub network_last_check: Option<DateTime<Utc>>,
    pub network_next_check: Option<DateTime<Utc>>,
    pub health_last_error: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Clone)]
pub struct SchedulerHandle {
    command_tx: mpsc::UnboundedSender<SchedulerCommand>,
}

impl SchedulerHandle {
    pub fn request_update(&self) -> Result<()> {
        self.command_tx
            .send(SchedulerCommand::UpdateNow)
            .map_err(|_| anyhow!("scheduler is not running"))
    }

    pub fn request_health_check(&self) -> Result<()> {
        self.command_tx
            .send(SchedulerCommand::HealthNow)
            .map_err(|_| anyhow!("scheduler is not running"))
    }

    pub fn reconfigure(&self) -> Result<()> {
        self.command_tx
            .send(SchedulerCommand::ConfigChanged)
            .map_err(|_| anyhow!("scheduler is not running"))
    }

    pub fn shutdown(&self) -> Result<()> {
        self.command_tx
            .send(SchedulerCommand::Shutdown)
            .map_err(|_| anyhow!("scheduler is not running"))
    }
}

enum SchedulerCommand {
    UpdateNow,
    HealthNow,
    ConfigChanged,
    Shutdown,
}

pub fn spawn_scheduler(shared: SharedState) -> (SchedulerHandle, JoinHandle<()>) {
    let (command_tx, mut command_rx) = mpsc::unbounded_channel();
    let handle = SchedulerHandle { command_tx };
    let task = tokio::spawn(async move {
        let mut next_update_run = initial_next_run(&shared).await;
        let mut next_health_run = Utc::now();

        loop {
            {
                let mut next_update = shared.next_update.write().await;
                *next_update = Some(next_update_run);
            }
            sync_runtime_entries(&shared, Some(next_health_run)).await;

            tokio::select! {
                _ = sleep(duration_until(next_update_run)) => {
                    spawn_update_task(shared.clone());
                    next_update_run = compute_next_run_from_now(&shared).await;
                }
                _ = sleep(duration_until(next_health_run)) => {
                    spawn_health_task(shared.clone());
                    next_health_run = compute_next_health_run_from_now(&shared).await;
                }
                command = command_rx.recv() => {
                    match command {
                        Some(SchedulerCommand::UpdateNow) => {
                            spawn_update_task(shared.clone());
                            next_update_run = compute_next_run_from_now(&shared).await;
                        }
                        Some(SchedulerCommand::HealthNow) => {
                            next_health_run = Utc::now();
                        }
                        Some(SchedulerCommand::ConfigChanged) => {
                            next_update_run = compute_next_run_from_now(&shared).await;
                            next_health_run = Utc::now();
                        }
                        Some(SchedulerCommand::Shutdown) | None => {
                            info!("scheduler shutdown requested");
                            break;
                        }
                    }
                }
            }
        }

        let mut next_update = shared.next_update.write().await;
        *next_update = None;
    });

    (handle, task)
}

fn spawn_update_task(shared: SharedState) {
    tokio::spawn(async move {
        if let Err(error) = run_update_cycle(shared).await {
            error!(error = %error, "subscription update cycle failed");
        }
    });
}

fn spawn_health_task(shared: SharedState) {
    tokio::spawn(async move {
        if let Err(error) = run_modem_health_checks(shared).await {
            error!(error = %error, "modem health check failed");
        }
    });
}

async fn initial_next_run(shared: &SharedState) -> DateTime<Utc> {
    let cache = shared.subscription_cache.read().await;
    let Some(cache) = cache.as_ref() else {
        return Utc::now();
    };

    let config = shared.config.read().await;
    cache.last_update + ChronoDuration::minutes(config.update_interval_minutes as i64)
}

async fn compute_next_run_from_now(shared: &SharedState) -> DateTime<Utc> {
    let config = shared.config.read().await;
    Utc::now() + ChronoDuration::minutes(config.update_interval_minutes as i64)
}

async fn compute_next_health_run_from_now(shared: &SharedState) -> DateTime<Utc> {
    let config = shared.config.read().await;
    Utc::now() + ChronoDuration::minutes(config.network_check_interval_minutes as i64)
}

fn duration_until(next_run: DateTime<Utc>) -> Duration {
    let now = Utc::now();
    let millis = (next_run - now).num_milliseconds();
    if millis <= 0 {
        Duration::from_secs(0)
    } else {
        Duration::from_millis(millis as u64)
    }
}

pub async fn run_update_cycle(shared: SharedState) -> Result<()> {
    if shared
        .is_updating
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        warn!("update request ignored because another update is already running");
        return Ok(());
    }
    let _guard = shared.update_lock.lock().await;

    let config_snapshot = shared.config.read().await.clone();
    reset_update_progress(&shared, &config_snapshot).await;
    let _ = shared.events_tx.send(UpdateEvent::update_started());
    info!("subscription update started");

    let result = run_update_cycle_inner(&shared, config_snapshot).await;

    if let Err(error) = &result {
        let message = error.to_string();
        {
            let mut progress = shared.update_progress.write().await;
            progress.is_updating = false;
            progress.stage = UpdateStage::Failed;
        }
        let _ = shared.events_tx.send(UpdateEvent::update_failed(message));
        let next_update = compute_next_run_from_now(&shared).await;
        let mut next_update_state = shared.next_update.write().await;
        *next_update_state = Some(next_update);
    }

    shared.is_updating.store(false, Ordering::SeqCst);
    result
}

async fn run_update_cycle_inner(shared: &SharedState, config_snapshot: AppConfig) -> Result<()> {
    let fetch_results = fetch_all(&shared.http_client, &config_snapshot.subscription_urls).await;

    let mut all_nodes = Vec::new();
    for (url, result) in fetch_results {
        match result {
            Ok(body) => match parser::parse_subscription(&body) {
                Ok(mut nodes) => all_nodes.append(&mut nodes),
                Err(error) => warn!(url = %url, error = %error, "failed to parse subscription"),
            },
            Err(error) => warn!(url = %url, error = %error, "failed to fetch subscription"),
        }
    }

    let nodes_total = all_nodes.len();
    {
        let mut progress = shared.update_progress.write().await;
        progress.nodes_total = nodes_total;
        progress.stage = UpdateStage::Dedup;
    }
    let mut fetch_event = UpdateEvent::new("fetch_complete");
    fetch_event.stage = Some("dedup".to_string());
    fetch_event.nodes_total = Some(nodes_total);
    let _ = shared.events_tx.send(fetch_event);

    let deduped_nodes = dedup_nodes(all_nodes);
    let nodes_after_dedup = deduped_nodes.len();
    {
        let mut progress = shared.update_progress.write().await;
        progress.nodes_after_dedup = nodes_after_dedup;
        progress.stage = UpdateStage::Modems;
    }
    let mut dedup_event = UpdateEvent::new("dedup_complete");
    dedup_event.stage = Some("modems".to_string());
    dedup_event.nodes_total = Some(nodes_total);
    dedup_event.nodes_after_dedup = Some(nodes_after_dedup);
    let _ = shared.events_tx.send(dedup_event);

    let mut branches = JoinSet::new();
    for modem in config_snapshot.modems.clone() {
        let shared = shared.clone();
        let branch_config = config_snapshot.clone();
        let branch_nodes = deduped_nodes.clone();
        branches.spawn(async move {
            run_modem_branch(shared, branch_config, modem, branch_nodes).await
        });
    }

    let mut successful_entries = BTreeMap::new();
    while let Some(result) = branches.join_next().await {
        match result {
            Ok(branch) => {
                if let Some(entry) = branch.entry {
                    successful_entries.insert(branch.modem_tag, entry);
                } else if let Some(error) = branch.error {
                    warn!(modem_tag = %branch.modem_tag, error = %error, "modem branch failed");
                }
            }
            Err(error) => warn!(error = %error, "modem branch task failed"),
        }
    }

    let now = Utc::now();
    let actual_config = shared.config.read().await.clone();
    let old_cache = shared.subscription_cache.read().await.clone();
    let mut next_cache = SubscriptionCache::empty(now, nodes_total, nodes_after_dedup);
    for modem in &actual_config.modems {
        if let Some(mut entry) = successful_entries.remove(&modem.modem_tag) {
            entry.modem_interface = modem.modem_interface.clone();
            next_cache.by_modem.insert(modem.modem_tag.clone(), entry);
            continue;
        }

        if let Some(entry) = old_cache
            .as_ref()
            .and_then(|cache| cache.by_modem.get(&modem.modem_tag))
        {
            next_cache
                .by_modem
                .insert(modem.modem_tag.clone(), entry.clone());
        }
    }

    config::save_subscription_cache(&shared.subscription_cache_path, &next_cache).await?;
    {
        let mut cache = shared.subscription_cache.write().await;
        *cache = Some(next_cache.clone());
    }

    {
        let mut progress = shared.update_progress.write().await;
        progress.is_updating = false;
        progress.stage = UpdateStage::Complete;
    }
    let next_update = compute_next_run_from_now(shared).await;
    {
        let mut next_update_state = shared.next_update.write().await;
        *next_update_state = Some(next_update);
    }

    let nodes_after_tunnel = next_cache
        .by_modem
        .values()
        .map(|entry| entry.nodes_after_tunnel)
        .sum::<usize>();
    let mut complete_event = UpdateEvent::new("update_complete");
    complete_event.stage = Some("complete".to_string());
    complete_event.nodes_total = Some(nodes_total);
    complete_event.nodes_after_dedup = Some(nodes_after_dedup);
    complete_event.nodes_after_tunnel = Some(nodes_after_tunnel);
    let _ = shared.events_tx.send(complete_event);
    info!(
        nodes_total,
        nodes_after_dedup, nodes_after_tunnel, "subscription update completed"
    );

    Ok(())
}

struct BranchResult {
    modem_tag: String,
    entry: Option<ModemCacheEntry>,
    error: Option<String>,
}

async fn run_modem_branch(
    shared: SharedState,
    config_snapshot: AppConfig,
    modem: ModemConfig,
    deduped_nodes: Vec<Node>,
) -> BranchResult {
    let modem_tag = modem.modem_tag.clone();
    let result = run_modem_branch_inner(
        shared.clone(),
        config_snapshot,
        modem.clone(),
        deduped_nodes,
    )
    .await;

    match result {
        Ok(entry) => BranchResult {
            modem_tag,
            entry: Some(entry),
            error: None,
        },
        Err(error) => {
            let message = error.to_string();
            update_modem_progress(&shared, &modem_tag, |progress| {
                progress.stage = ModemStage::Failed;
                progress.last_error = Some(message.clone());
            })
            .await;
            let mut event = UpdateEvent::modem_event("modem_failed", &modem);
            event.stage = Some("failed".to_string());
            event.message = Some(message.clone());
            let _ = shared.events_tx.send(event);
            BranchResult {
                modem_tag,
                entry: None,
                error: Some(message),
            }
        }
    }
}

async fn run_modem_branch_inner(
    shared: SharedState,
    config_snapshot: AppConfig,
    modem: ModemConfig,
    deduped_nodes: Vec<Node>,
) -> Result<ModemCacheEntry> {
    update_modem_progress(&shared, &modem.modem_tag, |progress| {
        progress.stage = ModemStage::Health;
        progress.last_error = None;
    })
    .await;

    let next_health_check = Some(
        Utc::now() + ChronoDuration::minutes(config_snapshot.network_check_interval_minutes as i64),
    );
    let health = refresh_modem_health_state(
        &shared,
        &modem,
        &config_snapshot.white_url,
        &config_snapshot.gray_url,
        Duration::from_millis(config_snapshot.ping_timeout_ms),
        next_health_check,
    )
    .await;

    if health.white_url_ok != Some(true) {
        let message = health
            .last_error
            .clone()
            .unwrap_or_else(|| "modem_offline".to_string());
        update_modem_progress(&shared, &modem.modem_tag, |progress| {
            progress.stage = ModemStage::Skipped;
            progress.last_error = Some(message.clone());
        })
        .await;
        return Err(anyhow!(message));
    }

    validate_interface_binding(&modem.modem_interface).map_err(|error| {
        anyhow!(
            "failed to bind TCP socket to interface {}: {}",
            modem.modem_interface,
            error
        )
    })?;

    let ping_total = deduped_nodes.len();
    update_modem_progress(&shared, &modem.modem_tag, |progress| {
        progress.stage = ModemStage::Ping;
        progress.ping_done = 0;
        progress.ping_total = ping_total;
        progress.nodes_after_ping = 0;
    })
    .await;

    let ping_callback = progress_callback(
        shared.clone(),
        modem.clone(),
        "modem_ping_progress",
        ProgressKind::Ping,
    );
    let online_nodes = ping_nodes_via_interface(
        deduped_nodes,
        Duration::from_millis(config_snapshot.ping_timeout_ms),
        config_snapshot.max_concurrent_pings,
        &modem.modem_interface,
        Some(ping_callback),
    )
    .await;
    let nodes_after_ping = online_nodes.len();
    update_modem_progress(&shared, &modem.modem_tag, |progress| {
        progress.ping_done = ping_total;
        progress.ping_total = ping_total;
        progress.nodes_after_ping = nodes_after_ping;
    })
    .await;
    let mut ping_complete = UpdateEvent::modem_event("modem_ping_complete", &modem);
    ping_complete.stage = Some("ping".to_string());
    ping_complete.nodes_after_ping = Some(nodes_after_ping);
    let _ = shared.events_tx.send(ping_complete);

    let tunnel_total = online_nodes.len();
    update_modem_progress(&shared, &modem.modem_tag, |progress| {
        progress.stage = ModemStage::Tunnel;
        progress.tunnel_done = 0;
        progress.tunnel_total = tunnel_total;
        progress.nodes_after_tunnel = 0;
    })
    .await;

    let tunnel_callback = progress_callback(
        shared.clone(),
        modem.clone(),
        "modem_tunnel_progress",
        ProgressKind::Tunnel,
    );
    let tunnel_probe_input = online_nodes;
    let mut tunnel_nodes = probe_tunnels(
        tunnel_probe_input.clone(),
        Duration::from_millis(config_snapshot.ping_timeout_ms),
        config_snapshot.max_concurrent_tunnels,
        Some(tunnel_callback.clone()),
        Some(modem.modem_interface.clone()),
    )
    .await;

    if health.whitelist_mode {
        tunnel_nodes = probe_tunnels(
            tunnel_probe_input,
            Duration::from_millis(config_snapshot.ping_timeout_ms),
            config_snapshot.max_concurrent_tunnels,
            Some(tunnel_callback),
            Some(modem.modem_interface.clone()),
        )
        .await;
    }

    tunnel_nodes.sort_by_key(|node| node.tunnel_ms.unwrap_or(u64::MAX));
    rename_nodes(&mut tunnel_nodes);
    let nodes_after_tunnel = tunnel_nodes.len();
    update_modem_progress(&shared, &modem.modem_tag, |progress| {
        progress.stage = ModemStage::Complete;
        progress.tunnel_done = tunnel_total;
        progress.tunnel_total = tunnel_total;
        progress.nodes_after_tunnel = nodes_after_tunnel;
        progress.last_error = None;
    })
    .await;

    let mut modem_complete = UpdateEvent::modem_event("modem_complete", &modem);
    modem_complete.stage = Some("complete".to_string());
    modem_complete.nodes_after_ping = Some(nodes_after_ping);
    modem_complete.nodes_after_tunnel = Some(nodes_after_tunnel);
    let _ = shared.events_tx.send(modem_complete);

    Ok(ModemCacheEntry {
        modem_tag: modem.modem_tag,
        modem_interface: modem.modem_interface,
        last_update: Utc::now(),
        nodes_after_ping,
        nodes_after_tunnel,
        nodes: tunnel_nodes,
    })
}

#[derive(Clone, Copy)]
enum ProgressKind {
    Ping,
    Tunnel,
}

fn progress_callback(
    shared: SharedState,
    modem: ModemConfig,
    event_name: &'static str,
    kind: ProgressKind,
) -> ProgressCallback {
    let events_tx = shared.events_tx.clone();
    Arc::new(move |done, total| {
        let mut event = UpdateEvent::modem_event(event_name, &modem);
        event.done = Some(done);
        event.total = Some(total);
        event.stage = Some(
            match kind {
                ProgressKind::Ping => "ping",
                ProgressKind::Tunnel => "tunnel",
            }
            .to_string(),
        );
        let _ = events_tx.send(event);

        let shared = shared.clone();
        let modem_tag = modem.modem_tag.clone();
        tokio::spawn(async move {
            update_modem_progress(&shared, &modem_tag, |progress| match kind {
                ProgressKind::Ping => {
                    progress.ping_done = done;
                    progress.ping_total = total;
                }
                ProgressKind::Tunnel => {
                    progress.tunnel_done = done;
                    progress.tunnel_total = total;
                }
            })
            .await;
        });
    })
}

pub async fn run_modem_health_checks(shared: SharedState) -> Result<()> {
    let _guard = match shared.health_check_lock.try_lock() {
        Ok(guard) => guard,
        Err(_) => {
            warn!("modem health check skipped because another check is still running");
            return Ok(());
        }
    };

    let config_snapshot = shared.config.read().await.clone();
    let next_check = Some(
        Utc::now() + ChronoDuration::minutes(config_snapshot.network_check_interval_minutes as i64),
    );
    sync_runtime_entries(&shared, next_check).await;

    let semaphore = Arc::new(tokio::sync::Semaphore::new(
        config_snapshot
            .max_concurrent_tunnels
            .max(1)
            .min(config_snapshot.modems.len().max(1)),
    ));
    let mut checks = JoinSet::new();
    for modem in config_snapshot.modems.clone() {
        let shared = shared.clone();
        let semaphore = semaphore.clone();
        let white_url = config_snapshot.white_url.clone();
        let gray_url = config_snapshot.gray_url.clone();
        let timeout = Duration::from_millis(config_snapshot.ping_timeout_ms);
        checks.spawn(async move {
            let permit = semaphore.acquire_owned().await.ok();
            let state = refresh_modem_health_state(
                &shared, &modem, &white_url, &gray_url, timeout, next_check,
            )
            .await;
            drop(permit);
            state
        });
    }

    while let Some(result) = checks.join_next().await {
        if let Err(error) = result {
            warn!(error = %error, "modem health worker failed");
        }
    }

    Ok(())
}

async fn refresh_modem_health_state(
    shared: &SharedState,
    modem: &ModemConfig,
    white_url: &str,
    gray_url: &str,
    probe_timeout: Duration,
    next_check: Option<DateTime<Utc>>,
) -> ModemHealthState {
    let started_state = {
        let mut states = shared.health_state.write().await;
        let state = states
            .entry(modem.modem_tag.clone())
            .or_insert_with(|| ModemHealthState::pending(modem, next_check));
        state.modem_interface = modem.modem_interface.clone();
        state.check_in_progress = true;
        state.next_check = next_check;
        state.last_error = None;
        state.clone()
    };
    let _ = shared.events_tx.send(UpdateEvent::health_event(
        "modem_health_started",
        &started_state,
    ));

    let mut state = check_modem_health(modem, white_url, gray_url, probe_timeout).await;
    state.next_check = next_check;
    state.check_in_progress = false;

    {
        let mut states = shared.health_state.write().await;
        states.insert(modem.modem_tag.clone(), state.clone());
    }

    let event_name = if state.last_error.is_some() {
        "modem_health_failed"
    } else {
        "modem_health_result"
    };
    let _ = shared
        .events_tx
        .send(UpdateEvent::health_event(event_name, &state));

    state
}

pub async fn check_modem_health(
    modem: &ModemConfig,
    white_url: &str,
    gray_url: &str,
    probe_timeout: Duration,
) -> ModemHealthState {
    let mut state = ModemHealthState::pending(modem, None);
    state.last_check = Some(Utc::now());

    match probe_http_url(white_url, &modem.modem_interface, probe_timeout).await {
        Ok(true) => {
            state.white_url_ok = Some(true);
            state.modem_online = Some(true);
        }
        Ok(false) => {
            state.white_url_ok = Some(false);
            state.gray_url_ok = None;
            state.modem_online = Some(false);
            return state;
        }
        Err(error) => {
            state.white_url_ok = Some(false);
            state.gray_url_ok = None;
            state.modem_online = Some(false);
            state.last_error = Some(error.to_string());
            return state;
        }
    }

    match probe_http_url(gray_url, &modem.modem_interface, probe_timeout).await {
        Ok(gray_ok) => {
            state.gray_url_ok = Some(gray_ok);
            state.whitelist_mode = !gray_ok;
        }
        Err(error) => {
            state.gray_url_ok = Some(false);
            state.whitelist_mode = true;
            state.last_error = Some(error.to_string());
        }
    }

    state
}

async fn probe_http_url(url: &str, interface: &str, probe_timeout: Duration) -> Result<bool> {
    let timeout_secs = probe_timeout.as_secs().max(1).to_string();
    let mut command = Command::new("curl");
    command
        .arg("--silent")
        .arg("--show-error")
        .arg("--fail")
        .arg("--location")
        .arg("--output")
        .arg("/dev/null")
        .arg("--connect-timeout")
        .arg(&timeout_secs)
        .arg("--max-time")
        .arg(&timeout_secs)
        .arg("--interface")
        .arg(interface)
        .arg(url);

    let command_timeout = probe_timeout + Duration::from_secs(1);
    let output = match timeout(command_timeout, command.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => return Err(anyhow!("failed to execute curl: {error}")),
        Err(_) => {
            return Err(anyhow!(
                "curl probe timed out after {} ms",
                probe_timeout.as_millis()
            ))
        }
    };

    if output.status.success() {
        return Ok(true);
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let status_code = output.status.code();
    warn!(url = %url, interface = %interface, status = %output.status, stderr = %stderr, "interface-bound URL probe failed");

    if matches!(status_code, Some(28) | Some(45)) {
        return Err(anyhow!(
            "curl exited with status {} while checking {} via {}: {}",
            output.status,
            url,
            interface,
            stderr
        ));
    }

    Ok(false)
}

pub async fn subscription_cache_snapshot(shared: &SharedState) -> Option<SubscriptionCache> {
    shared.subscription_cache.read().await.clone()
}

pub async fn next_update_snapshot(shared: &SharedState) -> Option<DateTime<Utc>> {
    shared.next_update.read().await.clone()
}

pub async fn stats_snapshot(shared: &SharedState) -> Result<StatsSnapshot> {
    let config = shared.config.read().await.clone();
    if config.update_interval_minutes == 0 || config.network_check_interval_minutes == 0 {
        return Err(anyhow!("invalid update interval state"));
    }

    let next_update = next_update_snapshot(shared).await;
    let is_updating = shared.is_updating.load(Ordering::SeqCst);
    let progress = shared.update_progress.read().await.clone();
    let cache = shared.subscription_cache.read().await.clone();
    let health = shared.health_state.read().await.clone();

    let stage = if is_updating {
        progress.stage
    } else {
        UpdateStage::Idle
    };
    let nodes_total = if is_updating {
        progress.nodes_total
    } else {
        cache.as_ref().map(|cache| cache.nodes_total).unwrap_or(0)
    };
    let nodes_after_dedup = if is_updating {
        progress.nodes_after_dedup
    } else {
        cache
            .as_ref()
            .map(|cache| cache.nodes_after_dedup)
            .unwrap_or(0)
    };

    let mut modems = Vec::with_capacity(config.modems.len());
    for modem in &config.modems {
        let progress_entry = progress.modems.get(&modem.modem_tag);
        let cache_entry = cache
            .as_ref()
            .and_then(|cache| cache.by_modem.get(&modem.modem_tag));
        let health_state = health
            .get(&modem.modem_tag)
            .cloned()
            .unwrap_or_else(|| ModemHealthState::pending(modem, None));
        let last_error = progress_entry.and_then(|progress| progress.last_error.clone());

        let stats = if is_updating {
            modem_stats_from_progress(
                modem,
                progress_entry,
                cache_entry,
                &health_state,
                last_error,
            )
        } else {
            modem_stats_from_cache(modem, cache_entry, &health_state, last_error)
        };
        modems.push(stats);
    }

    Ok(StatsSnapshot {
        last_update: cache.as_ref().map(|cache| cache.last_update),
        next_update,
        is_updating,
        stage,
        nodes_total,
        nodes_after_dedup,
        modems,
    })
}

fn modem_stats_from_progress(
    modem: &ModemConfig,
    progress: Option<&ModemProgress>,
    cache: Option<&ModemCacheEntry>,
    health: &ModemHealthState,
    last_error: Option<String>,
) -> ModemStats {
    let fallback = ModemProgress::pending(modem);
    let progress = progress.unwrap_or(&fallback);
    ModemStats {
        modem_tag: modem.modem_tag.clone(),
        modem_interface: modem.modem_interface.clone(),
        stage: progress.stage,
        last_update: cache.map(|entry| entry.last_update),
        ping_done: progress.ping_done,
        ping_total: progress.ping_total,
        nodes_after_ping: progress.nodes_after_ping,
        tunnel_done: progress.tunnel_done,
        tunnel_total: progress.tunnel_total,
        nodes_after_tunnel: progress.nodes_after_tunnel,
        cached_nodes: cache.map(|entry| entry.nodes.len()).unwrap_or(0),
        health_check_in_progress: health.check_in_progress,
        white_url_ok: health.white_url_ok,
        gray_url_ok: health.gray_url_ok,
        whitelist_mode: health.whitelist_mode,
        modem_online: health.modem_online,
        network_last_check: health.last_check,
        network_next_check: health.next_check,
        health_last_error: health.last_error.clone(),
        last_error,
    }
}

fn modem_stats_from_cache(
    modem: &ModemConfig,
    cache: Option<&ModemCacheEntry>,
    health: &ModemHealthState,
    last_error: Option<String>,
) -> ModemStats {
    let stage = if last_error.is_some() {
        ModemStage::Failed
    } else if cache.is_some() {
        ModemStage::Complete
    } else {
        ModemStage::Pending
    };
    let nodes_after_ping = cache.map(|entry| entry.nodes_after_ping).unwrap_or(0);
    let nodes_after_tunnel = cache.map(|entry| entry.nodes_after_tunnel).unwrap_or(0);
    ModemStats {
        modem_tag: modem.modem_tag.clone(),
        modem_interface: modem.modem_interface.clone(),
        stage,
        last_update: cache.map(|entry| entry.last_update),
        ping_done: nodes_after_ping,
        ping_total: nodes_after_ping,
        nodes_after_ping,
        tunnel_done: nodes_after_tunnel,
        tunnel_total: nodes_after_tunnel,
        nodes_after_tunnel,
        cached_nodes: cache.map(|entry| entry.nodes.len()).unwrap_or(0),
        health_check_in_progress: health.check_in_progress,
        white_url_ok: health.white_url_ok,
        gray_url_ok: health.gray_url_ok,
        whitelist_mode: health.whitelist_mode,
        modem_online: health.modem_online,
        network_last_check: health.last_check,
        network_next_check: health.next_check,
        health_last_error: health.last_error.clone(),
        last_error,
    }
}

pub async fn apply_config_prune(shared: &SharedState) -> Result<()> {
    let config = shared.config.read().await.clone();
    let cache_to_save = {
        let mut cache_guard = shared.subscription_cache.write().await;
        if let Some(cache) = cache_guard.as_mut() {
            cache.retain_configured_modems(&config.modems);
            Some(cache.clone())
        } else {
            None
        }
    };
    if let Some(cache) = cache_to_save {
        config::save_subscription_cache(&shared.subscription_cache_path, &cache).await?;
    }

    sync_runtime_entries(shared, Some(Utc::now())).await;
    Ok(())
}

async fn reset_update_progress(shared: &SharedState, config: &AppConfig) {
    let mut progress = shared.update_progress.write().await;
    *progress = UpdateProgress::started_for_config(config);
}

async fn update_modem_progress<F>(shared: &SharedState, modem_tag: &str, update: F)
where
    F: FnOnce(&mut ModemProgress),
{
    let mut progress = shared.update_progress.write().await;
    if let Some(modem_progress) = progress.modems.get_mut(modem_tag) {
        update(modem_progress);
    }
}

async fn sync_runtime_entries(shared: &SharedState, next_health_check: Option<DateTime<Utc>>) {
    let config = shared.config.read().await.clone();
    let configured_tags = config
        .modems
        .iter()
        .map(|modem| modem.modem_tag.as_str())
        .collect::<HashSet<_>>();

    {
        let mut states = shared.health_state.write().await;
        states.retain(|tag, _| configured_tags.contains(tag.as_str()));
        for modem in &config.modems {
            states
                .entry(modem.modem_tag.clone())
                .and_modify(|state| {
                    state.modem_interface = modem.modem_interface.clone();
                    state.next_check = next_health_check;
                })
                .or_insert_with(|| ModemHealthState::pending(modem, next_health_check));
        }
    }

    {
        let mut progress = shared.update_progress.write().await;
        progress
            .modems
            .retain(|tag, _| configured_tags.contains(tag.as_str()));
        for modem in &config.modems {
            progress
                .modems
                .entry(modem.modem_tag.clone())
                .and_modify(|state| {
                    state.modem_interface = modem.modem_interface.clone();
                })
                .or_insert_with(|| ModemProgress::pending(modem));
        }
    }
}

fn initial_health_state(
    config: &AppConfig,
    next_check: Option<DateTime<Utc>>,
) -> BTreeMap<String, ModemHealthState> {
    config
        .modems
        .iter()
        .map(|modem| {
            (
                modem.modem_tag.clone(),
                ModemHealthState::pending(modem, next_check),
            )
        })
        .collect()
}

fn initial_modem_progress(modems: &[ModemConfig]) -> BTreeMap<String, ModemProgress> {
    modems
        .iter()
        .map(|modem| (modem.modem_tag.clone(), ModemProgress::pending(modem)))
        .collect()
}
