use std::{
    env,
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
    task::JoinHandle,
    time::{sleep, Duration},
};
use tracing::{error, info, warn};

use crate::{
    config::{self, AppConfig},
    dedup::dedup_nodes,
    fetcher::fetch_all,
    parser::{self, Node},
    pinger::{ping_nodes, ProgressCallback},
    renamer::rename_nodes,
    tunnel::probe_tunnels,
};

const DEFAULT_MODEM_INTERFACE: &str = "enx020d0c073330";

#[derive(Clone)]
pub struct SharedState {
    pub config_path: PathBuf,
    pub subscription_cache_path: PathBuf,
    pub config: Arc<RwLock<AppConfig>>,
    pub nodes_cache: Arc<RwLock<Option<Vec<Node>>>>,
    pub connectivity_state: Arc<RwLock<ConnectivityState>>,
    pub modem_interface: String,
    pub next_update: Arc<RwLock<Option<DateTime<Utc>>>>,
    pub is_updating: Arc<AtomicBool>,
    pub update_lock: Arc<Mutex<()>>,
    pub events_tx: broadcast::Sender<UpdateEvent>,
    pub http_client: reqwest::Client,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectivityState {
    pub last_check: Option<DateTime<Utc>>,
    pub white_url_ok: Option<bool>,
    pub gray_url_ok: Option<bool>,
    pub fallback_to_default_internet: bool,
    pub whitelist_mode: bool,
}

impl Default for ConnectivityState {
    fn default() -> Self {
        Self {
            last_check: None,
            white_url_ok: None,
            gray_url_ok: None,
            fallback_to_default_internet: false,
            whitelist_mode: false,
        }
    }
}

impl SharedState {
    pub fn new(
        config_path: PathBuf,
        subscription_cache_path: PathBuf,
        config: AppConfig,
        cached_subscription: Option<Vec<Node>>,
        events_tx: broadcast::Sender<UpdateEvent>,
        http_client: reqwest::Client,
    ) -> Self {
        let next_update = if cached_subscription.is_some() {
            config
                .last_update
                .map(|last_update| {
                    last_update + ChronoDuration::minutes(config.update_interval_minutes as i64)
                })
                .or_else(|| Some(Utc::now()))
        } else {
            Some(Utc::now())
        };

        Self {
            config_path,
            subscription_cache_path,
            config: Arc::new(RwLock::new(config)),
            nodes_cache: Arc::new(RwLock::new(cached_subscription)),
            connectivity_state: Arc::new(RwLock::new(ConnectivityState::default())),
            modem_interface: resolve_modem_interface(),
            next_update: Arc::new(RwLock::new(next_update)),
            is_updating: Arc::new(AtomicBool::new(false)),
            update_lock: Arc::new(Mutex::new(())),
            events_tx,
            http_client,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct UpdateEvent {
    pub event: String,
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
    pub fallback_to_default_internet: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub whitelist_mode: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub white_url_ok: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gray_url_ok: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_check: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl UpdateEvent {
    fn ping_progress(done: usize, total: usize) -> Self {
        Self {
            event: "ping_progress".to_string(),
            done: Some(done),
            total: Some(total),
            nodes_total: None,
            nodes_after_dedup: None,
            nodes_after_ping: None,
            nodes_after_tunnel: None,
            fallback_to_default_internet: None,
            whitelist_mode: None,
            white_url_ok: None,
            gray_url_ok: None,
            last_check: None,
            message: None,
        }
    }

    fn tunnel_progress(done: usize, total: usize) -> Self {
        Self {
            event: "tunnel_progress".to_string(),
            done: Some(done),
            total: Some(total),
            nodes_total: None,
            nodes_after_dedup: None,
            nodes_after_ping: None,
            nodes_after_tunnel: None,
            fallback_to_default_internet: None,
            whitelist_mode: None,
            white_url_ok: None,
            gray_url_ok: None,
            last_check: None,
            message: None,
        }
    }

    fn update_complete(
        nodes_total: usize,
        nodes_after_dedup: usize,
        nodes_after_ping: usize,
        nodes_after_tunnel: usize,
    ) -> Self {
        Self {
            event: "update_complete".to_string(),
            done: None,
            total: None,
            nodes_total: Some(nodes_total),
            nodes_after_dedup: Some(nodes_after_dedup),
            nodes_after_ping: Some(nodes_after_ping),
            nodes_after_tunnel: Some(nodes_after_tunnel),
            fallback_to_default_internet: None,
            whitelist_mode: None,
            white_url_ok: None,
            gray_url_ok: None,
            last_check: None,
            message: None,
        }
    }

    fn update_started() -> Self {
        Self {
            event: "update_started".to_string(),
            done: None,
            total: None,
            nodes_total: None,
            nodes_after_dedup: None,
            nodes_after_ping: None,
            nodes_after_tunnel: None,
            fallback_to_default_internet: None,
            whitelist_mode: None,
            white_url_ok: None,
            gray_url_ok: None,
            last_check: None,
            message: Some("Update started".to_string()),
        }
    }

    fn connectivity_stage(message: String) -> Self {
        Self {
            event: "connectivity_stage".to_string(),
            done: None,
            total: None,
            nodes_total: None,
            nodes_after_dedup: None,
            nodes_after_ping: None,
            nodes_after_tunnel: None,
            fallback_to_default_internet: None,
            whitelist_mode: None,
            white_url_ok: None,
            gray_url_ok: None,
            last_check: None,
            message: Some(message),
        }
    }

    fn connectivity_result(state: &ConnectivityState) -> Self {
        Self {
            event: "connectivity_result".to_string(),
            done: None,
            total: None,
            nodes_total: None,
            nodes_after_dedup: None,
            nodes_after_ping: None,
            nodes_after_tunnel: None,
            fallback_to_default_internet: Some(state.fallback_to_default_internet),
            whitelist_mode: Some(state.whitelist_mode),
            white_url_ok: state.white_url_ok,
            gray_url_ok: state.gray_url_ok,
            last_check: state.last_check,
            message: None,
        }
    }

    fn update_failed(message: String) -> Self {
        Self {
            event: "update_failed".to_string(),
            done: None,
            total: None,
            nodes_total: None,
            nodes_after_dedup: None,
            nodes_after_ping: None,
            nodes_after_tunnel: None,
            fallback_to_default_internet: None,
            whitelist_mode: None,
            white_url_ok: None,
            gray_url_ok: None,
            last_check: None,
            message: Some(message),
        }
    }
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
    ConfigChanged,
    Shutdown,
}

pub fn spawn_scheduler(shared: SharedState) -> (SchedulerHandle, JoinHandle<()>) {
    let (command_tx, mut command_rx) = mpsc::unbounded_channel();
    let handle = SchedulerHandle { command_tx };
    let task = tokio::spawn(async move {
        let mut next_run = initial_next_run(&shared).await;

        loop {
            {
                let mut next_update = shared.next_update.write().await;
                *next_update = Some(next_run);
            }

            let sleep_duration = duration_until(next_run);
            tokio::select! {
                _ = sleep(sleep_duration) => {
                    if let Err(error) = run_update_cycle(shared.clone()).await {
                        error!(error = %error, "subscription update cycle failed");
                    }
                    next_run = compute_next_run_from_now(&shared).await;
                }
                command = command_rx.recv() => {
                    match command {
                        Some(SchedulerCommand::UpdateNow) => {
                            if let Err(error) = run_update_cycle(shared.clone()).await {
                                error!(error = %error, "forced subscription update failed");
                            }
                            next_run = compute_next_run_from_now(&shared).await;
                        }
                        Some(SchedulerCommand::ConfigChanged) => {
                            next_run = compute_next_run_from_now(&shared).await;
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

async fn initial_next_run(shared: &SharedState) -> DateTime<Utc> {
    let has_cache = shared.nodes_cache.read().await.is_some();
    if !has_cache {
        return Utc::now();
    }

    let config = shared.config.read().await;
    config
        .last_update
        .map(|last_update| {
            last_update + ChronoDuration::minutes(config.update_interval_minutes as i64)
        })
        .unwrap_or_else(Utc::now)
}

async fn compute_next_run_from_now(shared: &SharedState) -> DateTime<Utc> {
    let config = shared.config.read().await;
    Utc::now() + ChronoDuration::minutes(config.update_interval_minutes as i64)
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

    let _ = shared.events_tx.send(UpdateEvent::update_started());
    info!("subscription update started");

    let result = async {
        let config_snapshot = shared.config.read().await.clone();
        let fetch_results =
            fetch_all(&shared.http_client, &config_snapshot.subscription_urls).await;

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
        let deduped_nodes = dedup_nodes(all_nodes);
        let nodes_after_dedup = deduped_nodes.len();

        let event_sender = shared.events_tx.clone();
        let progress_callback: ProgressCallback = Arc::new(move |done, total| {
            let _ = event_sender.send(UpdateEvent::ping_progress(done, total));
        });

        let online_nodes = ping_nodes(
            deduped_nodes,
            Duration::from_millis(config_snapshot.ping_timeout_ms),
            config_snapshot.max_concurrent_pings,
            Some(progress_callback),
        )
        .await;
        let nodes_after_ping = online_nodes.len();

        let mut connectivity_state = shared.connectivity_state.read().await.clone();
        let should_check_connectivity = should_refresh_connectivity_state(
            &connectivity_state,
            config_snapshot.network_check_interval_minutes,
        );

        let mut should_repeat_tunnel_probe = false;
        if should_check_connectivity {
            let refreshed_state = refresh_connectivity_state(
                &shared,
                &config_snapshot,
                Duration::from_millis(config_snapshot.ping_timeout_ms),
            )
            .await;
            should_repeat_tunnel_probe = refreshed_state.whitelist_mode;
            connectivity_state = refreshed_state;
        }

        let bind_interface = if connectivity_state.fallback_to_default_internet {
            None
        } else {
            Some(shared.modem_interface.clone())
        };

        let event_sender = shared.events_tx.clone();
        let tunnel_progress_callback: ProgressCallback = Arc::new(move |done, total| {
            let _ = event_sender.send(UpdateEvent::tunnel_progress(done, total));
        });

        let tunnel_probe_input = online_nodes;
        let mut tunnel_nodes = probe_tunnels(
            tunnel_probe_input.clone(),
            Duration::from_millis(config_snapshot.ping_timeout_ms),
            config_snapshot.max_concurrent_tunnels,
            Some(tunnel_progress_callback.clone()),
            bind_interface.clone(),
        )
        .await;

        if should_repeat_tunnel_probe {
            let _ = shared.events_tx.send(UpdateEvent::connectivity_stage(
                "White-list mode detected, repeating tunnel checks".to_string(),
            ));
            tunnel_nodes = probe_tunnels(
                tunnel_probe_input,
                Duration::from_millis(config_snapshot.ping_timeout_ms),
                config_snapshot.max_concurrent_tunnels,
                Some(tunnel_progress_callback),
                bind_interface,
            )
            .await;
        }
        tunnel_nodes.sort_by_key(|node| node.tunnel_ms.unwrap_or(u64::MAX));
        let nodes_after_tunnel = tunnel_nodes.len();

        rename_nodes(&mut tunnel_nodes);
        config::save_nodes_cache(&shared.subscription_cache_path, &tunnel_nodes).await?;

        {
            let mut cache = shared.nodes_cache.write().await;
            *cache = Some(tunnel_nodes);
        }

        let now = Utc::now();
        let config_to_save = {
            let mut config = shared.config.write().await;
            config.last_update = Some(now);
            config.nodes_total = nodes_total;
            config.nodes_after_dedup = nodes_after_dedup;
            config.nodes_after_ping = nodes_after_ping;
            config.nodes_after_tunnel = nodes_after_tunnel;
            config.clone()
        };
        config::save_config_atomic(&shared.config_path, &config_to_save).await?;

        let next_update =
            now + ChronoDuration::minutes(config_snapshot.update_interval_minutes as i64);
        {
            let mut next_update_state = shared.next_update.write().await;
            *next_update_state = Some(next_update);
        }

        let _ = shared.events_tx.send(UpdateEvent::update_complete(
            nodes_total,
            nodes_after_dedup,
            nodes_after_ping,
            nodes_after_tunnel,
        ));
        info!(
            nodes_total,
            nodes_after_dedup,
            nodes_after_ping,
            nodes_after_tunnel,
            "subscription update completed"
        );

        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(error) = &result {
        let message = error.to_string();
        let _ = shared
            .events_tx
            .send(UpdateEvent::update_failed(message.clone()));
        let next_update = compute_next_run_from_now(&shared).await;
        let mut next_update_state = shared.next_update.write().await;
        *next_update_state = Some(next_update);
    }

    shared.is_updating.store(false, Ordering::SeqCst);
    result
}

pub async fn subscription_snapshot(shared: &SharedState) -> Option<Vec<Node>> {
    shared.nodes_cache.read().await.clone()
}

pub async fn next_update_snapshot(shared: &SharedState) -> Option<DateTime<Utc>> {
    shared.next_update.read().await.clone()
}

pub async fn stats_snapshot(
    shared: &SharedState,
) -> Result<(AppConfig, Option<DateTime<Utc>>, bool, ConnectivityState)> {
    let config = shared.config.read().await.clone();
    let next_update = next_update_snapshot(shared).await;
    let is_updating = shared.is_updating.load(Ordering::SeqCst);
    let connectivity_state = shared.connectivity_state.read().await.clone();
    if config.update_interval_minutes == 0 || config.network_check_interval_minutes == 0 {
        return Err(anyhow!("invalid update interval state"));
    }
    Ok((config, next_update, is_updating, connectivity_state))
}

fn resolve_modem_interface() -> String {
    env::var("MODEM_INTERFACE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_MODEM_INTERFACE.to_string())
}

fn should_refresh_connectivity_state(state: &ConnectivityState, interval_minutes: u64) -> bool {
    if interval_minutes == 0 {
        return true;
    }

    let Some(last_check) = state.last_check else {
        return true;
    };

    let elapsed = Utc::now().signed_duration_since(last_check).num_minutes();
    elapsed >= interval_minutes as i64
}

async fn refresh_connectivity_state(
    shared: &SharedState,
    config_snapshot: &AppConfig,
    timeout: Duration,
) -> ConnectivityState {
    let modem_interface = shared.modem_interface.as_str();

    let _ = shared
        .events_tx
        .send(UpdateEvent::connectivity_stage(format!(
            "Checking white URL via modem interface: {}",
            config_snapshot.white_url
        )));

    let white_url_ok =
        probe_http_url(&config_snapshot.white_url, Some(modem_interface), timeout).await;

    let (gray_url_ok, fallback_to_default_internet, whitelist_mode) = if !white_url_ok {
        (None, true, false)
    } else {
        let _ = shared
            .events_tx
            .send(UpdateEvent::connectivity_stage(format!(
                "Checking gray URL via modem interface: {}",
                config_snapshot.gray_url
            )));
        let gray_ok =
            probe_http_url(&config_snapshot.gray_url, Some(modem_interface), timeout).await;
        (Some(gray_ok), false, !gray_ok)
    };

    let state = ConnectivityState {
        last_check: Some(Utc::now()),
        white_url_ok: Some(white_url_ok),
        gray_url_ok,
        fallback_to_default_internet,
        whitelist_mode,
    };

    {
        let mut shared_state = shared.connectivity_state.write().await;
        *shared_state = state.clone();
    }

    let _ = shared
        .events_tx
        .send(UpdateEvent::connectivity_result(&state));
    state
}

async fn probe_http_url(url: &str, interface: Option<&str>, timeout: Duration) -> bool {
    let timeout_secs = timeout.as_secs().max(1).to_string();
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
        .arg(&timeout_secs);

    if let Some(interface_name) = interface.filter(|value| !value.trim().is_empty()) {
        command.arg("--interface").arg(interface_name);
    }

    command.arg(url);

    match command.output().await {
        Ok(output) if output.status.success() => true,
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(url = %url, interface = ?interface, status = %output.status, stderr = %stderr.trim(), "internet probe failed");
            false
        }
        Err(error) => {
            warn!(url = %url, interface = ?interface, error = %error, "failed to execute curl for internet probe");
            false
        }
    }
}
