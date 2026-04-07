use std::{path::PathBuf, sync::{atomic::{AtomicBool, Ordering}, Arc}};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::Serialize;
use tokio::{sync::{broadcast, mpsc, Mutex, RwLock}, task::JoinHandle, time::{sleep, Duration}};
use tracing::{error, info, warn};

use crate::{config::{self, AppConfig}, dedup::dedup_nodes, encoder::encode_subscription, fetcher::fetch_all, parser, pinger::{ping_nodes, ProgressCallback}, renamer::rename_nodes};

#[derive(Clone)]
pub struct SharedState {
    pub config_path: PathBuf,
    pub subscription_cache_path: PathBuf,
    pub config: Arc<RwLock<AppConfig>>,
    pub subscription_cache: Arc<RwLock<Option<String>>>,
    pub next_update: Arc<RwLock<Option<DateTime<Utc>>>>,
    pub is_updating: Arc<AtomicBool>,
    pub update_lock: Arc<Mutex<()>>,
    pub events_tx: broadcast::Sender<UpdateEvent>,
    pub http_client: reqwest::Client,
}

impl SharedState {
    pub fn new(
        config_path: PathBuf,
        subscription_cache_path: PathBuf,
        config: AppConfig,
        cached_subscription: Option<String>,
        events_tx: broadcast::Sender<UpdateEvent>,
        http_client: reqwest::Client,
    ) -> Self {
        let next_update = if cached_subscription.is_some() {
            config
                .last_update
                .map(|last_update| last_update + ChronoDuration::minutes(config.update_interval_minutes as i64))
                .or_else(|| Some(Utc::now()))
        } else {
            Some(Utc::now())
        };

        Self {
            config_path,
            subscription_cache_path,
            config: Arc::new(RwLock::new(config)),
            subscription_cache: Arc::new(RwLock::new(cached_subscription)),
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
            message: None,
        }
    }

    fn update_complete(nodes_total: usize, nodes_after_dedup: usize, nodes_after_ping: usize) -> Self {
        Self {
            event: "update_complete".to_string(),
            done: None,
            total: None,
            nodes_total: Some(nodes_total),
            nodes_after_dedup: Some(nodes_after_dedup),
            nodes_after_ping: Some(nodes_after_ping),
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
            message: Some("Update started".to_string()),
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
    let has_cache = shared.subscription_cache.read().await.is_some();
    if !has_cache {
        return Utc::now();
    }

    let config = shared.config.read().await;
    config
        .last_update
        .map(|last_update| last_update + ChronoDuration::minutes(config.update_interval_minutes as i64))
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
        let deduped_nodes = dedup_nodes(all_nodes);
        let nodes_after_dedup = deduped_nodes.len();

        let event_sender = shared.events_tx.clone();
        let progress_callback: ProgressCallback = Arc::new(move |done, total| {
            let _ = event_sender.send(UpdateEvent::ping_progress(done, total));
        });

        let mut online_nodes = ping_nodes(
            deduped_nodes,
            Duration::from_millis(config_snapshot.ping_timeout_ms),
            config_snapshot.max_concurrent_pings,
            Some(progress_callback),
        ).await;
        let nodes_after_ping = online_nodes.len();

        rename_nodes(&mut online_nodes);
        let encoded_subscription = encode_subscription(&online_nodes);
        config::save_subscription_cache(&shared.subscription_cache_path, &encoded_subscription).await?;

        {
            let mut cache = shared.subscription_cache.write().await;
            *cache = Some(encoded_subscription);
        }

        let now = Utc::now();
        let config_to_save = {
            let mut config = shared.config.write().await;
            config.last_update = Some(now);
            config.nodes_total = nodes_total;
            config.nodes_after_dedup = nodes_after_dedup;
            config.nodes_after_ping = nodes_after_ping;
            config.clone()
        };
        config::save_config_atomic(&shared.config_path, &config_to_save).await?;

        let next_update = now + ChronoDuration::minutes(config_snapshot.update_interval_minutes as i64);
        {
            let mut next_update_state = shared.next_update.write().await;
            *next_update_state = Some(next_update);
        }

        let _ = shared.events_tx.send(UpdateEvent::update_complete(
            nodes_total,
            nodes_after_dedup,
            nodes_after_ping,
        ));
        info!(nodes_total, nodes_after_dedup, nodes_after_ping, "subscription update completed");

        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(error) = &result {
        let message = error.to_string();
        let _ = shared.events_tx.send(UpdateEvent::update_failed(message.clone()));
        let next_update = compute_next_run_from_now(&shared).await;
        let mut next_update_state = shared.next_update.write().await;
        *next_update_state = Some(next_update);
    }

    shared.is_updating.store(false, Ordering::SeqCst);
    result
}

pub async fn subscription_snapshot(shared: &SharedState) -> Option<String> {
    shared.subscription_cache.read().await.clone()
}

pub async fn next_update_snapshot(shared: &SharedState) -> Option<DateTime<Utc>> {
    shared.next_update.read().await.clone()
}

pub async fn stats_snapshot(shared: &SharedState) -> Result<(AppConfig, Option<DateTime<Utc>>, bool)> {
    let config = shared.config.read().await.clone();
    let next_update = next_update_snapshot(shared).await;
    let is_updating = shared.is_updating.load(Ordering::SeqCst);
    if config.update_interval_minutes == 0 {
        return Err(anyhow!("invalid update interval state"));
    }
    Ok((config, next_update, is_updating))
}
