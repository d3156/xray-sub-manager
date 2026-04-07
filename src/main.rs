mod config;
mod dedup;
mod encoder;
mod fetcher;
mod parser;
mod pinger;
mod renamer;
mod scheduler;
mod web;

use std::{env, net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use reqwest::Client;
use tokio::{net::TcpListener, sync::{broadcast, Notify}};
use tracing::{error, info};

use crate::{config::{cache_path_for, load_or_init_config, load_subscription_cache, resolve_config_path}, scheduler::{spawn_scheduler, SharedState}, web::{build_router, AppState}};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli_config_path = env::args().nth(1);
    let config_path = resolve_config_path(cli_config_path);
    let config = load_or_init_config(&config_path).await.context("failed to load config")?;
    let subscription_cache_path = cache_path_for(&config_path);
    let cached_subscription = load_subscription_cache(&subscription_cache_path)
        .await
        .context("failed to load subscription cache")?;

    let http_client = Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("failed to build HTTP client")?;

    let (events_tx, _) = broadcast::channel(256);

    let shared = SharedState::new(
        config_path.clone(),
        subscription_cache_path,
        config,
        cached_subscription,
        events_tx,
        http_client,
    );

    let (scheduler_handle, scheduler_task) = spawn_scheduler(shared.clone());
    let app_state = AppState {
        shared: shared.clone(),
        scheduler: scheduler_handle.clone(),
    };
    let app = build_router(app_state);

    let config_snapshot = shared.config.read().await.clone();
    let addr = SocketAddr::from(([0, 0, 0, 0], config_snapshot.web_port));
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind HTTP server on {addr}"))?;
    drop(config_snapshot);

    info!(address = %addr, "xray-sub-manager started");

    let shutdown_notify = Arc::new(Notify::new());
    let server_shutdown = shutdown_notify.clone();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                server_shutdown.notified().await;
            })
            .await
    });

    tokio::select! {
        server_result = &mut server => {
            match server_result {
                Ok(Ok(())) => info!("HTTP server stopped"),
                Ok(Err(error)) => error!(error = %error, "HTTP server failed"),
                Err(error) => error!(error = %error, "HTTP server task join failure"),
            }
            let _ = scheduler_handle.shutdown();
        }
        signal_result = shutdown_signal() => {
            if let Err(error) = signal_result {
                error!(error = %error, "failed to install shutdown signal handler");
            } else {
                info!("shutdown signal received");
            }
            let _ = scheduler_handle.shutdown();
            shutdown_notify.notify_waiters();

            match server.await {
                Ok(Ok(())) => info!("HTTP server stopped gracefully"),
                Ok(Err(error)) => error!(error = %error, "HTTP server stopped with error"),
                Err(error) => error!(error = %error, "HTTP server task join failure"),
            }
        }
    }

    match scheduler_task.await {
        Ok(()) => info!("scheduler stopped gracefully"),
        Err(error) => error!(error = %error, "scheduler task join failure"),
    }

    Ok(())
}

fn init_tracing() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = signal(SignalKind::terminate()).context("failed to register SIGTERM handler")?;
        tokio::select! {
            ctrl_c = tokio::signal::ctrl_c() => {
                ctrl_c.context("failed to listen for Ctrl+C")?;
            }
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("failed to listen for Ctrl+C")?;
    }

    Ok(())
}
