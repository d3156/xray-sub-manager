use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{config::{save_config_atomic, AppConfig, UpdateConfigRequest}, encoder::encode_subscription, scheduler::{stats_snapshot, subscription_snapshot, SchedulerHandle, SharedState, UpdateEvent}};

const INDEX_HTML: &str = include_str!("../static/index.html");

#[derive(Clone)]
pub struct AppState {
    pub shared: SharedState,
    pub scheduler: SchedulerHandle,
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    token: String,
}

#[derive(Debug, Deserialize)]
struct SubscriptionQuery {
    token: String,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct BasicResponse {
    ok: bool,
    message: String,
}

#[derive(Debug, Serialize)]
struct StatsResponse {
    last_update: Option<chrono::DateTime<chrono::Utc>>,
    next_update: Option<chrono::DateTime<chrono::Utc>>,
    nodes_total: usize,
    nodes_after_dedup: usize,
    nodes_after_ping: usize,
    nodes_after_tunnel: usize,
    is_updating: bool,
}

pub fn build_router(app_state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/login", post(login))
        .route("/api/config", get(get_config).post(update_config))
        .route("/api/stats", get(get_stats))
        .route("/api/update-now", post(update_now))
        .route("/api/ws", get(ws_handler))
        .route("/sub", get(get_subscription))
        .with_state(app_state)
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn login(State(state): State<AppState>, Json(payload): Json<LoginRequest>) -> impl IntoResponse {
    let config = state.shared.config.read().await;
    if payload.token != config.admin_token {
        return (
            StatusCode::UNAUTHORIZED,
            Json(BasicResponse {
                ok: false,
                message: "Invalid token".to_string(),
            }),
        )
            .into_response();
    }

    let mut headers = HeaderMap::new();
    if let Ok(cookie_value) = HeaderValue::from_str(&format!(
        "auth_token={}; Path=/; HttpOnly; SameSite=Strict",
        config.admin_token
    )) {
        headers.insert(header::SET_COOKIE, cookie_value);
    }

    (
        StatusCode::OK,
        headers,
        Json(BasicResponse {
            ok: true,
            message: "Authenticated".to_string(),
        }),
    )
        .into_response()
}

async fn get_config(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if !is_admin(&state.shared, &headers).await {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let config = state.shared.config.read().await;
    Json(config.public_view()).into_response()
}

async fn update_config(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<UpdateConfigRequest>,
) -> impl IntoResponse {
    if !is_admin(&state.shared, &headers).await {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    if payload.update_interval_minutes == 0
        || payload.ping_timeout_ms < 100
        || payload.ping_timeout_ms > 30_000
        || payload.max_concurrent_pings == 0
        || payload.max_concurrent_pings > 500
        || payload.max_concurrent_tunnels == 0
        || payload.max_concurrent_tunnels > 500
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(BasicResponse {
                ok: false,
                message: "Invalid configuration values".to_string(),
            }),
        )
            .into_response();
    }

    let sanitized_urls = payload
        .subscription_urls
        .into_iter()
        .map(|url| url.trim().to_string())
        .filter(|url| !url.is_empty())
        .collect::<Vec<_>>();

    let (config_to_save, should_refresh) = {
        let current = state.shared.config.read().await.clone();
        let should_refresh = current.subscription_urls != sanitized_urls
            || current.ping_timeout_ms != payload.ping_timeout_ms
            || current.max_concurrent_pings != payload.max_concurrent_pings
            || current.max_concurrent_tunnels != payload.max_concurrent_tunnels;
        (
            AppConfig {
                web_port: current.web_port,
                admin_token: current.admin_token,
                subscription_token: current.subscription_token,
                update_interval_minutes: payload.update_interval_minutes,
                ping_timeout_ms: payload.ping_timeout_ms,
                max_concurrent_pings: payload.max_concurrent_pings,
                max_concurrent_tunnels: payload.max_concurrent_tunnels,
                subscription_urls: sanitized_urls,
                last_update: current.last_update,
                nodes_total: current.nodes_total,
                nodes_after_dedup: current.nodes_after_dedup,
                nodes_after_ping: current.nodes_after_ping,
                nodes_after_tunnel: current.nodes_after_tunnel,
            },
            should_refresh,
        )
    };

    match save_config_atomic(&state.shared.config_path, &config_to_save).await {
        Ok(()) => {
            let mut config = state.shared.config.write().await;
            *config = config_to_save;
            drop(config);
            if let Err(error) = state.scheduler.reconfigure() {
                warn!(error = %error, "failed to notify scheduler about config change");
            }
            if should_refresh {
                if let Err(error) = state.scheduler.request_update() {
                    warn!(error = %error, "failed to schedule immediate refresh after config change");
                }
            }
            (
                StatusCode::OK,
                Json(BasicResponse {
                    ok: true,
                    message: "Configuration updated".to_string(),
                }),
            )
                .into_response()
        }
        Err(error) => {
            warn!(error = %error, "failed to persist updated configuration");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(BasicResponse {
                    ok: false,
                    message: "Failed to save configuration".to_string(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_stats(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if !is_admin(&state.shared, &headers).await {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    match stats_snapshot(&state.shared).await {
        Ok((config, next_update, is_updating)) => Json(StatsResponse {
            last_update: config.last_update,
            next_update,
            nodes_total: config.nodes_total,
            nodes_after_dedup: config.nodes_after_dedup,
            nodes_after_ping: config.nodes_after_ping,
            nodes_after_tunnel: config.nodes_after_tunnel,
            is_updating,
        })
        .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn update_now(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if !is_admin(&state.shared, &headers).await {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    match state.scheduler.request_update() {
        Ok(()) => Json(BasicResponse {
            ok: true,
            message: "Update started".to_string(),
        })
        .into_response(),
        Err(error) => {
            warn!(error = %error, "failed to send manual update request");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(BasicResponse {
                    ok: false,
                    message: "Scheduler is unavailable".to_string(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_subscription(
    State(state): State<AppState>,
    Query(query): Query<SubscriptionQuery>,
) -> impl IntoResponse {
    let config = state.shared.config.read().await;
    if query.token != config.subscription_token {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    drop(config);

    let Some(nodes) = subscription_snapshot(&state.shared).await else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };

    let limit = query.limit.unwrap_or(nodes.len()).min(nodes.len());
    let subscription = encode_subscription(&nodes[..limit]);

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    headers.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, OPTIONS"));
    headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("Content-Type"));

    (StatusCode::OK, headers, subscription).into_response()
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    if !is_admin(&state.shared, &headers).await {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let receiver = state.shared.events_tx.subscribe();
    Ok(ws.on_upgrade(move |socket| handle_ws(socket, receiver)).into_response())
}

async fn handle_ws(mut socket: WebSocket, mut receiver: tokio::sync::broadcast::Receiver<UpdateEvent>) {
    loop {
        tokio::select! {
            message = receiver.recv() => {
                match message {
                    Ok(event) => {
                        let payload = match serde_json::to_string(&event) {
                            Ok(payload) => payload,
                            Err(error) => {
                                warn!(error = %error, "failed to serialize WebSocket update event");
                                continue;
                            }
                        };
                        if socket.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "WebSocket client lagged behind update events");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(error)) => {
                        warn!(error = %error, "WebSocket receive error");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn is_admin(shared: &SharedState, headers: &HeaderMap) -> bool {
    let token = extract_bearer_token(headers).or_else(|| extract_cookie_token(headers));
    let Some(token) = token else {
        return false;
    };

    let config = shared.config.read().await;
    token == config.admin_token
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let header = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    header.strip_prefix("Bearer ").map(ToString::to_string)
}

fn extract_cookie_token(headers: &HeaderMap) -> Option<String> {
    let cookie_header = headers.get(header::COOKIE)?.to_str().ok()?;
    for part in cookie_header.split(';') {
        let trimmed = part.trim();
        if let Some(value) = trimmed.strip_prefix("auth_token=") {
            return Some(value.to_string());
        }
    }
    None
}
