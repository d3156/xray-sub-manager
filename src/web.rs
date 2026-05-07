use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    config::{save_config_atomic, UpdateConfigRequest},
    encoder::encode_subscription,
    parser::Node,
    scheduler::{
        apply_config_prune, stats_snapshot, subscription_cache_snapshot, SchedulerHandle,
        SharedState, UpdateEvent,
    },
};

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
    modem: Option<String>,
}

#[derive(Debug, Serialize)]
struct BasicResponse {
    ok: bool,
    message: String,
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

async fn login(
    State(state): State<AppState>,
    Json(payload): Json<LoginRequest>,
) -> impl IntoResponse {
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

    let current = state.shared.config.read().await.clone();
    let config_to_save = payload.into_config(&current);
    if let Err(error) = config_to_save.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(BasicResponse {
                ok: false,
                message: error.to_string(),
            }),
        )
            .into_response();
    }

    let modems_changed = current.modems != config_to_save.modems;
    let subscription_settings_changed =
        current.subscription_urls != config_to_save.subscription_urls;
    let probe_settings_changed = current.white_url != config_to_save.white_url
        || current.gray_url != config_to_save.gray_url
        || current.network_check_interval_minutes != config_to_save.network_check_interval_minutes;
    let should_refresh = modems_changed
        || subscription_settings_changed
        || probe_settings_changed
        || current.ping_timeout_ms != config_to_save.ping_timeout_ms
        || current.max_concurrent_pings != config_to_save.max_concurrent_pings
        || current.max_concurrent_tunnels != config_to_save.max_concurrent_tunnels;
    let should_check_health = modems_changed || probe_settings_changed;

    match save_config_atomic(&state.shared.config_path, &config_to_save).await {
        Ok(()) => {
            {
                let mut config = state.shared.config.write().await;
                *config = config_to_save;
            }

            if let Err(error) = apply_config_prune(&state.shared).await {
                warn!(error = %error, "failed to prune runtime state after config update");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(BasicResponse {
                        ok: false,
                        message: "Configuration saved, but runtime pruning failed".to_string(),
                    }),
                )
                    .into_response();
            }

            if let Err(error) = state.scheduler.reconfigure() {
                warn!(error = %error, "failed to notify scheduler about config change");
            }
            if should_check_health {
                if let Err(error) = state.scheduler.request_health_check() {
                    warn!(error = %error, "failed to schedule immediate modem health check");
                }
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
        Ok(stats) => Json(stats).into_response(),
        Err(error) => {
            warn!(error = %error, "failed to build stats snapshot");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
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
    let config = state.shared.config.read().await.clone();
    if query.token != config.subscription_token {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let Some(cache) = subscription_cache_snapshot(&state.shared).await else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };

    let modem = query
        .modem
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty());
    let nodes = if let Some(modem_tag) = modem {
        if !config
            .modems
            .iter()
            .any(|modem| modem.modem_tag == modem_tag)
        {
            return StatusCode::NOT_FOUND.into_response();
        }
        let Some(entry) = cache.by_modem.get(modem_tag) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        limited_nodes(entry.nodes.clone(), query.limit)
    } else {
        let mut available = Vec::new();
        for modem in &config.modems {
            if let Some(entry) = cache.by_modem.get(&modem.modem_tag) {
                available.push(entry.nodes.as_slice());
            }
        }
        if available.is_empty() {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        combine_modem_nodes(&available, query.limit)
    };

    let subscription = encode_subscription(&nodes);
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, OPTIONS"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Content-Type"),
    );

    (StatusCode::OK, headers, subscription).into_response()
}

fn limited_nodes(mut nodes: Vec<Node>, limit: Option<usize>) -> Vec<Node> {
    if let Some(limit) = limit {
        nodes.truncate(limit);
    }
    nodes
}

fn combine_modem_nodes(modem_nodes: &[&[Node]], limit: Option<usize>) -> Vec<Node> {
    let Some(limit) = limit else {
        return modem_nodes
            .iter()
            .flat_map(|nodes| nodes.iter().cloned())
            .collect();
    };

    if limit == 0 || modem_nodes.is_empty() {
        return Vec::new();
    }

    let modem_limit = limit.div_ceil(modem_nodes.len());
    let mut combined = Vec::new();
    for nodes in modem_nodes {
        combined.extend(nodes.iter().take(modem_limit).cloned());
    }
    combined.truncate(limit);
    combined
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
    Ok(ws
        .on_upgrade(move |socket| handle_ws(socket, receiver))
        .into_response())
}

async fn handle_ws(
    mut socket: WebSocket,
    mut receiver: tokio::sync::broadcast::Receiver<UpdateEvent>,
) {
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
