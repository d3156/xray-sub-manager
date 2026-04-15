use std::{env, path::{Path, PathBuf}, process::Stdio, sync::{atomic::{AtomicUsize, Ordering}, Arc}};

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Map, Value};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    sync::Semaphore,
    task::JoinSet,
    time::{sleep, timeout, Duration, Instant},
};
use tracing::warn;
use uuid::Uuid;

use crate::{parser::{Node, Protocol}, pinger::ProgressCallback};

const PROBE_HOST: [u8; 4] = [1, 1, 1, 1];
const PROBE_PORT: u16 = 443;

pub async fn probe_tunnels(
    nodes: Vec<Node>,
    probe_timeout: Duration,
    max_concurrent: usize,
    progress_callback: Option<ProgressCallback>,
) -> Vec<Node> {
    if nodes.is_empty() {
        return nodes;
    }

    let total = nodes.len();
    let semaphore = Arc::new(Semaphore::new(max_concurrent.max(1)));
    let done_counter = Arc::new(AtomicUsize::new(0));
    let mut tasks = JoinSet::new();

    for node in nodes {
        let semaphore = semaphore.clone();
        let done_counter = done_counter.clone();
        let progress_callback = progress_callback.clone();

        tasks.spawn(async move {
            let permit = semaphore.acquire_owned().await.ok();
            let checked_node = probe_single_tunnel(node, probe_timeout).await;

            drop(permit);
            let done = done_counter.fetch_add(1, Ordering::SeqCst) + 1;
            if let Some(callback) = progress_callback {
                callback(done, total);
            }

            checked_node
        });
    }

    let mut online = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(node) if node.tunnel_ok => online.push(node),
            Ok(_) => {}
            Err(error) => warn!(error = %error, "tunnel worker task failed"),
        }
    }

    online
}

async fn probe_single_tunnel(mut node: Node, probe_timeout: Duration) -> Node {
    let node_name = node.display_name.clone();
    let node_target = format!("{}:{}", node.host, node.port);

    match probe_single_tunnel_inner(&mut node, probe_timeout).await {
        Ok(()) => node,
        Err(error) => {
            warn!(node = %node_name, target = %node_target, error = %error, "tunnel probe failed");
            node.tunnel_ok = false;
            node.tunnel_ms = None;
            node
        }
    }
}

async fn probe_single_tunnel_inner(node: &mut Node, probe_timeout: Duration) -> Result<()> {
    let temp_dir = tunnel_work_dir();
    fs::create_dir_all(&temp_dir)
        .await
        .with_context(|| format!("failed to create tunnel temp dir {}", temp_dir.display()))?;

    let listen_port = reserve_local_port().await?;
    let config_path = temp_dir.join("sing-box.json");
    let config = build_sing_box_config(node, listen_port)?;
    let serialized = serde_json::to_vec_pretty(&config).context("failed to serialize sing-box config")?;
    fs::write(&config_path, serialized)
        .await
        .with_context(|| format!("failed to write tunnel config {}", config_path.display()))?;

    let binary = env::var("SING_BOX_BIN").unwrap_or_else(|_| "/usr/bin/sing-box".to_string());
    let mut child = Command::new(&binary)
        .arg("run")
        .arg("-c")
        .arg(&config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to start {binary}"))?;

    let local_addr = format!("127.0.0.1:{listen_port}");
    let startup_result = timeout(probe_timeout, wait_for_local_proxy(&mut child, &local_addr)).await;
    match startup_result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            cleanup_child_and_dir(&mut child, &temp_dir).await;
            return Err(error);
        }
        Err(_) => {
            cleanup_child_and_dir(&mut child, &temp_dir).await;
            return Err(anyhow!("tunnel startup timed out after {} ms", probe_timeout.as_millis()));
        }
    }

    let started_at = Instant::now();
    let measure_result = timeout(probe_timeout, measure_socks_connect(&local_addr)).await;
    match measure_result {
        Ok(Ok(())) => {
            node.tunnel_ok = true;
            node.tunnel_ms = Some(started_at.elapsed().as_millis() as u64);
        }
        Ok(Err(error)) => {
            cleanup_child_and_dir(&mut child, &temp_dir).await;
            return Err(error);
        }
        Err(_) => {
            cleanup_child_and_dir(&mut child, &temp_dir).await;
            return Err(anyhow!("tunnel probe timed out after {} ms", probe_timeout.as_millis()));
        }
    }

    cleanup_child_and_dir(&mut child, &temp_dir).await;
    Ok(())
}

async fn reserve_local_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to allocate local tunnel port")?;
    let port = listener
        .local_addr()
        .context("failed to read local tunnel address")?
        .port();
    drop(listener);
    Ok(port)
}

async fn wait_for_local_proxy(child: &mut Child, local_addr: &str) -> Result<()> {
    loop {
        if let Some(status) = child.try_wait().context("failed to poll sing-box process")? {
            return Err(anyhow!("sing-box exited early with status {status}"));
        }

        match TcpStream::connect(local_addr).await {
            Ok(stream) => {
                drop(stream);
                return Ok(());
            }
            Err(_) => sleep(Duration::from_millis(50)).await,
        }
    }
}

async fn measure_socks_connect(local_addr: &str) -> Result<()> {
    let mut stream = TcpStream::connect(local_addr)
        .await
        .with_context(|| format!("failed to connect to local proxy {local_addr}"))?;

    stream
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .context("failed to send SOCKS5 greeting")?;

    let mut method = [0u8; 2];
    stream
        .read_exact(&mut method)
        .await
        .context("failed to read SOCKS5 greeting response")?;
    if method != [0x05, 0x00] {
        return Err(anyhow!("SOCKS5 proxy rejected no-auth method"));
    }

    let request = [
        0x05,
        0x01,
        0x00,
        0x01,
        PROBE_HOST[0],
        PROBE_HOST[1],
        PROBE_HOST[2],
        PROBE_HOST[3],
        (PROBE_PORT >> 8) as u8,
        (PROBE_PORT & 0xff) as u8,
    ];
    stream
        .write_all(&request)
        .await
        .context("failed to send SOCKS5 connect request")?;

    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .await
        .context("failed to read SOCKS5 connect response")?;
    if header[0] != 0x05 {
        return Err(anyhow!("invalid SOCKS5 version in connect response"));
    }
    if header[1] != 0x00 {
        return Err(anyhow!("SOCKS5 connect failed with reply code {}", header[1]));
    }

    consume_bound_address(&mut stream, header[3]).await?;
    Ok(())
}

async fn consume_bound_address(stream: &mut TcpStream, atyp: u8) -> Result<()> {
    match atyp {
        0x01 => {
            let mut buf = [0u8; 6];
            stream.read_exact(&mut buf).await.context("failed to read IPv4 bind address")?;
        }
        0x03 => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await.context("failed to read domain length")?;
            let mut buf = vec![0u8; len[0] as usize + 2];
            stream.read_exact(&mut buf).await.context("failed to read domain bind address")?;
        }
        0x04 => {
            let mut buf = [0u8; 18];
            stream.read_exact(&mut buf).await.context("failed to read IPv6 bind address")?;
        }
        _ => return Err(anyhow!("unsupported SOCKS5 address type {atyp}")),
    }

    Ok(())
}

async fn cleanup_child_and_dir(child: &mut Child, temp_dir: &Path) {
    match child.try_wait() {
        Ok(Some(_)) => {}
        Ok(None) => {
            if let Err(error) = child.start_kill() {
                warn!(error = %error, "failed to stop sing-box process");
            }
            if let Err(error) = child.wait().await {
                warn!(error = %error, "failed to wait for sing-box shutdown");
            }
        }
        Err(error) => warn!(error = %error, "failed to poll sing-box shutdown status"),
    }

    if let Err(error) = fs::remove_dir_all(temp_dir).await {
        warn!(path = %temp_dir.display(), error = %error, "failed to remove tunnel temp dir");
    }
}

fn tunnel_work_dir() -> PathBuf {
    env::temp_dir().join(format!("xray-sub-manager-tunnel-{}", Uuid::new_v4()))
}

fn build_sing_box_config(node: &Node, listen_port: u16) -> Result<Value> {
    let outbound = build_outbound(node)?;

    Ok(json!({
        "log": { "disabled": true },
        "inbounds": [
            {
                "type": "socks",
                "tag": "local-socks",
                "listen": "127.0.0.1",
                "listen_port": listen_port
            }
        ],
        "outbounds": [outbound],
        "route": {
            "final": "proxy"
        }
    }))
}

fn build_outbound(node: &Node) -> Result<Value> {
    let mut outbound = Map::new();
    let modem_interface = std::env::var("MODEM_INTERFACE")
        .unwrap_or_else(|_| "enx020d0c073330".to_string());

    outbound.insert("tag".to_string(), Value::String("proxy".to_string()));
    outbound.insert("server".to_string(), Value::String(node.host.clone()));
    outbound.insert("server_port".to_string(), Value::Number(node.port.into()));
    if !modem_interface.is_empty() {
        outbound.insert("bind_interface".to_string(), Value::String(modem_interface));
    }

    match node.protocol {
        Protocol::Vmess => {
            outbound.insert("type".to_string(), Value::String("vmess".to_string()));
            outbound.insert("uuid".to_string(), string_param(node, "id")?);
            outbound.insert(
                "security".to_string(),
                Value::String(node.params.get("scy").cloned().unwrap_or_else(|| "auto".to_string())),
            );
            if let Some(aid) = node.params.get("aid").and_then(|value| value.parse::<u64>().ok()) {
                outbound.insert("alter_id".to_string(), Value::Number(aid.into()));
            }
        }
        Protocol::Vless => {
            outbound.insert("type".to_string(), Value::String("vless".to_string()));
            outbound.insert("uuid".to_string(), string_param(node, "id")?);
            if let Some(flow) = node.params.get("flow") {
                outbound.insert("flow".to_string(), Value::String(flow.clone()));
            }
        }
        Protocol::Trojan => {
            outbound.insert("type".to_string(), Value::String("trojan".to_string()));
            outbound.insert("password".to_string(), string_param(node, "password")?);
        }
        Protocol::Shadowsocks => {
            if node.params.contains_key("plugin") {
                return Err(anyhow!("shadowsocks plugins are not supported by tunnel probe"));
            }
            outbound.insert("type".to_string(), Value::String("shadowsocks".to_string()));
            outbound.insert("method".to_string(), string_param(node, "method")?);
            outbound.insert("password".to_string(), string_param(node, "password")?);
        }
        Protocol::ShadowsocksR => {
            return Err(anyhow!("shadowsocksr is not supported by sing-box tunnel probe"));
        }
        Protocol::Hysteria2 => {
            outbound.insert("type".to_string(), Value::String("hysteria2".to_string()));
            outbound.insert("password".to_string(), string_param(node, "password")?);
            if let Some(obfs) = node.params.get("obfs") {
                let mut obfs_config = Map::new();
                obfs_config.insert("type".to_string(), Value::String(obfs.clone()));
                if let Some(password) = node.params.get("obfs-password") {
                    obfs_config.insert("password".to_string(), Value::String(password.clone()));
                }
                outbound.insert("obfs".to_string(), Value::Object(obfs_config));
            }
        }
        Protocol::Tuic => {
            outbound.insert("type".to_string(), Value::String("tuic".to_string()));
            outbound.insert("uuid".to_string(), string_param(node, "uuid")?);
            outbound.insert("password".to_string(), string_param(node, "password")?);
            if let Some(value) = node.params.get("congestion_control") {
                outbound.insert("congestion_control".to_string(), Value::String(value.clone()));
            }
            if let Some(value) = node.params.get("udp_relay_mode") {
                outbound.insert("udp_relay_mode".to_string(), Value::String(value.clone()));
            }
        }
    }

    if let Some(transport) = build_transport(node)? {
        outbound.insert("transport".to_string(), transport);
    }
    if let Some(tls) = build_tls(node) {
        outbound.insert("tls".to_string(), tls);
    }

    Ok(Value::Object(outbound))
}

fn build_transport(node: &Node) -> Result<Option<Value>> {
    let network = node
        .params
        .get("type")
        .or_else(|| node.params.get("net"))
        .map(|value| value.as_str())
        .unwrap_or("tcp");

    let transport = match network {
        "tcp" | "raw" => return Ok(None),
        "ws" => {
            let mut transport = Map::new();
            transport.insert("type".to_string(), Value::String("ws".to_string()));
            if let Some(path) = node.params.get("path") {
                transport.insert("path".to_string(), Value::String(path.clone()));
            }
            if let Some(host) = node.params.get("host") {
                transport.insert("headers".to_string(), json!({ "Host": host }));
            }
            Value::Object(transport)
        }
        "grpc" => {
            let mut transport = Map::new();
            transport.insert("type".to_string(), Value::String("grpc".to_string()));
            if let Some(service_name) = node.params.get("serviceName").or_else(|| node.params.get("path")) {
                transport.insert(
                    "service_name".to_string(),
                    Value::String(service_name.trim_start_matches('/').to_string()),
                );
            }
            Value::Object(transport)
        }
        "http" | "h2" => {
            let mut transport = Map::new();
            transport.insert("type".to_string(), Value::String("http".to_string()));
            if let Some(path) = node.params.get("path") {
                transport.insert("path".to_string(), Value::String(path.clone()));
            }
            if let Some(host) = node.params.get("host") {
                let hosts = host
                    .split(',')
                    .map(|value| Value::String(value.trim().to_string()))
                    .collect::<Vec<_>>();
                if !hosts.is_empty() {
                    transport.insert("host".to_string(), Value::Array(hosts));
                }
            }
            Value::Object(transport)
        }
        "httpupgrade" => {
            let mut transport = Map::new();
            transport.insert("type".to_string(), Value::String("httpupgrade".to_string()));
            if let Some(host) = node.params.get("host") {
                transport.insert("host".to_string(), Value::String(host.clone()));
            }
            if let Some(path) = node.params.get("path") {
                transport.insert("path".to_string(), Value::String(path.clone()));
            }
            Value::Object(transport)
        }
        "quic" => {
            let mut transport = Map::new();
            transport.insert("type".to_string(), Value::String("quic".to_string()));
            Value::Object(transport)
        }
        other => return Err(anyhow!("unsupported transport type {other}")),
    };

    Ok(Some(transport))
}

fn build_tls(node: &Node) -> Option<Value> {
    let security = node.params.get("security").map(|value| value.as_str()).unwrap_or("none");
    let has_tls = security == "tls"
        || security == "reality"
        || node.params.contains_key("sni")
        || node.params.contains_key("alpn")
        || matches!(node.protocol, Protocol::Hysteria2 | Protocol::Tuic);
    if !has_tls {
        return None;
    }

    let server_name = node.params.get("sni").cloned().unwrap_or_else(|| node.host.clone());
    let mut tls = Map::new();
    tls.insert("enabled".to_string(), Value::Bool(true));
    tls.insert("server_name".to_string(), Value::String(server_name));
    tls.insert("insecure".to_string(), Value::Bool(true));

    if let Some(alpn) = node.params.get("alpn") {
        let items = alpn
            .split(',')
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| Value::String(value.to_string()))
            .collect::<Vec<_>>();
        if !items.is_empty() {
            tls.insert("alpn".to_string(), Value::Array(items));
        }
    }

    if security == "reality" {
        let mut reality = Map::new();
        reality.insert("enabled".to_string(), Value::Bool(true));
        if let Some(public_key) = node.params.get("pbk") {
            reality.insert("public_key".to_string(), Value::String(public_key.clone()));
        }
        if let Some(short_id) = node.params.get("sid") {
            reality.insert("short_id".to_string(), Value::String(short_id.clone()));
        }
        tls.insert("reality".to_string(), Value::Object(reality));

        if let Some(fingerprint) = node.params.get("fp") {
            tls.insert(
                "utls".to_string(),
                json!({
                    "enabled": true,
                    "fingerprint": fingerprint,
                }),
            );
        }
    }

    Some(Value::Object(tls))
}

fn string_param(node: &Node, key: &str) -> Result<Value> {
    node.params
        .get(key)
        .cloned()
        .map(Value::String)
        .ok_or_else(|| anyhow!("node missing required parameter {key}"))
}
