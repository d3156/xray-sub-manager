use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use base64::{
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD},
    Engine,
};
use percent_encoding::{
    percent_decode_str, utf8_percent_encode, AsciiSet, CONTROLS, NON_ALPHANUMERIC,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracing::warn;
use url::form_urlencoded;

const USERINFO_ENCODE_SET: &AsciiSet = NON_ALPHANUMERIC;
const FRAGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'`');

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Vmess,
    Vless,
    Trojan,
    Shadowsocks,
    ShadowsocksR,
    Hysteria2,
    Tuic,
}

impl Protocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Vmess => "vmess",
            Self::Vless => "vless",
            Self::Trojan => "trojan",
            Self::Shadowsocks => "ss",
            Self::ShadowsocksR => "ssr",
            Self::Hysteria2 => "hysteria2",
            Self::Tuic => "tuic",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub protocol: Protocol,
    pub host: String,
    pub port: u16,
    pub params: HashMap<String, String>,
    pub raw_name: String,
    pub display_name: String,
    pub original_uri: String,
    pub ping_ok: bool,
    pub ping_ms: Option<u64>,
    #[serde(default)]
    pub tunnel_ok: bool,
    #[serde(default)]
    pub tunnel_ms: Option<u64>,
}

pub fn parse_subscription(body: &str) -> Result<Vec<Node>> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    if let Ok(json_value) = serde_json::from_str::<Value>(trimmed) {
        if let Some(outbounds) = json_value.get("outbounds").and_then(Value::as_array) {
            return parse_sing_box(outbounds);
        }

        if let Some(servers) = json_value.get("servers").and_then(Value::as_array) {
            return parse_sip008(servers);
        }

        return Err(anyhow!("unsupported JSON subscription format"));
    }

    if let Some(decoded) = decode_possible_base64_list(trimmed)? {
        return parse_uri_list(&decoded);
    }

    parse_uri_list(trimmed)
}

fn parse_uri_list(content: &str) -> Result<Vec<Node>> {
    let mut nodes = Vec::new();
    let mut invalid_lines = 0usize;

    for line in content.lines() {
        let candidate = line.trim();
        if candidate.is_empty() {
            continue;
        }

        if !has_supported_prefix(candidate) {
            invalid_lines += 1;
            warn!(line = %candidate, "skipping unsupported node URI line");
            continue;
        }

        match parse_uri_node(candidate) {
            Ok(node) => nodes.push(node),
            Err(error) => {
                invalid_lines += 1;
                warn!(uri = %candidate, error = %error, "failed to parse node URI");
            }
        }
    }

    if nodes.is_empty() && invalid_lines > 0 {
        return Err(anyhow!("no valid nodes found in URI list"));
    }

    Ok(nodes)
}

fn parse_uri_node(uri: &str) -> Result<Node> {
    if let Some(encoded) = uri.strip_prefix("vmess://") {
        return parse_vmess_uri(uri, encoded);
    }
    if uri.starts_with("vless://") {
        return parse_standard_uri(uri, Protocol::Vless);
    }
    if uri.starts_with("trojan://") {
        return parse_standard_uri(uri, Protocol::Trojan);
    }
    if uri.starts_with("hysteria2://") {
        return parse_standard_uri(uri, Protocol::Hysteria2);
    }
    if uri.starts_with("tuic://") {
        return parse_standard_uri(uri, Protocol::Tuic);
    }
    if let Some(content) = uri.strip_prefix("ss://") {
        return parse_shadowsocks_uri(uri, content);
    }
    if let Some(content) = uri.strip_prefix("ssr://") {
        return parse_shadowsocksr_uri(uri, content);
    }

    Err(anyhow!("unsupported URI scheme"))
}

fn parse_vmess_uri(original_uri: &str, encoded: &str) -> Result<Node> {
    let decoded = decode_base64_flexible(encoded).context("failed to decode vmess payload")?;
    let payload = String::from_utf8(decoded).context("vmess payload is not valid UTF-8")?;
    let value = serde_json::from_str::<Value>(&payload).context("invalid vmess JSON payload")?;

    let host = value
        .get("add")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("vmess node missing add field"))?
        .to_string();
    let port =
        parse_port(value.get("port")).ok_or_else(|| anyhow!("vmess node missing port field"))?;
    let raw_name = value
        .get("ps")
        .and_then(Value::as_str)
        .map(decode_fragment)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "Unnamed".to_string());

    let mut params = collect_scalar_fields(value.as_object());
    if let Some(id) = value.get("id").and_then(Value::as_str) {
        params.insert("id".to_string(), id.to_string());
    }
    if let Some(aid) = value.get("aid") {
        params.insert("aid".to_string(), scalar_to_string(aid));
    }
    if let Some(net) = value.get("net").and_then(Value::as_str) {
        params.insert("net".to_string(), net.to_string());
    }
    if let Some(path) = value.get("path").and_then(Value::as_str) {
        params.insert("path".to_string(), path.to_string());
    }
    if let Some(header_host) = value.get("host").and_then(Value::as_str) {
        params.insert("host".to_string(), header_host.to_string());
    }

    Ok(Node {
        protocol: Protocol::Vmess,
        host,
        port,
        params,
        raw_name: raw_name.clone(),
        display_name: raw_name,
        original_uri: original_uri.to_string(),
        ping_ok: false,
        ping_ms: None,
        tunnel_ok: false,
        tunnel_ms: None,
    })
}

fn parse_standard_uri(original_uri: &str, protocol: Protocol) -> Result<Node> {
    let url = url::Url::parse(original_uri)
        .with_context(|| format!("invalid {} URI", protocol.as_str()))?;
    let host = url
        .host_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("node missing host"))?
        .to_string();
    let port = url.port().ok_or_else(|| anyhow!("node missing port"))?;

    let mut params = HashMap::new();
    for (key, value) in url.query_pairs() {
        params.insert(key.into_owned(), value.into_owned());
    }

    let username = percent_decode_str(url.username())
        .decode_utf8_lossy()
        .to_string();
    let password = url
        .password()
        .map(|value| percent_decode_str(value).decode_utf8_lossy().to_string());

    match protocol {
        Protocol::Vless => {
            if username.is_empty() {
                return Err(anyhow!("vless node missing UUID"));
            }
            params.insert("id".to_string(), username);
        }
        Protocol::Trojan | Protocol::Hysteria2 => {
            if username.is_empty() {
                return Err(anyhow!("node missing password"));
            }
            params.insert("password".to_string(), username);
        }
        Protocol::Tuic => {
            if username.is_empty() {
                return Err(anyhow!("tuic node missing UUID"));
            }
            params.insert("uuid".to_string(), username);
            if let Some(password) = password {
                params.insert("password".to_string(), password);
            }
        }
        _ => {}
    }

    let raw_name = url
        .fragment()
        .map(decode_fragment)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "Unnamed".to_string());

    Ok(Node {
        protocol,
        host,
        port,
        params,
        raw_name: raw_name.clone(),
        display_name: raw_name,
        original_uri: original_uri.to_string(),
        ping_ok: false,
        ping_ms: None,
        tunnel_ok: false,
        tunnel_ms: None,
    })
}

fn parse_shadowsocks_uri(original_uri: &str, content: &str) -> Result<Node> {
    let (without_fragment, raw_name) = split_fragment(content);
    let (main, query) = split_query(without_fragment);
    let mut params = parse_query_params(query);
    let (host, port, method, password) = if let Some((cred_part, server_part)) =
        main.rsplit_once('@')
    {
        let decoded_credentials = String::from_utf8(
            decode_base64_flexible(cred_part).context("failed to decode ss credentials")?,
        )
        .context("ss credential payload is not valid UTF-8")?;
        let (method, password) = decoded_credentials
            .split_once(':')
            .ok_or_else(|| anyhow!("invalid ss credential payload"))?;
        let (host, port) = parse_host_port(server_part)?;
        (host, port, method.to_string(), password.to_string())
    } else {
        let decoded_full =
            String::from_utf8(decode_base64_flexible(main).context("failed to decode ss payload")?)
                .context("ss payload is not valid UTF-8")?;
        let (credentials, server_part) = decoded_full
            .rsplit_once('@')
            .ok_or_else(|| anyhow!("invalid ss URI payload"))?;
        let (method, password) = credentials
            .split_once(':')
            .ok_or_else(|| anyhow!("invalid ss credential format"))?;
        let (host, port) = parse_host_port(server_part)?;
        (host, port, method.to_string(), password.to_string())
    };

    params.insert("method".to_string(), method);
    params.insert("password".to_string(), password);

    Ok(Node {
        protocol: Protocol::Shadowsocks,
        host,
        port,
        params,
        raw_name: raw_name.clone(),
        display_name: raw_name,
        original_uri: original_uri.to_string(),
        ping_ok: false,
        ping_ms: None,
        tunnel_ok: false,
        tunnel_ms: None,
    })
}

fn parse_shadowsocksr_uri(original_uri: &str, content: &str) -> Result<Node> {
    let decoded =
        String::from_utf8(decode_base64_flexible(content).context("failed to decode ssr payload")?)
            .context("ssr payload is not valid UTF-8")?;

    let (main, query) = decoded.split_once("/?").unwrap_or((&decoded, ""));
    let fields: Vec<&str> = main.split(':').collect();
    if fields.len() < 6 {
        return Err(anyhow!("invalid ssr URI payload"));
    }

    let host = fields[0].to_string();
    let port = fields[1]
        .parse::<u16>()
        .with_context(|| format!("invalid ssr port {}", fields[1]))?;
    let protocol_name = fields[2].to_string();
    let method = fields[3].to_string();
    let obfs = fields[4].to_string();
    let password = String::from_utf8(
        decode_base64_flexible(fields[5]).context("failed to decode ssr password")?,
    )
    .context("ssr password is not valid UTF-8")?;

    let mut params = HashMap::new();
    params.insert("protocol".to_string(), protocol_name);
    params.insert("method".to_string(), method);
    params.insert("obfs".to_string(), obfs);
    params.insert("password".to_string(), password);

    let mut raw_name = "Unnamed".to_string();
    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        let decoded_value = decode_base64_string(&value);
        if key == "remarks" {
            raw_name = decoded_value;
        } else {
            params.insert(key.into_owned(), decoded_value);
        }
    }

    Ok(Node {
        protocol: Protocol::ShadowsocksR,
        host,
        port,
        params,
        raw_name: raw_name.clone(),
        display_name: raw_name,
        original_uri: original_uri.to_string(),
        ping_ok: false,
        ping_ms: None,
        tunnel_ok: false,
        tunnel_ms: None,
    })
}

fn parse_sing_box(outbounds: &[Value]) -> Result<Vec<Node>> {
    let mut nodes = Vec::new();

    for outbound in outbounds {
        let Some(object) = outbound.as_object() else {
            continue;
        };

        match parse_sing_box_outbound(object) {
            Ok(Some(node)) => nodes.push(node),
            Ok(None) => {}
            Err(error) => warn!(error = %error, "failed to parse sing-box outbound"),
        }
    }

    if nodes.is_empty() {
        return Err(anyhow!(
            "sing-box subscription did not contain supported outbounds"
        ));
    }

    Ok(nodes)
}

fn parse_sip008(servers: &[Value]) -> Result<Vec<Node>> {
    let mut nodes = Vec::new();

    for server in servers {
        let Some(object) = server.as_object() else {
            continue;
        };

        match parse_sip008_server(object) {
            Ok(node) => nodes.push(node),
            Err(error) => warn!(error = %error, "failed to parse SIP008 server entry"),
        }
    }

    if nodes.is_empty() {
        return Err(anyhow!(
            "sip008 subscription did not contain supported servers"
        ));
    }

    Ok(nodes)
}

fn parse_sip008_server(object: &Map<String, Value>) -> Result<Node> {
    let host = get_string(object, "server").ok_or_else(|| anyhow!("sip008 server missing host"))?;
    let port =
        get_u16(object, "server_port").ok_or_else(|| anyhow!("sip008 server missing port"))?;
    let method =
        get_string(object, "method").ok_or_else(|| anyhow!("sip008 server missing method"))?;
    let password =
        get_string(object, "password").ok_or_else(|| anyhow!("sip008 server missing password"))?;
    let raw_name = get_string(object, "remarks").unwrap_or_else(|| "Unnamed".to_string());

    let mut params = collect_scalar_fields(Some(object));
    params.insert("method".to_string(), method.clone());
    params.insert("password".to_string(), password.clone());

    let original_uri = build_ss_uri(&host, port, &method, &password, &params, &raw_name);
    Ok(Node {
        protocol: Protocol::Shadowsocks,
        host,
        port,
        params,
        raw_name: raw_name.clone(),
        display_name: raw_name,
        original_uri,
        ping_ok: false,
        ping_ms: None,
        tunnel_ok: false,
        tunnel_ms: None,
    })
}

fn parse_sing_box_outbound(object: &Map<String, Value>) -> Result<Option<Node>> {
    let outbound_type =
        get_string(object, "type").ok_or_else(|| anyhow!("sing-box outbound missing type"))?;
    let tag = get_string(object, "tag").unwrap_or_else(|| "Unnamed".to_string());
    let host = match get_string(object, "server") {
        Some(host) => host,
        None => return Ok(None),
    };
    let port = match get_u16(object, "server_port") {
        Some(port) => port,
        None => return Ok(None),
    };

    let transport = object.get("transport").and_then(Value::as_object);
    let tls = object.get("tls").and_then(Value::as_object);
    let network = transport.and_then(|value| get_string(value, "type"));
    let path = transport.and_then(extract_transport_path);
    let host_header = transport.and_then(extract_transport_host);
    let service_name = transport.and_then(|value| get_string(value, "service_name"));
    let mode = transport.and_then(|value| get_string(value, "mode"));
    let header_type = transport.and_then(extract_header_type);
    let security = if tls
        .and_then(|tls_obj| get_bool(tls_obj, "enabled"))
        .unwrap_or(false)
    {
        "tls".to_string()
    } else {
        get_string(object, "security").unwrap_or_else(|| "none".to_string())
    };
    let sni = tls.and_then(|value| get_string(value, "server_name"));
    let alpn = tls
        .and_then(|value| value.get("alpn"))
        .and_then(stringify_value)
        .filter(|value| !value.is_empty());

    let mut params = collect_scalar_fields(Some(object));
    flatten_value("transport", object.get("transport"), &mut params);
    flatten_value("tls", object.get("tls"), &mut params);
    if let Some(network) = &network {
        params.insert("net".to_string(), network.clone());
        params.insert("type".to_string(), network.clone());
    }
    if let Some(path) = &path {
        params.insert("path".to_string(), path.clone());
    }
    if let Some(host_header) = &host_header {
        params.insert("host".to_string(), host_header.clone());
    }
    if let Some(service_name) = &service_name {
        params.insert("serviceName".to_string(), service_name.clone());
    }
    if let Some(mode) = &mode {
        params.insert("mode".to_string(), mode.clone());
    }
    if let Some(header_type) = &header_type {
        params.insert("headerType".to_string(), header_type.clone());
    }
    params.insert("security".to_string(), security.clone());
    if let Some(sni) = &sni {
        params.insert("sni".to_string(), sni.clone());
    }
    if let Some(alpn) = &alpn {
        params.insert("alpn".to_string(), alpn.clone());
    }

    let protocol = match outbound_type.as_str() {
        "vmess" => Protocol::Vmess,
        "vless" => Protocol::Vless,
        "trojan" => Protocol::Trojan,
        "shadowsocks" => Protocol::Shadowsocks,
        "hysteria2" => Protocol::Hysteria2,
        "tuic" => Protocol::Tuic,
        _ => return Ok(None),
    };

    let original_uri = match protocol {
        Protocol::Vmess => {
            let id = get_string(object, "uuid")
                .or_else(|| get_string(object, "id"))
                .ok_or_else(|| anyhow!("vmess outbound missing uuid"))?;
            let aid = get_string(object, "alter_id").unwrap_or_else(|| "0".to_string());
            params.insert("id".to_string(), id.clone());
            params.insert("aid".to_string(), aid.clone());

            let mut vmess_payload = serde_json::Map::new();
            vmess_payload.insert("v".to_string(), Value::String("2".to_string()));
            vmess_payload.insert("ps".to_string(), Value::String(tag.clone()));
            vmess_payload.insert("add".to_string(), Value::String(host.clone()));
            vmess_payload.insert("port".to_string(), Value::String(port.to_string()));
            vmess_payload.insert("id".to_string(), Value::String(id));
            vmess_payload.insert("aid".to_string(), Value::String(aid));
            vmess_payload.insert(
                "scy".to_string(),
                Value::String(get_string(object, "security").unwrap_or_else(|| "auto".to_string())),
            );
            vmess_payload.insert(
                "net".to_string(),
                Value::String(network.clone().unwrap_or_else(|| "tcp".to_string())),
            );
            vmess_payload.insert(
                "type".to_string(),
                Value::String(header_type.clone().unwrap_or_else(|| "none".to_string())),
            );
            vmess_payload.insert(
                "host".to_string(),
                Value::String(host_header.clone().unwrap_or_default()),
            );
            vmess_payload.insert(
                "path".to_string(),
                Value::String(path.clone().unwrap_or_default()),
            );
            vmess_payload.insert(
                "tls".to_string(),
                Value::String(if security == "tls" { "tls" } else { "" }.to_string()),
            );
            if let Some(sni) = sni.clone() {
                vmess_payload.insert("sni".to_string(), Value::String(sni));
            }

            format!(
                "vmess://{}",
                STANDARD.encode(
                    serde_json::to_string(&vmess_payload)
                        .context("failed to serialize vmess JSON")?
                )
            )
        }
        Protocol::Vless => {
            let id = get_string(object, "uuid")
                .or_else(|| get_string(object, "id"))
                .ok_or_else(|| anyhow!("vless outbound missing uuid"))?;
            params.insert("id".to_string(), id.clone());

            let mut query = HashMap::new();
            if let Some(network) = &network {
                query.insert("type".to_string(), network.clone());
            }
            if let Some(path) = &path {
                query.insert("path".to_string(), path.clone());
            }
            if let Some(host_header) = &host_header {
                query.insert("host".to_string(), host_header.clone());
            }
            if let Some(service_name) = &service_name {
                query.insert("serviceName".to_string(), service_name.clone());
            }
            if let Some(mode) = &mode {
                query.insert("mode".to_string(), mode.clone());
            }
            if let Some(header_type) = &header_type {
                query.insert("headerType".to_string(), header_type.clone());
            }
            if let Some(flow) = get_string(object, "flow") {
                query.insert("flow".to_string(), flow);
            }
            query.insert("security".to_string(), security.clone());
            if let Some(sni) = &sni {
                query.insert("sni".to_string(), sni.clone());
            }
            if let Some(alpn) = &alpn {
                query.insert("alpn".to_string(), alpn.clone());
            }
            build_standard_uri("vless", &id, None, &host, port, &query, &tag)
        }
        Protocol::Trojan => {
            let password = get_string(object, "password")
                .ok_or_else(|| anyhow!("trojan outbound missing password"))?;
            params.insert("password".to_string(), password.clone());

            let mut query = HashMap::new();
            if let Some(network) = &network {
                query.insert("type".to_string(), network.clone());
            }
            if let Some(path) = &path {
                query.insert("path".to_string(), path.clone());
            }
            if let Some(host_header) = &host_header {
                query.insert("host".to_string(), host_header.clone());
            }
            if let Some(sni) = &sni {
                query.insert("sni".to_string(), sni.clone());
            }
            if let Some(alpn) = &alpn {
                query.insert("alpn".to_string(), alpn.clone());
            }
            build_standard_uri("trojan", &password, None, &host, port, &query, &tag)
        }
        Protocol::Shadowsocks => {
            let method = get_string(object, "method")
                .ok_or_else(|| anyhow!("shadowsocks outbound missing method"))?;
            let password = get_string(object, "password")
                .ok_or_else(|| anyhow!("shadowsocks outbound missing password"))?;
            params.insert("method".to_string(), method.clone());
            params.insert("password".to_string(), password.clone());
            build_ss_uri(&host, port, &method, &password, &params, &tag)
        }
        Protocol::Hysteria2 => {
            let password = get_string(object, "password")
                .ok_or_else(|| anyhow!("hysteria2 outbound missing password"))?;
            params.insert("password".to_string(), password.clone());

            let mut query = HashMap::new();
            if let Some(sni) = &sni {
                query.insert("sni".to_string(), sni.clone());
            }
            if let Some(alpn) = &alpn {
                query.insert("alpn".to_string(), alpn.clone());
            }
            if let Some(obfs) = get_nested_string(object, &["obfs", "type"]) {
                query.insert("obfs".to_string(), obfs.clone());
                params.insert("obfs".to_string(), obfs);
            }
            if let Some(obfs_password) = get_nested_string(object, &["obfs", "password"]) {
                query.insert("obfs-password".to_string(), obfs_password.clone());
                params.insert("obfs-password".to_string(), obfs_password);
            }
            build_standard_uri("hysteria2", &password, None, &host, port, &query, &tag)
        }
        Protocol::Tuic => {
            let uuid =
                get_string(object, "uuid").ok_or_else(|| anyhow!("tuic outbound missing uuid"))?;
            let password = get_string(object, "password")
                .ok_or_else(|| anyhow!("tuic outbound missing password"))?;
            params.insert("uuid".to_string(), uuid.clone());
            params.insert("password".to_string(), password.clone());

            let mut query = HashMap::new();
            if let Some(congestion_control) = get_string(object, "congestion_control") {
                query.insert("congestion_control".to_string(), congestion_control);
            }
            if let Some(udp_relay_mode) = get_string(object, "udp_relay_mode") {
                query.insert("udp_relay_mode".to_string(), udp_relay_mode);
            }
            if let Some(sni) = &sni {
                query.insert("sni".to_string(), sni.clone());
            }
            if let Some(alpn) = &alpn {
                query.insert("alpn".to_string(), alpn.clone());
            }
            build_standard_uri("tuic", &uuid, Some(&password), &host, port, &query, &tag)
        }
        Protocol::ShadowsocksR => return Ok(None),
    };

    Ok(Some(Node {
        protocol,
        host,
        port,
        params,
        raw_name: tag.clone(),
        display_name: tag,
        original_uri,
        ping_ok: false,
        ping_ms: None,
        tunnel_ok: false,
        tunnel_ms: None,
    }))
}

fn get_string(object: &Map<String, Value>, key: &str) -> Option<String> {
    object
        .get(key)
        .and_then(stringify_value)
        .filter(|value| !value.is_empty())
}

fn get_u16(object: &Map<String, Value>, key: &str) -> Option<u16> {
    object.get(key).and_then(|value| parse_port(Some(value)))
}

fn get_bool(object: &Map<String, Value>, key: &str) -> Option<bool> {
    object.get(key).and_then(Value::as_bool)
}

fn get_nested_string(object: &Map<String, Value>, path: &[&str]) -> Option<String> {
    let mut current = Value::Object(object.clone());
    for key in path {
        current = current.get(*key)?.clone();
    }
    stringify_value(&current)
}

fn flatten_value(prefix: &str, value: Option<&Value>, params: &mut HashMap<String, String>) {
    let Some(value) = value else {
        return;
    };

    match value {
        Value::Object(object) => {
            for (key, nested) in object {
                let next_prefix = format!("{prefix}.{key}");
                flatten_value(&next_prefix, Some(nested), params);
            }
        }
        Value::Array(items) => {
            let joined = items
                .iter()
                .filter_map(stringify_value)
                .collect::<Vec<_>>()
                .join(",");
            if !joined.is_empty() {
                params.insert(prefix.to_string(), joined);
            }
        }
        _ => {
            if let Some(value) = stringify_value(value) {
                params.insert(prefix.to_string(), value);
            }
        }
    }
}

fn stringify_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Array(items) => Some(
            items
                .iter()
                .filter_map(stringify_value)
                .collect::<Vec<_>>()
                .join(","),
        ),
        _ => None,
    }
}

fn collect_scalar_fields(object: Option<&Map<String, Value>>) -> HashMap<String, String> {
    let mut params = HashMap::new();
    let Some(object) = object else {
        return params;
    };

    for (key, value) in object {
        if let Some(scalar) = stringify_value(value) {
            params.insert(key.clone(), scalar);
        }
    }

    params
}

fn extract_transport_path(transport: &Map<String, Value>) -> Option<String> {
    get_string(transport, "path")
        .or_else(|| get_string(transport, "service_name"))
        .or_else(|| get_nested_string_from_object(transport, &["headers", "path"]))
}

fn extract_transport_host(transport: &Map<String, Value>) -> Option<String> {
    if let Some(host) = get_string(transport, "host") {
        return Some(host);
    }
    if let Some(hosts) = transport.get("host").and_then(Value::as_array) {
        let values = hosts.iter().filter_map(stringify_value).collect::<Vec<_>>();
        if let Some(first) = values.into_iter().next() {
            return Some(first);
        }
    }
    if let Some(headers) = transport.get("headers").and_then(Value::as_object) {
        if let Some(host) = headers.get("Host").and_then(stringify_value) {
            return Some(host);
        }
        if let Some(host) = headers.get("host").and_then(stringify_value) {
            return Some(host);
        }
    }
    None
}

fn extract_header_type(transport: &Map<String, Value>) -> Option<String> {
    get_string(transport, "header_type")
        .or_else(|| get_nested_string_from_object(transport, &["header", "type"]))
}

fn get_nested_string_from_object(object: &Map<String, Value>, path: &[&str]) -> Option<String> {
    let mut current = Value::Object(object.clone());
    for key in path {
        current = current.get(*key)?.clone();
    }
    stringify_value(&current)
}

fn build_standard_uri(
    scheme: &str,
    username: &str,
    password: Option<&str>,
    host: &str,
    port: u16,
    params: &HashMap<String, String>,
    tag: &str,
) -> String {
    let mut uri = String::new();
    uri.push_str(scheme);
    uri.push_str("://");
    uri.push_str(&encode_userinfo(username));
    if let Some(password) = password {
        uri.push(':');
        uri.push_str(&encode_userinfo(password));
    }
    uri.push('@');
    uri.push_str(host);
    uri.push(':');
    uri.push_str(&port.to_string());

    if !params.is_empty() {
        let mut pairs: Vec<_> = params.iter().collect();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        let mut serializer = form_urlencoded::Serializer::new(String::new());
        for (key, value) in pairs {
            serializer.append_pair(key, value);
        }
        let query = serializer.finish();
        if !query.is_empty() {
            uri.push('?');
            uri.push_str(&query);
        }
    }

    uri.push('#');
    uri.push_str(&encode_fragment_component(tag));
    uri
}

fn build_ss_uri(
    host: &str,
    port: u16,
    method: &str,
    password: &str,
    params: &HashMap<String, String>,
    tag: &str,
) -> String {
    let credentials = STANDARD.encode(format!("{method}:{password}"));
    let mut uri = format!("ss://{credentials}@{host}:{port}");

    let mut query_params = HashMap::new();
    if let Some(plugin) = params.get("plugin") {
        query_params.insert("plugin".to_string(), plugin.clone());
    }
    if let Some(plugin_opts) = params.get("plugin_opts") {
        query_params.insert("plugin_opts".to_string(), plugin_opts.clone());
    }
    if let Some(udp_over_tcp) = params.get("udp_over_tcp") {
        query_params.insert("udp_over_tcp".to_string(), udp_over_tcp.clone());
    }

    if !query_params.is_empty() {
        let mut pairs: Vec<_> = query_params.iter().collect();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        let mut serializer = form_urlencoded::Serializer::new(String::new());
        for (key, value) in pairs {
            serializer.append_pair(key, value);
        }
        let query = serializer.finish();
        if !query.is_empty() {
            uri.push('?');
            uri.push_str(&query);
        }
    }

    uri.push('#');
    uri.push_str(&encode_fragment_component(tag));
    uri
}

fn encode_userinfo(value: &str) -> String {
    utf8_percent_encode(value, USERINFO_ENCODE_SET).to_string()
}

fn encode_fragment_component(value: &str) -> String {
    utf8_percent_encode(value, FRAGMENT_ENCODE_SET).to_string()
}

fn split_fragment(content: &str) -> (&str, String) {
    if let Some((head, fragment)) = content.split_once('#') {
        (head, decode_fragment(fragment))
    } else {
        (content, "Unnamed".to_string())
    }
}

fn split_query(content: &str) -> (&str, &str) {
    if let Some((head, query)) = content.split_once('?') {
        (head, query)
    } else {
        (content, "")
    }
}

fn parse_query_params(query: &str) -> HashMap<String, String> {
    form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect()
}

fn parse_host_port(server: &str) -> Result<(String, u16)> {
    let Some((host, port_text)) = server.rsplit_once(':') else {
        return Err(anyhow!("missing host or port"));
    };
    let port = port_text
        .parse::<u16>()
        .with_context(|| format!("invalid port {port_text}"))?;
    Ok((host.to_string(), port))
}

fn decode_fragment(fragment: &str) -> String {
    percent_decode_str(fragment).decode_utf8_lossy().to_string()
}

fn decode_base64_string(value: &str) -> String {
    decode_base64_flexible(value)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .unwrap_or_else(|| percent_decode_str(value).decode_utf8_lossy().to_string())
}

fn decode_possible_base64_list(value: &str) -> Result<Option<String>> {
    let normalized: String = value.chars().filter(|char| !char.is_whitespace()).collect();
    let decoded = match decode_base64_flexible(&normalized) {
        Ok(decoded) => decoded,
        Err(_) => return Ok(None),
    };
    let decoded_text =
        String::from_utf8(decoded).context("decoded base64 payload is not valid UTF-8")?;
    if decoded_text
        .lines()
        .any(|line| has_supported_prefix(line.trim()))
    {
        return Ok(Some(decoded_text));
    }

    Ok(None)
}

fn decode_base64_flexible(value: &str) -> Result<Vec<u8>> {
    STANDARD
        .decode(value)
        .or_else(|_| STANDARD_NO_PAD.decode(value))
        .or_else(|_| URL_SAFE.decode(value))
        .or_else(|_| URL_SAFE_NO_PAD.decode(value))
        .with_context(|| format!("invalid base64 payload: {value}"))
}

fn has_supported_prefix(line: &str) -> bool {
    line.starts_with("vmess://")
        || line.starts_with("vless://")
        || line.starts_with("trojan://")
        || line.starts_with("ss://")
        || line.starts_with("ssr://")
        || line.starts_with("hysteria2://")
        || line.starts_with("tuic://")
}

fn parse_port(value: Option<&Value>) -> Option<u16> {
    let value = value?;
    match value {
        Value::Number(number) => number.as_u64().and_then(|value| u16::try_from(value).ok()),
        Value::String(text) => text.parse::<u16>().ok(),
        _ => None,
    }
}

fn scalar_to_string(value: &Value) -> String {
    stringify_value(value).unwrap_or_default()
}
