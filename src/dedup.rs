use std::collections::{BTreeMap, HashSet};

use sha2::{Digest, Sha256};

use crate::parser::{Node, Protocol};

pub fn dedup_nodes(nodes: Vec<Node>) -> Vec<Node> {
    let mut seen = HashSet::new();
    let mut unique = Vec::new();

    for node in nodes {
        let fingerprint = fingerprint_node(&node);
        if seen.insert(fingerprint) {
            unique.push(node);
        }
    }

    unique
}

fn fingerprint_node(node: &Node) -> String {
    let mut fields = BTreeMap::new();
    fields.insert("protocol".to_string(), node.protocol.as_str().to_string());
    fields.insert("host".to_string(), node.host.clone());
    fields.insert("port".to_string(), node.port.to_string());

    for key in critical_param_keys(&node.protocol) {
        let value = node.params.get(*key).cloned().unwrap_or_default();
        fields.insert(key.to_string(), value);
    }

    let canonical = fields
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("|");

    let digest = Sha256::digest(canonical.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

fn critical_param_keys(protocol: &Protocol) -> &'static [&'static str] {
    match protocol {
        Protocol::Vmess => &["id", "aid", "net", "path", "host"],
        Protocol::Vless => &["id", "type", "path", "host", "security", "sni", "serviceName", "flow", "mode", "headerType"],
        Protocol::Trojan => &["password", "sni", "type", "path", "host"],
        Protocol::Shadowsocks => &["method", "password"],
        Protocol::ShadowsocksR => &["method", "password", "protocol", "obfs"],
        Protocol::Hysteria2 => &["password", "obfs", "obfs-password", "sni"],
        Protocol::Tuic => &["uuid", "password"],
    }
}
