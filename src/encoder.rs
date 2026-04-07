use base64::{engine::general_purpose::STANDARD, Engine};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};

use crate::parser::Node;

const FRAGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'`');

pub fn encode_subscription(nodes: &[Node]) -> String {
    let payload = nodes
        .iter()
        .map(|node| replace_fragment(&node.original_uri, &node.display_name))
        .collect::<Vec<_>>()
        .join("\n");

    STANDARD.encode(payload)
}

fn replace_fragment(uri: &str, display_name: &str) -> String {
    let encoded_name = utf8_percent_encode(display_name, FRAGMENT_ENCODE_SET).to_string();
    let base = uri.split('#').next().unwrap_or(uri);
    format!("{base}#{encoded_name}")
}
