use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use tokio::{
    net::TcpStream,
    sync::Semaphore,
    task::JoinSet,
    time::{timeout, Duration},
};
use tracing::warn;

use crate::parser::Node;

pub type ProgressCallback = Arc<dyn Fn(usize, usize) + Send + Sync>;

pub async fn ping_nodes(
    nodes: Vec<Node>,
    ping_timeout: Duration,
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
            let started_at = Instant::now();
            let target = format!("{}:{}", node.host, node.port);
            let result = timeout(ping_timeout, TcpStream::connect(&target)).await;

            let mut node = node;
            match result {
                Ok(Ok(_stream)) => {
                    node.ping_ok = true;
                    node.ping_ms = Some(started_at.elapsed().as_millis() as u64);
                }
                Ok(Err(error)) => {
                    warn!(target = %target, error = %error, "node TCP probe failed");
                }
                Err(_) => {
                    warn!(target = %target, timeout_ms = ping_timeout.as_millis(), "node TCP probe timed out");
                }
            }

            drop(permit);
            let done = done_counter.fetch_add(1, Ordering::SeqCst) + 1;
            if let Some(callback) = progress_callback {
                callback(done, total);
            }

            node
        });
    }

    let mut online = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(node) if node.ping_ok => online.push(node),
            Ok(_) => {}
            Err(error) => warn!(error = %error, "ping worker task failed"),
        }
    }

    online
}
