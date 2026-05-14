use std::{
    ffi::CString,
    io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

use tokio::{
    net::{lookup_host, TcpSocket},
    sync::Semaphore,
    task::JoinSet,
    time::{timeout, Duration},
};
use tracing::warn;

use crate::parser::Node;

pub type ProgressCallback = Arc<dyn Fn(usize, usize) + Send + Sync>;

pub fn validate_interface_binding(modem_interface: &str) -> io::Result<()> {
    let socket = TcpSocket::new_v4()?;
    bind_tcp_socket_to_device(&socket, modem_interface)
}

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
            let host = node.host.clone();
            let port = node.port;
            let result = ping_single_node(&host, port, ping_timeout).await;

            let mut node = node;
            match result {
                Ok(()) => {
                    node.ping_ok = true;
                    node.ping_ms = Some(started_at.elapsed().as_millis() as u64);
                }
                Err(error) => {
                    warn!(
                        host = %host,
                        port,
                        error = %error,
                        "node TCP probe failed"
                    );
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

async fn ping_single_node(host: &str, port: u16, ping_timeout: Duration) -> io::Result<()> {
    let target = format!("{host}:{port}");
    let addresses = match timeout(ping_timeout, lookup_host((host, port))).await {
        Ok(Ok(addresses)) => addresses.collect::<Vec<_>>(),
        Ok(Err(error)) => return Err(error),
        Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("DNS lookup timed out for {target}"),
            ));
        }
    };

    if addresses.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("DNS lookup returned no addresses for {target}"),
        ));
    }

    let mut last_error = None;
    for address in addresses {
        match connect_addr(address, ping_timeout).await {
            Ok(()) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("all resolved addresses failed for {target}"),
        )
    }))
}

async fn connect_addr(address: SocketAddr, ping_timeout: Duration) -> io::Result<()> {
    let socket = if address.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };

    match timeout(ping_timeout, socket.connect(address)).await {
        Ok(Ok(stream)) => {
            drop(stream);
            Ok(())
        }
        Ok(Err(error)) => Err(error),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("connect timed out for {address}"),
        )),
    }
}

#[cfg(target_os = "linux")]
fn bind_tcp_socket_to_device(socket: &TcpSocket, modem_interface: &str) -> io::Result<()> {
    let interface = CString::new(modem_interface).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "modem_interface contains an interior NUL byte",
        )
    })?;
    let bytes = interface.as_bytes_with_nul();
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_BINDTODEVICE,
            bytes.as_ptr().cast(),
            bytes.len() as libc::socklen_t,
        )
    };

    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn bind_tcp_socket_to_device(_socket: &TcpSocket, _modem_interface: &str) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "SO_BINDTODEVICE is only supported on Linux",
    ))
}
