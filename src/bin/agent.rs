use clap::Parser;
use macha::RequestLog;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::SystemTime,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{broadcast, RwLock},
};
use tracing_subscriber::EnvFilter;

/// Expose a local port to the internet via macha.live
#[derive(Parser, Debug)]
#[command(name = "macha", version, about)]
struct Args {
    /// Local port to expose (e.g. 3000)
    #[arg(short, long, env = "PORT")]
    port: u16,

    /// Subdomain to register (e.g. "myapp" → myapp.macha.live); random if omitted
    #[arg(short, long, env = "SUBDOMAIN")]
    subdomain: Option<String>,

    /// Tunnel server hostname
    #[arg(long, default_value = "macha.live", env = "MACHA_SERVER")]
    server: String,

    /// Server control-plane port
    #[arg(long, default_value_t = 9000, env = "CONTROL_PORT")]
    control_port: u16,

    /// Server data-plane port
    #[arg(long, default_value_t = 9001, env = "DATA_PORT")]
    data_port: u16,

    /// Authentication token required by the server
    #[arg(long, env = "MACHA_TOKEN")]
    token: Option<String>,

    /// Enable TLS (uses Mozilla root certs — required when server has a public cert)
    #[arg(long)]
    tls: bool,

    /// Enable TLS with a custom CA certificate file (for self-signed server certs)
    #[arg(long, value_name = "PATH")]
    tls_ca: Option<std::path::PathBuf>,

    /// Enable TLS without certificate verification — development only
    #[arg(long)]
    tls_insecure: bool,

    /// Disable auto-reconnect
    #[arg(long)]
    no_reconnect: bool,

    /// Dashboard port (default: 4040, set 0 to disable)
    #[arg(long, default_value_t = 4040, env = "MACHA_DASHBOARD_PORT")]
    dashboard_port: u16,
}

// ── Shared dashboard state ────────────────────────────────────────────────────

struct DashState {
    tunnel_url: RwLock<String>,
    connected: AtomicBool,
    total_requests: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    start_ms: u64,
    local_port: u16,
    recent: Mutex<VecDeque<RequestLog>>,
    // SSE subscribers — each gets a clone of the broadcast sender
    sse_tx: broadcast::Sender<String>,
}

impl DashState {
    fn new(local_port: u16, sse_tx: broadcast::Sender<String>) -> Arc<Self> {
        let start_ms = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Arc::new(Self {
            tunnel_url: RwLock::new(String::new()),
            connected: AtomicBool::new(false),
            total_requests: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            start_ms,
            local_port,
            recent: Mutex::new(VecDeque::new()),
            sse_tx,
        })
    }

    async fn status_json(&self) -> String {
        let url = self.tunnel_url.read().await.clone();
        let connected = self.connected.load(Ordering::Relaxed);
        format!(
            r#"{{"url":"{url}","connected":{connected},"start_ms":{},"local_port":{}}}"#,
            self.start_ms, self.local_port
        )
    }

    fn push_log(&self, log: &RequestLog) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.bytes_in.fetch_add(log.bytes_in, Ordering::Relaxed);
        self.bytes_out.fetch_add(log.bytes_out, Ordering::Relaxed);
        {
            let mut q = self.recent.lock().unwrap();
            if q.len() >= 200 {
                q.pop_front();
            }
            q.push_back(log.clone());
        }
        let json = log_to_json(log);
        let _ = self.sse_tx.send(format!("event: log\ndata: {json}\n\n"));
    }

    async fn broadcast_status(&self) {
        let json = self.status_json().await;
        let _ = self
            .sse_tx
            .send(format!("event: status\ndata: {json}\n\n"));
    }
}

fn log_to_json(l: &RequestLog) -> String {
    let path = l.path.replace('"', "\\\"");
    let method = l.method.replace('"', "\\\"");
    format!(
        r#"{{"subdomain":"{sub}","method":"{method}","path":"{path}","bytes_in":{bin},"bytes_out":{bout},"duration_ms":{dur},"timestamp_ms":{ts}}}"#,
        sub = l.subdomain,
        bin = l.bytes_in,
        bout = l.bytes_out,
        dur = l.duration_ms,
        ts = l.timestamp_ms,
    )
}

// ── Dashboard HTTP server ─────────────────────────────────────────────────────

static DASHBOARD_HTML: &str = include_str!("../../web/dashboard.html");

async fn serve_dashboard(
    state: Arc<DashState>,
    mut stream: tokio::net::TcpStream,
) {
    let mut buf = [0u8; 4096];
    let Ok(n) = stream.read(&mut buf).await else {
        return;
    };
    if n == 0 {
        return;
    }
    let req = std::str::from_utf8(&buf[..n]).unwrap_or("");
    let first_line = req.lines().next().unwrap_or("");
    let path = first_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/");
    let path = path.split('?').next().unwrap_or("/");

    match path {
        "/" => {
            let body = DASHBOARD_HTML.as_bytes();
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(header.as_bytes()).await;
            let _ = stream.write_all(body).await;
        }
        "/api/status" => {
            let body = state.status_json().await;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(header.as_bytes()).await;
            let _ = stream.write_all(body.as_bytes()).await;
        }
        "/events" => {
            // SSE stream
            let header = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nX-Accel-Buffering: no\r\nConnection: keep-alive\r\n\r\n";
            if stream.write_all(header).await.is_err() {
                return;
            }

            // Subscribe FIRST — any log that fires between backfill and
            // live-stream would otherwise be silently dropped.
            let mut rx = state.sse_tx.subscribe();

            // Initial status event
            let status_json = state.status_json().await;
            let init = format!("event: status\ndata: {status_json}\n\n");
            if stream.write_all(init.as_bytes()).await.is_err() {
                return;
            }

            // Backfill: replay last N requests for this browser tab
            let backfill: Vec<RequestLog> = state.recent.lock().unwrap().iter().cloned().collect();
            for log in &backfill {
                let json = log_to_json(log);
                let msg = format!("event: log\ndata: {json}\n\n");
                if stream.write_all(msg.as_bytes()).await.is_err() {
                    return;
                }
            }
            loop {
                match rx.recv().await {
                    Ok(msg) => {
                        if stream.write_all(msg.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
        _ => {
            let _ = stream
                .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await;
        }
    }
}

async fn run_dashboard(state: Arc<DashState>, port: u16) {
    let listener = match TcpListener::bind(("127.0.0.1", port)).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("dashboard: could not bind 127.0.0.1:{port} — {e}");
            return;
        }
    };
    while let Ok((stream, _)) = listener.accept().await {
        let s = Arc::clone(&state);
        tokio::spawn(serve_dashboard(s, stream));
    }
}

fn random_subdomain() -> String {
    // 8 lowercase hex chars from a UUID — always a valid DNS label
    uuid::Uuid::new_v4().simple().to_string()[..8].to_string()
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let subdomain = args.subdomain.unwrap_or_else(|| {
        let s = random_subdomain();
        eprintln!("  No subdomain specified — using random: {s}");
        s
    });

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    // Broadcast channel: tunnel → log aggregator → SSE clients
    let (log_tx, mut log_rx) = broadcast::channel::<RequestLog>(512);
    // SSE broadcast: log aggregator → each SSE connection
    let (sse_tx, _) = broadcast::channel::<String>(256);

    let state = DashState::new(args.port, sse_tx);

    // Background task: drain RequestLog events, update stats, push to SSE
    let state_for_agg = Arc::clone(&state);
    tokio::spawn(async move {
        loop {
            match log_rx.recv().await {
                Ok(log) => state_for_agg.push_log(&log),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Start dashboard
    let dashboard_enabled = args.dashboard_port != 0;
    if dashboard_enabled {
        let state_for_dash = Arc::clone(&state);
        let dp = args.dashboard_port;
        tokio::spawn(run_dashboard(state_for_dash, dp));
        eprintln!(
            "  Dashboard → http://127.0.0.1:{}\n",
            args.dashboard_port
        );
    }

    // Build tunnel
    let mut builder = macha::Tunnel::builder(&subdomain, args.port)
        .server(&args.server)
        .control_port(args.control_port)
        .data_port(args.data_port)
        .reconnect(!args.no_reconnect)
        .log_channel(log_tx);

    if let Some(token) = args.token {
        builder = builder.token(token);
    }

    builder = if args.tls_insecure {
        builder.tls_insecure()
    } else if let Some(ca) = args.tls_ca {
        builder.tls_custom_ca(ca)
    } else if args.tls {
        builder.tls()
    } else {
        builder
    };

    let tunnel = builder.build().expect("invalid configuration");

    // Once tunnel registers, broadcast the URL + connected status
    // We do this via a small wrapper: run in a task and watch stdout.
    // The simpler approach: after the tunnel prints "✓  Tunnel: <url>", we set state.
    // We hook this by running the tunnel in a task and intercepting the print.
    // Actually the cleanest: set connected=true now, URL is updated when we have it.
    // The lib prints the URL; we mirror that to state by capturing the first OK line.
    // Since lib.rs prints "✓  Tunnel: <url>" to stdout, we can't intercept.
    // Instead: expose an on_connect hook — or just patch lib to return URL.
    // For now: mark connected after run() starts, URL set to expected value.
    let expected_url = format!("https://{subdomain}.{}", args.server);
    *state.tunnel_url.write().await = expected_url;
    state.connected.store(true, Ordering::Relaxed);
    state.broadcast_status().await;

    if let Err(e) = tunnel.run().await {
        state.connected.store(false, Ordering::Relaxed);
        state.broadcast_status().await;
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
