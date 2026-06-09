use dashmap::DashMap;
use std::{
    env,
    io::BufReader as StdBufReader,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time,
};
use tracing::{info, warn};

const LANDING_HTML: &str = include_str!("../../web/index.html");

const POOL_SIZE: usize = 5;
const SERVER_PING_SECS: u64 = 25;
const DATA_CONNECT_TIMEOUT_SECS: u64 = 15;
const PEEK_TIMEOUT_SECS: u64 = 5;

const RESERVED_SUBDOMAINS: &[&str] = &[
    "api", "www", "admin", "root", "mail", "server", "dashboard", "status", "health",
    "macha", "app", "staging", "prod", "beta", "static", "assets", "cdn", "auth",
    "login", "signup", "register", "support", "help", "docs", "blog", "dev", "test",
];

// ── Rate limiting ─────────────────────────────────────────────────────────────

struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
    capacity: f64,
    refill_per_sec: f64,
}

impl TokenBucket {
    fn new(capacity: f64, refill_per_sec: f64) -> Self {
        Self { tokens: capacity, last_refill: Instant::now(), capacity, refill_per_sec }
    }

    fn try_consume(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

// Per-subdomain request rate limiter: 100 req/s sustained, burst 200.
type RateLimits = Arc<DashMap<String, Mutex<TokenBucket>>>;

// Per-IP registration tracker: max 5 REGISTERs per minute.
struct RegTracker {
    count: u32,
    window_start: Instant,
}

impl RegTracker {
    fn new() -> Self {
        Self { count: 0, window_start: Instant::now() }
    }

    fn allow(&mut self) -> bool {
        let now = Instant::now();
        if now.duration_since(self.window_start) > Duration::from_secs(60) {
            self.count = 0;
            self.window_start = now;
        }
        if self.count < 5 {
            self.count += 1;
            true
        } else {
            false
        }
    }
}

type RegLimits = Arc<DashMap<String, Mutex<RegTracker>>>;

// ── Stream supertrait ─────────────────────────────────────────────────────────

trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncReadWrite for T {}
type AnyStream = Box<dyn AsyncReadWrite>;

// Maps subdomain → replenish-signal channel
type Agents = Arc<DashMap<String, mpsc::Sender<()>>>;

// Idle pool: data handler pushes; public handler pops.
type IdleTx = Arc<DashMap<String, mpsc::Sender<AnyStream>>>;
type IdleRx = Arc<DashMap<String, Arc<tokio::sync::Mutex<mpsc::Receiver<AnyStream>>>>>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("server=info".parse().unwrap()),
        )
        .init();

    let public_port  = env_port("PUBLIC_PORT",  8080);
    let control_port = env_port("CONTROL_PORT", 9000);
    let data_port    = env_port("DATA_PORT",    9001);
    let domain: Arc<String> = Arc::new(
        env::var("DOMAIN").unwrap_or_else(|_| "macha.live".into()).trim().to_string(),
    );
    let scheme: Arc<String> = Arc::new(
        env::var("PUBLIC_SCHEME").unwrap_or_else(|_| "https".into()).trim().to_string(),
    );
    let auth_token: Option<Arc<String>> =
        env::var("AUTH_TOKEN").ok().map(|t| Arc::new(t.trim().to_string()));

    let tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>> =
        match (env::var("TLS_CERT"), env::var("TLS_KEY")) {
            (Ok(cert), Ok(key)) => match make_tls_acceptor(&cert, &key) {
                Ok(a) => {
                    info!("TLS enabled on control and data ports");
                    Some(Arc::new(a))
                }
                Err(e) => {
                    eprintln!("TLS config error: {e}");
                    std::process::exit(1);
                }
            },
            _ => {
                warn!("TLS_CERT / TLS_KEY not set — control and data ports are plain TCP");
                None
            }
        };

    let public_listener  = TcpListener::bind(format!("0.0.0.0:{public_port}")).await.expect("bind PUBLIC_PORT");
    let control_listener = TcpListener::bind(format!("0.0.0.0:{control_port}")).await.expect("bind CONTROL_PORT");
    let data_listener    = TcpListener::bind(format!("0.0.0.0:{data_port}")).await.expect("bind DATA_PORT");

    info!("macha server ready");
    info!("  domain  → {scheme}://{domain}");
    info!("  public  → 0.0.0.0:{public_port}");
    info!("  control → 0.0.0.0:{control_port}");
    info!("  data    → 0.0.0.0:{data_port}");

    let agents:    Agents    = Arc::new(DashMap::new());
    let idle_tx:   IdleTx   = Arc::new(DashMap::new());
    let idle_rx:   IdleRx   = Arc::new(DashMap::new());
    let rate_limits: RateLimits = Arc::new(DashMap::new());
    let reg_limits:  RegLimits  = Arc::new(DashMap::new());

    loop {
        tokio::select! {
            // listening for any clients reaching to 8080
            Ok((stream, peer)) = public_listener.accept() => {
                tokio::spawn(handle_public(stream, peer, agents.clone(), idle_rx.clone(), rate_limits.clone(), domain.clone()));
            }
            // listening for any agents tries to connect to the control listener
            Ok((tcp, peer)) = control_listener.accept() => {
                let acceptor = tls_acceptor.clone();
                let ctx = ControlCtx {
                    agents: agents.clone(),
                    idle_tx: idle_tx.clone(),
                    idle_rx: idle_rx.clone(),
                    auth_token: auth_token.clone(),
                    reg_limits: reg_limits.clone(),
                    domain: domain.clone(),
                    scheme: scheme.clone(),
                };
                tokio::spawn(async move {
                    match wrap_tls(tcp, acceptor.as_deref()).await {
                        Ok(stream) => handle_control(stream, peer, ctx).await,
                        Err(e) => warn!("TLS handshake failed: {e}"),
                    }
                });
            }
            // listening on 9001
            Ok((tcp, _)) = data_listener.accept() => {
                let acceptor = tls_acceptor.clone();
                let idle_tx  = idle_tx.clone();
                tokio::spawn(async move {
                    match wrap_tls(tcp, acceptor.as_deref()).await {
                        Ok(stream) => handle_data(stream, idle_tx).await,
                        Err(e) => warn!("TLS handshake failed on data port: {e}"),
                    }
                });
            }
        }
    }
}

// ── Public handler ────────────────────────────────────────────────────────────

async fn handle_public(
    mut stream: TcpStream,
    _peer: SocketAddr,
    agents: Agents,
    idle_rx: IdleRx,
    rate_limits: RateLimits,
    domain: Arc<String>,
) {
    let mut buf = [0u8; 4096];
    let n = match time::timeout(
        Duration::from_secs(PEEK_TIMEOUT_SECS),
        stream.peek(&mut buf),
    )
    .await
    {
        Ok(Ok(n)) if n > 0 => n,
        _ => return,
    };

    let subdomain = match parse_subdomain(&String::from_utf8_lossy(&buf[..n]), &domain) {
        Some(s) => s,
        None => {
            return serve_html(&mut stream, 200, "text/html; charset=utf-8", LANDING_HTML).await;
        }
    };

    // Rate limit check
    {
        let bucket = rate_limits
            .entry(subdomain.clone())
            .or_insert_with(|| Mutex::new(TokenBucket::new(200.0, 100.0)));
        if !bucket.lock().unwrap().try_consume() {
            return serve_text(&mut stream, 429, "Too Many Requests\n").await;
        }
    }

    let agent_tx = match agents.get(&subdomain).map(|r| r.clone()) {
        Some(tx) => tx,
        None => {
            let body = format!(
                "<!doctype html><html><body style='font:16px monospace;padding:2rem'>\
                <h2>No tunnel for <code>{subdomain}.{domain}</code></h2>\
                <p>Run: <code>macha --port 3000 --subdomain {subdomain}</code></p>\
                <p><a href='/'>← {domain}</a></p></body></html>"
            );
            return serve_html(&mut stream, 502, "text/html", &body).await;
        }
    };

    let agent_stream = {
        let rx_arc = match idle_rx.get(&subdomain).map(|r| r.clone()) {
            Some(r) => r,
            None => return serve_text(&mut stream, 502, "Agent pool unavailable.\n").await,
        };
        let mut rx = rx_arc.lock().await;
        match rx.try_recv() {
            Ok(s) => s,
            Err(_) => {
                match time::timeout(Duration::from_secs(DATA_CONNECT_TIMEOUT_SECS), rx.recv()).await {
                    Ok(Some(s)) => s,
                    _ => return serve_text(&mut stream, 504, "Gateway timeout — no idle channel.\n").await,
                }
            }
        }
    };

    let _ = agent_tx.send(()).await;

    let (mut vr, mut vw) = stream.into_split();
    let (mut ar, mut aw) = tokio::io::split(agent_stream);
    let _ = tokio::join!(
        tokio::io::copy(&mut vr, &mut aw),
        tokio::io::copy(&mut ar, &mut vw),
    );
}

// ── Control handler ───────────────────────────────────────────────────────────

struct ControlCtx {
    agents: Agents,
    idle_tx: IdleTx,
    idle_rx: IdleRx,
    auth_token: Option<Arc<String>>,
    reg_limits: RegLimits,
    domain: Arc<String>,
    scheme: Arc<String>,
}

async fn handle_control(mut stream: AnyStream, peer: SocketAddr, ctx: ControlCtx) {
    // Per-IP registration rate limit
    {
        let ip = peer.ip().to_string();
        let tracker = ctx.reg_limits
            .entry(ip)
            .or_insert_with(|| Mutex::new(RegTracker::new()));
        if !tracker.lock().unwrap().allow() {
            let _ = stream.write_all(b"ERR too many registration attempts\n").await;
            return;
        }
    }

    let line = match read_line(&mut *stream).await {
        Ok(l) => l,
        Err(_) => return,
    };

    let parts: Vec<&str> = line.splitn(3, ' ').collect();
    if parts[0] != "REGISTER" || parts.len() < 2 {
        let _ = stream.write_all(b"ERR expected REGISTER\n").await;
        return;
    }
    let subdomain = parts[1].trim().to_string();
    let provided_token = parts.get(2).map(|t| t.trim());

    // Validate subdomain characters + length
    if subdomain.is_empty()
        || subdomain.len() > 63
        || !subdomain.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        || subdomain.starts_with('-')
        || subdomain.ends_with('-')
    {
        let _ = stream.write_all(b"ERR invalid subdomain\n").await;
        return;
    }

    // Block reserved subdomains
    if RESERVED_SUBDOMAINS.contains(&subdomain.as_str()) {
        let _ = stream.write_all(b"ERR reserved subdomain\n").await;
        return;
    }

    // Validate token
    if let Some(required) = &ctx.auth_token {
        match provided_token {
            Some(t) if t == required.as_str() => {}
            _ => {
                let _ = stream.write_all(b"ERR invalid or missing token\n").await;
                return;
            }
        }
    }

    // Reject if already in use
    if ctx.agents.contains_key(&subdomain) {
        let _ = stream.write_all(b"ERR subdomain already in use\n").await;
        return;
    }

    let (replenish_tx, mut replenish_rx) = mpsc::channel::<()>(POOL_SIZE * 2);
    let (pool_tx, pool_rx) = mpsc::channel::<AnyStream>(POOL_SIZE * 2);

    ctx.agents.insert(subdomain.clone(), replenish_tx);
    ctx.idle_tx.insert(subdomain.clone(), pool_tx);
    ctx.idle_rx.insert(subdomain.clone(), Arc::new(tokio::sync::Mutex::new(pool_rx)));

    let url = format!("{}://{subdomain}.{}", ctx.scheme, ctx.domain);
    if stream.write_all(format!("OK {url}\n").as_bytes()).await.is_err() {
        cleanup(&subdomain, &ctx.agents, &ctx.idle_tx, &ctx.idle_rx);
        return;
    }

    info!(%subdomain, %peer, "agent registered");

    for _ in 0..POOL_SIZE {
        if stream.write_all(b"CONNECT\n").await.is_err() {
            cleanup(&subdomain, &ctx.agents, &ctx.idle_tx, &ctx.idle_rx);
            return;
        }
    }

    let (reader, writer_half) = tokio::io::split(stream);

    let write_task = tokio::spawn(async move {
        let mut writer = writer_half;
        loop {
            tokio::select! {
                Some(_) = replenish_rx.recv() => {
                    if writer.write_all(b"CONNECT\n").await.is_err() { break; }
                }
                _ = time::sleep(Duration::from_secs(SERVER_PING_SECS)) => {
                    if writer.write_all(b"PING\n").await.is_err() { break; }
                }
                else => break,
            }
        }
    });

    let mut drain = BufReader::new(reader).lines();
    while let Ok(Some(_)) = drain.next_line().await {}

    write_task.abort();
    cleanup(&subdomain, &ctx.agents, &ctx.idle_tx, &ctx.idle_rx);
    info!(%subdomain, "agent disconnected");
}

fn cleanup(subdomain: &str, agents: &Agents, idle_tx: &IdleTx, idle_rx: &IdleRx) {
    agents.remove(subdomain);
    idle_tx.remove(subdomain);
    idle_rx.remove(subdomain);
}

// ── Data handler ──────────────────────────────────────────────────────────────

async fn handle_data(mut stream: AnyStream, idle_tx: IdleTx) {
    let line = match read_line(&mut *stream).await {
        Ok(l) => l,
        Err(_) => return,
    };

    let subdomain = match line.strip_prefix("IDLE ") {
        Some(s) => s.trim().to_string(),
        None => {
            warn!("unexpected data handshake: {line:?}");
            return;
        }
    };

    if stream.write_all(b"READY\n").await.is_err() {
        return;
    }

    if let Some(tx) = idle_tx.get(&subdomain) {
        let _ = tx.send(stream).await;
    }
}

// ── TLS helpers ───────────────────────────────────────────────────────────────

async fn wrap_tls(
    tcp: TcpStream,
    acceptor: Option<&tokio_rustls::TlsAcceptor>,
) -> std::io::Result<AnyStream> {
    match acceptor {
        None => Ok(Box::new(tcp)),
        Some(acc) => Ok(Box::new(acc.accept(tcp).await?)),
    }
}

fn make_tls_acceptor(
    cert_path: &str,
    key_path: &str,
) -> Result<tokio_rustls::TlsAcceptor, Box<dyn std::error::Error>> {
    use rustls::ServerConfig;

    let cert_file = std::fs::File::open(cert_path)?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut StdBufReader::new(cert_file))
        .collect::<std::result::Result<_, _>>()?;

    let key_file = std::fs::File::open(key_path)?;
    let key = rustls_pemfile::private_key(&mut StdBufReader::new(key_file))?
        .ok_or("no private key found")?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn read_line<R: AsyncReadExt + Unpin + ?Sized>(r: &mut R) -> std::io::Result<String> {
    let mut buf = Vec::with_capacity(128);
    let mut byte = [0u8; 1];
    loop {
        r.read_exact(&mut byte).await?;
        if byte[0] == b'\n' {
            break;
        }
        if byte[0] != b'\r' {
            buf.push(byte[0]);
        }
        if buf.len() > 512 {
            return Err(std::io::Error::other("handshake line too long"));
        }
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

fn parse_subdomain(req: &str, domain: &str) -> Option<String> {
    let suffix = format!(".{domain}");
    for line in req.lines() {
        if !line.to_ascii_lowercase().starts_with("host:") {
            continue;
        }
        let host = line[5..].trim().split(':').next().unwrap_or("").trim();
        if host == domain || host == format!("www.{domain}") || host.is_empty() {
            return None;
        }
        if let Some(sub) = host.strip_suffix(suffix.as_str())
            && !sub.is_empty()
        {
            return Some(sub.to_string());
        }
        break;
    }
    None
}

async fn serve_html(stream: &mut TcpStream, status: u16, content_type: &str, body: &str) {
    let reason = match status {
        200 => "OK",
        429 => "Too Many Requests",
        502 => "Bad Gateway",
        504 => "Gateway Timeout",
        _   => "Error",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
        Content-Type: {content_type}\r\n\
        Content-Length: {}\r\n\
        Connection: close\r\n\
        X-Content-Type-Options: nosniff\r\n\
        X-Frame-Options: DENY\r\n\
        X-XSS-Protection: 1; mode=block\r\n\
        Referrer-Policy: strict-origin-when-cross-origin\r\n\
        \r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn serve_text(stream: &mut TcpStream, status: u16, body: &str) {
    serve_html(stream, status, "text/plain", body).await;
}

fn env_port(key: &str, default: u16) -> u16 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
