//! # Macha — expose localhost to the internet
//!
//! ```no_run
//! #[tokio::main]
//! async fn main() -> macha::Result<()> {
//!     macha::start("myapp", 3000).await
//! }
//! ```

mod error;
mod proto;

pub use error::{Error, Result};

use std::time::Duration;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::broadcast,
    time,
};
use tracing::{debug, info, warn};

trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncReadWrite for T {}
type AnyStream = Box<dyn AsyncReadWrite>;

// ── Request log ───────────────────────────────────────────────────────────────

/// One completed tunnel request. Sent through the log channel after each bridge
/// closes so the agent dashboard can display live traffic.
#[derive(Clone, Debug)]
pub struct RequestLog {
    pub subdomain: String,
    /// HTTP method parsed from the first line of the request head.
    pub method: String,
    /// Request path parsed from the first line of the request head.
    pub path: String,
    /// Bytes received from the visitor (request body + headers).
    pub bytes_in: u64,
    /// Bytes sent back to the visitor (response).
    pub bytes_out: u64,
    pub duration_ms: u64,
    /// Unix epoch milliseconds — use to display wall-clock time in the dashboard.
    pub timestamp_ms: u128,
}

// ── TLS mode ──────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default)]
pub enum TlsMode {
    #[default]
    Off,
    /// TLS verified with Mozilla root certificates (works for `macha.live`).
    On,
    /// TLS verified with a custom CA certificate file.
    CustomCa(std::path::PathBuf),
    /// TLS without certificate verification — dev only.
    Insecure,
}

// ── Builder ───────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct TunnelBuilder {
    server_host: String,
    subdomain: String,
    local_port: u16,
    control_port: u16,
    data_port: u16,
    reconnect: bool,
    token: Option<String>,
    tls: TlsMode,
    log_tx: Option<broadcast::Sender<RequestLog>>,
}

impl TunnelBuilder {
    fn new(subdomain: impl Into<String>, local_port: u16) -> Self {
        Self {
            server_host: "macha.live".into(),
            subdomain: subdomain.into(),
            local_port,
            control_port: proto::CONTROL_PORT,
            data_port: proto::DATA_PORT,
            reconnect: true,
            token: None,
            tls: TlsMode::Off,
            log_tx: None,
        }
    }

    pub fn server(mut self, host: impl Into<String>) -> Self {
        self.server_host = host.into();
        self
    }

    pub fn control_port(mut self, port: u16) -> Self {
        self.control_port = port;
        self
    }

    pub fn data_port(mut self, port: u16) -> Self {
        self.data_port = port;
        self
    }

    pub fn reconnect(mut self, enabled: bool) -> Self {
        self.reconnect = enabled;
        self
    }

    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    pub fn tls(mut self) -> Self {
        self.tls = TlsMode::On;
        self
    }

    pub fn tls_custom_ca(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.tls = TlsMode::CustomCa(path.into());
        self
    }

    pub fn tls_insecure(mut self) -> Self {
        self.tls = TlsMode::Insecure;
        self
    }

    /// Attach a broadcast channel for live request logging.
    /// The agent dashboard subscribes to this to power the request table.
    pub fn log_channel(mut self, tx: broadcast::Sender<RequestLog>) -> Self {
        self.log_tx = Some(tx);
        self
    }

    pub fn build(self) -> Result<Tunnel> {
        validate_subdomain(&self.subdomain)?;
        Ok(Tunnel(self))
    }
}

fn validate_subdomain(s: &str) -> Result<()> {
    let ok = !s.is_empty()
        && s.len() <= 63
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        && !s.starts_with('-')
        && !s.ends_with('-');
    if ok { Ok(()) } else { Err(Error::InvalidSubdomain(s.into())) }
}

// ── Tunnel ────────────────────────────────────────────────────────────────────

pub struct Tunnel(TunnelBuilder);

impl Tunnel {
    pub fn builder(subdomain: impl Into<String>, local_port: u16) -> TunnelBuilder {
        TunnelBuilder::new(subdomain, local_port)
    }

    pub async fn run(&self) -> Result<()> {
        let cfg = &self.0;
        let mut backoff = Duration::from_secs(1);

        loop {
            info!(subdomain = %cfg.subdomain, server = %cfg.server_host, "connecting");
            match run_session(cfg).await {
                Ok(()) => {
                    if !cfg.reconnect {
                        return Ok(());
                    }
                    warn!("session ended, reconnecting in {backoff:?}");
                }
                Err(e @ Error::Rejected(..)) | Err(e @ Error::InvalidSubdomain(..)) => {
                    return Err(e);
                }
                Err(e) => {
                    if !cfg.reconnect {
                        return Err(e);
                    }
                    warn!(error = %e, "session error, reconnecting in {backoff:?}");
                }
            }
            time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(proto::MAX_BACKOFF_SECS));
        }
    }
}

pub async fn start(subdomain: &str, local_port: u16) -> Result<()> {
    Tunnel::builder(subdomain, local_port).build()?.run().await
}

// ── Session ───────────────────────────────────────────────────────────────────

async fn run_session(cfg: &TunnelBuilder) -> Result<()> {
    let stream = connect_any(&cfg.server_host, cfg.control_port, &cfg.tls).await?;
    let (reader, mut writer) = tokio::io::split(stream);
    let mut lines = BufReader::new(reader).lines();

    let reg = match &cfg.token {
        Some(tok) => format!("REGISTER {} {}\n", cfg.subdomain, tok),
        None => format!("REGISTER {}\n", cfg.subdomain),
    };
    writer.write_all(reg.as_bytes()).await?;

    let reply = time::timeout(Duration::from_secs(10), lines.next_line())
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(Error::Io)?
        .ok_or(Error::ControlClosed)?;

    if let Some(rest) = reply.strip_prefix("OK ") {
        info!("tunnel active: {rest}");
        println!("✓  Tunnel: {rest}");
    } else if let Some(reason) = reply.strip_prefix("ERR ") {
        return Err(Error::Rejected(cfg.subdomain.clone(), reason.into()));
    } else {
        return Err(Error::ControlClosed);
    }

    let server_host = cfg.server_host.clone();
    let data_port = cfg.data_port;
    let local_port = cfg.local_port;
    let subdomain = cfg.subdomain.clone();
    let tls = cfg.tls.clone();
    let log_tx = cfg.log_tx.clone();
    let idle_timeout = Duration::from_secs(proto::AGENT_IDLE_TIMEOUT_SECS);

    loop {
        let line = time::timeout(idle_timeout, lines.next_line())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::Io)?
            .ok_or(Error::ControlClosed)?;

        if line == "CONNECT" {
            debug!("opening idle data channel");
            let sh = server_host.clone();
            let sub = subdomain.clone();
            let t = tls.clone();
            let tx = log_tx.clone();
            tokio::spawn(async move {
                if let Err(e) = open_idle_channel(&sh, data_port, local_port, &sub, &t, tx).await {
                    debug!(error = %e, "idle channel error");
                }
            });
        } else if line == "PING" {
            writer.write_all(b"PONG\n").await?;
        }
    }
}

// ── Idle channel ──────────────────────────────────────────────────────────────

async fn open_idle_channel(
    server_host: &str,
    data_port: u16,
    local_port: u16,
    subdomain: &str,
    tls: &TlsMode,
    log_tx: Option<broadcast::Sender<RequestLog>>,
) -> Result<()> {
    let mut server_stream: AnyStream = time::timeout(
        Duration::from_secs(10),
        connect_any(server_host, data_port, tls),
    )
    .await
    .map_err(|_| Error::Timeout)??;

    server_stream.write_all(format!("IDLE {subdomain}\n").as_bytes()).await?;

    // Wait for server to acknowledge the idle slot.
    let mut ack_buf = [0u8; 8];
    let n: usize = time::timeout(Duration::from_secs(10), server_stream.read(&mut ack_buf))
        .await
        .map_err(|_| Error::Timeout)??;
    if std::str::from_utf8(&ack_buf[..n]).unwrap_or("").trim() != "READY" {
        return Ok(());
    }

    // Wait for visitor traffic — first bytes = start of an HTTP request.
    let mut head = vec![0u8; 8192];
    let n: usize = server_stream.read(&mut head).await?;
    if n == 0 {
        return Ok(()); // channel closed before any visitor arrived
    }
    head.truncate(n);

    let start = std::time::Instant::now();
    let (method, path) = parse_request_line(&head);

    let mut local = TcpStream::connect(("127.0.0.1", local_port))
        .await
        .map_err(|_| Error::LocalRefused(local_port))?;

    // Forward the buffered request head to the local app before splicing.
    local.write_all(&head).await?;

    let (mut sr, mut sw) = tokio::io::split(server_stream);
    let (mut lr, mut lw) = local.into_split();

    // Both directions run concurrently. The log is emitted inside the
    // response arm (lr → sw) as soon as the response is fully sent —
    // before the visitor side (sr → lw) closes. Without this, HTTP
    // keep-alive connections would hold the log until the browser
    // disconnects, leaving the dashboard blank during active sessions.
    let _ = tokio::join!(
        tokio::io::copy(&mut sr, &mut lw),
        async {
            let bytes_out = tokio::io::copy(&mut lr, &mut sw).await.unwrap_or(0);
            if let Some(tx) = &log_tx {
                let _ = tx.send(RequestLog {
                    subdomain: subdomain.to_owned(),
                    method: method.clone(),
                    path: path.clone(),
                    bytes_in: head.len() as u64,
                    bytes_out,
                    duration_ms: start.elapsed().as_millis() as u64,
                    timestamp_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis(),
                });
            }
        },
    );
    Ok(())
}

fn parse_request_line(head: &[u8]) -> (String, String) {
    let s = std::str::from_utf8(head).unwrap_or("");
    let first = s.lines().next().unwrap_or("");
    let mut parts = first.splitn(3, ' ');
    let method = parts.next().unwrap_or("?").to_owned();
    let path = parts.next().unwrap_or("/").to_owned();
    (method, path)
}

// ── TLS ───────────────────────────────────────────────────────────────────────

async fn connect_any(host: &str, port: u16, tls: &TlsMode) -> Result<AnyStream> {
    let tcp = TcpStream::connect((host, port)).await?;
    match tls {
        TlsMode::Off => Ok(Box::new(tcp)),
        _ => {
            let connector = make_tls_connector(tls)?;
            let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
                .map_err(|e| Error::Io(std::io::Error::other(format!("invalid server name: {e}"))))?;
            let tls_stream = connector.connect(server_name, tcp).await?;
            Ok(Box::new(tls_stream))
        }
    }
}

fn make_tls_connector(tls: &TlsMode) -> Result<tokio_rustls::TlsConnector> {
    use rustls::ClientConfig;
    use std::sync::Arc;

    let config = match tls {
        TlsMode::On => {
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            ClientConfig::builder().with_root_certificates(roots).with_no_client_auth()
        }
        TlsMode::CustomCa(path) => {
            let file = std::fs::File::open(path)
                .map_err(|e| Error::Io(std::io::Error::other(format!("CA cert: {e}"))))?;
            let mut reader = std::io::BufReader::new(file);
            let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| Error::Io(std::io::Error::other(format!("CA cert parse: {e}"))))?;
            let mut roots = rustls::RootCertStore::empty();
            for cert in certs {
                roots.add(cert).map_err(|e| {
                    Error::Io(std::io::Error::other(format!("CA cert add: {e}")))
                })?;
            }
            ClientConfig::builder().with_root_certificates(roots).with_no_client_auth()
        }
        TlsMode::Insecure => ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertVerification))
            .with_no_client_auth(),
        TlsMode::Off => unreachable!(),
    };
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

#[derive(Debug)]
struct NoCertVerification;

impl rustls::client::danger::ServerCertVerifier for NoCertVerification {
    fn verify_server_cert(
        &self,
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self, _: &[u8], _: &rustls::pki_types::CertificateDer<'_>, _: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self, _: &[u8], _: &rustls::pki_types::CertificateDer<'_>, _: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        use rustls::SignatureScheme::*;
        vec![
            RSA_PKCS1_SHA256, RSA_PKCS1_SHA384, RSA_PKCS1_SHA512,
            ECDSA_NISTP256_SHA256, ECDSA_NISTP384_SHA384,
            RSA_PSS_SHA256, RSA_PSS_SHA384, RSA_PSS_SHA512,
            ED25519,
        ]
    }
}
