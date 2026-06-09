use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("server rejected subdomain '{0}': {1}")]
    Rejected(String, String),

    #[error("invalid subdomain '{0}': use only letters, digits, hyphens (max 63 chars)")]
    InvalidSubdomain(String),

    #[error("control channel closed unexpectedly")]
    ControlClosed,

    #[error("local port {0} refused connection — is your server running?")]
    LocalRefused(u16),

    #[error("timed out waiting for server response")]
    Timeout,
}

pub type Result<T> = std::result::Result<T, Error>;
