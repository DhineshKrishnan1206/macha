// Default ports — override via env vars or builder methods.
pub const CONTROL_PORT: u16 = 9000;
pub const DATA_PORT: u16 = 9001;

// Agents time out if no message arrives within this window.
// The server PINGs every 25 s, so 80 s ≈ 3 missed pings.
pub const AGENT_IDLE_TIMEOUT_SECS: u64 = 80;

// Reconnect backoff cap.
pub const MAX_BACKOFF_SECS: u64 = 60;
