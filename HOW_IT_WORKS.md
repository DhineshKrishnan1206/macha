# How Macha Works

Macha is a reverse tunnel — it lets a server running on your laptop accept traffic from the public internet without opening any inbound firewall ports. This is the same idea as ngrok, but simpler and self-hosted.

---

## The Problem It Solves

Your laptop is behind a NAT router. The internet cannot reach `localhost:3000` directly. Macha solves this by having your machine reach *out* to a known public server and keep a persistent connection open. Incoming traffic then rides that outbound connection back to you.

---

## Components

```
┌─────────────────────────────────────────────────────────────┐
│                      macha.live (VPS)                       │
│                                                             │
│  Port 8080 ──── public listener  (HTTP traffic)            │
│  Port 9000 ──── control listener (agent registration)      │
│  Port 9001 ──── data listener    (pre-warmed idle pool)    │
└──────────────────────────┬──────────────────────────────────┘
                           │  outbound TCP (your machine
                           │  initiates both connections)
                ┌──────────┴──────────┐
                │     macha agent     │  (runs on your laptop)
                │  src/bin/agent.rs   │
                │                     │
                │  dashboard :4040    │  (browser UI, live logs)
                └──────────┬──────────┘
                           │  connects to
                ┌──────────┴──────────┐
                │   your local app    │  e.g. localhost:3000
                └─────────────────────┘
```

There are three separate actors:

1. **The server** — a Rust binary running on `macha.live`, always on.
2. **The agent** — a Rust binary (or library) running on your machine. Also runs a local dashboard server on `localhost:4040`.
3. **The browser / client** — whoever visits `myapp.macha.live`.

---

## Startup Sequence

When you run `macha --port 3000 --subdomain myapp`, the following happens before any visitor arrives:

```
1.  agent → server:9000     REGISTER myapp [token]\n
2.  server validates:       subdomain format, not reserved, not taken, token matches
3.  server → agent          OK https://myapp.macha.live\n
4.  server → agent          CONNECT\n  (×5, to prime the pool)
5.  agent spawns 5 tasks, each opens a new TCP to server:9001
6.  each sends:             IDLE myapp\n
7.  server replies:         READY\n
8.  server stores each socket in  idle_pool["myapp"]
```

After step 8, five live TCP sockets are parked on the server, pointing back through the internet to the agent. They sit idle, waiting. The pool is warm.

Why 5? A browser typically opens 4–6 parallel connections to the same host for resource loading. Pre-warming 5 slots means the first burst of requests is served instantly.

---

## The Full Request Flow

Here is what happens when someone visits `https://myapp.macha.live`:

```
Browser                   Server (macha.live)              Agent (your laptop)
  │                              │                                │
  │  GET / HTTP/1.1              │                                │
  │  Host: myapp.macha.live      │                                │
  │─────────────────────────────>│                                │
  │                              │                                │
  │                    peek Host header (no bytes consumed)       │
  │                    subdomain = "myapp"                        │
  │                    rate limit check → allowed                 │
  │                    pop idle socket from pool["myapp"]         │
  │                    send replenish signal → agent              │
  │                              │                                │
  │                              │  CONNECT\n (replace slot)     │
  │                              │──────────────────────────────>│
  │                              │                                │
  │                    bridge: browser ↔ idle socket              │
  │                              │                                │
  │                              │  visitor bytes arrive          │
  │                              │──────────────────────────────>│
  │                              │                                │
  │                              │           lazy-connect localhost:3000
  │                              │           write buffered request head
  │                              │           splice both directions
  │                              │                                │
  │<══════════════════ full bidirectional pipe ═════════════════>│
  │                              │                                │
  │                              │           [connection closes]  │
  │                              │           emit RequestLog      │
  │                              │           → dashboard updates  │
```

The HTTP request bytes flow unmodified all the way to your local app. The response flows back the same way. This works for any TCP protocol — HTTP, WebSocket, gRPC.

---

## The Control Channel

When the agent starts, the very first thing it does is open one persistent TCP connection to **port 9000**. This is the *control channel*. It stays open for the entire session.

**Handshake:**
```
Agent  →  Server:   REGISTER myapp [token]\n
Server →  Agent:    OK https://myapp.macha.live\n
```

After registration the control channel carries:

- `CONNECT` — server asks the agent to open one more idle data channel.
- `PING` — server → agent every 25 seconds (keepalive).
- `PONG` — agent → server in response to PING.

The PING/PONG keepalive exists because cloud providers (AWS, DigitalOcean, etc.) have NAT boxes that silently kill idle TCP connections after ~60–90 seconds. Without pings, the control channel would die and the agent wouldn't know. If the agent receives nothing for 80 seconds (≈ 3 missed pings), it considers the connection dead and reconnects.

---

## The Connection Pool — Solving HTTP Keep-Alive

Modern browsers hold TCP connections open across multiple requests (HTTP/1.1 keep-alive). In a naive design, a new data channel would be opened for every HTTP request, adding one full round-trip of latency to each request (~100–300ms depending on geography).

Macha uses a **pre-warmed idle pool** to eliminate this latency entirely:

1. After registration, the server sends 5 `CONNECT` messages immediately.
2. The agent opens 5 idle data channels to port 9001, each sending `IDLE myapp\n`.
3. The server stores these 5 ready streams in `idle_pool["myapp"]`.
4. When a visitor arrives, the server immediately pops one stream from the pool.
5. The server sends one `CONNECT` to ask the agent to replenish — the pool returns to size.

```
Pool (server side):  idle_pool["myapp"] = [stream, stream, stream, stream, stream]
                                               ↑ visitor grabs one instantly
                                               ↑ agent opens a replacement (async)
```

**Lazy local connection.** Each idle data channel does **not** connect to `localhost:3000` until actual visitor bytes start flowing through it. This means:

- Idle channels consume no resources on the local app.
- If the local app restarts between requests, the new connection is always fresh.
- HTTP keep-alive works correctly — each browser TCP connection gets its own idle channel.

---

## Data Channel Protocol

```
Agent → Server (port 9001):   IDLE myapp\n
Server → Agent:               READY\n
                              ... waits in pool until a visitor is routed here ...
[visitor request bytes flow through]
Agent detects first bytes → connects to localhost:3000 → bridges streams
```

---

## Authentication

If the server has `AUTH_TOKEN=secret` set, agents must include the token in `REGISTER`:

```
Agent → Server:  REGISTER myapp my-secret-token\n
```

Without a valid token the server replies `ERR invalid or missing token` and closes. The token is validated by constant-time string comparison. Tokens should be sent over TLS to avoid interception.

---

## TLS for Agent↔Server Traffic

Ports 9000 (control) and 9001 (data) can be wrapped in TLS. The public port (8080) stays plain TCP because it sits behind nginx/caddy which handles HTTPS termination for the `*.macha.live` wildcard.

**Server:** set `TLS_CERT=/path/to/cert.pem` and `TLS_KEY=/path/to/key.pem`. The server wraps every accepted control/data connection in `tokio-rustls`.

**Agent:** use `--tls` (Mozilla root certs, works with a public cert on macha.live), `--tls-ca /path/to/ca.pem` (custom CA for self-signed certs), or `--tls-insecure` (skip verification — dev only).

The type system uses `Box<dyn AsyncReadWrite>` (called `AnyStream`) to unify `TcpStream` and `TlsStream<TcpStream>` behind a single type:

```rust
trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncReadWrite for T {}
type AnyStream = Box<dyn AsyncReadWrite>;
```

This allows TLS and plain TCP connections to be stored in the same pool and passed to the same splice functions without any branching downstream.

---

## Rate Limiting and Abuse Prevention

Three independent layers prevent abuse:

**1. Per-subdomain request rate limit (token bucket)**

Each subdomain gets a token bucket with a capacity of 200 tokens, refilling at 100 tokens/second. Every incoming request consumes one token. When the bucket is empty the server returns HTTP `429 Too Many Requests` without touching the agent pool. The bucket is stored in a `DashMap<subdomain → Mutex<TokenBucket>>` so each subdomain's bucket is independent.

```
Burst limit:      200 requests
Sustained limit:  100 requests/second
Response when exceeded: 429 Too Many Requests
```

**2. Per-IP registration rate limit**

Each unique client IP is tracked in a `RegTracker` with a sliding 60-second window. An IP that sends more than 5 `REGISTER` commands in one minute is rejected with `ERR too many registration attempts`. This prevents automated subdomain squatting.

**3. Reserved subdomains**

The following subdomains cannot be registered by agents — they are permanently blocked:

```
api, www, admin, root, mail, server, dashboard, status, health,
macha, app, staging, prod, beta, static, assets, cdn, auth,
login, signup, register, support, help, docs, blog, dev, test
```

---

## Security Headers

Every HTML and text response from the public port includes:

```
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
X-XSS-Protection: 1; mode=block
Referrer-Policy: strict-origin-when-cross-origin
```

These protect visitors to the landing page and error pages from MIME sniffing, clickjacking, and reflected XSS.

---

## Agent Dashboard

When the agent starts, it also binds a lightweight HTTP server on `localhost:4040`. Open it in your browser to see a live view of tunnel traffic.

```
http://localhost:4040/         → dashboard UI (dark green theme)
http://localhost:4040/events   → SSE stream (used by the UI)
http://localhost:4040/api/status → JSON status
```

**How the data flows:**

```
open_idle_channel() finishes a request
    ↓
emits RequestLog into broadcast::channel<RequestLog>
    ↓
background aggregator task receives it:
  - increments AtomicU64 counters (requests, bytes_in, bytes_out)
  - appends to VecDeque<RequestLog> (last 200 entries)
  - serializes to JSON, sends into broadcast::channel<String>
    ↓
each SSE connection has a Receiver on that channel
  → writes "event: log\ndata: {...}\n\n" to the browser
```

The dashboard uses **SSE (Server-Sent Events)** rather than WebSocket — SSE is one-directional (server → browser), which is all a log stream needs. The browser reconnects automatically if the connection drops. When a new browser tab opens `/events`, it first receives the last 200 requests as a backfill, then live events going forward.

The `dashboard.html` file is embedded directly in the agent binary at compile time via `include_str!`. There is no file path to manage at runtime.

---

## Reconnection (Library Side)

The library's `Tunnel::run()` method wraps a single session in a retry loop with exponential backoff:

```
run()
 └─ loop {
      match run_session().await {
        Ok / connection-closed    →  sleep(backoff), backoff *= 2 (max 60s), retry
        Err(Rejected)             →  return error immediately (fatal — retrying won't help)
        Err(InvalidSubdomain)     →  return error immediately (fatal)
      }
    }
```

Transient errors (I/O failure, timeout, control channel closed) trigger a reconnect. Fatal errors (server rejected the subdomain, bad subdomain format) surface immediately because the same attempt will always fail.

---

## Source Map

```
src/
  lib.rs          Public API: Tunnel, TunnelBuilder, RequestLog, start()
  error.rs        Error enum (thiserror) — fatal vs. transient distinction
  proto.rs        Shared constants: ports, timeouts
  bin/
    server.rs     Three listeners, connection pool, rate limiting, security headers
    agent.rs      CLI, dashboard HTTP server, log aggregator, DashState

web/
  index.html      Landing page served at macha.live root
  dashboard.html  Agent dashboard UI (embedded in agent binary at compile time)

Dockerfile        Multi-stage release build
docker-compose.yml  One-command deployment
```

### `src/lib.rs`

The public API. `TunnelBuilder` validates the subdomain and builds a `Tunnel`. `Tunnel::run()` owns the retry loop. `run_session()` handles one connection lifetime: register, receive CONNECT/PING, spawn idle channels. `open_idle_channel()` is one idle channel's full lifetime: handshake → wait for visitor bytes → lazy-connect local → splice → emit `RequestLog`.

### `src/bin/server.rs`

Three async tasks in a `tokio::select!` loop:

- **`handle_public`** — peeks the HTTP request with a 5-second timeout, extracts the subdomain, checks rate limit, pops an idle `AnyStream` from the pool, sends a replenish signal, bridges visitor ↔ idle channel.
- **`handle_control`** — checks per-IP registration rate, validates token, rejects reserved/taken subdomains, registers agent, primes pool with 5 CONNECTs, write task (replenish→CONNECT + PING every 25s), read task (drains PONG, detects disconnect), cleanup on exit.
- **`handle_data`** — reads `IDLE <subdomain>`, sends `READY`, pushes the `AnyStream` into the idle pool for the public handler.

### `src/bin/agent.rs`

Two concurrent jobs:

1. **Tunnel** — calls `Tunnel::run()` with a `log_channel` wired in. All actual tunneling logic lives in `lib.rs`.
2. **Dashboard server** — `TcpListener` on `localhost:4040`, hand-rolled HTTP routing, SSE streaming. `DashState` (Arc-shared) holds atomic counters, a ring buffer of recent logs, and a `broadcast::Sender` for fan-out to SSE subscribers.

---

## Deployment

Recommended production stack:

```
Internet → Caddy / nginx  (TLS termination, ports 80/443, wildcard *.macha.live cert)
               ↓ proxy_pass
           macha server (port 8080 internal)

Agents connect directly to:
  macha.live:9000  (control, optionally TLS)
  macha.live:9001  (data,    optionally TLS)
```

The Rust server does not handle HTTPS for the public port — Caddy or nginx terminates it and forwards plain HTTP to port 8080. The agent-to-server connections on 9000/9001 can be TLS-wrapped independently if desired.

---

## What "Transparent Proxy" Means

The server uses `socket.peek()` to read just enough bytes to find the `Host:` header. `peek()` leaves the bytes in the kernel receive buffer — they are not consumed. When the two streams are spliced, the full original request including those peeked bytes reaches the local app unmodified.

This means:
- `X-Forwarded-For` is **not** added (unless your nginx/caddy layer adds it).
- WebSocket upgrades work because the splice is at the TCP level.
- HTTP/1.1 keep-alive, chunked encoding, and binary protocols all work because nothing parses or re-serializes the HTTP.
