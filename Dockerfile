# ── Build stage ──────────────────────────────────────────────────────────────
FROM rust:1.85-slim AS builder

WORKDIR /build

# Cache dependencies separately from source.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin && \
    echo 'fn main(){}' > src/bin/server.rs && \
    echo 'fn main(){}' > src/bin/agent.rs && \
    echo '' > src/lib.rs && \
    cargo build --release --bin server 2>/dev/null || true

COPY . .
RUN cargo build --release --bin server

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/server /usr/local/bin/macha-server

ENV PUBLIC_PORT=8080 \
    CONTROL_PORT=9000 \
    DATA_PORT=9001 \
    RUST_LOG=server=info

EXPOSE 8080 9000 9001

CMD ["macha-server"]
