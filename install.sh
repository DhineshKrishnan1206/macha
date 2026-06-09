#!/usr/bin/env bash
set -euo pipefail

# Macha agent installer — Mac and Linux
# Usage:  curl -fsSL https://macha.live/install.sh | bash
#   or:   bash install.sh [--server tunnel.mycompany.com]

REPO="dhineshk/macha"           # GitHub repo for pre-built releases
BINARY="macha"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

# ── Detect platform ────────────────────────────────────────────────────────────
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
  Darwin)
    case "$ARCH" in
      arm64)  TARGET="aarch64-apple-darwin" ;;
      x86_64) TARGET="x86_64-apple-darwin"  ;;
      *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
    esac
    ;;
  Linux)
    case "$ARCH" in
      x86_64)  TARGET="x86_64-unknown-linux-musl"  ;;
      aarch64) TARGET="aarch64-unknown-linux-musl"  ;;
      *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
    esac
    ;;
  *)
    echo "Unsupported OS: $OS" >&2
    exit 1
    ;;
esac

# ── Try pre-built binary from GitHub releases ──────────────────────────────────
LATEST_URL="https://github.com/${REPO}/releases/latest/download/${BINARY}-${TARGET}.tar.gz"

install_from_release() {
  echo "Downloading macha for ${TARGET}..."
  TMP="$(mktemp -d)"
  trap 'rm -rf "$TMP"' EXIT

  if curl -fsSL "$LATEST_URL" -o "$TMP/macha.tar.gz" 2>/dev/null; then
    tar -xzf "$TMP/macha.tar.gz" -C "$TMP"
    chmod +x "$TMP/$BINARY"

    if [ -w "$INSTALL_DIR" ]; then
      mv "$TMP/$BINARY" "$INSTALL_DIR/$BINARY"
    else
      sudo mv "$TMP/$BINARY" "$INSTALL_DIR/$BINARY"
    fi
    echo "Installed $BINARY to $INSTALL_DIR/$BINARY"
    return 0
  fi
  return 1
}

# ── Fall back to cargo install ─────────────────────────────────────────────────
install_from_cargo() {
  if ! command -v cargo &>/dev/null; then
    echo ""
    echo "cargo not found. Install Rust first:"
    echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    echo ""
    echo "Then re-run this script, or run:"
    echo "  cargo install --git https://github.com/${REPO} macha"
    exit 1
  fi
  echo "Building from source with cargo (this takes ~1 minute)..."
  cargo install --git "https://github.com/${REPO}" --bin macha
}

if ! install_from_release; then
  echo "No pre-built release found — falling back to cargo install."
  install_from_cargo
fi

# ── Done ───────────────────────────────────────────────────────────────────────
echo ""
echo "  macha installed successfully!"
echo ""
echo "  Usage:"
echo "    macha --port 3000 --subdomain myapp"
echo ""
echo "  Self-hosted server:"
echo "    macha --port 3000 --subdomain myapp --server tunnel.mycompany.com"
echo ""
echo "  Run 'macha --help' for all options."
