#!/usr/bin/env bash
set -euo pipefail

# Qonduit install script
# Usage: curl -sSfL https://raw.githubusercontent.com/alez04/qonduit/main/install.sh | bash
# Or with a specific version: VERSION=0.1.0 bash install.sh

REPO="alez04/qonduit"
BINARY="qonduit"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
VERSION="${VERSION:-latest}"

# --- Detect platform ---
detect_platform() {
    local os arch

    case "$(uname -s)" in
        Linux*)     os="unknown-linux-gnu" ;;
        Darwin*)    os="apple-darwin" ;;
        MINGW*|MSYS*|CYGWIN*) os="pc-windows-msvc" ;;
        *)          echo "Error: unsupported OS $(uname -s)" >&2; exit 1 ;;
    esac

    case "$(uname -m)" in
        x86_64|amd64)   arch="x86_64" ;;
        aarch64|arm64)   arch="aarch64" ;;
        *)               echo "Error: unsupported architecture $(uname -m)" >&2; exit 1 ;;
    esac

    # Windows uses .exe
    if [[ "$os" == "pc-windows-msvc" ]]; then
        echo "${arch}-${os}"
    else
        echo "${arch}-${os}"
    fi
}

# --- Detect archive format ---
detect_ext() {
    local target="$1"
    case "$target" in
        *windows*) echo "zip" ;;
        *)         echo "tar.gz" ;;
    esac
}

# --- Get latest version from GitHub API ---
get_latest_version() {
    curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
        | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"//;s/".*//'
}

# --- Download and install ---
main() {
    local target ext version archive_url tmp_dir

    target="$(detect_platform)"
    ext="$(detect_ext "$target")"

    if [[ "$VERSION" == "latest" ]]; then
        echo "Fetching latest release..."
        VERSION="$(get_latest_version)"
        if [[ -z "$VERSION" ]]; then
            echo "Error: no releases found at https://github.com/${REPO}/releases" >&2
            echo "" >&2
            echo "No prebuilt binaries available yet. You can:" >&2
            echo "  1. Build from source:" >&2
            echo "     git clone https://github.com/${REPO}.git && cd qonduit && cargo build --release" >&2
            echo "  2. Wait for a release to be published" >&2
            exit 1
        fi
    fi

    echo "Installing Qonduit ${VERSION} for ${target}"

    archive="qonduit-${target}.${ext}"
    archive_url="https://github.com/${REPO}/releases/download/${VERSION}/${archive}"

    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "${tmp_dir:-}"' EXIT

    echo "Downloading ${archive_url}..."
    if ! curl -fsSL -o "${tmp_dir}/${archive}" "$archive_url"; then
        echo "Error: failed to download ${archive_url}" >&2
        echo "Check that release ${VERSION} exists and has a ${target} build." >&2
        exit 1
    fi

    echo "Extracting..."
    case "$ext" in
        tar.gz)
            tar xzf "${tmp_dir}/${archive}" -C "$tmp_dir"
            ;;
        zip)
            unzip -q "${tmp_dir}/${archive}" -d "$tmp_dir"
            ;;
    esac

    # Find the binary (may be in a subdirectory or suffixed with target)
    local bin_path
    bin_path="$(find "$tmp_dir" -maxdepth 2 \( -name "${BINARY}" -o -name "${BINARY}.exe" -o -name "${BINARY}-*" \) -type f | head -1)"
    if [[ -z "$bin_path" ]]; then
        echo "Error: binary not found in archive" >&2
        exit 1
    fi

    # Install
    echo "Installing to ${INSTALL_DIR}/${BINARY}..."
    install -d "$INSTALL_DIR"
    install -m 755 "$bin_path" "${INSTALL_DIR}/${BINARY}"

    echo ""
    echo "Installed ${BINARY} ${VERSION} to ${INSTALL_DIR}/${BINARY}"
    echo ""
    "${INSTALL_DIR}/${BINARY}" --version 2>/dev/null || true
    echo ""
    echo "Get started:"
    echo "  qonduit --help"
}

main "$@"
