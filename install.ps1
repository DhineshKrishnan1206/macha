# Macha agent installer — Windows (PowerShell)
# Usage:  irm https://macha.live/install.ps1 | iex
#   or:   .\install.ps1

$ErrorActionPreference = "Stop"

$Repo    = "dhineshk/macha"
$Binary  = "macha.exe"
$Target  = "x86_64-pc-windows-msvc"
$InstallDir = "$env:LOCALAPPDATA\macha\bin"

# ── Download pre-built binary ─────────────────────────────────────────────────
$ReleaseUrl = "https://github.com/$Repo/releases/latest/download/macha-$Target.zip"

function Install-FromRelease {
    Write-Host "Downloading macha for Windows..."
    $Tmp = [System.IO.Path]::GetTempPath() + [System.Guid]::NewGuid().ToString()
    New-Item -ItemType Directory -Path $Tmp | Out-Null

    try {
        Invoke-WebRequest -Uri $ReleaseUrl -OutFile "$Tmp\macha.zip" -UseBasicParsing
        Expand-Archive -Path "$Tmp\macha.zip" -DestinationPath $Tmp -Force

        if (-not (Test-Path $InstallDir)) {
            New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
        }
        Copy-Item "$Tmp\$Binary" "$InstallDir\$Binary" -Force
        Write-Host "Installed to $InstallDir\$Binary"
        return $true
    } catch {
        return $false
    } finally {
        Remove-Item $Tmp -Recurse -Force -ErrorAction SilentlyContinue
    }
}

# ── Fall back to cargo install ────────────────────────────────────────────────
function Install-FromCargo {
    if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
        Write-Host ""
        Write-Host "cargo not found. Install Rust from https://rustup.rs then re-run this script."
        Write-Host "Or run:  cargo install --git https://github.com/$Repo macha"
        exit 1
    }
    Write-Host "Building from source with cargo (this takes ~1 minute)..."
    cargo install --git "https://github.com/$Repo" --bin macha
}

$ok = Install-FromRelease
if (-not $ok) {
    Write-Host "No pre-built release found — falling back to cargo install."
    Install-FromCargo
}

# ── Add to PATH for this session ──────────────────────────────────────────────
$CurrentPath = [System.Environment]::GetEnvironmentVariable("PATH", "User")
if ($CurrentPath -notlike "*$InstallDir*") {
    [System.Environment]::SetEnvironmentVariable(
        "PATH", "$CurrentPath;$InstallDir", "User"
    )
    $env:PATH += ";$InstallDir"
    Write-Host "Added $InstallDir to your PATH."
    Write-Host "Restart your terminal for the PATH change to take effect in new windows."
}

# ── Done ──────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "  macha installed successfully!"
Write-Host ""
Write-Host "  Usage:"
Write-Host "    macha --port 3000 --subdomain myapp"
Write-Host ""
Write-Host "  Self-hosted server:"
Write-Host "    macha --port 3000 --subdomain myapp --server tunnel.mycompany.com"
Write-Host ""
Write-Host "  Run 'macha --help' for all options."
