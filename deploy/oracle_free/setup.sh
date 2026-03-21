#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${REPO_URL:-}"
BRANCH="${BRANCH:-main}"
APP_DIR="${APP_DIR:-/opt/lalacore_omega}"
PORT="${PORT:-8000}"
DOMAIN="${DOMAIN:-}"
EMAIL="${EMAIL:-}"

if [ -z "$REPO_URL" ]; then
  echo "REPO_URL is required (export REPO_URL=...)."
  exit 1
fi

SERVICE_USER="$(id -un)"

echo "==> Installing system packages"
sudo apt-get update -y
sudo apt-get install -y \
  git \
  python3 \
  python3-venv \
  python3-pip \
  build-essential \
  curl \
  tesseract-ocr \
  poppler-utils \
  libgl1 \
  libglib2.0-0 \
  libsm6 \
  libxrender1 \
  libxext6

echo "==> Cloning repo"
sudo mkdir -p "$APP_DIR"
sudo chown -R "$SERVICE_USER":"$SERVICE_USER" "$APP_DIR"
if [ ! -d "$APP_DIR/.git" ]; then
  git clone "$REPO_URL" "$APP_DIR"
fi
git -C "$APP_DIR" fetch --all --prune
git -C "$APP_DIR" checkout "$BRANCH"
git -C "$APP_DIR" pull --ff-only origin "$BRANCH"

echo "==> Preparing venv"
python3 -m venv "$APP_DIR/venv"
"$APP_DIR/venv/bin/pip" install --upgrade pip
"$APP_DIR/venv/bin/pip" install -r "$APP_DIR/requirements.txt"

echo "==> Ensuring data directories"
mkdir -p "$APP_DIR/data"

if [ ! -f "$APP_DIR/.env" ] && [ -f "$APP_DIR/.env.example" ]; then
  cp "$APP_DIR/.env.example" "$APP_DIR/.env"
  echo "Created .env from .env.example (fill in your keys)."
fi

echo "==> Creating systemd service"
SERVICE_FILE="/etc/systemd/system/lalacore-omega.service"
BIND_HOST="0.0.0.0"
if [ -n "$DOMAIN" ]; then
  BIND_HOST="127.0.0.1"
fi

sudo tee "$SERVICE_FILE" >/dev/null <<EOF
[Unit]
Description=LalaCore Omega API
After=network.target

[Service]
Type=simple
WorkingDirectory=$APP_DIR
Environment=PYTHONUNBUFFERED=1
Environment=LC9_HTTP_PORT=$PORT
ExecStart=$APP_DIR/venv/bin/uvicorn app.main:app --host $BIND_HOST --port $PORT
Restart=on-failure
User=$SERVICE_USER
Group=$SERVICE_USER

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable lalacore-omega
sudo systemctl restart lalacore-omega

if [ -n "$DOMAIN" ]; then
  echo "==> Installing Caddy for HTTPS"
  sudo apt-get install -y debian-keyring debian-archive-keyring apt-transport-https
  curl -1sLf "https://dl.cloudsmith.io/public/caddy/stable/gpg.key" | \
    sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
  curl -1sLf "https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt" | \
    sudo tee /etc/apt/sources.list.d/caddy-stable.list >/dev/null
  sudo apt-get update -y
  sudo apt-get install -y caddy

  sudo tee /etc/caddy/Caddyfile >/dev/null <<EOF
$DOMAIN {
  reverse_proxy 127.0.0.1:$PORT
}
EOF

  if [ -n "$EMAIL" ]; then
    sudo systemctl set-environment CADDY_EMAIL="$EMAIL"
  fi
  sudo systemctl restart caddy
fi

echo "==> Done."
echo "Service status: systemctl status lalacore-omega"
