# Oracle Always Free Deployment (Recommended)

This deploys the FastAPI backend on an always-free VM with persistent storage.
It avoids free PaaS sleep/ephemeral disk limitations and keeps all features
working (uploads, long-running AI calls, websockets).

## 1) Create VM

- Create an Oracle Cloud Always Free VM.
- Use an Ubuntu image.
- Open inbound ports 80, 443, and 8000 on the VM security list.

## 2) SSH to the VM

## 3) Run the installer

```
export REPO_URL="<your git repo url>"
export BRANCH="main"
export APP_DIR="/opt/lalacore_omega"
export PORT="8000"
# Optional for HTTPS (recommended):
export DOMAIN="api.example.com"
export EMAIL="admin@example.com"

bash deploy/oracle_free/setup.sh
```

## 4) Configure secrets

Edit `APP_DIR/.env` with your keys (OPENROUTER_KEYS, GROQ_KEYS, GEMINI_KEYS).

## 5) Verify

```
systemctl status lalacore-omega
```

If DOMAIN is set, your API will be available over HTTPS at:
- `https://<DOMAIN>/auth/action`
- `https://<DOMAIN>/app/action`
- `https://<DOMAIN>/solve`

If DOMAIN is not set, use HTTP via the VM IP and PORT.
