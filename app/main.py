from dotenv import load_dotenv
from fastapi import FastAPI
import logging
import os

from app.live_classes_api import router as live_classes_router
from app.routes import router
from app.discovery import start_discovery_responder
from core.bootstrap import initialize_keys
from core.db.connection import Database

load_dotenv()
logger = logging.getLogger(__name__)

app = FastAPI(title="LalaCore Omega")


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _validate_env() -> None:
    if not _bool_env("OTP_EMAIL_ENABLED", True):
        logger.info("OTP email validation skipped (OTP_EMAIL_ENABLED=false)")
        return

    sender = os.getenv("OTP_SENDER_EMAIL", "").strip()
    sender_password = os.getenv("OTP_SENDER_PASSWORD", "").strip()
    if not sender or not sender_password:
        # Keep the server bootable in local/dev mode and let auth service
        # transparently fall back to local OTP generation.
        os.environ["OTP_EMAIL_ENABLED"] = "false"
        logger.warning(
            "OTP_EMAIL_ENABLED=true but sender credentials are missing; "
            "falling back to local OTP delivery."
        )
        return

    smtp_host = os.getenv("OTP_SMTP_HOST", "").strip()
    if not smtp_host:
        os.environ["OTP_SMTP_HOST"] = "smtp.gmail.com"
        logger.warning(
            "OTP_SMTP_HOST missing; defaulting to smtp.gmail.com for OTP email."
        )

    smtp_port = os.getenv("OTP_SMTP_PORT", "").strip()
    if not smtp_port:
        os.environ["OTP_SMTP_PORT"] = "587"
        logger.warning("OTP_SMTP_PORT missing; defaulting to 587 for OTP email.")
    else:
        try:
            int(smtp_port)
        except ValueError:
            os.environ["OTP_SMTP_PORT"] = "587"
            logger.warning(
                "Invalid OTP_SMTP_PORT=%s; defaulting to 587 for OTP email.",
                smtp_port,
            )


@app.on_event("startup")
async def startup_event():
    _validate_env()
    initialize_keys()
    try:
        discovery_port = int(os.getenv("LC9_DISCOVERY_PORT", "37999"))
    except ValueError:
        discovery_port = 37999
    try:
        http_port = int(os.getenv("LC9_HTTP_PORT", os.getenv("PORT", "8000")))
    except ValueError:
        http_port = 8000
    start_discovery_responder(port=discovery_port, http_port=http_port)
    try:
        await Database.init()
    except Exception as exc:
        # DB is optional for file-backed research mode.
        logger.warning("Database init skipped: %s", exc)


@app.on_event("shutdown")
async def shutdown_event():
    try:
        await Database.close()
    except Exception:
        pass


@app.get("/")
async def root():
    return {"status": "Omega running"}


@app.get("/health")
async def health():
    return {"ok": True, "status": "healthy"}


@app.get("/health/ready")
async def health_ready():
    db_ready = False
    try:
        db_ready = await Database.health_check()
    except Exception:
        db_ready = False
    return {"ok": True, "status": "ready", "db_ready": bool(db_ready)}


app.include_router(router)
app.include_router(live_classes_router)
