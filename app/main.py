import asyncio
import base64
import json
from dotenv import load_dotenv
from fastapi import Body, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse
import logging
import os
import sys

from app.live_classes_api import router as live_classes_router
from app import routes as app_routes
from app.discovery import start_discovery_responder
from core.bootstrap import initialize_keys
from core.api.entrypoint import warm_atlas_runtime
from core.db.connection import Database
from core.multimodal.ocr_engine import OCREngine
from core.multimodal.pdf_processor import PDFProcessor
from services.app_update_release_notifier import AppUpdateReleaseNotifierScheduler
from services.atlas_maintenance_service import AtlasMaintenanceScheduler

load_dotenv()
logger = logging.getLogger(__name__)

app = FastAPI(title="LalaCore Omega")
_atlas_maintenance_scheduler: AtlasMaintenanceScheduler | None = None
_app_update_release_notifier_scheduler: AppUpdateReleaseNotifierScheduler | None = None
_atlas_maintenance_service = app_routes._ATLAS_MAINTENANCE
_app_update_release_notifier_service = app_routes._APP_UPDATE_RELEASE_NOTIFIER
_atlas_runtime_warm_task = None
_atlas_runtime_warm_report: dict[str, object] = {}
_public_ocr = OCREngine(enable_remote_worker=False)
_public_pdf = PDFProcessor(
    ocr_engine=_public_ocr,
    enable_remote_worker=False,
)


class AtlasMaintenanceLockMiddleware:
    def __init__(self, app, *, service) -> None:
        self.app = app
        self._service = service
        self._allowed_prefixes = (
            "/health",
            "/ops/atlas-maintenance",
        )

    def _is_allowed(self, path: str) -> bool:
        return any(path.startswith(prefix) for prefix in self._allowed_prefixes)

    async def __call__(self, scope, receive, send) -> None:
        scope_type = str(scope.get("type") or "")
        path = str(scope.get("path") or "")
        if scope_type not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return
        if self._is_allowed(path) or not self._service.is_running():
            await self.app(scope, receive, send)
            return
        status = self._service.status_snapshot()
        if scope_type == "websocket":
            await send(
                {
                    "type": "websocket.close",
                    "code": 1013,
                    "reason": "Atlas maintenance in progress",
                }
            )
            return
        response = JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "status": "MAINTENANCE",
                "message": (
                    "Atlas weekly maintenance is in progress. The app is temporarily inaccessible "
                    "until the maintenance sweep completes."
                ),
                "maintenance": status,
            },
        )
        await response(scope, receive, send)


app.add_middleware(AtlasMaintenanceLockMiddleware, service=_atlas_maintenance_service)


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
    if not sender:
        fallback_sender = os.getenv("FORGOT_OTP_SENDER_EMAIL", "").strip()
        if fallback_sender:
            os.environ["OTP_SENDER_EMAIL"] = fallback_sender
            sender = fallback_sender
            logger.info(
                "OTP_SENDER_EMAIL missing; using FORGOT_OTP_SENDER_EMAIL for OTP email."
            )
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


def _detect_http_port() -> int:
    for env_name in ("LC9_HTTP_PORT", "APP_PUBLIC_PORT", "PORT", "UVICORN_PORT"):
        raw = os.getenv(env_name, "").strip()
        if not raw:
            continue
        try:
            return int(raw)
        except ValueError:
            continue

    argv = list(sys.argv)
    for index, token in enumerate(argv):
        lowered = str(token).strip().lower()
        if lowered == "--port" and index + 1 < len(argv):
            try:
                return int(str(argv[index + 1]).strip())
            except ValueError:
                continue
        if lowered.startswith("--port="):
            try:
                return int(lowered.split("=", 1)[1].strip())
            except ValueError:
                continue
    return 8000


@app.on_event("startup")
async def startup_event():
    global _atlas_maintenance_scheduler, _app_update_release_notifier_scheduler
    global _atlas_runtime_warm_task
    _validate_env()
    initialize_keys()
    try:
        discovery_port = int(os.getenv("LC9_DISCOVERY_PORT", "37999"))
    except ValueError:
        discovery_port = 37999
    http_port = _detect_http_port()
    os.environ["LC9_HTTP_PORT"] = str(http_port)
    if _bool_env("LC9_DISABLE_DISCOVERY", False):
        logger.info("LC9 discovery responder disabled by configuration.")
    else:
        start_discovery_responder(port=discovery_port, http_port=http_port)
    try:
        await Database.init()
    except Exception as exc:
        # DB is optional for file-backed research mode.
        logger.warning("Database init skipped: %s", exc)
    if _atlas_runtime_warm_task is None or _atlas_runtime_warm_task.done():
        _atlas_runtime_warm_task = asyncio.create_task(_warm_atlas_runtime_in_background())
    if _bool_env("ATLAS_MAINTENANCE_ENABLED", True):
        try:
            _atlas_maintenance_scheduler = AtlasMaintenanceScheduler(
                service=_atlas_maintenance_service
            )
            _atlas_maintenance_scheduler.start()
        except Exception as exc:
            logger.warning("Atlas maintenance scheduler startup skipped: %s", exc)
    if _bool_env("APP_UPDATE_CONFIRMATION_ENABLED", True):
        try:
            _app_update_release_notifier_scheduler = AppUpdateReleaseNotifierScheduler(
                service=_app_update_release_notifier_service
            )
            _app_update_release_notifier_scheduler.start()
        except Exception as exc:
            logger.warning("App update confirmation scheduler startup skipped: %s", exc)


@app.on_event("shutdown")
async def shutdown_event():
    global _atlas_maintenance_scheduler, _app_update_release_notifier_scheduler
    global _atlas_runtime_warm_task
    try:
        if _atlas_maintenance_scheduler is not None:
            await _atlas_maintenance_scheduler.stop()
    except Exception:
        pass
    _atlas_maintenance_scheduler = None
    try:
        if _app_update_release_notifier_scheduler is not None:
            await _app_update_release_notifier_scheduler.stop()
    except Exception:
        pass
    _app_update_release_notifier_scheduler = None
    task = _atlas_runtime_warm_task
    _atlas_runtime_warm_task = None
    try:
        if task is not None:
            task.cancel()
            await task
    except asyncio.CancelledError:
        pass
    except Exception:
        pass
    try:
        await Database.close()
    except Exception:
        pass


async def _warm_atlas_runtime_in_background() -> None:
    global _atlas_runtime_warm_report
    try:
        _atlas_runtime_warm_report = dict(await warm_atlas_runtime())
        logger.info("Atlas runtime warmup completed.")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _atlas_runtime_warm_report = {"ok": False, "error": str(exc)[:400]}
        logger.warning("Atlas runtime warmup skipped: %s", exc)


@app.get("/")
async def root():
    return {"status": "Omega running"}


@app.get("/health")
async def health():
    return {"ok": True, "status": "healthy"}


@app.get("/health/live")
async def health_live():
    return {"ok": True, "status": "live"}


@app.get("/health/ready")
async def health_ready():
    db_ready = False
    try:
        db_ready = await Database.health_check()
    except Exception:
        db_ready = False
    return {"ok": True, "status": "ready", "db_ready": bool(db_ready)}


@app.post("/ocr/frame")
async def ocr_frame(payload: dict = Body(default_factory=dict)):
    raw_image = str(
        payload.get("image_base64")
        or payload.get("base64_image")
        or payload.get("image")
        or ""
    ).strip()
    if not raw_image:
        raise HTTPException(status_code=400, detail="image_base64 is required")
    if raw_image.startswith("data:") and "," in raw_image:
        raw_image = raw_image.split(",", 1)[1]
    try:
        image_bytes = base64.b64decode(raw_image, validate=False)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"invalid_image_base64:{exc}",
        ) from exc
    ocr_data = await _public_ocr.extract_async(
        image_bytes,
        page_number=max(1, int(payload.get("page_number") or 1)),
        math_aware=bool(payload.get("math_aware", True)),
        optional_web_snippets=[
            dict(x)
            for x in (payload.get("optional_web_snippets") or [])
            if isinstance(x, dict)
        ],
    )
    text = (
        str(ocr_data.get("clean_text") or "").strip()
        or str(ocr_data.get("math_normalized_text") or "").strip()
        or str(ocr_data.get("raw_text") or "").strip()
    )
    return {
        "ok": True,
        "status": "SUCCESS",
        "text": text,
        "ocr_data": ocr_data,
    }


@app.post("/ocr/pdf")
async def ocr_pdf(
    file: UploadFile = File(...),
    optional_web_snippets_json: str = Form(""),
):
    blob = await file.read()
    snippets: list[dict] = []
    if optional_web_snippets_json.strip():
        try:
            decoded = json.loads(optional_web_snippets_json)
            if isinstance(decoded, list):
                snippets = [dict(x) for x in decoded if isinstance(x, dict)]
        except Exception:
            snippets = []
    pdf_data = await _public_pdf.process(
        blob,
        optional_web_snippets=snippets,
    )
    return {
        "ok": True,
        "status": "SUCCESS",
        "text": str(pdf_data.get("merged_text") or "").strip(),
        "pdf_data": pdf_data,
    }


app.include_router(app_routes.router)
app.include_router(live_classes_router)
