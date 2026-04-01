# Railway Deploy

## Runtime Entry

- Start command: `uvicorn core.api.entrypoint:app --host 0.0.0.0 --port $PORT --timeout-keep-alive 60`
- Health check: `/health`
- Readiness check: `/health/ready`

## Required Environment Variables

- `OPENROUTER_KEYS` or `OPENROUTER_API_KEY`
- `GROQ_KEYS` or `GROQ_API_KEY`
- `GEMINI_KEYS` or `GEMINI_API_KEY`
- `HF_KEYS` or `HF_API_KEY`

## Recommended Runtime Controls

- `LC9_PIPELINE_MAX_CONCURRENCY=2`
- `LC9_PIPELINE_TIMEOUT_S=55`
- `OTP_EMAIL_ENABLED=false` if SMTP is not configured yet

## Deployment Steps

1. Push the repository to GitHub.
2. Create a new Railway project and connect the GitHub repository.
3. Ensure Railway detects `nixpacks.toml`, `railway.json`, `Procfile`, and `requirements.txt`.
4. Add the required provider keys and database env vars in Railway Variables.
5. Deploy the service and wait for `/health` to return `{"ok": true, "status": "healthy"}`.
6. Verify `/health/ready`, then test `POST /solve` with a representative pipeline-heavy request.

## Notes

- OCR/PDF stages require `tesseract` and `poppler_utils`; `nixpacks.toml` installs them.
- The FastAPI app is exported from `core.api.entrypoint` through a lazy ASGI wrapper so Railway can boot from the pipeline module without changing route contracts.
- Pipeline concurrency and timeout guards are enforced inside `lalacore_entry`, so `/solve`, app actions, and other internal callers share the same runtime protections.
- For the production same-project live-class API plus worker setup, see [railway_same_project_live_classes.md](/Users/ritamsaha/lalacore_omega/deploy/railway_same_project_live_classes.md).
