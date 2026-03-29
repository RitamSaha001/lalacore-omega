FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        tesseract-ocr \
        poppler-utils \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt

RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install -r /app/requirements.txt

COPY *.py /app/
COPY app /app/app
COPY core /app/core
COPY services /app/services
COPY verification /app/verification
COPY config /app/config
COPY engine /app/engine
COPY migrations /app/migrations
COPY models /app/models
COPY telemetry /app/telemetry
COPY vault /app/vault
COPY credentials /app/credentials
COPY data/vault /app/data/vault
COPY data/app/import_question_bank.json /app/data/app/import_question_bank.json
COPY data/app/JEE_BANK_X.json /app/data/app/JEE_BANK_X.json
COPY data/app/app_data.sqlite3 /app/data/app/app_data.sqlite3
COPY data/app/assessments.json /app/data/app/assessments.json
COPY data/app/materials.json /app/data/app/materials.json
COPY data/app/live_class_schedule.json /app/data/app/live_class_schedule.json
COPY data/app/uploads.json /app/data/app/uploads.json
COPY data/app/ai_generated_quizzes.json /app/data/app/ai_generated_quizzes.json
COPY data/app/results.json /app/data/app/results.json
COPY data/app/teacher_review_queue.json /app/data/app/teacher_review_queue.json
COPY data/app/import_drafts.json /app/data/app/import_drafts.json
COPY data/app/chat_threads.json /app/data/app/chat_threads.json
COPY data/app/chat_users.json /app/data/app/chat_users.json
COPY data/auth/users.json /app/data/auth/users.json
COPY data/auth/otp.json /app/data/auth/otp.json

RUN mkdir -p \
    /app/data/app/uploads \
    /app/data/app/quizzes \
    /app/data/auth \
    /app/data/lc9 \
    /app/data/logs \
    /app/data/replay \
    /app/data/zaggle

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000} --timeout-keep-alive 60"]
