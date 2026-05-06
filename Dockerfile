# ============================================================
# SR DataLake (SDL) — Production Docker Image
# ============================================================
# Multi-stage build for minimal image size
# ============================================================

# ── Stage 1: Build dependencies ──
FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev python3-dev libmagic1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: Production image ──
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 libmagic1 curl postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r sdl && useradd -r -g sdl -d /app sdl

COPY --from=builder /install /usr/local

WORKDIR /app
COPY app.py .
COPY requirements.txt .
COPY backend/ backend/
COPY templates/ templates/
COPY static/ static/

RUN chown -R sdl:sdl /app

USER sdl

EXPOSE 5001

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -sf http://localhost:5001/login || exit 1

CMD ["gunicorn", \
     "--bind", "0.0.0.0:5001", \
     "--workers", "4", \
     "--timeout", "120", \
     "--access-logfile", "-", \
     "--error-logfile", "-", \
     "app:app"]
