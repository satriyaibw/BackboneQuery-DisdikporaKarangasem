# Best practice: slim base + uv + non-root + layer caching (lihat Context7 /docker/docs)
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    TZ=Asia/Jakarta

# tzdata dibutuhkan ZoneInfo(SCHEDULE_TIMEZONE); bersihkan apt cache (best practice)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# uv binary dari image resmi (lebih cepat & reproducible daripada pip install uv)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# User non-root (Context7: COPY --chown + USER app)
RUN useradd -ms /bin/bash -u 1001 appuser

# 1. Copy metadata dependensi dulu agar layer pip/uv ter-cache
COPY pyproject.toml uv.lock .python-version ./

# 2. Install deps ke .venv proyek. Cache mount hemat rebuild.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev && \
    chown -R appuser:appuser /app/.venv

# 3. Copy source dengan ownership benar
COPY --chown=appuser:appuser main.py ./main.py
COPY --chown=appuser:appuser backbone_pull ./backbone_pull

USER appuser

ENTRYPOINT ["python", "main.py"]
# Default --run-once (mode verifikasi lokal). Override via `command:` di compose.
CMD ["--run-once"]
