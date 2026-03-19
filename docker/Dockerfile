# ---- Stage 1: builder -------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build

# System dependencies needed to compile some Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies into an isolated prefix so we can copy them cleanly
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --prefix=/install --no-cache-dir -r requirements.txt

# ---- Stage 2: final runtime image -------------------------------------------
FROM python:3.11-slim AS final

LABEL org.opencontainers.image.title="AR Reconciliation Pipeline"
LABEL org.opencontainers.image.description="Cloud-native AR discrepancy detection pipeline"
LABEL org.opencontainers.image.vendor="DataEngineering Team"

# Runtime system packages only
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1001 appgroup \
    && useradd --uid 1001 --gid appgroup --no-create-home --shell /bin/false appuser

WORKDIR /app

# Copy installed Python packages from the builder stage
COPY --from=builder /install /usr/local

# Copy application source
COPY src/       ./src/
COPY config/    ./config/
COPY data/      ./data/
COPY pyproject.toml ./

# Install the project itself (editable-style, no deps since they're already present)
RUN pip install --no-deps -e . --quiet

# Switch to non-root user
USER appuser

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO \
    ENVIRONMENT=prod

# Default: run the pipeline against the sample data
ENTRYPOINT ["python", "-m", "src.pipeline.orchestrator"]
CMD ["--invoice-source", "data/sample/invoices.csv", \
     "--payment-source", "data/sample/payments.csv", \
     "--returns-source", "data/sample/returns.csv"]
