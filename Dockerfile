FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Copy the equity-aggregator project
COPY . /app

# sync the uv environment; asserting lockfile up-to-date
RUN uv sync --locked

# Use System Python Environment by default
ENV UV_SYSTEM_PYTHON=1

# Set the environment file for equity-aggregator
ENV UV_ENV_FILE=".env"

# Persist the database and logs under the mounted /app/data volume
ENV DATA_STORE_DIR="/app/data/data_store"
ENV LOG_DIR="/app/data/logs"

# Run as a non-root user that owns /app (incl. the /app/data store and venv)
RUN groupadd --system aggregator \
    && useradd --system --gid aggregator --home-dir /app aggregator \
    && mkdir -p /app/data \
    && chown -R aggregator:aggregator /app
USER aggregator

# Aggregate and seed canonical equities database
CMD ["uv", "run", "equity-aggregator", "seed"]
