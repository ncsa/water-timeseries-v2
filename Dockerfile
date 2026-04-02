FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    curl \
    cmake \
    git \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Copy all files needed for package metadata first
COPY pyproject.toml uv.lock README.md ./

# Create and activate virtual environment
RUN uv venv
ENV PATH="/app/.venv/bin:$PATH"

# Install h3 first
RUN uv pip install "h3>=4.0.0"

# Sync remaining dependencies
RUN uv sync --frozen --no-dev

# Copy the rest of the application
COPY . .

# Install the package in editable mode
RUN uv pip install -e .

# Keep the venv active for runtime
ENV PATH="/app/.venv/bin:$PATH"
ENV VIRTUAL_ENV=/app/.venv

ENTRYPOINT ["uv", "run", "water-timeseries"]