FROM python:3.12-slim

# Install system dependencies including cmake (as fallback)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Try to install with binary wheels first
RUN uv sync --frozen --no-dev --only-binary h3 || \
    (echo "Binary install failed, retrying without restrictions..." && \
     uv sync --frozen --no-dev)

# Copy the rest of the application
COPY . .

# Install the package
RUN uv pip install -e .

ENTRYPOINT ["uv", "run", "water-timeseries"]