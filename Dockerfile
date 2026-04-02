FROM python:3.12-slim

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    curl \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /app

# First, try to install h3 from a pre-built wheel
RUN uv pip install --only-binary h3 "h3>=4.0.0"

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install remaining dependencies, skipping h3 (already installed)
RUN uv sync --frozen --no-dev

# Copy the rest of the application
COPY . .

# Install the package itself (will reuse existing h3)
RUN uv pip install -e . --no-deps

ENTRYPOINT ["uv", "run", "water-timeseries"]