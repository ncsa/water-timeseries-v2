FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    git \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /app

# Copy only pyproject.toml first
COPY pyproject.toml ./

# Extract and install h3 first (try to get a binary wheel)
RUN uv pip install --only-binary=:all: h3 || \
    (echo "Binary wheel not available, installing with cmake..." && \
     uv pip install h3 --no-binary h3)

# Now copy the lock file and sync everything else
COPY uv.lock ./

# Install all dependencies except h3 (already installed)
RUN uv sync --frozen --no-dev

# Copy the rest of the application
COPY . .

# Reinstall the local package (will reuse existing h3)
RUN uv pip install -e .

ENTRYPOINT ["uv", "run", "water-timeseries"]