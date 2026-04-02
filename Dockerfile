FROM python:3.12-slim

# Install build dependencies (minimal set needed)
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

# Install dependencies with uv - this should use pre-built wheels for h3 4.4.2
RUN uv sync --frozen --no-dev

# Copy the rest of the application
COPY . .

# Install the package itself
RUN uv pip install -e .

# Set the entrypoint
ENTRYPOINT ["uv", "run", "water-timeseries"]