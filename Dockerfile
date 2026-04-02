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

COPY pyproject.toml uv.lock ./

# Create a virtual environment first
RUN uv venv

# Activate the virtual environment for subsequent commands
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Now install h3
RUN uv pip install "h3>=4.0.0"

# Sync the rest of dependencies
RUN uv sync --frozen --no-dev

COPY . .

# Install the package
RUN uv pip install -e .

ENTRYPOINT ["uv", "run", "water-timeseries"]