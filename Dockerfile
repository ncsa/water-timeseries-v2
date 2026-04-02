FROM condaforge/mambaforge:latest

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install h3 and other geospatial packages via conda
RUN mamba install -c conda-forge -y \
    python=3.12 \
    h3 \
    h3-pandas \
    geopandas \
    xarray \
    && mamba clean -afy

# Install uv and sync remaining dependencies
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY . .
RUN uv pip install -e .

ENTRYPOINT ["uv", "run", "water-timeseries"]