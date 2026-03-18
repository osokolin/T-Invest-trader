FROM python:3.11-slim AS base

WORKDIR /app

# Copy everything needed for install
COPY pyproject.toml .
COPY tinvest_trader/ tinvest_trader/

# Install packaging tools explicitly, then install runtime extras used in deployment.
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir --no-build-isolation ".[telegram]"

CMD ["python", "-m", "tinvest_trader.app.main"]
