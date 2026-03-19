FROM python:3.11-slim AS base

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy everything needed for install
COPY pyproject.toml .
COPY tinvest_trader/ tinvest_trader/
COPY deploy/certs/ /usr/local/share/ca-certificates/

RUN update-ca-certificates

# Install packaging tools explicitly, then install runtime extras used in deployment.
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir --no-build-isolation ".[telegram]"

CMD ["python", "-m", "tinvest_trader.app.main"]
