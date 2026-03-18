FROM python:3.11-slim AS base

WORKDIR /app

# Copy everything needed for install
COPY pyproject.toml .
COPY tinvest_trader/ tinvest_trader/

# Install the package and its dependencies
RUN pip install --no-cache-dir .

CMD ["python", "-m", "tinvest_trader.app.main"]
