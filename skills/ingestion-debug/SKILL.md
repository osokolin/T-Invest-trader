---
name: ingestion-debug
description: Debug source-ingestion lag, missing records, duplicate suppression, or source-specific failures in T-Invest-trader. Use when Telegram, broker events, MOEX, CBR, global context, quote sync, or global market data ingestion stops advancing, inserts fewer rows than expected, or behaves non-idempotently.
---

# Ingestion Debug

Trace ingestion problems without leaking decision or delivery logic into the
source layer.

## Workflow

1. Identify the failing source and the symptom.
   Symptoms usually fall into one of four buckets: no new rows, too few rows,
   duplicate rows, or a source-specific exception.
2. Confirm how the source is wired.
   Inspect `tinvest_trader/services/background_runner.py`,
   `tinvest_trader/app/container.py`, and the relevant CLI entrypoints before
   changing code.
3. Open only the source service and its nearest tests.
   Prefer the narrowest service file plus matching repository or adapter code.
4. Check idempotency and dedup before changing parsing logic.
   In this repo, duplicate behavior is usually more dangerous than underfetch.
5. Keep fixes inside ingestion boundaries.
   Do not solve ingestion symptoms by changing signal generation, delivery, or
   operator alerting.
6. Verify with the smallest source-specific command or test set.

## Source Map

- Telegram sentiment:
  `tinvest_trader/services/telegram_sentiment_service.py`
  `tests/test_sentiment_service.py`
- Broker events:
  `tinvest_trader/services/broker_event_ingestion_service.py`
  `tests/test_broker_event_service.py`
  `tests/test_broker_event_ticker_alignment.py`
- Quote sync:
  `tinvest_trader/services/quote_sync.py`
  `tests/test_quote_sync.py`
- Global context:
  `tinvest_trader/services/global_context_ingestion.py`
  `tests/test_global_context.py`

## Guardrails

- Preserve source-specific failure isolation.
- Preserve raw-to-normalized traceability.
- Prefer partial success over broad failure.
- Treat duplicate creation as a higher-severity regression than a skipped row
  caused by a safe guardrail.

## Verification

Use only the commands relevant to the affected source.

```bash
./.venv/bin/pytest -q tests/test_sentiment_service.py tests/test_broker_event_service.py tests/test_quote_sync.py tests/test_global_context.py
python -m tinvest_trader.cli telegram-ingest-status
python -m tinvest_trader.cli broker-fetch-policy-status
python -m tinvest_trader.cli ingest-telegram --limit-per-source 5
```

Run one-shot CLI commands only when the corresponding source and database
configuration are available.
