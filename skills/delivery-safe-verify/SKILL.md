---
name: delivery-safe-verify
description: Verify Telegram delivery behavior in T-Invest-trader without weakening duplicate-send protections. Use when message formatting changes, delivery dedup logic changes, callback handling is touched, or a bug may resend, skip, or misformat pending signals.
---

# Delivery Safe Verify

Protect transport-only delivery behavior and duplicate-send safety.

## Workflow

1. Identify whether the issue is transport, formatting, or callback handling.
   Do not start by editing signal creation code.
2. Read delivery code in this order:
   `tinvest_trader/services/signal_delivery.py`,
   `tinvest_trader/services/signal_delivery_dedup.py`,
   `tinvest_trader/services/telegram_bot_handler.py`.
3. Check duplicate-send risk before any other optimization.
   In ambiguous state, bias toward not sending.
4. Keep formatting side effects separate from delivery state.
   Formatting may read stats and AI context, but it should not change decision
   semantics.
5. Prefer preview and dry verification before live transport checks.
6. If a live send is required, confirm config and reduce blast radius.

## Key Files

- `tinvest_trader/services/signal_delivery.py`
- `tinvest_trader/services/signal_delivery_dedup.py`
- `tinvest_trader/services/telegram_bot_handler.py`
- `tinvest_trader/services/signal_severity.py`

## Guardrails

- Keep delivery transport-only.
- Preserve delivery-state discipline and dedup.
- Do not change calibration or gating semantics while fixing delivery.
- Treat callback handling as bot interaction, not as a new decision layer.

## Verification

```bash
./.venv/bin/pytest -q tests/test_signal_delivery.py tests/test_signal_delivery_dedup.py tests/test_bot_inline_nav.py tests/test_bot_commands.py
python -m tinvest_trader.cli preview-signal-message SBER --direction up --confidence 0.65
python -m tinvest_trader.cli deliver-signals
```

Run `deliver-signals` only when Telegram credentials and database state are
configured and you intend to exercise live transport.
