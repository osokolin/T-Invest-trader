# PROJECT_CONTEXT.md

## Overview

This project implements an automated trading system using T-Invest API.

The goal is to build a **reliable execution engine** for algorithmic trading on traditional financial markets, starting from sandbox testing and gradually moving to controlled production deployment.

The system is designed with a strong focus on:
- correctness of order execution
- state consistency
- risk management
- observability
- extensibility for future strategies

---

## Objectives

Primary objectives:

1. Build a **robust trading execution system**
2. Ensure **safe interaction with broker API**
3. Implement **strict risk controls**
4. Enable **strategy experimentation without breaking core system**
5. Support **incremental rollout: paper → sandbox → production**

Non-goals (for MVP):
- high-frequency trading
- complex multi-asset portfolio optimization
- latency-sensitive arbitrage

---

## System Architecture

The system is modular and layered.

### High-level components

- Market Data Layer
- Strategy Engine
- Risk Management
- Execution Engine
- Portfolio State
- Reconciliation Engine
- Infrastructure Layer
- Journaling & Observability

---

## Architecture Principles

See [SYSTEM_GUARDRAILS.md](SYSTEM_GUARDRAILS.md) for strict pipeline boundaries,
shadow-first development rules, and the PR checklist.

---

## Core Components

### 1. Market Data
Responsible for instrument metadata, prices, candles, order book, trading status.

### 2. Strategy Engine
Pure decision-making. Outputs BUY / SELL / HOLD.

### 3. Risk Management
Enforces limits and safety rules.

### 4. Execution Engine
Handles order lifecycle and API interaction.

### 5. Portfolio State
Local representation of positions and PnL.

### 6. Reconciliation Engine
Syncs local state with broker.

### 7. Infrastructure Layer
API client, storage, logging.

### 8. Journaling & Observability
Structured logging of all events.

### 9. Shadow Market Activity Research
T-Bank minute candles are persisted as activity observations and explainable
volume/price spikes. A separate outcome resolver compares momentum and
reversion at configurable horizons using stored candles only. This research
path cannot create signals, positions, order intents, or broker orders.
Observations remain auditable outside market hours, while spike generation is
restricted to a configured Moscow trading session by default.

### 10. Activity Paper Strategy
Isolated virtual portfolios compare momentum and reversion after qualified
market-activity spikes. Optional confirmed-volume arms test pure-volume spikes only
after the following closed candle confirms direction. Capital, exposure,
cooldown, commission, and slippage are simulated locally. Activity-paper
records never represent broker positions and cannot reach the execution engine.
Unresolvable virtual positions expire without fabricated prices or PnL so that
missing candle windows cannot permanently consume paper capacity.
A stricter v2 arm adds score, volume-ratio, confirmation-move, cooldown, and
daily-entry gates without changing the original v1 control group.
An optional strict-entry profile for momentum and confirmed-volume v2 requires
same-direction closed-minute confirmation, limits overextended moves and entry
age, and caps daily turnover. CLI and Grafana separate legacy/strict entries and
virtual long/short outcomes; original confirmed-volume v1 and reversion remain
unchanged.

### 11. Medium-Term Paper Strategy
Stored MOEX daily bars drive three isolated long-only virtual portfolios:
staircase trailing stops, ATR trailing stops, and a hybrid breakeven/ATR arm.
Signals use completed daily bars and virtual entries use the next available
daily open. Risk-based sizing, exposure limits, costs, gap-aware stops, and a
maximum holding period are simulated locally. These records cannot reach the
execution engine or represent broker positions or stop orders.
Historical replay uses the same pure signal and stop-policy functions, persists
immutable research runs separately, and compares daily net-liquidation equity
against an equal-weight total-return benchmark. Persisted MOEX split ratios
normalize historical OHLCV, while persisted T-Bank RUB dividends are credited
only to positions held on the entitlement session. Replay remains network-free.

---

## Environments

- Paper Trading
- Sandbox
- Production

---

## Trading Flow

1. Fetch market data
2. Generate signal
3. Apply risk checks
4. Execute order
5. Update portfolio
6. Reconcile state

---

## Critical Requirements

- Idempotency
- Rate limiting
- State consistency

---

## Risk Controls

- max position size
- max order size
- max daily loss
- max trades per session
- kill switch

---

## Milestones

- Skeleton
- Market Data
- Paper Trading
- Sandbox Execution
- Protective Orders
- Production Launch

---

## Key Principle

Correctness and safety over strategy complexity.
