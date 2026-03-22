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
