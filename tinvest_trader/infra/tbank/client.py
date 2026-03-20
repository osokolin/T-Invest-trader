"""T-Invest (T-Bank) API client.

Market-data and order methods are still stubbed for now, while broker-event
methods use the real InstrumentsService REST endpoints.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from urllib import error as urllib_error
from urllib import request as urllib_request

from tinvest_trader.app.config import BrokerConfig

_INSTRUMENTS_SERVICE_BASE_URL = (
    "https://invest-public-api.tbank.ru/rest/"
    "tinkoff.public.invest.api.contract.v1.InstrumentsService"
)
_HTTP_TIMEOUT_SECONDS = 15.0


class TBankApiError(RuntimeError):
    """Raised when a T-Bank API request fails."""


class TBankClient:
    """T-Invest API client."""

    def __init__(self, config: BrokerConfig, logger: logging.Logger) -> None:
        self._config = config
        self._logger = logger
        self._connected = False

    def connect(self) -> None:
        """Establish connection to the broker (stub)."""
        self._logger.info(
            "broker client connected (stub)",
            extra={"component": "tbank_client"},
        )
        self._connected = True

    def health_check(self) -> bool:
        """Check broker connectivity (stub)."""
        healthy = self._connected
        self._logger.info(
            "broker health check",
            extra={"component": "tbank_client"},
        )
        return healthy

    # -- Market data methods (stubs) --

    def get_instrument(self, figi: str) -> dict:
        """Return broker-shaped instrument payload.

        Falls back to a stub when the broker token is not configured so that
        purely local/offline tests keep working without network access.
        """
        if not self._has_token():
            self._logger.info(
                "get_instrument (stub)",
                extra={"component": "tbank_client", "figi": figi},
            )
            return self._stub_instrument(figi)

        response = self._post_instruments_service(
            method_name="GetInstrumentBy",
            payload={
                "idType": "INSTRUMENT_ID_TYPE_FIGI",
                "id": figi,
            },
        )
        instrument = response.get("instrument", {})
        return {
            "figi": instrument.get("figi", figi),
            "ticker": instrument.get("ticker", ""),
            "name": instrument.get("name", ""),
            "uid": instrument.get("uid") or instrument.get("instrumentUid"),
        }

    def get_instrument_by_ticker(
        self,
        ticker: str,
        class_code: str = "TQBR",
    ) -> dict | None:
        """Look up instrument by ticker via T-Bank API.

        Returns dict with figi, ticker, name, uid, isin or None.
        Falls back to stub when token is not configured.
        """
        if not self._has_token():
            self._logger.info(
                "get_instrument_by_ticker (stub)",
                extra={"component": "tbank_client", "ticker": ticker},
            )
            return None

        try:
            response = self._post_instruments_service(
                method_name="GetInstrumentBy",
                payload={
                    "idType": "INSTRUMENT_ID_TYPE_TICKER",
                    "classCode": class_code,
                    "id": ticker,
                },
            )
        except Exception:
            self._logger.exception(
                "get_instrument_by_ticker failed",
                extra={"component": "tbank_client", "ticker": ticker},
            )
            return None

        instrument = response.get("instrument", {})
        if not instrument:
            return None
        return {
            "figi": instrument.get("figi", ""),
            "ticker": instrument.get("ticker", ticker),
            "name": instrument.get("name", ""),
            "uid": instrument.get("uid") or instrument.get("instrumentUid", ""),
            "isin": instrument.get("isin", ""),
        }

    def get_trading_status(self, figi: str) -> str:
        """Return broker-shaped trading status string (stub)."""
        self._logger.info(
            "get_trading_status (stub)",
            extra={"component": "tbank_client", "figi": figi},
        )
        return "SECURITY_TRADING_STATUS_NORMAL_TRADING"

    def get_last_price(self, figi: str) -> dict:
        """Return broker-shaped last price payload (stub)."""
        self._logger.info(
            "get_last_price (stub)",
            extra={"component": "tbank_client", "figi": figi},
        )
        return {
            "currency": "RUB",
            "units": 100,
            "nano": 500_000_000,
        }

    def get_recent_candles(self, figi: str, interval: str) -> list[dict]:
        """Return broker-shaped candle list (stub)."""
        self._logger.info(
            "get_recent_candles (stub)",
            extra={"component": "tbank_client", "figi": figi},
        )
        stub_price = {"currency": "RUB", "units": 100, "nano": 0}
        return [
            {
                "open": stub_price,
                "high": {**stub_price, "units": 101},
                "low": {**stub_price, "units": 99},
                "close": stub_price,
                "volume": 1000,
                "time": None,
            },
        ]

    # -- Structured event methods (stubs) --

    def get_dividends(
        self,
        figi: str,
        from_time: datetime,
        to_time: datetime,
    ) -> list[dict]:
        """Return broker-shaped dividend events for an instrument."""
        instrument_id = self._normalize_instrument_id(figi)
        self._logger.info(
            "get_dividends",
            extra={
                "component": "tbank_client",
                "figi": figi,
                "instrument_id": instrument_id,
                "from_time": from_time.isoformat(),
                "to_time": to_time.isoformat(),
            },
        )
        response = self._post_instruments_service(
            method_name="GetDividends",
            payload={
                "instrumentId": instrument_id,
                "from": self._format_timestamp(from_time),
                "to": self._format_timestamp(to_time),
            },
        )
        return [
            {
                "dividend_net": dividend.get("dividendNet"),
                "payment_date": dividend.get("paymentDate"),
                "declared_date": dividend.get("declaredDate"),
                "last_buy_date": dividend.get("lastBuyDate"),
                "dividend_type": dividend.get("dividendType"),
                "record_date": dividend.get("recordDate"),
                "regularity": dividend.get("regularity"),
                "close_price": dividend.get("closePrice"),
                "yield_value": dividend.get("yieldValue"),
                "created_at": dividend.get("createdAt"),
            }
            for dividend in response.get("dividends", [])
            if isinstance(dividend, dict)
        ]

    def get_asset_reports(
        self,
        instrument_uid: str,
        from_time: datetime,
        to_time: datetime,
    ) -> list[dict]:
        """Return broker-shaped asset report events for an instrument."""
        resolved_instrument_id = self._resolve_event_instrument_id(
            instrument_uid,
            prefer_uid=True,
        )
        self._logger.info(
            "get_asset_reports",
            extra={
                "component": "tbank_client",
                "instrument_uid": instrument_uid,
                "instrument_id": resolved_instrument_id,
                "from_time": from_time.isoformat(),
                "to_time": to_time.isoformat(),
            },
        )
        response = self._post_instruments_service(
            method_name="GetAssetReports",
            payload={
                "instrumentId": resolved_instrument_id,
                "from": self._format_timestamp(from_time),
                "to": self._format_timestamp(to_time),
            },
        )
        return [
            {
                "instrument_id": event.get("instrumentId"),
                "report_date": event.get("reportDate"),
                "period_year": event.get("periodYear"),
                "period_num": event.get("periodNum"),
                "period_type": event.get("periodType"),
                "created_at": event.get("createdAt"),
            }
            for event in response.get("events", [])
            if isinstance(event, dict)
        ]

    def get_insider_deals(self, instrument_uid: str, limit: int = 100) -> list[dict]:
        """Return broker-shaped insider deal events for an instrument."""
        resolved_instrument_id = self._resolve_event_instrument_id(
            instrument_uid,
            prefer_uid=True,
        )
        capped_limit = max(1, min(limit, 100))
        self._logger.info(
            "get_insider_deals",
            extra={
                "component": "tbank_client",
                "instrument_uid": instrument_uid,
                "instrument_id": resolved_instrument_id,
                "limit": capped_limit,
            },
        )
        insider_deals: list[dict] = []
        next_cursor = ""

        while True:
            payload = {
                "instrumentId": resolved_instrument_id,
                "limit": capped_limit,
            }
            if next_cursor:
                payload["nextCursor"] = next_cursor

            response = self._post_instruments_service(
                method_name="GetInsiderDeals",
                payload=payload,
            )
            raw_page = response.get("insiderDeals", [])
            insider_deals.extend(
                {
                    "trade_id": deal.get("tradeId"),
                    "direction": deal.get("direction"),
                    "currency": deal.get("currency"),
                    "date": deal.get("date"),
                    "quantity": deal.get("quantity"),
                    "price": deal.get("price"),
                    "instrument_uid": deal.get("instrumentUid"),
                    "ticker": deal.get("ticker"),
                    "investor_name": deal.get("investorName"),
                    "investor_position": deal.get("investorPosition"),
                    "percentage": deal.get("percentage"),
                    "is_option_execution": deal.get("isOptionExecution"),
                    "disclosure_date": deal.get("disclosureDate"),
                }
                for deal in raw_page
                if isinstance(deal, dict)
            )
            next_cursor = str(response.get("nextCursor") or "").strip()
            if not next_cursor or not raw_page:
                break

        return insider_deals

    # -- Order methods (stubs) --

    def post_order(
        self,
        figi: str,
        quantity: int,
        direction: str,
        order_type: str,
        idempotency_key: str,
    ) -> dict:
        """Submit an order to the broker (stub)."""
        self._logger.info(
            "post_order (stub)",
            extra={
                "component": "tbank_client",
                "figi": figi,
                "direction": direction,
                "quantity": quantity,
                "idempotency_key": idempotency_key,
            },
        )
        return {
            "order_id": f"stub-order-{idempotency_key[:8]}",
            "figi": figi,
            "direction": direction,
            "requested_quantity": quantity,
            "filled_quantity": quantity,
            "status": "EXECUTION_REPORT_STATUS_FILL",
            "message": "",
        }

    def get_order_state(self, order_id: str) -> dict:
        """Get current state of an order (stub)."""
        self._logger.info(
            "get_order_state (stub)",
            extra={"component": "tbank_client", "order_id": order_id},
        )
        return {
            "order_id": order_id,
            "figi": "UNKNOWN",
            "direction": "ORDER_DIRECTION_BUY",
            "requested_quantity": 1,
            "filled_quantity": 1,
            "status": "EXECUTION_REPORT_STATUS_FILL",
            "message": "",
        }

    def cancel_order(self, order_id: str) -> dict:
        """Cancel an order (stub)."""
        self._logger.info(
            "cancel_order (stub)",
            extra={"component": "tbank_client", "order_id": order_id},
        )
        return {
            "order_id": order_id,
            "status": "EXECUTION_REPORT_STATUS_CANCELLED",
        }

    def _has_token(self) -> bool:
        return bool(self._config.token.strip())

    def _stub_instrument(self, figi: str) -> dict:
        return {
            "figi": figi,
            "ticker": None,
            "name": "Stub Instrument",
            "uid": f"uid-{figi}",
        }

    def _normalize_instrument_id(self, value: str) -> str:
        normalized = value.strip()
        if normalized.startswith("uid-"):
            return normalized[4:]
        return normalized

    def _resolve_event_instrument_id(self, value: str, *, prefer_uid: bool) -> str:
        instrument_id = self._normalize_instrument_id(value)
        if not prefer_uid or instrument_id != value:
            instrument = self.get_instrument(instrument_id)
            resolved_uid = str(instrument.get("uid") or "").strip()
            if resolved_uid and not resolved_uid.startswith("uid-"):
                return resolved_uid
        return instrument_id

    def _format_timestamp(self, value: datetime) -> str:
        normalized = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return normalized.astimezone(UTC).isoformat().replace("+00:00", "Z")

    def _post_instruments_service(self, method_name: str, payload: dict) -> dict:
        if not self._has_token():
            raise TBankApiError(
                f"{method_name} requires TINVEST_TOKEN to be configured",
            )

        request_body = json.dumps(payload).encode("utf-8")
        request = urllib_request.Request(
            url=f"{_INSTRUMENTS_SERVICE_BASE_URL}/{method_name}",
            data=request_body,
            headers={
                "Authorization": f"Bearer {self._config.token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": self._config.app_name or "tinvest_trader",
            },
            method="POST",
        )
        try:
            with urllib_request.urlopen(
                request,
                timeout=_HTTP_TIMEOUT_SECONDS,
            ) as response:
                raw_body = response.read().decode("utf-8")
        except urllib_error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            self._logger.exception(
                "tbank instruments api http error",
                extra={
                    "component": "tbank_client",
                    "method_name": method_name,
                    "status_code": exc.code,
                    "response_body": error_body,
                },
            )
            raise TBankApiError(
                f"{method_name} failed with HTTP {exc.code}",
            ) from exc
        except urllib_error.URLError as exc:
            self._logger.exception(
                "tbank instruments api transport error",
                extra={
                    "component": "tbank_client",
                    "method_name": method_name,
                },
            )
            raise TBankApiError(f"{method_name} transport error") from exc

        try:
            parsed = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            self._logger.exception(
                "tbank instruments api invalid json response",
                extra={
                    "component": "tbank_client",
                    "method_name": method_name,
                },
            )
            raise TBankApiError(f"{method_name} returned invalid JSON") from exc

        if not isinstance(parsed, dict):
            raise TBankApiError(f"{method_name} returned unexpected response payload")
        return parsed
