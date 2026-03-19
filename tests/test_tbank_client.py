import io
import json
import logging
from datetime import datetime
from urllib.error import HTTPError

import pytest

from tinvest_trader.app.config import BrokerConfig
from tinvest_trader.infra.tbank.client import TBankApiError, TBankClient


class _FakeHttpResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> "_FakeHttpResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


def _make_client(token: str = "token") -> TBankClient:
    return TBankClient(
        config=BrokerConfig(token=token, app_name="tinvest-tests"),
        logger=logging.getLogger("test"),
    )


def test_get_instrument_falls_back_to_stub_without_token():
    client = _make_client(token="")

    result = client.get_instrument("BBG000B9XRY4")

    assert result == {
        "figi": "BBG000B9XRY4",
        "ticker": None,
        "name": "Stub Instrument",
        "uid": "uid-BBG000B9XRY4",
    }


def test_get_dividends_uses_real_api_and_maps_response(monkeypatch):
    seen_requests: list[dict] = []

    def fake_urlopen(request, timeout):  # noqa: ANN001
        seen_requests.append(
            {
                "url": request.full_url,
                "timeout": timeout,
                "payload": json.loads(request.data.decode("utf-8")),
                "headers": dict(request.header_items()),
            },
        )
        return _FakeHttpResponse(
            {
                "dividends": [
                    {
                        "dividendNet": {"currency": "RUB", "units": "10", "nano": 0},
                        "paymentDate": "2026-03-20T00:00:00Z",
                        "declaredDate": "2026-03-10T00:00:00Z",
                        "lastBuyDate": "2026-03-14T00:00:00Z",
                        "dividendType": "Regular Cash",
                        "recordDate": "2026-03-15T00:00:00Z",
                        "regularity": "Quarterly",
                        "closePrice": {"currency": "RUB", "units": "125", "nano": 0},
                        "yieldValue": {"units": "8", "nano": 0},
                        "createdAt": "2026-03-10T12:00:00Z",
                    },
                ],
            },
        )

    monkeypatch.setattr("tinvest_trader.infra.tbank.client.urllib_request.urlopen", fake_urlopen)
    client = _make_client()

    result = client.get_dividends(
        figi="BBG000B9XRY4",
        from_time=datetime(2026, 3, 1),
        to_time=datetime(2026, 3, 31),
    )

    assert len(result) == 1
    assert result[0]["dividend_net"]["currency"] == "RUB"
    assert result[0]["payment_date"] == "2026-03-20T00:00:00Z"
    assert seen_requests[0]["url"].endswith("/GetDividends")
    assert seen_requests[0]["payload"]["instrumentId"] == "BBG000B9XRY4"


def test_get_asset_reports_resolves_synthetic_stub_uid(monkeypatch):
    seen_payloads: list[dict] = []

    def fake_urlopen(request, timeout):  # noqa: ANN001
        payload = json.loads(request.data.decode("utf-8"))
        seen_payloads.append({"url": request.full_url, "payload": payload, "timeout": timeout})
        if request.full_url.endswith("/GetInstrumentBy"):
            return _FakeHttpResponse(
                {
                    "instrument": {
                        "figi": "BBG000B9XRY4",
                        "ticker": "SBER",
                        "name": "Sberbank",
                        "uid": "real-uid-1",
                    },
                },
            )
        return _FakeHttpResponse(
            {
                "events": [
                    {
                        "instrumentId": "real-uid-1",
                        "reportDate": "2026-03-18T00:00:00Z",
                        "periodYear": 2025,
                        "periodNum": 4,
                        "periodType": "PERIOD_TYPE_QUARTER",
                        "createdAt": "2026-03-01T10:00:00Z",
                    },
                ],
            },
        )

    monkeypatch.setattr("tinvest_trader.infra.tbank.client.urllib_request.urlopen", fake_urlopen)
    client = _make_client()

    result = client.get_asset_reports(
        instrument_uid="uid-BBG000B9XRY4",
        from_time=datetime(2026, 3, 1),
        to_time=datetime(2026, 3, 31),
    )

    assert len(result) == 1
    assert result[0]["instrument_id"] == "real-uid-1"
    assert seen_payloads[0]["url"].endswith("/GetInstrumentBy")
    assert seen_payloads[0]["payload"] == {
        "idType": "INSTRUMENT_ID_TYPE_FIGI",
        "id": "BBG000B9XRY4",
    }
    assert seen_payloads[1]["url"].endswith("/GetAssetReports")
    assert seen_payloads[1]["payload"]["instrumentId"] == "real-uid-1"


def test_get_insider_deals_follows_pagination(monkeypatch):
    seen_payloads: list[dict] = []

    def fake_urlopen(request, timeout):  # noqa: ANN001
        payload = json.loads(request.data.decode("utf-8"))
        seen_payloads.append(payload)
        if len(seen_payloads) == 1:
            return _FakeHttpResponse(
                {
                    "insiderDeals": [
                        {
                            "tradeId": "101",
                            "direction": "TRADE_DIRECTION_BUY",
                            "currency": "RUB",
                            "date": "2026-03-17T00:00:00Z",
                            "quantity": "10",
                            "price": {"units": "120", "nano": 0},
                            "instrumentUid": "real-uid-1",
                            "ticker": "SBER",
                            "investorName": "Investor A",
                            "investorPosition": "Director",
                            "percentage": 0.02,
                            "isOptionExecution": False,
                            "disclosureDate": "2026-03-18T00:00:00Z",
                        },
                    ],
                    "nextCursor": "cursor-2",
                },
            )
        return _FakeHttpResponse(
            {
                "insiderDeals": [
                    {
                        "tradeId": "102",
                        "direction": "TRADE_DIRECTION_SELL",
                        "currency": "RUB",
                        "date": "2026-03-19T00:00:00Z",
                        "quantity": "12",
                        "price": {"units": "121", "nano": 500000000},
                        "instrumentUid": "real-uid-1",
                        "ticker": "SBER",
                        "investorName": "Investor B",
                        "investorPosition": "CEO",
                        "percentage": 0.03,
                        "isOptionExecution": False,
                        "disclosureDate": "2026-03-20T00:00:00Z",
                    },
                ],
                "nextCursor": "",
            },
        )

    monkeypatch.setattr("tinvest_trader.infra.tbank.client.urllib_request.urlopen", fake_urlopen)
    client = _make_client()

    result = client.get_insider_deals(instrument_uid="real-uid-1", limit=100)

    assert [row["trade_id"] for row in result] == ["101", "102"]
    assert seen_payloads[0] == {"instrumentId": "real-uid-1", "limit": 100}
    assert seen_payloads[1] == {
        "instrumentId": "real-uid-1",
        "limit": 100,
        "nextCursor": "cursor-2",
    }


def test_get_dividends_raises_clear_error_on_http_failure(monkeypatch):
    def fake_urlopen(request, timeout):  # noqa: ANN001
        raise HTTPError(
            request.full_url,
            401,
            "Unauthorized",
            hdrs=None,
            fp=io.BytesIO(b'{"message":"unauthorized"}'),
        )

    monkeypatch.setattr("tinvest_trader.infra.tbank.client.urllib_request.urlopen", fake_urlopen)
    client = _make_client()

    with pytest.raises(TBankApiError, match="GetDividends failed with HTTP 401"):
        client.get_dividends(
            figi="BBG000B9XRY4",
            from_time=datetime(2026, 3, 1),
            to_time=datetime(2026, 3, 31),
        )
