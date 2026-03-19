"""Tests for MOEX ISS parser -- offline, deterministic."""

from datetime import date

from tinvest_trader.moex.parser import (
    _iss_table_to_dicts,
    _to_float,
    _to_int,
    parse_history_cursor,
    parse_history_rows,
    parse_security_info,
)

SAMPLE_SECURITY_JSON = {
    "description": {
        "columns": ["name", "title", "value", "type", "sort_order", "is_hidden", "precision"],
        "data": [
            ["SECID", "Ticker", "SBER", "string", 1, 0, None],
            ["NAME", "Name", "Sberbank", "string", 2, 0, None],
            ["SHORTNAME", "Short name", "Sberbank AO", "string", 3, 0, None],
            ["ISIN", "ISIN", "RU0009029540", "string", 4, 0, None],
            ["REGNUMBER", "Reg number", "10301481B", "string", 5, 0, None],
            ["ISSUESIZE", "Issue size", "21586948000", "string", 6, 0, None],
            ["LISTLEVEL", "List level", "1", "string", 7, 0, None],
            ["ISQUALIFIEDINVESTORS", "Qualified", "0", "string", 8, 0, None],
            ["GROUP", "Group", "stock_shares", "string", 9, 0, None],
        ],
    },
    "boards": {
        "columns": ["secid", "boardid", "title", "is_primary", "is_traded"],
        "data": [
            ["SBER", "TQBR", "T+ Shares", 1, 1],
            ["SBER", "EQBR", "Eq Shares", 0, 1],
        ],
    },
}

SAMPLE_HISTORY_JSON = {
    "history": {
        "columns": [
            "BOARDID", "TRADEDATE", "SHORTNAME", "SECID",
            "NUMTRADES", "VALUE", "OPEN", "LOW", "HIGH",
            "LEGALCLOSEPRICE", "WAPRICE", "CLOSE", "VOLUME",
        ],
        "data": [
            [
                "TQBR", "2025-03-14", "Sberbank", "SBER",
                50000, 1500000000.0, 280.5, 278.0, 285.0,
                283.0, 282.5, 284.0, 5000000,
            ],
            [
                "TQBR", "2025-03-17", "Sberbank", "SBER",
                45000, 1400000000.0, 284.0, 282.0, 288.0,
                286.0, 285.5, 287.0, 4800000,
            ],
        ],
    },
    "history.cursor": {
        "columns": ["INDEX", "TOTAL", "PAGESIZE"],
        "data": [[0, 2, 100]],
    },
}


def test_iss_table_to_dicts():
    section = {
        "columns": ["a", "b"],
        "data": [[1, 2], [3, 4]],
    }
    result = _iss_table_to_dicts(section)
    assert result == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


def test_iss_table_to_dicts_empty():
    assert _iss_table_to_dicts({}) == []
    assert _iss_table_to_dicts({"columns": [], "data": []}) == []


def test_parse_security_info():
    info = parse_security_info(SAMPLE_SECURITY_JSON, "SBER")
    assert info is not None
    assert info.secid == "SBER"
    assert info.name == "Sberbank"
    assert info.short_name == "Sberbank AO"
    assert info.isin == "RU0009029540"
    assert info.reg_number == "10301481B"
    assert info.issue_size == 21586948000
    assert info.list_level == 1
    assert info.group == "stock_shares"
    assert info.primary_boardid == "TQBR"


def test_parse_security_info_no_description():
    assert parse_security_info({}, "SBER") is None


def test_parse_security_info_no_boards():
    data = {
        "description": {
            "columns": ["name", "title", "value"],
            "data": [["SECID", "Ticker", "TEST"]],
        },
    }
    info = parse_security_info(data, "TEST")
    assert info is not None
    assert info.primary_boardid == ""


def test_parse_history_rows():
    rows = parse_history_rows(SAMPLE_HISTORY_JSON)
    assert len(rows) == 2

    row = rows[0]
    assert row.secid == "SBER"
    assert row.boardid == "TQBR"
    assert row.trade_date == date(2025, 3, 14)
    assert row.open == 280.5
    assert row.high == 285.0
    assert row.low == 278.0
    assert row.close == 284.0
    assert row.legal_close == 283.0
    assert row.waprice == 282.5
    assert row.volume == 5000000
    assert row.value == 1500000000.0
    assert row.num_trades == 50000


def test_parse_history_rows_empty():
    assert parse_history_rows({}) == []
    assert parse_history_rows({"history": {"columns": [], "data": []}}) == []


def test_parse_history_rows_bad_date():
    data = {
        "history": {
            "columns": ["BOARDID", "TRADEDATE", "SECID"],
            "data": [["TQBR", "not-a-date", "SBER"]],
        },
    }
    assert parse_history_rows(data) == []


def test_parse_history_rows_missing_secid():
    data = {
        "history": {
            "columns": ["BOARDID", "TRADEDATE"],
            "data": [["TQBR", "2025-03-14"]],
        },
    }
    assert parse_history_rows(data) == []


def test_parse_history_cursor():
    index, total, pagesize = parse_history_cursor(SAMPLE_HISTORY_JSON)
    assert index == 0
    assert total == 2
    assert pagesize == 100


def test_parse_history_cursor_missing():
    assert parse_history_cursor({}) == (0, 0, 0)


def test_parse_history_cursor_empty():
    data = {"history.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"], "data": []}}
    assert parse_history_cursor(data) == (0, 0, 0)


def test_to_float():
    assert _to_float(1.5) == 1.5
    assert _to_float(None) is None
    assert _to_float("abc") is None
    assert _to_float("3.14") == 3.14


def test_to_int():
    assert _to_int(42) == 42
    assert _to_int(None) is None
    assert _to_int("abc") is None
    assert _to_int("100") == 100


def test_parse_history_rows_null_prices():
    data = {
        "history": {
            "columns": [
                "BOARDID", "TRADEDATE", "SECID",
                "OPEN", "HIGH", "LOW", "CLOSE",
                "LEGALCLOSEPRICE", "WAPRICE", "VOLUME", "VALUE", "NUMTRADES",
            ],
            "data": [
                ["TQBR", "2025-03-14", "SBER",
                 None, None, None, None,
                 None, None, None, None, None],
            ],
        },
    }
    rows = parse_history_rows(data)
    assert len(rows) == 1
    assert rows[0].open is None
    assert rows[0].high is None
    assert rows[0].volume is None
