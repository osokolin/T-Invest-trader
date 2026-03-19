"""MOEX ISS response parser -- converts ISS JSON into domain models."""

from __future__ import annotations

from datetime import date

from tinvest_trader.moex.models import MoexHistoryRow, MoexSecurityInfo


def _iss_table_to_dicts(section: dict) -> list[dict]:
    """Convert ISS columns+data format to list of dicts."""
    columns = section.get("columns", [])
    rows = section.get("data", [])
    return [dict(zip(columns, row, strict=False)) for row in rows]


def parse_security_info(data: dict, secid: str) -> MoexSecurityInfo | None:
    """Parse /iss/securities/{secid}.json into MoexSecurityInfo."""
    desc_section = data.get("description")
    if not desc_section:
        return None

    desc_rows = _iss_table_to_dicts(desc_section)
    desc_map: dict[str, str] = {}
    for row in desc_rows:
        name = row.get("name", "")
        value = row.get("value", "")
        if name:
            desc_map[name] = value or ""

    boards_section = data.get("boards")
    primary_boardid = ""
    if boards_section:
        board_rows = _iss_table_to_dicts(boards_section)
        for br in board_rows:
            if br.get("is_primary") == 1:
                primary_boardid = br.get("boardid", "")
                break

    issue_size_str = desc_map.get("ISSUESIZE", "")
    list_level_str = desc_map.get("LISTLEVEL", "")

    return MoexSecurityInfo(
        secid=secid,
        name=desc_map.get("NAME", ""),
        short_name=desc_map.get("SHORTNAME", ""),
        isin=desc_map.get("ISIN", ""),
        reg_number=desc_map.get("REGNUMBER", ""),
        list_level=int(list_level_str) if list_level_str else None,
        issuer=desc_map.get("ISQUALIFIEDINVESTORS", ""),
        issue_size=int(issue_size_str) if issue_size_str else None,
        group=desc_map.get("GROUP", ""),
        primary_boardid=primary_boardid,
        raw_description=desc_map,
    )


def parse_history_rows(data: dict) -> list[MoexHistoryRow]:
    """Parse /iss/history/.../securities/{secid}.json history section."""
    history_section = data.get("history")
    if not history_section:
        return []

    rows = _iss_table_to_dicts(history_section)
    result: list[MoexHistoryRow] = []
    for row in rows:
        secid = row.get("SECID", "")
        trade_date_str = row.get("TRADEDATE", "")
        if not secid or not trade_date_str:
            continue

        try:
            trade_date = date.fromisoformat(trade_date_str)
        except ValueError:
            continue

        result.append(MoexHistoryRow(
            secid=secid,
            boardid=row.get("BOARDID", ""),
            trade_date=trade_date,
            open=_to_float(row.get("OPEN")),
            high=_to_float(row.get("HIGH")),
            low=_to_float(row.get("LOW")),
            close=_to_float(row.get("CLOSE")),
            legal_close=_to_float(row.get("LEGALCLOSEPRICE")),
            waprice=_to_float(row.get("WAPRICE")),
            volume=_to_int(row.get("VOLUME")),
            value=_to_float(row.get("VALUE")),
            num_trades=_to_int(row.get("NUMTRADES")),
        ))

    return result


def parse_history_cursor(data: dict) -> tuple[int, int, int]:
    """Parse history.cursor section for pagination.

    Returns (index, total, pagesize).
    """
    cursor_section = data.get("history.cursor")
    if not cursor_section:
        return (0, 0, 0)

    cursor_rows = _iss_table_to_dicts(cursor_section)
    if not cursor_rows:
        return (0, 0, 0)

    c = cursor_rows[0]
    return (
        int(c.get("INDEX", 0)),
        int(c.get("TOTAL", 0)),
        int(c.get("PAGESIZE", 0)),
    )


def _to_float(val: object) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _to_int(val: object) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None
