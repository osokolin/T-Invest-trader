"""Tests for macro tagging and mapping services."""

from __future__ import annotations

from tinvest_trader.services.macro_mapping import (
    MARKET_TO_SECTOR,
    MARKET_TO_TICKERS,
    get_affected_tickers,
    get_sectors,
)
from tinvest_trader.services.macro_tagging import tag_macro_message

# ── tag_macro_message ──


class TestTagMacroMessage:
    def test_empty_text_returns_empty(self):
        assert tag_macro_message("") == []

    def test_none_text_returns_empty(self):
        assert tag_macro_message(None) == []

    def test_no_macro_content(self):
        assert tag_macro_message("SBER акции растут, покупаем") == []

    def test_oil_russian(self):
        tags = tag_macro_message("Цена на нефть Brent упала на 5%")
        assert "oil" in tags

    def test_oil_english(self):
        tags = tag_macro_message("Brent crude oil drops sharply")
        assert "oil" in tags

    def test_opec(self):
        tags = tag_macro_message("ОПЕК+ увеличивает добычу")
        assert "oil" in tags

    def test_gas_russian(self):
        tags = tag_macro_message("Газпром наращивает поставки газа в Китай")
        assert "gas" in tags

    def test_lng(self):
        tags = tag_macro_message("Экспорт СПГ растёт")
        assert "lng" in tags

    def test_risk(self):
        tags = tag_macro_message("Рынки в панике, обвал продолжается")
        assert "risk" in tags

    def test_geopolitics(self):
        tags = tag_macro_message("Новые санкции против России")
        assert "geopolitics" in tags

    def test_inflation(self):
        tags = tag_macro_message("Инфляция в РФ ускорилась до 7.5%")
        assert "inflation" in tags

    def test_rates(self):
        tags = tag_macro_message("ЦБ поднял ключевую ставку до 16%")
        assert "rates" in tags

    def test_macro(self):
        tags = tag_macro_message("Рост ВВП замедлился до 1.2%")
        assert "macro" in tags

    def test_multiple_tags(self):
        tags = tag_macro_message(
            "Кризис на рынке нефти, ОПЕК обсуждает санкции"
        )
        assert "oil" in tags
        assert "risk" in tags
        assert "geopolitics" in tags

    def test_case_insensitive(self):
        tags = tag_macro_message("BRENT УПАЛ")
        assert "oil" in tags

    def test_tags_are_sorted(self):
        tags = tag_macro_message("кризис нефть санкции")
        assert tags == sorted(tags)

    def test_no_false_positive_on_short_gas(self):
        # "газ" requires trailing space to avoid matching "газета"
        tags = tag_macro_message("газета вышла сегодня")
        assert "gas" not in tags

    def test_gas_with_trailing_space(self):
        tags = tag_macro_message("цена на газ выросла")
        assert "gas" in tags

    def test_recession(self):
        tags = tag_macro_message("рецессия в Европе")
        assert "risk" in tags


# ── Mapping ──


class TestMacroMapping:
    def test_all_tags_have_tickers(self):
        """Every tag in TAG_KEYWORDS should have a ticker mapping."""
        from tinvest_trader.services.macro_tagging import _TAG_KEYWORDS

        for tag in _TAG_KEYWORDS:
            assert tag in MARKET_TO_TICKERS, f"missing tickers for tag: {tag}"

    def test_all_tags_have_sectors(self):
        """Every tag in TAG_KEYWORDS should have a sector mapping."""
        from tinvest_trader.services.macro_tagging import _TAG_KEYWORDS

        for tag in _TAG_KEYWORDS:
            assert tag in MARKET_TO_SECTOR, f"missing sector for tag: {tag}"

    def test_get_affected_tickers_oil(self):
        tickers = get_affected_tickers(["oil"])
        assert "LKOH" in tickers
        assert "ROSN" in tickers

    def test_get_affected_tickers_multiple(self):
        tickers = get_affected_tickers(["oil", "gas"])
        assert "LKOH" in tickers
        assert "GAZP" in tickers

    def test_get_affected_tickers_deduplicates(self):
        tickers = get_affected_tickers(["macro", "risk"])
        # Both macro and risk include SBER, VTBR
        assert tickers == sorted(set(tickers))

    def test_get_affected_tickers_empty(self):
        assert get_affected_tickers([]) == []

    def test_get_affected_tickers_unknown_tag(self):
        assert get_affected_tickers(["unknown"]) == []

    def test_get_sectors_oil(self):
        sectors = get_sectors(["oil"])
        assert "energy" in sectors

    def test_get_sectors_multiple(self):
        sectors = get_sectors(["oil", "rates"])
        assert "energy" in sectors
        assert "financials" in sectors

    def test_get_sectors_deduplicates(self):
        sectors = get_sectors(["gas", "lng"])
        # Both map to "gas" sector
        assert sectors.count("gas") == 1

    def test_get_sectors_empty(self):
        assert get_sectors([]) == []
