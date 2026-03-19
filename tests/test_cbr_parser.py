"""Tests for CBR RSS parser -- offline, deterministic."""


from tinvest_trader.cbr.parser import (
    _classify_event_type,
    _parse_pub_date,
    _strip_html,
    normalize_item,
    parse_rss_items,
)

EVENTRSS_URL = "http://www.cbr.ru/rss/eventrss"
PRESS_URL = "http://www.cbr.ru/rss/RssPress"

SAMPLE_RSS = b"""\
<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <item>
      <title>Key rate decision</title>
      <link>https://www.cbr.ru/press/event/?id=12345</link>
      <guid isPermaLink="false">12345</guid>
      <description />
      <pubDate>Fri, 14 Mar 2025 15:00:00 +0300</pubDate>
    </item>
    <item>
      <title>Inflation report</title>
      <link>https://www.cbr.ru/press/event/?id=12346</link>
      <guid isPermaLink="false">12346</guid>
      <description>&lt;p&gt;Detailed &amp;amp; thorough report&lt;/p&gt;</description>
      <pubDate>Sat, 15 Mar 2025 10:30:00 +0300</pubDate>
    </item>
  </channel>
</rss>
"""


def test_parse_rss_items_count():
    items = parse_rss_items(SAMPLE_RSS, EVENTRSS_URL)
    assert len(items) == 2


def test_parse_rss_item_fields():
    items = parse_rss_items(SAMPLE_RSS, EVENTRSS_URL)
    item = items[0]
    assert item.title == "Key rate decision"
    assert item.link == "https://www.cbr.ru/press/event/?id=12345"
    assert item.item_uid == "12345"
    assert item.source_url == EVENTRSS_URL
    assert item.published_at is not None
    assert item.published_at.tzinfo is not None


def test_parse_rss_description_with_html():
    items = parse_rss_items(SAMPLE_RSS, EVENTRSS_URL)
    item = items[1]
    assert "&lt;" not in item.description or "<p>" in item.description


def test_parse_pub_date_valid():
    dt = _parse_pub_date("Fri, 14 Mar 2025 15:00:00 +0300")
    assert dt is not None
    assert dt.year == 2025
    assert dt.month == 3
    assert dt.day == 14
    assert dt.tzinfo is not None


def test_parse_pub_date_none():
    assert _parse_pub_date(None) is None
    assert _parse_pub_date("") is None


def test_parse_pub_date_invalid():
    assert _parse_pub_date("not a date") is None


def test_strip_html():
    assert _strip_html("<p>Hello &amp; world</p>") == "Hello & world"
    assert _strip_html("plain text") == "plain text"
    assert _strip_html("") == ""


def test_classify_event_type_eventrss():
    assert _classify_event_type("http://www.cbr.ru/rss/eventrss") == "event"


def test_classify_event_type_press():
    assert _classify_event_type("http://www.cbr.ru/rss/RssPress") == "press_release"


def test_classify_event_type_news():
    assert _classify_event_type("http://www.cbr.ru/rss/RssNews") == "news"


def test_classify_event_type_unknown():
    assert _classify_event_type("http://example.com/feed") == "rss_item"


def test_normalize_item_event():
    items = parse_rss_items(SAMPLE_RSS, EVENTRSS_URL)
    event = normalize_item(items[0])
    assert event.event_type == "event"
    assert event.title == "Key rate decision"
    assert event.event_key == "12345"
    assert event.url == "https://www.cbr.ru/press/event/?id=12345"


def test_normalize_item_press():
    items = parse_rss_items(SAMPLE_RSS, PRESS_URL)
    event = normalize_item(items[0])
    assert event.event_type == "press_release"


def test_normalize_item_html_summary():
    items = parse_rss_items(SAMPLE_RSS, PRESS_URL)
    event = normalize_item(items[1])
    assert "<p>" not in event.summary
    assert "&lt;" not in event.summary
    assert "Detailed & thorough report" in event.summary


def test_parse_empty_channel():
    xml = b"""\
<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel><title>Empty</title></channel></rss>
"""
    items = parse_rss_items(xml, EVENTRSS_URL)
    assert items == []


def test_parse_no_channel():
    xml = b'<?xml version="1.0"?><rss version="2.0"></rss>'
    items = parse_rss_items(xml, EVENTRSS_URL)
    assert items == []


def test_item_without_guid_uses_link():
    xml = b"""\
<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>No GUID</title>
    <item>
      <title>No guid item</title>
      <link>https://example.com/item/99</link>
      <description>desc</description>
      <pubDate>Mon, 10 Mar 2025 12:00:00 +0000</pubDate>
    </item>
  </channel>
</rss>
"""
    items = parse_rss_items(xml, EVENTRSS_URL)
    assert len(items) == 1
    assert items[0].item_uid == "https://example.com/item/99"


def test_item_without_guid_and_link_skipped():
    xml = b"""\
<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>Bad</title>
    <item>
      <title>No uid</title>
      <description>no guid no link</description>
    </item>
  </channel>
</rss>
"""
    items = parse_rss_items(xml, EVENTRSS_URL)
    assert items == []


def test_parse_pub_date_utc_fallback():
    """Dates without timezone info should get UTC."""
    dt = _parse_pub_date("Mon, 10 Mar 2025 12:00:00 +0000")
    assert dt is not None
    assert dt.tzinfo is not None
