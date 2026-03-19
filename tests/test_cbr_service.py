"""Tests for CBR ingestion service -- offline, mocked."""

import logging
from unittest.mock import MagicMock, patch

from tinvest_trader.services.cbr_ingestion_service import CbrIngestionService

EVENTRSS_URL = "http://www.cbr.ru/rss/eventrss"

SAMPLE_RSS = b"""\
<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>Test</title>
    <item>
      <title>Event A</title>
      <link>https://www.cbr.ru/press/event/?id=100</link>
      <guid isPermaLink="false">100</guid>
      <description />
      <pubDate>Fri, 14 Mar 2025 15:00:00 +0300</pubDate>
    </item>
    <item>
      <title>Event B</title>
      <link>https://www.cbr.ru/press/event/?id=101</link>
      <guid isPermaLink="false">101</guid>
      <description>Some text</description>
      <pubDate>Fri, 14 Mar 2025 16:00:00 +0300</pubDate>
    </item>
  </channel>
</rss>
"""


def _make_service(store_raw=True):
    repo = MagicMock()
    logger = logging.getLogger("test_cbr")
    svc = CbrIngestionService(
        repository=repo,
        logger=logger,
        rss_urls=(EVENTRSS_URL,),
        store_raw_payloads=store_raw,
    )
    return svc, repo


@patch("tinvest_trader.services.cbr_ingestion_service.fetch_rss")
def test_ingest_feed_persists_raw_and_events(mock_fetch):
    mock_fetch.return_value = SAMPLE_RSS
    svc, repo = _make_service()
    repo.insert_cbr_feed_raw.return_value = True
    repo.insert_cbr_event.return_value = True

    count = svc.ingest_feed(EVENTRSS_URL)
    assert count == 2
    assert repo.insert_cbr_feed_raw.call_count == 2
    assert repo.insert_cbr_event.call_count == 2


@patch("tinvest_trader.services.cbr_ingestion_service.fetch_rss")
def test_ingest_feed_skips_duplicates(mock_fetch):
    mock_fetch.return_value = SAMPLE_RSS
    svc, repo = _make_service()
    # First item: already seen (insert returns False)
    # Second item: new
    repo.insert_cbr_feed_raw.side_effect = [False, True]
    repo.insert_cbr_event.return_value = True

    count = svc.ingest_feed(EVENTRSS_URL)
    assert count == 1
    assert repo.insert_cbr_event.call_count == 1


@patch("tinvest_trader.services.cbr_ingestion_service.fetch_rss")
def test_ingest_feed_no_store_raw(mock_fetch):
    mock_fetch.return_value = SAMPLE_RSS
    svc, repo = _make_service(store_raw=False)
    repo.cbr_event_exists.return_value = False
    repo.insert_cbr_event.return_value = True

    count = svc.ingest_feed(EVENTRSS_URL)
    assert count == 2
    repo.insert_cbr_feed_raw.assert_not_called()
    assert repo.cbr_event_exists.call_count == 2


@patch("tinvest_trader.services.cbr_ingestion_service.fetch_rss")
def test_ingest_feed_no_store_raw_skips_existing(mock_fetch):
    mock_fetch.return_value = SAMPLE_RSS
    svc, repo = _make_service(store_raw=False)
    repo.cbr_event_exists.side_effect = [True, False]
    repo.insert_cbr_event.return_value = True

    count = svc.ingest_feed(EVENTRSS_URL)
    assert count == 1


@patch("tinvest_trader.services.cbr_ingestion_service.fetch_rss")
def test_ingest_feed_fetch_failure(mock_fetch):
    mock_fetch.return_value = None
    svc, repo = _make_service()

    count = svc.ingest_feed(EVENTRSS_URL)
    assert count == 0
    repo.insert_cbr_feed_raw.assert_not_called()


@patch("tinvest_trader.services.cbr_ingestion_service.fetch_rss")
def test_ingest_feed_parse_failure(mock_fetch):
    mock_fetch.return_value = b"not xml"
    svc, repo = _make_service()

    count = svc.ingest_feed(EVENTRSS_URL)
    assert count == 0


def test_ingest_all_no_repository():
    logger = logging.getLogger("test_cbr")
    svc = CbrIngestionService(
        repository=None,
        logger=logger,
        rss_urls=(EVENTRSS_URL,),
    )
    count = svc.ingest_all()
    assert count == 0


@patch("tinvest_trader.services.cbr_ingestion_service.fetch_rss")
def test_ingest_all_multiple_feeds(mock_fetch):
    mock_fetch.return_value = SAMPLE_RSS
    repo = MagicMock()
    logger = logging.getLogger("test_cbr")
    svc = CbrIngestionService(
        repository=repo,
        logger=logger,
        rss_urls=(EVENTRSS_URL, "http://www.cbr.ru/rss/RssPress"),
    )
    repo.insert_cbr_feed_raw.return_value = True
    repo.insert_cbr_event.return_value = True

    count = svc.ingest_all()
    assert count == 4  # 2 items x 2 feeds


@patch("tinvest_trader.services.cbr_ingestion_service.fetch_rss")
def test_ingest_feed_event_persist_error_continues(mock_fetch):
    """One event persist failure should not block other items."""
    mock_fetch.return_value = SAMPLE_RSS
    svc, repo = _make_service()
    repo.insert_cbr_feed_raw.return_value = True
    repo.insert_cbr_event.side_effect = [RuntimeError("db error"), True]

    count = svc.ingest_feed(EVENTRSS_URL)
    assert count == 1  # second item succeeded
