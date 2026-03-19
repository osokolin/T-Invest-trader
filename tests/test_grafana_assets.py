import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GRAFANA_ROOT = REPO_ROOT / "grafana"


def test_dashboard_json_files_are_present_and_valid() -> None:
    dashboard_files = {
        "telegram-sentiment.json": "Telegram Sentiment",
        "broker-events.json": "Broker Events",
        "signal-observations.json": "Signal Observations",
        "fused-signal-features.json": "Fused Signal Features",
        "cbr-events.json": "CBR Events",
        "moex-market-history.json": "MOEX Market History",
        "combined-overview.json": "Combined Overview",
    }

    for filename, expected_title in dashboard_files.items():
        dashboard_path = GRAFANA_ROOT / "dashboards" / filename
        assert dashboard_path.exists()

        dashboard = json.loads(dashboard_path.read_text())
        assert dashboard["title"] == expected_title
        assert dashboard["uid"]
        assert dashboard["panels"]


def test_broker_events_timeseries_query_avoids_coalesce_inside_grafana_macros() -> None:
    dashboard = json.loads((GRAFANA_ROOT / "dashboards" / "broker-events.json").read_text())
    panel = next(item for item in dashboard["panels"] if item["title"] == "Broker Events by Type")
    query = panel["targets"][0]["rawSql"]

    assert "__timeGroupAlias(COALESCE(" not in query
    assert "__timeFilter(COALESCE(" not in query
    assert "COALESCE(event_time, recorded_at) AS event_ts" in query


def test_provisioning_files_reference_postgres_and_dashboards() -> None:
    datasource_path = GRAFANA_ROOT / "provisioning" / "datasources" / "postgres.yml"
    dashboards_path = GRAFANA_ROOT / "provisioning" / "dashboards" / "dashboards.yml"

    datasource_text = datasource_path.read_text()
    dashboards_text = dashboards_path.read_text()

    assert "name: Postgres" in datasource_text
    assert "uid: postgres" in datasource_text
    assert "url: postgres:5432" in datasource_text
    assert "database: tinvest" in datasource_text
    assert "jsonData:\n      database: tinvest" in datasource_text
    assert "path: /var/lib/grafana/dashboards" in dashboards_text


def test_docker_compose_includes_grafana_service() -> None:
    compose_text = (REPO_ROOT / "docker-compose.yml").read_text()

    assert "grafana:" in compose_text
    assert "grafana/grafana" in compose_text
    assert "/etc/grafana/custom-entrypoint.sh" in compose_text
    assert "./grafana/provisioning:/etc/grafana/provisioning:ro" in compose_text
    assert "./grafana/dashboards:/var/lib/grafana/dashboards:ro" in compose_text
    assert (
        "./deploy/grafana/custom-entrypoint.sh:/etc/grafana/custom-entrypoint.sh:ro"
        in compose_text
    )


def test_fused_signal_features_dashboard_has_expected_panels() -> None:
    dashboard = json.loads(
        (GRAFANA_ROOT / "dashboards" / "fused-signal-features.json").read_text(),
    )
    titles = [p["title"] for p in dashboard["panels"]]
    assert "Latest Fused Rows" in titles
    assert "Sentiment Balance Over Time by Ticker" in titles
    assert "Sentiment Count Over Time by Ticker/Window" in titles
    assert "Broker Event Counts Over Time" in titles
    assert "Event Flags (rows with broker events)" in titles


def test_fused_signal_features_queries_reference_correct_table() -> None:
    dashboard = json.loads(
        (GRAFANA_ROOT / "dashboards" / "fused-signal-features.json").read_text(),
    )
    for panel in dashboard["panels"]:
        for target in panel["targets"]:
            sql = target.get("rawSql", "")
            assert "fused_signal_features" in sql


def test_new_dashboards_reference_expected_tables() -> None:
    cbr_dashboard = json.loads((GRAFANA_ROOT / "dashboards" / "cbr-events.json").read_text())
    moex_dashboard = json.loads(
        (GRAFANA_ROOT / "dashboards" / "moex-market-history.json").read_text(),
    )
    combined_dashboard = json.loads(
        (GRAFANA_ROOT / "dashboards" / "combined-overview.json").read_text(),
    )

    cbr_queries = [
        target.get("rawSql", "")
        for panel in cbr_dashboard["panels"]
        for target in panel.get("targets", [])
    ]
    moex_queries = [
        target.get("rawSql", "")
        for panel in moex_dashboard["panels"]
        for target in panel.get("targets", [])
    ]
    combined_queries = [
        target.get("rawSql", "")
        for panel in combined_dashboard["panels"]
        for target in panel.get("targets", [])
    ]

    assert any("cbr_events" in query for query in cbr_queries)
    assert any("moex_market_history" in query for query in moex_queries)
    assert any("moex_security_reference" in query for query in moex_queries)
    assert any("fused_signal_features" in query for query in combined_queries)
    assert any("broker_event_features" in query for query in combined_queries)
    assert any("cbr_events" in query for query in combined_queries)
    assert any("moex_market_history" in query for query in combined_queries)


def test_newest_first_sorting_for_table_panels_is_present() -> None:
    dashboard_paths = [
        GRAFANA_ROOT / "dashboards" / "cbr-events.json",
        GRAFANA_ROOT / "dashboards" / "moex-market-history.json",
        GRAFANA_ROOT / "dashboards" / "combined-overview.json",
    ]

    queries = []
    for dashboard_path in dashboard_paths:
        dashboard = json.loads(dashboard_path.read_text())
        for panel in dashboard["panels"]:
            if panel["type"] != "table":
                continue
            queries.extend(target.get("rawSql", "") for target in panel.get("targets", []))

    assert any("ORDER BY published_at DESC" in query for query in queries)
    assert any("ORDER BY trade_date DESC" in query for query in queries)
    assert any("ORDER BY recorded_at DESC" in query for query in queries)


def test_grafana_custom_entrypoint_syncs_password_from_env() -> None:
    entrypoint_text = (
        REPO_ROOT / "deploy" / "grafana" / "custom-entrypoint.sh"
    ).read_text()

    assert "GRAFANA_ADMIN_PASSWORD" in entrypoint_text
    assert "GRAFANA_ADMIN_USER" in entrypoint_text
    assert "grafana cli" in entrypoint_text
    assert "reset-admin-password" in entrypoint_text
    assert "/api/users/1" in entrypoint_text
