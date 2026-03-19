import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GRAFANA_ROOT = REPO_ROOT / "grafana"


def test_dashboard_json_files_are_present_and_valid() -> None:
    dashboard_files = {
        "telegram-sentiment.json": "Telegram Sentiment",
        "broker-events.json": "Broker Events",
        "signal-observations.json": "Signal Observations",
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


def test_grafana_custom_entrypoint_syncs_password_from_env() -> None:
    entrypoint_text = (
        REPO_ROOT / "deploy" / "grafana" / "custom-entrypoint.sh"
    ).read_text()

    assert "GRAFANA_ADMIN_PASSWORD" in entrypoint_text
    assert "GRAFANA_ADMIN_USER" in entrypoint_text
    assert "grafana cli" in entrypoint_text
    assert "reset-admin-password" in entrypoint_text
    assert "/api/users/1" in entrypoint_text
