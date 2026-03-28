import base64
import json
import sys
from pathlib import Path
from typing import Any, Optional, cast
from urllib.parse import quote
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

sys.path.append(str(Path(__file__).parent.parent))

from app.core.settings import get_settings


def _normalize_database_url(database_url: str) -> str:
    if "@postgres:5432" in database_url and not Path("/.dockerenv").exists():
        return database_url.replace("@postgres:5432", "@localhost:5432")
    return database_url


def _http_get_json(
    url: str, headers: Optional[dict[str, str]] = None
) -> dict[str, Any]:
    request = Request(url, headers=headers or {})
    try:
        with urlopen(request, timeout=10) as response:
            payload = response.read().decode("utf-8")
            loaded = json.loads(payload)
            if not isinstance(loaded, dict):
                raise RuntimeError(f"Expected object JSON payload from {url}")
            return cast(dict[str, Any], loaded)
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"Request failed for {url}: {exc.reason}") from exc


def _grafana_auth_headers(username: str, password: str) -> dict[str, str]:
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def verify_dashboard() -> None:
    settings = get_settings()

    grafana_port = str(settings.GRAFANA_PORT)
    grafana_admin_user = settings.GF_SECURITY_ADMIN_USER
    grafana_admin_password = settings.GF_SECURITY_ADMIN_PASSWORD
    dashboard_uid = "spark-kpi-overview"
    datasource_uid = "pyspart-postgres-kpi"
    datasource_name = "PySpart PostgreSQL KPIs"

    health_url = f"http://localhost:{grafana_port}/api/health"
    health_payload = _http_get_json(health_url)
    if str(health_payload.get("database")) != "ok":
        raise RuntimeError(f"Grafana health is not ready: {health_payload}")

    headers = _grafana_auth_headers(grafana_admin_user, grafana_admin_password)
    datasource_url = f"http://localhost:{grafana_port}/api/datasources/name/{quote(datasource_name, safe='')}"
    datasource_payload = _http_get_json(datasource_url, headers=headers)
    actual_datasource_uid = str(datasource_payload.get("uid"))
    if actual_datasource_uid != datasource_uid:
        raise RuntimeError(
            f"Grafana datasource UID mismatch: expected={datasource_uid}, actual={actual_datasource_uid}"
        )

    datasource_json_data = datasource_payload.get("jsonData")
    if not isinstance(datasource_json_data, dict):
        raise RuntimeError("Grafana datasource jsonData is missing")

    datasource_database = str(datasource_json_data.get("database"))
    if datasource_database != settings.GRAFANA_POSTGRES_DB:
        raise RuntimeError(
            f"Grafana datasource database mismatch: expected={settings.GRAFANA_POSTGRES_DB}, actual={datasource_database}"
        )

    datasource_health_url = (
        f"http://localhost:{grafana_port}/api/datasources/uid/{datasource_uid}/health"
    )
    datasource_health_payload = _http_get_json(datasource_health_url, headers=headers)
    if str(datasource_health_payload.get("status")).lower() != "ok":
        raise RuntimeError(
            f"Grafana datasource health is not OK: {datasource_health_payload}"
        )

    dashboard_url = (
        f"http://localhost:{grafana_port}/api/dashboards/uid/{dashboard_uid}"
    )
    dashboard_payload = _http_get_json(dashboard_url, headers=headers)
    dashboard_value = dashboard_payload.get("dashboard")
    if not isinstance(dashboard_value, dict):
        raise RuntimeError(
            "Grafana dashboard payload does not include dashboard object"
        )
    dashboard = cast(dict[str, Any], dashboard_value)

    if str(dashboard.get("uid")) != dashboard_uid:
        raise RuntimeError("Provisioned dashboard UID mismatch")

    panels_value = dashboard.get("panels")
    if not isinstance(panels_value, list):
        raise RuntimeError("Provisioned dashboard payload does not include panel list")

    raw_sqls: list[str] = []
    for panel in panels_value:
        if not isinstance(panel, dict):
            continue
        targets_value = panel.get("targets", [])
        if not isinstance(targets_value, list):
            continue
        for target in targets_value:
            if not isinstance(target, dict):
                continue
            raw_sql_value = target.get("rawSql")
            if isinstance(raw_sql_value, str) and raw_sql_value:
                raw_sqls.append(raw_sql_value.lower())

    if not raw_sqls:
        raise RuntimeError("No SQL queries found in provisioned dashboard panels")

    required_tables = ("daily_traffic_summary", "daily_conversion_funnel")
    forbidden_tables = ("product_views", "cart_events", "orders", "payments")

    for required in required_tables:
        if not any(required in raw_sql for raw_sql in raw_sqls):
            raise RuntimeError(
                f"Dashboard query set is missing required table: {required}"
            )

    for forbidden in forbidden_tables:
        if any(forbidden in raw_sql for raw_sql in raw_sqls):
            raise RuntimeError(
                f"Dashboard query uses forbidden raw table in v1 scope: {forbidden}"
            )

    grafana_db_user = settings.GRAFANA_POSTGRES_USER
    grafana_db_password = settings.GRAFANA_POSTGRES_PASSWORD
    normalized_database_url = _normalize_database_url(settings.DATABASE_URL)
    engine = create_engine(normalized_database_url, pool_pre_ping=True)
    grafana_engine = create_engine(
        make_url(normalized_database_url).set(
            username=grafana_db_user,
            password=grafana_db_password,
        ),
        pool_pre_ping=True,
    )
    try:
        with engine.connect() as connection:
            traffic_rows_in_demo_range = int(
                connection.execute(
                    text(
                        """
                    SELECT COUNT(*)
                    FROM daily_traffic_summary
                    WHERE summary_date BETWEEN '2026-03-01' AND '2026-03-31'
                    """
                    )
                ).scalar_one()
            )

            funnel_rows_in_demo_range = int(
                connection.execute(
                    text(
                        """
                    SELECT COUNT(*)
                    FROM daily_conversion_funnel
                    WHERE summary_date BETWEEN '2026-03-01' AND '2026-03-31'
                    """
                    )
                ).scalar_one()
            )

            invalid_rate_rows = int(
                connection.execute(
                    text(
                        """
                    SELECT COUNT(*)
                    FROM daily_conversion_funnel
                    WHERE cart_from_view_rate < 0
                       OR cart_from_view_rate > 1
                       OR order_from_cart_rate < 0
                       OR order_from_cart_rate > 1
                       OR payment_from_order_rate < 0
                       OR payment_from_order_rate > 1
                       OR payment_from_view_rate < 0
                       OR payment_from_view_rate > 1
                    """
                    )
                ).scalar_one()
            )

            conservative_aggregation_run_at = connection.execute(
                text(
                    """
                    SELECT LEAST(
                        (SELECT MAX(aggregation_run_at) FROM daily_traffic_summary),
                        (SELECT MAX(aggregation_run_at) FROM daily_conversion_funnel)
                    )
                    """
                )
            ).scalar_one_or_none()

        with grafana_engine.connect() as grafana_connection:
            reader_summary_rows = int(
                grafana_connection.execute(
                    text("SELECT COUNT(*) FROM daily_traffic_summary")
                ).scalar_one()
            )

            raw_access_denied = False
            try:
                _ = grafana_connection.execute(
                    text("SELECT COUNT(*) FROM product_views")
                ).scalar_one()
            except Exception:
                grafana_connection.rollback()
                raw_access_denied = True

            summary_write_denied = False
            try:
                _ = grafana_connection.execute(
                    text(
                        "UPDATE daily_traffic_summary SET dau_users = dau_users WHERE FALSE"
                    )
                )
            except Exception:
                grafana_connection.rollback()
                summary_write_denied = True

        if traffic_rows_in_demo_range < 31:
            raise RuntimeError(
                "Traffic summary is missing expected seeded rows for demo range"
            )

        if funnel_rows_in_demo_range < 31:
            raise RuntimeError(
                "Funnel summary is missing expected seeded rows for demo range"
            )

        if invalid_rate_rows != 0:
            raise RuntimeError("Funnel conversion rate values are outside 0-1 range")

        if conservative_aggregation_run_at is None:
            raise RuntimeError(
                "Freshness metadata is missing aggregation_run_at values"
            )

        if reader_summary_rows < 1:
            raise RuntimeError(
                "Grafana role cannot read summary tables through datasource credentials"
            )

        if not raw_access_denied:
            raise RuntimeError("Grafana role unexpectedly read raw event tables")

        if not summary_write_denied:
            raise RuntimeError(
                "Grafana role unexpectedly has write capability on summary tables"
            )
    finally:
        engine.dispose()
        grafana_engine.dispose()

    print("Grafana dashboard verification succeeded")


def main() -> None:
    verify_dashboard()


if __name__ == "__main__":
    main()
