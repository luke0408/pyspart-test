import sys
from pathlib import Path
from typing import cast

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

sys.path.append(str(Path(__file__).parent.parent))

from app.core.settings import get_settings


def _normalize_database_url(database_url: str) -> str:
    if "@postgres:5432" in database_url and not Path("/.dockerenv").exists():
        return database_url.replace("@postgres:5432", "@localhost:5432")
    return database_url


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _role_exists(connection: Connection, role_name: str) -> bool:
    exists = connection.execute(
        text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :role_name)"),
        {"role_name": role_name},
    ).scalar_one()
    return cast(bool, exists)


def _table_exists(connection: Connection, table_name: str) -> bool:
    regclass_value = connection.execute(
        text("SELECT to_regclass(:table_name)"),
        {"table_name": f"public.{table_name}"},
    ).scalar_one()
    return regclass_value is not None


def bootstrap_grafana_readonly_role() -> None:
    settings = get_settings()
    database_url = _normalize_database_url(settings.DATABASE_URL)

    grafana_db_user = settings.GRAFANA_POSTGRES_USER
    grafana_db_password = settings.GRAFANA_POSTGRES_PASSWORD
    grafana_db_name = settings.GRAFANA_POSTGRES_DB
    summary_tables = ("daily_traffic_summary", "daily_conversion_funnel")
    raw_tables = ("product_views", "cart_events", "orders", "payments", "products")

    engine = create_engine(database_url, pool_pre_ping=True)
    try:
        with engine.begin() as connection:
            database_name = str(
                connection.execute(text("SELECT current_database()")).scalar_one()
            )
            if database_name != grafana_db_name:
                raise RuntimeError(
                    f"DATABASE_URL targets '{database_name}' but GRAFANA_POSTGRES_DB is '{grafana_db_name}'"
                )
            quoted_role = _quote_identifier(grafana_db_user)
            quoted_password = _quote_literal(grafana_db_password)

            if _role_exists(connection, grafana_db_user):
                _ = connection.execute(
                    text(
                        f"ALTER ROLE {quoted_role} WITH LOGIN PASSWORD {quoted_password}"
                    )
                )
            else:
                _ = connection.execute(
                    text(
                        f"CREATE ROLE {quoted_role} WITH LOGIN PASSWORD {quoted_password}"
                    )
                )

            quoted_database = _quote_identifier(database_name)

            _ = connection.execute(
                text(f"GRANT CONNECT ON DATABASE {quoted_database} TO {quoted_role}")
            )

            _ = connection.execute(
                text(f"GRANT USAGE ON SCHEMA public TO {quoted_role}")
            )

            for table_name in summary_tables:
                if _table_exists(connection, table_name):
                    quoted_table = _quote_identifier(table_name)
                    _ = connection.execute(
                        text(
                            f"REVOKE ALL ON TABLE public.{quoted_table} FROM {quoted_role}"
                        )
                    )
                    _ = connection.execute(
                        text(
                            f"GRANT SELECT ON TABLE public.{quoted_table} TO {quoted_role}"
                        )
                    )

            for table_name in raw_tables:
                if _table_exists(connection, table_name):
                    quoted_table = _quote_identifier(table_name)
                    _ = connection.execute(
                        text(
                            f"REVOKE ALL ON TABLE public.{quoted_table} FROM {quoted_role}"
                        )
                    )

        print(
            "Grafana read-only role is ready:",
            f"role={grafana_db_user}",
            f"database={database_name}",
        )
    finally:
        engine.dispose()


def main() -> None:
    bootstrap_grafana_readonly_role()


if __name__ == "__main__":
    main()
