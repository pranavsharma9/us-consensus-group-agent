from typing import Any, Dict, List

import snowflake.connector

from app.core.config import Settings


class SnowflakeService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def _ensure_configured(self) -> None:
        required = [
            self._settings.snowflake_account,
            self._settings.snowflake_user,
            self._settings.snowflake_password,
            self._settings.snowflake_warehouse,
            self._settings.snowflake_database,
            self._settings.snowflake_schema,
        ]
        if any(not value for value in required):
            raise RuntimeError("Snowflake credentials/configuration are incomplete.")

    def _connect(self):
        self._ensure_configured()
        return snowflake.connector.connect(
            account=self._settings.snowflake_account,
            user=self._settings.snowflake_user,
            password=self._settings.snowflake_password,
            warehouse=self._settings.snowflake_warehouse,
            database=self._settings.snowflake_database,
            schema=self._settings.snowflake_schema,
            role=self._settings.snowflake_role,
        )

    def _quote_identifier(self, identifier: str) -> str:
        token = identifier.strip().strip('"')
        return f'"{token.replace(chr(34), chr(34) * 2)}"'

    @property
    def default_database(self) -> str:
        return self._settings.snowflake_database

    @property
    def default_schema(self) -> str:
        return self._settings.snowflake_schema

    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        connection = self._connect()
        try:
            with connection.cursor(snowflake.connector.DictCursor) as cursor:
                cursor.execute(sql)
                return [dict(row) for row in cursor.fetchall()]
        finally:
            connection.close()