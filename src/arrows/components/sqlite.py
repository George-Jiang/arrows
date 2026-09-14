"""Reference component: SQLite.

This is the smallest complete example of the component contract, and the file to
copy when adding a new integration. It needs no credentials, so it shows the
shape without the auth noise: declare config keys, acquire a client in
``setup``, expose it, and release it in ``close``.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import ClassVar

from ..core.component import Component, HealthStatus
from ..core.engine import get_duckdb, quote_literal
from ..core.secrets import SecretStore


class SqliteComponent(Component):
    name = 'sqlite'
    optional = ('SQLITE_DATABASE',)
    login_args: ClassVar[dict[str, str]] = {'database': 'SQLITE_DATABASE'}

    def __init__(self) -> None:
        super().__init__()
        self.database: str = ':memory:'
        self._attached: set[str] = set()

    def setup(self, secrets: SecretStore) -> None:
        self.import_module('duckdb')
        self.database = self.secret('SQLITE_DATABASE', ':memory:') or ':memory:'

    def attach(self, database: str | Path | None = None, alias: str = 'sqlite_db') -> str:
        """ATTACH a SQLite file to the shared DuckDB connection, once."""
        path = str(database or self.database)
        if alias not in self._attached:
            get_duckdb().execute(
                f'INSTALL sqlite; LOAD sqlite; ATTACH IF NOT EXISTS {quote_literal(path)} AS {alias} (TYPE sqlite);'
            )
            self._attached.add(alias)
        return alias

    def health_check(self) -> HealthStatus:
        try:
            get_duckdb().execute('INSTALL sqlite; LOAD sqlite;')
            return HealthStatus(self.name, True, f'database={self.database}')
        except Exception as exc:
            return HealthStatus(self.name, False, f'{type(exc).__name__}: {exc}')

    def close(self) -> None:
        connection = get_duckdb()
        for alias in self._attached:
            # Teardown is best effort: the alias may already be detached.
            with contextlib.suppress(Exception):
                connection.execute(f'DETACH {alias};')
        self._attached.clear()


COMPONENT = SqliteComponent
