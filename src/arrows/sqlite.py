"""Local SQLite files, read and written through DuckDB.

The data API paired with the reference component in
``arrows/components/sqlite.py``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .core.engine import get_duckdb, quote_identifier
from .core.session import get as _get_component
from .core.session import login as _login

__all__ = ['arrow_to_sqlite', 'execute_sql', 'fetch_arrow', 'fetch_dataframe', 'fetch_polars']


def _component() -> Any:
    return _get_component('sqlite')


def _attach(database: str | Path | None) -> str:
    return _component().attach(database)


def fetch_arrow(sql: str, database: str | Path | None = None, **kwargs):
    """Run a query against a SQLite file and return an Arrow table."""
    alias = _attach(database)
    from jinja2 import Template

    rendered = Template(sql).render(**kwargs) if kwargs else sql
    connection = get_duckdb()
    connection.execute(f'USE {quote_identifier(alias)};')
    try:
        return connection.sql(rendered).to_arrow_table()
    finally:
        connection.execute('USE memory;')


def fetch_polars(sql: str, database: str | Path | None = None, **kwargs):
    return fetch_arrow(sql, database=database, **kwargs).to_polars()


def fetch_dataframe(sql: str, database: str | Path | None = None, **kwargs):
    return fetch_arrow(sql, database=database, **kwargs).to_pandas()


def arrow_to_sqlite(arrow, table_name: str, database: str | Path | None = None, mode: str = 'append') -> None:
    """Write an Arrow table into a SQLite table."""
    alias = _attach(database)
    connection = get_duckdb()
    qualified = f'{quote_identifier(alias)}.{quote_identifier(table_name)}'
    if mode == 'overwrite':
        connection.execute(f'DROP TABLE IF EXISTS {qualified};')
    # `arrow` is resolved by DuckDB's replacement scan of this frame, not interpolated.
    connection.execute(f'CREATE TABLE IF NOT EXISTS {qualified} AS SELECT * FROM arrow LIMIT 0;')  # noqa: S608
    connection.execute(f'INSERT INTO {qualified} SELECT * FROM arrow;')  # noqa: S608


def execute_sql(sql: str, database: str | Path | None = None, **kwargs) -> None:
    alias = _attach(database)
    from jinja2 import Template

    connection = get_duckdb()
    connection.execute(f'USE {quote_identifier(alias)};')
    try:
        connection.execute(Template(sql).render(**kwargs) if kwargs else sql)
    finally:
        connection.execute('USE memory;')


def attach(database: str | Path, alias: str = 'sqlite_db') -> str:
    """ATTACH a SQLite file under ``alias`` and return the alias."""
    return _component().attach(database, alias=alias)


def login(database: str | Path | None = None, *, save: bool = False, prompt: bool | None = None, **kwargs):
    """Set the default SQLite database and load the component."""
    return _login('sqlite', database=str(database) if database else None, save=save, prompt=prompt, **kwargs)
