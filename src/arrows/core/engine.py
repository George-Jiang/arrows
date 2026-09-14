"""Shared DuckDB connection.

Components register DuckDB secrets (S3, Google Sheets) and the data API runs
queries; both must agree on *which* connection. Going through this module makes
the connection injectable — a test or an embedding application can hand arrows a
private connection instead of mutating DuckDB's process-wide default.
"""

from __future__ import annotations

from typing import Any

__all__ = ['get_duckdb', 'quote_literal', 'reset_duckdb', 'set_duckdb']

_connection: Any = None


def get_duckdb() -> Any:
    """Return the connection arrows uses, defaulting to DuckDB's own default."""
    global _connection
    if _connection is None:
        import duckdb

        _connection = duckdb.default_connection()
    return _connection


def set_duckdb(connection: Any) -> None:
    """Point arrows at a specific DuckDB connection."""
    global _connection
    _connection = connection


def reset_duckdb() -> None:
    global _connection
    _connection = None


def quote_literal(value: str) -> str:
    """Quote a value for inlining into SQL that cannot take a bind parameter.

    DuckDB's ``CREATE SECRET`` only accepts literals, so a token has to be
    inlined. Escaping it here keeps a malformed credential from terminating the
    statement and turning into injected SQL.
    """
    return "'" + str(value).replace("'", "''") + "'"


def quote_identifier(name: str) -> str:
    """Quote a table/column name, rejecting anything that is not a plain identifier.

    Identifiers cannot be bind parameters either, so a caller-supplied table name
    would otherwise be concatenated straight into SQL.
    """
    parts = str(name).split('.')
    if not all(part and all(c.isalnum() or c == '_' for c in part) for part in parts):
        raise ValueError(f'Invalid SQL identifier: {name!r}')
    return '.'.join(f'"{part}"' for part in parts)
