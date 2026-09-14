"""Google Sheets access, on top of the shared google component.

Reads go through the DuckDB ``gsheets`` community extension; metadata
operations (create, rename, share, delete) go through the Sheets/Drive APIs.
"""

from __future__ import annotations

from typing import Any

from ..core.component import Component, HealthStatus
from ..core.secrets import SecretStore


class GoogleSheetsComponent(Component):
    name = 'google_sheets'
    depends_on = ('google',)

    def setup(self, secrets: SecretStore) -> None:
        self.import_module('googleapiclient.discovery')
        self.google.register_duckdb_secret()

    @property
    def google(self) -> Any:
        return self.session.get('google')

    @property
    def sheets_service(self) -> Any:
        return self.google.service('sheets', 'v4')

    @property
    def drive_service(self) -> Any:
        return self.google.service('drive', 'v3')

    def prepare_duckdb(self) -> None:
        """Make sure DuckDB holds a fresh access token before a read/write."""
        self.google.register_duckdb_secret()

    def health_check(self) -> HealthStatus:
        try:
            self.prepare_duckdb()
            return HealthStatus(self.name, True, 'gsheets extension ready')
        except Exception as exc:
            return HealthStatus(self.name, False, f'{type(exc).__name__}: {exc}')


COMPONENT = GoogleSheetsComponent
