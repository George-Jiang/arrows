"""Google OAuth credentials, shared by the gmail and google_sheets components.

Best practice applied here: the token is refreshed **lazily and in memory**.
The previous implementation refreshed at import time (a network call as a side
effect of ``import arrows``) and then copied the whole token document into
``os.environ``, where every subprocess inherited it. Here the credential object
stays in the component, and the short-lived access token is re-issued only when
it actually expires.
"""

from __future__ import annotations

import contextlib
import json
from pathlib import Path
from typing import Any, ClassVar

from ..core.component import Component, HealthStatus
from ..core.engine import get_duckdb, quote_literal
from ..core.secrets import SecretStore

DEFAULT_SCOPES = (
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/gmail.send',
)


class GoogleComponent(Component):
    """Holds refreshable Google OAuth credentials and builds API services."""

    name = 'google'
    requires = ('GOOGLE_TOKEN_JSON',)
    optional = ('GOOGLE_SCOPES',)
    login_args: ClassVar[dict[str, str]] = {'token_json': 'GOOGLE_TOKEN_JSON', 'scopes': 'GOOGLE_SCOPES'}

    def __init__(self) -> None:
        super().__init__()
        self._credentials: Any = None
        self._duckdb_token: str | None = None
        self._services: dict[tuple[str, str], Any] = {}

    def setup(self, secrets: SecretStore) -> None:
        google_credentials = self.import_module('google.oauth2.credentials')
        token_document = json.loads(self.require_secret('GOOGLE_TOKEN_JSON'))
        scopes = self.secret('GOOGLE_SCOPES')
        self._credentials = google_credentials.Credentials.from_authorized_user_info(
            token_document,
            scopes=scopes.split(',') if scopes else list(DEFAULT_SCOPES),
        )

    # -- credentials -------------------------------------------------------
    @property
    def credentials(self) -> Any:
        """Valid credentials, refreshed on demand."""
        creds = self._credentials
        if creds is None:
            raise RuntimeError('google component is not set up')
        if not creds.valid and creds.refresh_token:
            from google.auth.transport.requests import Request

            creds.refresh(Request())
            self._services.clear()
        return creds

    def service(self, api: str, version: str) -> Any:
        """Cached ``googleapiclient`` service, rebuilt after a token refresh."""
        credentials = self.credentials
        key = (api, version)
        if key not in self._services:
            discovery = self.import_module('googleapiclient.discovery')
            self._services[key] = discovery.build(api, version, credentials=credentials, cache_discovery=False)
        return self._services[key]

    # -- duckdb ------------------------------------------------------------
    def register_duckdb_secret(self) -> None:
        """(Re)register the DuckDB ``gsheet`` secret with the current access token.

        DuckDB's ``CREATE SECRET`` takes literals only, so the token is inlined;
        it is quoted defensively and it is an access token that expires in an
        hour, never the refresh token.
        """
        token = self.credentials.token
        if token == self._duckdb_token:
            return
        connection = get_duckdb()
        connection.execute('INSTALL gsheets FROM community; LOAD gsheets;')
        connection.execute(
            f'CREATE OR REPLACE SECRET arrows_gsheet '
            f'(TYPE gsheet, PROVIDER access_token, TOKEN {quote_literal(token)});'
        )
        self._duckdb_token = token

    def health_check(self) -> HealthStatus:
        try:
            profile = self.service('oauth2', 'v2').userinfo().get().execute()
            return HealthStatus(self.name, True, f'user={profile.get("email", "?")}')
        except Exception as exc:
            return HealthStatus(self.name, False, f'{type(exc).__name__}: {exc}')

    def close(self) -> None:
        # Teardown is best effort: the connection may already be gone.
        with contextlib.suppress(Exception):
            get_duckdb().execute('DROP SECRET IF EXISTS arrows_gsheet;')
        self._services.clear()
        self._credentials = None
        self._duckdb_token = None


COMPONENT = GoogleComponent


def token_file_hint(credentials_dir: str | Path = '~/.credentials') -> str:
    path = Path(credentials_dir).expanduser() / 'google_token.json'
    return f'Place the OAuth token document at {path} (chmod 600) or set GOOGLE_TOKEN_JSON.'
