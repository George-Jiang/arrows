"""Redshift connection settings.

Best practice applied here: a static ``REDSHIFT_PASSWORD`` is supported but is
not the only path. When ``REDSHIFT_CLUSTER_IDENTIFIER`` is configured and no
password is available, the component mints a short-lived password through
``redshift:GetClusterCredentials`` using the already-loaded AWS identity, so no
database password needs to exist on disk at all.
"""

from __future__ import annotations

from typing import Any, ClassVar

from ..core.component import Component, HealthStatus
from ..core.secrets import SecretStore


class RedshiftComponent(Component):
    name = 'redshift'
    requires = ('REDSHIFT_HOST', 'REDSHIFT_DATABASE', 'REDSHIFT_USER')
    optional = (
        'REDSHIFT_PASSWORD',
        'REDSHIFT_PORT',
        'REDSHIFT_CLUSTER_IDENTIFIER',
        'REDSHIFT_IAM_DURATION_SECONDS',
    )
    depends_on = ('aws',)
    login_args: ClassVar[dict[str, str]] = {
        'host': 'REDSHIFT_HOST',
        'database': 'REDSHIFT_DATABASE',
        'user': 'REDSHIFT_USER',
        'password': 'REDSHIFT_PASSWORD',
        'port': 'REDSHIFT_PORT',
        'cluster_identifier': 'REDSHIFT_CLUSTER_IDENTIFIER',
    }

    def __init__(self) -> None:
        super().__init__()
        self.host: str = ''
        self.database: str = ''
        self.user: str = ''
        self.port: int = 5439
        self._password: str | None = None
        self._password_expiry: Any = None

    def setup(self, secrets: SecretStore) -> None:
        self.import_module('psycopg2')
        secrets.require_many(self.requires, component=self.name)
        self.host = self.require_secret('REDSHIFT_HOST')
        self.database = self.require_secret('REDSHIFT_DATABASE')
        self.user = self.require_secret('REDSHIFT_USER')
        self.port = int(self.secret('REDSHIFT_PORT', '5439') or 5439)
        self._password = self.secret('REDSHIFT_PASSWORD')
        if not self._password and not self.secret('REDSHIFT_CLUSTER_IDENTIFIER'):
            from ..core.errors import MissingSecretError

            raise MissingSecretError(
                'REDSHIFT_PASSWORD or REDSHIFT_CLUSTER_IDENTIFIER',
                component=self.name,
                providers=[p.name for p in secrets.providers],
            )

    def missing_secrets(self, secrets) -> list[str]:
        missing = [key for key in self.requires if not secrets.has(key)]
        if not secrets.has('REDSHIFT_PASSWORD') and not secrets.has('REDSHIFT_CLUSTER_IDENTIFIER'):
            # Ask for the password: minting IAM credentials is a deployment
            # choice, not something to prompt an interactive user for.
            missing.append('REDSHIFT_PASSWORD')
        return missing

    # -- credentials -------------------------------------------------------
    def password(self) -> str:
        """Return a usable password, minting a temporary one when configured."""
        if self._password:
            return self._password
        return self._iam_password()

    def _iam_password(self) -> str:
        import datetime as _dt

        now = _dt.datetime.now(_dt.UTC)
        if self._password_expiry and self._password_expiry > now + _dt.timedelta(minutes=2):
            return self._temporary_password
        aws = self.session.get('aws')
        client = aws.client('redshift')
        response = client.get_cluster_credentials(
            DbUser=self.user,
            DbName=self.database,
            ClusterIdentifier=self.require_secret('REDSHIFT_CLUSTER_IDENTIFIER'),
            DurationSeconds=int(self.secret('REDSHIFT_IAM_DURATION_SECONDS', '3600') or 3600),
            AutoCreate=False,
        )
        self.user = response['DbUser']
        self._temporary_password = response['DbPassword']
        self._password_expiry = response['Expiration']
        return self._temporary_password

    # -- clients -----------------------------------------------------------
    def connect(self) -> Any:
        """Open a new psycopg2 connection. The caller owns closing it."""
        import psycopg2

        return psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password(),
            port=self.port,
        )

    def dsn(self) -> str:
        """libpq URI for the ADBC driver.

        Contains the password: never log it, never put it in an error message.
        """
        from urllib.parse import quote

        return (
            f'postgresql://{quote(self.user, safe="")}:{quote(self.password(), safe="")}'
            f'@{self.host}:{self.port}/{self.database}'
        )

    def health_check(self) -> HealthStatus:
        try:
            connection = self.connect()
            try:
                with connection.cursor() as cursor:
                    cursor.execute('SELECT 1')
                    cursor.fetchone()
            finally:
                connection.close()
            return HealthStatus(self.name, True, f'{self.host}:{self.port}/{self.database}')
        except Exception as exc:
            return HealthStatus(self.name, False, f'{type(exc).__name__}: {exc}')


COMPONENT = RedshiftComponent
