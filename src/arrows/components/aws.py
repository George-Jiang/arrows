"""AWS credentials, shared by the s3 and redshift components.

Best practice applied here: arrows does **not** invent a credential format. It
defers to boto3's own resolution chain (env vars, ``~/.aws/credentials``, SSO,
``AWS_PROFILE``, EC2/ECS/EKS instance roles), which is the only path that
supports short-lived, automatically rotated credentials. Explicit keys from the
:class:`SecretStore` are honoured when present, for laptops that still use them.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any, ClassVar

from ..core.component import Component, HealthStatus
from ..core.engine import get_duckdb, quote_literal
from ..core.secrets import SecretStore

EXPLICIT_KEYS = ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN')


class LegacyAwsFileProvider:
    """Reads the historical ``~/.credentials/aws_credentials.txt`` layout.

    Kept so existing setups keep working; prefer a real ``~/.aws/credentials``
    profile or SSO, which boto3 refreshes on its own.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser()
        self.name = f'legacy-aws-file({self.path})'
        self._cache: dict[str, str] | None = None

    def _load(self) -> dict[str, str]:
        if self._cache is not None:
            return self._cache
        values: dict[str, str] = {}
        if self.path.is_file():
            from ..core.secrets import _warn_if_world_readable

            _warn_if_world_readable(self.path)
            for line in self.path.read_text(encoding='utf-8').splitlines():
                line = line.strip()
                if not line or line.startswith('[') or '=' not in line:
                    continue
                key, _, value = line.partition('=')
                key = key.strip().upper()
                if key in EXPLICIT_KEYS:
                    values[key] = value.strip()
        self._cache = values
        return values

    def get(self, key: str) -> str | None:
        return self._load().get(key)


class AwsComponent(Component):
    """Owns the boto3 session and registers DuckDB's S3 secret."""

    name = 'aws'
    optional = (*EXPLICIT_KEYS, 'AWS_REGION', 'AWS_PROFILE')
    login_args: ClassVar[dict[str, str]] = {
        'access_key_id': 'AWS_ACCESS_KEY_ID',
        'secret_access_key': 'AWS_SECRET_ACCESS_KEY',
        'session_token': 'AWS_SESSION_TOKEN',
        'region': 'AWS_REGION',
        'profile': 'AWS_PROFILE',
    }

    def __init__(self) -> None:
        super().__init__()
        self.boto3_session: Any = None
        self.region: str | None = None
        self._explicit_credentials = False

    def setup(self, secrets: SecretStore) -> None:
        boto3 = self.import_module('boto3')

        # Backwards compatibility only, and only when the file is really there:
        # a production store built without file providers must stay that way.
        credentials_dir = (
            Path(self.session.config.credentials_dir).expanduser()
            if self.session
            else Path('~/.credentials').expanduser()
        )
        legacy_file = credentials_dir / 'aws_credentials.txt'
        if legacy_file.is_file():
            secrets.add_provider(LegacyAwsFileProvider(legacy_file))

        self.region = self.secret('AWS_REGION') or self.secret('AWS_DEFAULT_REGION')
        access_key = self.secret('AWS_ACCESS_KEY_ID')
        kwargs: dict[str, Any] = {'region_name': self.region} if self.region else {}
        self._explicit_credentials = bool(access_key)
        if access_key:
            kwargs.update(
                aws_access_key_id=access_key,
                aws_secret_access_key=self.secret('AWS_SECRET_ACCESS_KEY'),
                aws_session_token=self.secret('AWS_SESSION_TOKEN'),
            )
        elif profile := self.secret('AWS_PROFILE'):
            kwargs['profile_name'] = profile

        self.boto3_session = boto3.Session(**kwargs)
        self._register_duckdb_secret()

    def _register_duckdb_secret(self) -> None:
        """Give DuckDB the same identity boto3 resolved.

        Two paths, and the difference matters:

        * When boto3 resolved credentials from its own chain (profile, SSO,
          instance role), DuckDB is pointed at that same chain — no key material
          is written into SQL, and DuckDB refreshes on its own.
        * When the credentials came from the arrows SecretStore, DuckDB cannot
          see that store, so the keys have to be handed over as a literal
          secret. They are then visible in ``duckdb_secrets()``, which is the
          cost of that setup and a reason to prefer a real AWS profile.

        A failure here is not fatal: boto3 and pyarrow still work.
        """
        import warnings

        credentials = self.boto3_session.get_credentials()
        if credentials is None:
            warnings.warn('No AWS credentials resolved; DuckDB S3 access is unconfigured.', stacklevel=2)
            return

        region = f', REGION {quote_literal(self.region)}' if self.region else ''
        if self._explicit_credentials:
            frozen = credentials.get_frozen_credentials()
            token = f', SESSION_TOKEN {quote_literal(frozen.token)}' if frozen.token else ''
            body = (
                f'TYPE s3, KEY_ID {quote_literal(frozen.access_key)}, '
                f'SECRET {quote_literal(frozen.secret_key)}{token}{region}'
            )
        else:
            body = f'TYPE s3, PROVIDER credential_chain{region}'

        try:
            get_duckdb().execute(f'CREATE OR REPLACE SECRET arrows_s3 ({body});')
        except Exception as exc:
            warnings.warn(f'Could not register the DuckDB S3 secret: {exc}', stacklevel=2)

    def client(self, service: str, **kwargs) -> Any:
        return self.boto3_session.client(service, **kwargs)

    def health_check(self) -> HealthStatus:
        if self.boto3_session is None:
            return HealthStatus(self.name, False, 'no boto3 session')
        try:
            identity = self.boto3_session.client('sts').get_caller_identity()
            return HealthStatus(self.name, True, f"arn={identity.get('Arn', '?')}")
        except Exception as exc:
            return HealthStatus(self.name, False, f'{type(exc).__name__}: {exc}')

    def close(self) -> None:
        # Teardown is best effort: the connection may already be gone.
        with contextlib.suppress(Exception):
            get_duckdb().execute('DROP SECRET IF EXISTS arrows_s3;')
        self.boto3_session = None


COMPONENT = AwsComponent
