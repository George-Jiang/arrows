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
        self._duckdb_credentials: tuple[str, str, str | None] | None = None

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

    # -- credentials -------------------------------------------------------
    def frozen_credentials(self) -> Any:
        """The credentials to use *right now*, or ``None`` if there are none.

        Every consumer has to ask for these at the moment of use rather than
        keeping a copy. botocore hands back a refreshable credential object for
        SSO, assumed roles and instance profiles, and ``get_frozen_credentials``
        is what triggers the renewal when the current ones are near expiry. A
        caller that froze them once is holding keys that stop working an hour
        later, with no way to notice.
        """
        credentials = self.boto3_session.get_credentials() if self.boto3_session else None
        return credentials.get_frozen_credentials() if credentials else None

    def credential_provider(self):
        """A Polars credential provider bound to this component's identity.

        Without one, Polars builds a *fresh* ``boto3.Session()`` of its own and
        resolves credentials from scratch. That session cannot see the arrows
        SecretStore, so a profile or key supplied through ``login()``, ``.env``
        or the keychain is invisible to it and it silently falls back to the
        default profile — which is how a working DuckDB read sits next to a
        Polars read failing on an expired SSO token.
        """

        def provider():
            credentials = self.boto3_session.get_credentials() if self.boto3_session else None
            if credentials is None:
                raise RuntimeError('No AWS credentials resolved; cannot read from S3.')
            # Freeze first: on a deferred credential object the expiry does not
            # exist until the keys have actually been resolved.
            frozen = credentials.get_frozen_credentials()
            values = {'aws_access_key_id': frozen.access_key, 'aws_secret_access_key': frozen.secret_key}
            if frozen.token is not None:
                values['aws_session_token'] = frozen.token
            expiry = getattr(credentials, '_expiry_time', None)
            return values, int(expiry.timestamp()) if expiry is not None else None

        return provider

    def storage_options(self) -> dict[str, str]:
        """Non-credential object-store settings for Polars."""
        return {'aws_region': self.region} if self.region else {}

    # -- duckdb ------------------------------------------------------------
    def prepare_duckdb(self) -> None:
        """Make sure DuckDB holds usable credentials before a query runs.

        Cheap and idempotent while nothing has rotated, so callers can put it in
        front of every S3 read and write.
        """
        if self._explicit_credentials:
            self._register_duckdb_secret()

    def _register_duckdb_secret(self) -> None:
        """Give DuckDB the same identity boto3 resolved.

        Two paths, and the difference matters:

        * When boto3 resolved credentials from its own chain (profile, SSO,
          instance role), DuckDB is pointed at that same chain — no key material
          is written into SQL, and DuckDB refreshes on its own. Registered once.
        * When the credentials came from the arrows SecretStore, DuckDB cannot
          see that store, so the keys have to be handed over as a literal
          secret. Those are a snapshot, so the secret is re-registered whenever
          the underlying credentials change. They are visible in
          ``duckdb_secrets()``, which is the cost of that setup and a reason to
          prefer a real AWS profile.

        A failure here is not fatal: boto3 and pyarrow still work.
        """
        import warnings

        frozen = self.frozen_credentials()
        if frozen is None:
            warnings.warn('No AWS credentials resolved; DuckDB S3 access is unconfigured.', stacklevel=2)
            return

        region = f', REGION {quote_literal(self.region)}' if self.region else ''
        if self._explicit_credentials:
            current = (frozen.access_key, frozen.secret_key, frozen.token)
            if current == self._duckdb_credentials:
                return
            token = f', SESSION_TOKEN {quote_literal(frozen.token)}' if frozen.token else ''
            body = (
                f'TYPE s3, KEY_ID {quote_literal(frozen.access_key)}, '
                f'SECRET {quote_literal(frozen.secret_key)}{token}{region}'
            )
        else:
            current = None
            body = f'TYPE s3, PROVIDER credential_chain{region}'

        try:
            get_duckdb().execute(f'CREATE OR REPLACE SECRET arrows_s3 ({body});')
        except Exception as exc:
            warnings.warn(f'Could not register the DuckDB S3 secret: {exc}', stacklevel=2)
            return
        self._duckdb_credentials = current

    def client(self, service: str, **kwargs) -> Any:
        return self.boto3_session.client(service, **kwargs)

    def health_check(self) -> HealthStatus:
        if self.boto3_session is None:
            return HealthStatus(self.name, False, 'no boto3 session')
        try:
            identity = self.boto3_session.client('sts').get_caller_identity()
            return HealthStatus(self.name, True, f'arn={identity.get("Arn", "?")}')
        except Exception as exc:
            return HealthStatus(self.name, False, f'{type(exc).__name__}: {exc}')

    def close(self) -> None:
        # Teardown is best effort: the connection may already be gone.
        with contextlib.suppress(Exception):
            get_duckdb().execute('DROP SECRET IF EXISTS arrows_s3;')
        self.boto3_session = None
        self._duckdb_credentials = None


COMPONENT = AwsComponent
