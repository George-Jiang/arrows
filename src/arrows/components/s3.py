"""S3 access: a pyarrow filesystem plus the default bucket."""

from __future__ import annotations

from typing import Any, ClassVar

from ..core.component import Component, HealthStatus
from ..core.secrets import SecretStore


class S3Component(Component):
    name = 's3'
    optional = ('ARROWS_DEFAULT_BUCKET', 'DEFAULT_BUCKET_NAME')
    depends_on = ('aws',)
    login_args: ClassVar[dict[str, str]] = {'bucket': 'ARROWS_DEFAULT_BUCKET'}

    def __init__(self) -> None:
        super().__init__()
        self._filesystem: Any = None
        self.default_bucket: str | None = None

    def setup(self, secrets: SecretStore) -> None:
        self.import_module('pyarrow.fs')
        configured = self.session.config.default_bucket if self.session else None
        self.default_bucket = configured or self.secret('ARROWS_DEFAULT_BUCKET') or self.secret('DEFAULT_BUCKET_NAME')

    @property
    def filesystem(self) -> Any:
        """Lazily built :class:`pyarrow.fs.S3FileSystem` bound to the AWS session."""
        if self._filesystem is None:
            from pyarrow.fs import S3FileSystem

            aws = self.session.get('aws')
            credentials = aws.boto3_session.get_credentials()
            frozen = credentials.get_frozen_credentials() if credentials else None
            kwargs: dict[str, Any] = {}
            if frozen is not None:
                kwargs = {
                    'access_key': frozen.access_key,
                    'secret_key': frozen.secret_key,
                    'session_token': frozen.token,
                }
            if aws.region:
                kwargs['region'] = aws.region
            self._filesystem = S3FileSystem(**kwargs)
        return self._filesystem

    def health_check(self) -> HealthStatus:
        if not self.default_bucket:
            return HealthStatus(self.name, True, 'no default bucket configured')
        try:
            self.filesystem.get_file_info(self.default_bucket)
            return HealthStatus(self.name, True, f'bucket={self.default_bucket}')
        except Exception as exc:
            return HealthStatus(self.name, False, f'{type(exc).__name__}: {exc}')

    def close(self) -> None:
        self._filesystem = None


COMPONENT = S3Component
