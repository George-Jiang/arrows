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
        self._filesystem_credentials: tuple[str, str, str | None] | None = None
        self.default_bucket: str | None = None

    def setup(self, secrets: SecretStore) -> None:
        self.import_module('pyarrow.fs')
        configured = self.session.config.default_bucket if self.session else None
        self.default_bucket = configured or self.secret('ARROWS_DEFAULT_BUCKET') or self.secret('DEFAULT_BUCKET_NAME')

    @property
    def filesystem(self) -> Any:
        """A :class:`pyarrow.fs.S3FileSystem` holding the current credentials.

        pyarrow copies the keys at construction and never refreshes them, so a
        filesystem cached for the life of the session stops working an hour into
        an SSO or instance-role session. Rebuilding it when the credentials
        change is the fix; constructing one is cheap and opens no connection.
        """
        from pyarrow.fs import S3FileSystem

        aws = self.session.get('aws')
        frozen = aws.frozen_credentials()
        current = (frozen.access_key, frozen.secret_key, frozen.token) if frozen else None
        if self._filesystem is not None and current == self._filesystem_credentials:
            return self._filesystem

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
        self._filesystem_credentials = current
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
        self._filesystem_credentials = None


COMPONENT = S3Component
