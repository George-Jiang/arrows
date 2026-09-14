"""Process-wide configuration, resolved from the environment.

Only non-secret settings live here. Secret *values* are resolved by
:class:`~arrows.core.secrets.SecretStore`; this object holds the knobs that say
*where* to look and *what* to load.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

__all__ = ['Config']

_TRUE = {'1', 'true', 'yes', 'on'}
_FALSE = {'0', 'false', 'no', 'off'}


def _env_bool(key: str, default: bool) -> bool:
    raw = os.environ.get(key)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    return default


def _env_list(key: str) -> tuple[str, ...]:
    raw = os.environ.get(key, '')
    return tuple(item.strip() for item in raw.split(',') if item.strip())


@dataclass
class Config:
    """Runtime settings.

    ``autoload`` decides what happens when a data API is used for a component
    that was never loaded: ``True`` loads it on demand (convenient default),
    ``False`` raises :class:`~arrows.core.errors.ComponentNotLoadedError`
    (recommended in production, so credential requirements fail fast at start-up
    rather than halfway through a job).
    """

    autoload: bool = True
    components: tuple[str, ...] = ()
    credentials_dir: str = '~/.credentials'
    default_bucket: str | None = None
    extra: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            autoload=_env_bool('ARROWS_AUTOLOAD', True),
            components=_env_list('ARROWS_COMPONENTS'),
            credentials_dir=os.environ.get('ARROWS_CREDENTIALS_DIR', '~/.credentials'),
            default_bucket=os.environ.get('ARROWS_DEFAULT_BUCKET') or os.environ.get('DEFAULT_BUCKET_NAME'),
        )

    def update(self, **kwargs) -> Config:
        for key, value in kwargs.items():
            if value is None:
                continue
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                self.extra[key] = value
        return self
