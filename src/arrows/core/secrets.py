"""Secret resolution.

Design rules
------------
1. A secret value is never stored in ``os.environ`` unless the caller asks for
   it explicitly (:meth:`SecretStore.export`): the process environment is
   inherited by every subprocess and shows up in crash reports.
2. A secret value is wrapped in :class:`Secret`, whose ``repr``/``str`` are
   redacted, so it cannot leak into a log line or a traceback by accident.
3. Where a secret comes from is a deployment decision, not a code decision.
   Components ask for a *key*; a chain of :class:`SecretProvider` resolves it
   (env -> .env -> local files -> keyring -> cloud secret manager).
"""

from __future__ import annotations

import hmac
import json
import os
import stat
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Protocol, runtime_checkable

from .errors import MissingSecretError

_TRUTHY = {'1', 'true', 'yes', 'on'}

__all__ = [
    'AwsSecretsManagerProvider',
    'DotEnvProvider',
    'EnvProvider',
    'JsonFileProvider',
    'KeyringProvider',
    'MappingProvider',
    'Secret',
    'SecretFileProvider',
    'SecretProvider',
    'SecretStore',
]


class Secret:
    """An opaque string whose value must be requested explicitly.

    ``str(secret)`` and ``repr(secret)`` are redacted; use :meth:`reveal` at the
    exact call site that needs the plaintext.
    """

    __slots__ = ('_value', 'key', 'source')

    def __init__(self, value: str, key: str = '', source: str = ''):
        self._value = value
        self.key = key
        self.source = source

    def reveal(self) -> str:
        """Return the plaintext. Keep the result on the stack, never in a log."""
        return self._value

    def __repr__(self) -> str:
        origin = f' from {self.source}' if self.source else ''
        return f'Secret({self.key!r}{origin}, value=***)'

    def __str__(self) -> str:
        return '***'

    def __bool__(self) -> bool:
        return bool(self._value)

    def __len__(self) -> int:
        return len(self._value)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Secret):
            return hmac.compare_digest(self._value, other._value)
        if isinstance(other, str):
            return hmac.compare_digest(self._value, other)
        return NotImplemented

    def __hash__(self) -> int:  # keep Secret usable as a dict key
        return hash((self.key, self._value))


@runtime_checkable
class SecretProvider(Protocol):
    """Anything that can resolve a secret key to a plaintext value."""

    name: str

    def get(self, key: str) -> str | None:
        """Return the value for ``key``, or ``None`` if this provider has no opinion."""


class MappingProvider:
    """Secrets supplied in-process. Useful in tests and for explicit wiring."""

    def __init__(self, values: Mapping[str, str], name: str = 'mapping'):
        self.name = name
        self._values = dict(values)

    def get(self, key: str) -> str | None:
        return self._values.get(key)


class EnvProvider:
    """Process environment, optionally namespaced by ``prefix`` (12-factor default)."""

    def __init__(self, prefix: str = ''):
        self.name = f'env(prefix={prefix!r})' if prefix else 'env'
        self.prefix = prefix

    def get(self, key: str) -> str | None:
        return os.environ.get(f'{self.prefix}{key}')


class DotEnvProvider:
    """A ``.env`` file, for local development only.

    Deliberately minimal (``KEY=value``, ``#`` comments, optional quotes) so the
    library does not grow a dependency for a dev-time convenience.
    """

    def __init__(self, path: str | os.PathLike[str] = '.env'):
        self.path = Path(path)
        self.name = f'dotenv({self.path})'
        self._cache: dict[str, str] | None = None

    def _load(self) -> dict[str, str]:
        if self._cache is not None:
            return self._cache
        values: dict[str, str] = {}
        if self.path.is_file():
            _warn_if_world_readable(self.path)
            for raw in self.path.read_text(encoding='utf-8').splitlines():
                line = raw.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, _, value = line.partition('=')
                key = key.removeprefix('export ').strip()
                value = value.strip().strip('\'"')
                values[key] = value
        self._cache = values
        return values

    def get(self, key: str) -> str | None:
        return self._load().get(key)


class JsonFileProvider:
    """A flat ``{"KEY": "value"}`` JSON file.

    Supports the legacy ``~/.credentials/*.json`` layout. ``wrap_key`` makes the
    whole document available under a single key (e.g. a Google token document
    exposed as ``GOOGLE_TOKEN_JSON``).
    """

    def __init__(self, path: str | os.PathLike[str], wrap_key: str | None = None):
        self.path = Path(path).expanduser()
        self.wrap_key = wrap_key
        self.name = f'json({self.path})'
        self._cache: dict[str, str] | None = None

    def _load(self) -> dict[str, str]:
        if self._cache is not None:
            return self._cache
        values: dict[str, str] = {}
        if self.path.is_file():
            _warn_if_world_readable(self.path)
            try:
                document = json.loads(self.path.read_text(encoding='utf-8'))
            except json.JSONDecodeError:
                document = {}
            if self.wrap_key:
                values[self.wrap_key] = json.dumps(document)
            elif isinstance(document, dict):
                values = {k: v for k, v in document.items() if isinstance(v, str)}
        self._cache = values
        return values

    def get(self, key: str) -> str | None:
        return self._load().get(key)


class SecretFileProvider:
    """One file per secret, e.g. Docker/Kubernetes ``/run/secrets/<KEY>``."""

    def __init__(self, directory: str | os.PathLike[str] = '/run/secrets'):
        self.directory = Path(directory).expanduser()
        self.name = f'files({self.directory})'

    def get(self, key: str) -> str | None:
        path = self.directory / key
        if not path.is_file():
            return None
        _warn_if_world_readable(path)
        return path.read_text(encoding='utf-8').strip()


class KeyringProvider:
    """OS keychain (macOS Keychain, libsecret, Windows Credential Manager).

    Requires the optional ``keyring`` package; silently inert when absent so it
    can stay in a default chain.
    """

    def __init__(self, service: str = 'arrows'):
        self.service = service
        self.name = f'keyring({service})'

    def get(self, key: str) -> str | None:
        try:
            import keyring
        except ImportError:
            return None
        try:
            return keyring.get_password(self.service, key)
        except Exception:  # a locked or unavailable backend must not break resolution
            return None


def is_interactive() -> bool:
    """True when there is a human who can answer a prompt.

    A prompt in a scheduled job does not fail — it hangs forever, holding a
    worker. So prompting is allowed only from a terminal or an IPython kernel,
    and ``ARROWS_NON_INTERACTIVE=1`` turns it off everywhere.
    """
    if os.environ.get('ARROWS_NON_INTERACTIVE', '').strip().lower() in _TRUTHY:
        return False
    try:
        from IPython import get_ipython

        if get_ipython() is not None:
            return True
    except ImportError:
        pass
    return bool(getattr(sys.stdin, 'isatty', lambda: False)())


class PromptProvider:
    """Ask the user for a secret, masked, at the moment it is first needed.

    Intended as the *last* link in a chain, for notebooks and one-off scripts:
    anything already configured is used, and only what is genuinely missing is
    asked for. Values live in memory for the session and are never written to
    disk unless ``save_to_keyring`` is set.

    A value for a ``*_JSON`` or ``*_FILE`` key that names an existing file is
    read from that file, so a Google token document can be supplied by path
    instead of pasted.
    """

    def __init__(
        self,
        allow: Iterable[str] | None = None,
        save_to_keyring: bool = False,
        service: str = 'arrows',
        prompt: str = '{key}: ',
    ):
        self.name = 'prompt'
        self.allow = set(allow) if allow is not None else None
        self.save_to_keyring = save_to_keyring
        self.service = service
        self.prompt = prompt
        self._answers: dict[str, str | None] = {}

    def get(self, key: str) -> str | None:
        if key in self._answers:  # never ask the same question twice
            return self._answers[key]
        if self.allow is not None and key not in self.allow:
            return None
        if not is_interactive():
            return None

        import getpass

        try:
            value = getpass.getpass(self.prompt.format(key=key)).strip()
        except (EOFError, KeyboardInterrupt):
            value = ''

        value = _read_if_path(key, value)
        self._answers[key] = value or None
        if value and self.save_to_keyring:
            self._save(key, value)
        return self._answers[key]

    def _save(self, key: str, value: str) -> None:
        try:
            import keyring

            keyring.set_password(self.service, key, value)
        except Exception as exc:  # a missing or locked backend is not fatal
            import warnings

            warnings.warn(f'Could not save {key!r} to the keyring: {exc}', stacklevel=2)

    def forget(self, key: str | None = None) -> None:
        """Drop remembered answers so the next lookup asks again."""
        self._answers.pop(key, None) if key else self._answers.clear()


def _read_if_path(key: str, value: str) -> str:
    """Let a ``*_JSON``/``*_FILE`` key be answered with a path to the document."""
    if not value or not key.endswith(('_JSON', '_FILE')):
        return value
    candidate = Path(value).expanduser()
    if candidate.is_file():
        _warn_if_world_readable(candidate)
        return candidate.read_text(encoding='utf-8').strip()
    return value


class AwsSecretsManagerProvider:
    """A single AWS Secrets Manager secret holding a flat JSON document.

    The natural production provider: rotation, audit trail and IAM-scoped access
    instead of long-lived files on a laptop.
    """

    def __init__(self, secret_id: str, boto3_session=None, region_name: str | None = None):
        self.secret_id = secret_id
        self.name = f'aws-secrets-manager({secret_id})'
        self._session = boto3_session
        self._region_name = region_name
        self._cache: dict[str, str] | None = None

    def _load(self) -> dict[str, str]:
        if self._cache is not None:
            return self._cache
        try:
            import boto3
        except ImportError:
            self._cache = {}
            return self._cache
        session = self._session or boto3.Session()
        client = session.client('secretsmanager', region_name=self._region_name)
        payload = client.get_secret_value(SecretId=self.secret_id)['SecretString']
        document = json.loads(payload)
        self._cache = {k: str(v) for k, v in document.items()}
        return self._cache

    def get(self, key: str) -> str | None:
        return self._load().get(key)


def _warn_if_world_readable(path: Path) -> None:
    """Emit a warning when a credential file is readable by group or others."""
    try:
        mode = path.stat().st_mode
    except OSError:
        return
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        import warnings

        warnings.warn(
            f'Credential file {path} is accessible to other users (mode {stat.filemode(mode)}). Run: chmod 600 {path}',
            stacklevel=3,
        )


class SecretStore:
    """An ordered chain of providers, first hit wins.

    Resolved values are cached for the lifetime of the store; call
    :meth:`clear_cache` after rotating a secret.
    """

    def __init__(self, providers: Iterable[SecretProvider] | None = None):
        self.providers: list[SecretProvider] = list(providers or [EnvProvider()])
        self._cache: dict[str, Secret | None] = {}

    @classmethod
    def default(cls, credentials_dir: str | os.PathLike[str] | None = None, prompt: bool = False) -> SecretStore:
        """The conventional chain: env -> .env -> ~/.credentials -> OS keychain.

        ``prompt=True`` appends a :class:`PromptProvider`, so anything still
        missing is asked for interactively instead of raising.
        """
        directory = Path(credentials_dir or os.environ.get('ARROWS_CREDENTIALS_DIR', '~/.credentials')).expanduser()
        return cls(
            [
                EnvProvider(),
                DotEnvProvider('.env'),
                SecretFileProvider(directory),
                JsonFileProvider(directory / 'redshift_credentials.json'),
                JsonFileProvider(directory / 'google_token.json', wrap_key='GOOGLE_TOKEN_JSON'),
                KeyringProvider(),
                *([PromptProvider()] if prompt else []),
            ]
        )

    def add_provider(self, provider: SecretProvider, *, first: bool = False) -> SecretStore:
        """Register another provider; ``first=True`` gives it priority."""
        self.providers.insert(0, provider) if first else self.providers.append(provider)
        self._cache.clear()
        return self

    def _resolve(self, key: str) -> Secret | None:
        """First provider with a non-empty value wins."""
        for provider in self.providers:
            try:
                value = provider.get(key)
            except Exception as exc:  # one broken provider must not hide the rest
                import warnings

                warnings.warn(f'Secret provider {provider.name} failed on {key!r}: {exc}', stacklevel=2)
                continue
            if value:
                return Secret(value, key=key, source=provider.name)
        return None

    def get(self, key: str, default: str | None = None) -> Secret | None:
        if key not in self._cache:
            self._cache[key] = self._resolve(key)
        found = self._cache[key]
        if found is not None:
            return found
        return Secret(default, key=key, source='default') if default is not None else None

    def require(self, key: str, component: str | None = None) -> Secret:
        found = self.get(key)
        if found is None:
            raise MissingSecretError(key, component=component, providers=[p.name for p in self.providers])
        return found

    def require_many(self, keys: Iterable[str], component: str | None = None) -> dict[str, Secret]:
        missing: list[str] = []
        resolved: dict[str, Secret] = {}
        for key in keys:
            found = self.get(key)
            if found is None:
                missing.append(key)
            else:
                resolved[key] = found
        if missing:
            raise MissingSecretError(
                ', '.join(missing), component=component, providers=[p.name for p in self.providers]
            )
        return resolved

    def has(self, key: str) -> bool:
        return self.get(key) is not None

    def export(self, keys: Iterable[str], environ: dict[str, str] | None = None) -> None:
        """Copy secrets into the environment.

        Opt-in only: needed by third-party libraries that read ``os.environ``
        directly (boto3, DuckDB's credential chain). Every call is a deliberate
        widening of the blast radius, so keep the key list minimal.
        """
        target = os.environ if environ is None else environ
        for key in keys:
            found = self.get(key)
            if found is not None:
                target[key] = found.reveal()

    def clear_cache(self) -> None:
        self._cache.clear()
        for provider in self.providers:
            if hasattr(provider, '_cache'):
                provider._cache = None  # type: ignore[attr-defined]

    def __repr__(self) -> str:
        return f'SecretStore(providers=[{", ".join(p.name for p in self.providers)}])'
