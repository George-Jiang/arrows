"""The component contract.

A *component* is one pluggable integration (s3, redshift, google, ...). It owns
exactly three things: which optional dependencies it needs, which secrets it
needs, and how to turn those into a ready-to-use client. Everything else —
the data API — lives in the ordinary module next to it.

Adding a component means writing a subclass and registering a
:class:`ComponentSpec`; see ``arrows/components/sqlite.py`` for a minimal one.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from .errors import MissingDependencyError

if TYPE_CHECKING:
    from .secrets import SecretStore

__all__ = ['Component', 'ComponentSpec', 'HealthStatus']


@dataclass(frozen=True)
class HealthStatus:
    """Outcome of :meth:`Component.health_check`."""

    name: str
    ok: bool
    detail: str = ''

    def __bool__(self) -> bool:
        return self.ok


class Component:
    """Base class for a pluggable integration.

    Subclasses override :meth:`setup` (and usually :meth:`close` /
    :meth:`health_check`). ``setup`` receives the :class:`SecretStore` and must
    not be called directly — the session calls it exactly once, and guarantees
    that declared dependencies and secrets are present first.
    """

    #: Component name, unique across the registry.
    name: str = ''
    #: Secret keys that must resolve before :meth:`setup` runs.
    requires: tuple[str, ...] = ()
    #: Secret keys used when present.
    optional: tuple[str, ...] = ()
    #: Other components that must be loaded first.
    depends_on: tuple[str, ...] = ()
    #: Friendly keyword -> secret key, for passing credentials in code:
    #: ``{'password': 'REDSHIFT_PASSWORD'}`` makes ``login(password=...)`` work.
    login_args: ClassVar[dict[str, str]] = {}

    def __init__(self) -> None:
        self.secrets: SecretStore | None = None
        self.session: Any = None  # back-reference to the owning Session
        self._loaded = False

    # -- lifecycle ---------------------------------------------------------
    def setup(self, secrets: SecretStore) -> None:
        """Acquire credentials and build clients. Must be idempotent."""

    def close(self) -> None:
        """Release connections, revoke registered engine secrets."""

    def health_check(self) -> HealthStatus:
        """Cheap liveness probe; override with a real round-trip where useful."""
        return HealthStatus(self.name, self._loaded, 'loaded' if self._loaded else 'not loaded')

    def missing_secrets(self, secrets: SecretStore) -> list[str]:
        """Keys that must be supplied before :meth:`setup` can succeed.

        Defaults to the unresolved entries of ``requires``. Override when the
        requirement is a choice rather than a list — see
        :class:`~arrows.components.redshift.RedshiftComponent`, which needs a
        password *or* a cluster identifier.
        """
        return [key for key in self.requires if not secrets.has(key)]

    # -- helpers for subclasses -------------------------------------------
    @property
    def loaded(self) -> bool:
        return self._loaded

    def import_module(self, module: str, hint: str = '') -> Any:
        """Import a component's library, or raise an actionable error.

        ``hint`` is the install command to suggest; leave it empty for a
        built-in, whose libraries ship with arrows.
        """
        try:
            return importlib.import_module(module)
        except ImportError as exc:
            raise MissingDependencyError(self.name, module, hint=hint) from exc

    def secret(self, key: str, default: str | None = None) -> str | None:
        """Reveal a secret value for immediate use by a client constructor."""
        assert self.secrets is not None, 'component used before setup()'
        found = self.secrets.get(key, default)
        return found.reveal() if found is not None else None

    def require_secret(self, key: str) -> str:
        assert self.secrets is not None, 'component used before setup()'
        return self.secrets.require(key, component=self.name).reveal()

    def __repr__(self) -> str:
        state = 'loaded' if self._loaded else 'unloaded'
        return f'<{type(self).__name__} {self.name!r} {state}>'


@dataclass(frozen=True)
class ComponentSpec:
    """Registry entry describing how to build a component *without importing it*.

    ``module``/``attr`` are resolved lazily, which is what keeps
    ``import arrows`` free of boto3, psycopg2 and friends.
    """

    name: str
    module: str
    attr: str = 'COMPONENT'
    #: Install command to suggest when this component's libraries are missing.
    #: Built-ins leave it empty — their dependencies ship with arrows. A
    #: separately distributed component sets it, e.g. "pip install arrows-clickhouse".
    install_hint: str = ''
    summary: str = ''
    depends_on: tuple[str, ...] = ()
    aliases: tuple[str, ...] = field(default=())

    def build(self) -> Component:
        """Import the defining module and instantiate the component."""
        module = importlib.import_module(self.module)
        target = getattr(module, self.attr)
        component: Component = target() if isinstance(target, type) or callable(target) else target
        if not component.name:
            object.__setattr__(component, 'name', self.name)
        return component


#: Convenience alias for component factories registered via entry points.
ComponentFactory = Callable[[], Component]
