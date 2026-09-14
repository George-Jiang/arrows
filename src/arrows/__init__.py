"""arrows — an Arrow-native ETL toolkit with pluggable components.

Nothing heavy is imported at module load. Components (and the third-party
libraries they need) are pulled in the first time they are used::

    import arrows

    arrows.load('s3', 'redshift')          # load exactly what this job needs
    arrow = arrows.redshift.fetch_arrow('select 1')

``arrows.list_components()`` shows what is available, including components
contributed by other packages through the ``arrows.components`` entry point.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

# Registering the built-in specs costs nothing: only metadata is imported here.
from . import components as _components  # noqa: F401
from .core.component import Component, ComponentSpec, HealthStatus
from .core.errors import (
    ArrowsError,
    ComponentNotFoundError,
    ComponentNotLoadedError,
    ComponentSetupError,
    MissingDependencyError,
    MissingSecretError,
)
from .core.registry import ENTRY_POINT_GROUP, register, specs
from .core.secrets import PromptProvider, Secret, SecretStore
from .core.session import (
    Session,
    close,
    configure,
    default_session,
    get,
    health,
    is_loaded,
    load,
    login,
    unload,
    use_session,
)

__version__ = '0.2.0'

#: Submodules exposed as attributes but imported on first access.
_LAZY_MODULES = {
    'auth',
    'gmail',
    'google_sheets',
    'redshift',
    's3',
    'sqlite',
    'template_renderer',
    'utils',
}

if TYPE_CHECKING:  # give editors and type checkers the real modules
    from . import auth, gmail, google_sheets, redshift, s3, sqlite, template_renderer, utils

__all__ = [
    'ENTRY_POINT_GROUP',
    'ArrowsError',
    'Component',
    'ComponentNotFoundError',
    'ComponentNotLoadedError',
    'ComponentSetupError',
    'ComponentSpec',
    'HealthStatus',
    'MissingDependencyError',
    'MissingSecretError',
    'PromptProvider',
    'Secret',
    'SecretStore',
    'Session',
    'auth',
    'close',
    'configure',
    'default_session',
    'get',
    'gmail',
    'google_sheets',
    'health',
    'is_loaded',
    'list_components',
    'load',
    'load_credentials',
    'login',
    'redshift',
    'register',
    's3',
    'sqlite',
    'template_renderer',
    'unload',
    'use_session',
    'utils',
]


def __getattr__(name: str) -> Any:
    """PEP 562 lazy submodule import.

    ``arrows.redshift`` works without ``import arrows`` dragging in psycopg2,
    boto3, awswrangler and the Google client for every process that only wanted
    to send an email.
    """
    if name in _LAZY_MODULES:
        module = importlib.import_module(f'.{name}', __name__)
        globals()[name] = module
        return module
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')


def __dir__() -> list[str]:
    return sorted(set(__all__) | set(globals()))


def list_components() -> list[dict[str, Any]]:
    """Describe every registered component: name, dependencies, load state."""
    session = default_session()
    return [
        {
            'name': spec.name,
            'summary': spec.summary,
            'depends_on': list(spec.depends_on),
            'install_hint': spec.install_hint,
            'loaded': session.is_loaded(spec.name),
        }
        for spec in sorted(specs().values(), key=lambda s: s.name)
    ]


def load_credentials(*names: str) -> Session:
    """Deprecated alias for :func:`load`.

    The old behaviour — loading AWS, Redshift and Google unconditionally — is
    kept only when called with no arguments, and warns.
    """
    import warnings

    if not names:
        warnings.warn(
            'load_credentials() without arguments loads every component and fails if any '
            "credential is missing. Prefer arrows.load('s3', 'redshift').",
            DeprecationWarning,
            stacklevel=2,
        )
        names = ('s3', 'redshift', 'google')
    return load(*names)
