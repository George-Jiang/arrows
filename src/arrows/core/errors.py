"""Exception hierarchy for arrows.

Every error raised by arrows inherits from :class:`ArrowsError`, so callers can
catch the whole library with a single ``except``.
"""

from __future__ import annotations


class ArrowsError(Exception):
    """Base class for all arrows errors."""


class ComponentNotFoundError(ArrowsError, KeyError):
    """Raised when an unknown component name is requested."""

    def __init__(self, name: str, available: list[str] | None = None):
        self.name = name
        self.available = sorted(available or [])
        hint = f' Available components: {", ".join(self.available)}.' if self.available else ''
        super().__init__(f'Unknown component {name!r}.{hint}')


class ComponentNotLoadedError(ArrowsError, RuntimeError):
    """Raised when a component is used before it has been loaded."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(
            f'Component {name!r} is not loaded. '
            f'Call arrows.load({name!r}) first, or enable autoload '
            f'(arrows.configure(autoload=True) / ARROWS_AUTOLOAD=1).'
        )


class MissingDependencyError(ArrowsError, ImportError):
    """Raised when a library a component needs is not importable."""

    def __init__(self, component: str, module: str | None = None, hint: str = '', via: str | None = None):
        self.component = component
        self.module = module
        self.hint = hint
        self.via = via
        what = f'Module {module!r} is required by' if module else 'Missing dependencies for'
        # `via` is the component the caller actually asked for, which is rarely
        # the one whose import failed.
        owner = f'component {component!r}' + (f' (a dependency of {via!r})' if via else '')
        # Every built-in component's libraries are hard dependencies of arrows,
        # so a failure here means an incomplete environment. A third-party
        # component supplies its own hint.
        remedy = hint or 'pip install --force-reinstall arrows'
        super().__init__(f'{what} {owner} but is not installed. Try: {remedy}')


class MissingSecretError(ArrowsError, LookupError):
    """Raised when a required secret cannot be resolved by any provider."""

    def __init__(self, key: str, component: str | None = None, providers: list[str] | None = None):
        self.key = key
        self.component = component
        self.providers = providers or []
        owner = f' (required by component {component!r})' if component else ''
        tried = f' Providers tried: {", ".join(self.providers)}.' if self.providers else ''
        super().__init__(f'Secret {key!r} not found{owner}.{tried}')


class ComponentSetupError(ArrowsError, RuntimeError):
    """Raised when a component fails to initialise."""

    def __init__(self, name: str, cause: BaseException):
        self.name = name
        self.cause = cause
        super().__init__(f'Failed to set up component {name!r}: {cause}')
