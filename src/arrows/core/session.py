"""The session: what is loaded, with which secrets.

``Session`` owns component instances and their lifecycle. A module-level default
session backs the ``arrows.load()`` / ``arrows.get()`` shortcuts, while explicit
``Session`` objects let a process talk to two environments at once (e.g. staging
and prod credentials side by side) without global state.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from . import registry
from .component import Component, HealthStatus
from .config import Config
from .errors import ComponentNotLoadedError, ComponentSetupError, MissingDependencyError
from .secrets import MappingProvider, PromptProvider, SecretStore, is_interactive

__all__ = ['Session', 'close', 'configure', 'default_session', 'get', 'health', 'is_loaded', 'load', 'unload']


class Session:
    """A set of loaded components sharing one :class:`SecretStore`."""

    def __init__(self, secrets: SecretStore | None = None, config: Config | None = None):
        self.config = config or Config.from_env()
        self.secrets = secrets or SecretStore.default(self.config.credentials_dir)
        self._components: dict[str, Component] = {}

    # -- loading -----------------------------------------------------------
    def load(self, *names: str, strict: bool = True) -> Session:
        """Load the named components (dependencies first).

        With ``strict=False`` a component that cannot be set up is skipped with a
        warning instead of raising — useful for a shared notebook profile where
        only some credentials are available.
        """
        requested = _flatten(names) or self.config.components
        for name in requested:
            try:
                self._load_one(name)
            except Exception as exc:
                if strict:
                    raise
                import warnings

                warnings.warn(f'Skipping component {name!r}: {exc}', stacklevel=2)
        return self

    def _load_one(self, name: str, _loading: frozenset[str] = frozenset()) -> Component:
        spec = registry.get_spec(name)
        existing = self._components.get(spec.name)
        if existing is not None:
            return existing
        if spec.name in _loading:
            raise ComponentSetupError(spec.name, RuntimeError(f'circular dependency via {sorted(_loading)}'))

        component = spec.build()
        for dependency in (*spec.depends_on, *component.depends_on):
            try:
                self._load_one(dependency, _loading | {spec.name})
            except (MissingDependencyError, ComponentSetupError) as exc:
                missing = _as_missing_dependency(exc)
                if missing is None:
                    raise
                # Name the component that was asked for: it is rarely the one
                # whose import failed, and its hint is the useful one.
                raise MissingDependencyError(
                    missing.component,
                    missing.module,
                    hint=spec.install_hint or missing.hint,
                    via=spec.name,
                ) from exc

        component.secrets = self.secrets
        component.session = self
        try:
            component.setup(self.secrets)
        except Exception as exc:
            raise ComponentSetupError(spec.name, exc) from exc
        component._loaded = True
        self._components[spec.name] = component
        return component

    def login(
        self,
        *names: str,
        save: bool = False,
        include_optional: bool = False,
        prompt: bool | None = None,
        **credentials,
    ) -> Session:
        """Supply credentials for these components, then load them.

        Two ways to answer, and they can be mixed:

        * **In code** — ``login('redshift', user='analyst', password=...)``. A
          value passed here is a deliberate instruction, so it takes priority
          over the environment and every other provider, and re-loads a
          component that was already running with different credentials.
        * **By prompt** — anything still missing is asked for interactively,
          masked. Prompted values fill gaps only; they never shadow configuration
          that already exists. ``prompt=False`` disables this, ``prompt=True``
          requires it.

        Keywords accept a component's friendly names (see ``Component.login_args``)
        or a raw ``UPPER_CASE`` secret key. ``save=True`` writes the answers to
        the OS keychain for the next session.
        """
        requested = _with_dependencies(_flatten(names) or self.config.components)
        if not requested:
            raise ValueError('login() needs at least one component name.')

        components = {name: self._components.get(name) or registry.get_spec(name).build() for name in requested}
        explicit = _map_credentials(components, credentials)

        if explicit:
            # Explicit beats ambient: inserted at the front of the chain.
            self.secrets.add_provider(MappingProvider(explicit, name='login'), first=True)
            for name in self._affected_by(explicit_names=requested):
                self.unload(name)
            if save:
                _save_to_keyring(explicit)

        should_prompt = is_interactive() if prompt is None else prompt
        if prompt and not is_interactive():
            raise RuntimeError(
                'prompt=True needs an interactive session (a terminal or a notebook). '
                'Pass the credentials as arguments instead, or unset ARROWS_NON_INTERACTIVE.'
            )

        if should_prompt:
            asker = PromptProvider(save_to_keyring=save)
            answers: dict[str, str] = {}
            for name, component in components.items():
                wanted = list(component.missing_secrets(self.secrets))
                if include_optional:
                    wanted += [key for key in component.optional if not self.secrets.has(key)]
                if wanted:
                    print(f'{name}: enter {len(wanted)} secret(s), blank to skip')
                for key in wanted:
                    value = asker.get(key)
                    if value:
                        answers[key] = value
            if answers:
                # Appended last: a prompt fills gaps, it does not override.
                self.secrets.add_provider(MappingProvider(answers, name='prompt-answers'))

        return self.load(*requested)

    def _affected_by(self, explicit_names: Iterable[str]) -> list[str]:
        """Loaded components that must be rebuilt after a credential change.

        Includes anything that depends on them: an ``s3`` component caches a
        filesystem built from the ``aws`` component's credentials.
        """
        targets = set(explicit_names)
        growing = True
        while growing:
            growing = False
            for name, component in self._components.items():
                if name in targets:
                    continue
                dependencies = set(component.depends_on) | set(registry.get_spec(name).depends_on)
                if dependencies & targets:
                    targets.add(name)
                    growing = True
        return [name for name in targets if name in self._components]

    # -- access ------------------------------------------------------------
    def get(self, name: str) -> Component:
        """Return a loaded component, loading it on demand when ``autoload``."""
        spec = registry.get_spec(name)
        component = self._components.get(spec.name)
        if component is not None:
            return component
        if not self.config.autoload:
            raise ComponentNotLoadedError(name)
        return self._load_one(spec.name)

    def is_loaded(self, name: str) -> bool:
        try:
            return registry.get_spec(name).name in self._components
        except Exception:
            return False

    @property
    def loaded(self) -> list[str]:
        return sorted(self._components)

    def __getitem__(self, name: str) -> Component:
        return self.get(name)

    def __contains__(self, name: str) -> bool:
        return self.is_loaded(name)

    def __iter__(self) -> Iterator[Component]:
        return iter(self._components.values())

    # -- teardown ----------------------------------------------------------
    def unload(self, name: str) -> None:
        spec = registry.get_spec(name)
        component = self._components.pop(spec.name, None)
        if component is not None:
            component.close()
            component._loaded = False

    def close(self) -> None:
        for name in list(self._components):
            self.unload(name)

    def health(self) -> list[HealthStatus]:
        return [component.health_check() for component in self._components.values()]

    def activate(self):
        """Make this session the one the module-level data API talks to.

        The functions in ``arrows.s3``, ``arrows.redshift`` and friends resolve
        their component through the *default* session. Use this to point them at
        an explicit session for a block of code::

            with Session(secrets=store).activate() as session:
                session.load('s3')
                arrows.s3.get_dataset('s3://bucket/path/')
        """
        from contextlib import contextmanager

        @contextmanager
        def _activated():
            global _default
            previous = _default
            _default = self
            try:
                yield self
            finally:
                _default = previous

        return _activated()

    def __enter__(self) -> Session:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def __repr__(self) -> str:
        return f'Session(loaded={self.loaded}, autoload={self.config.autoload})'


_default: Session | None = None


def default_session() -> Session:
    """The implicit session used by the module-level helpers.

    Creating it must stay side-effect free: no credential is read and no
    component is set up here. ``ARROWS_COMPONENTS`` is applied by ``load()``
    with no arguments, so a caller can install its own SecretStore through
    :func:`configure` before anything authenticates.
    """
    global _default
    if _default is None:
        _default = Session()
    return _default


def configure(
    *,
    secrets: SecretStore | None = None,
    autoload: bool | None = None,
    credentials_dir: str | None = None,
    default_bucket: str | None = None,
    reset: bool = False,
) -> Session:
    """Configure the default session. Call before loading components."""
    global _default
    if reset and _default is not None:
        _default.close()
        _default = None
    session = default_session()
    session.config.update(autoload=autoload, credentials_dir=credentials_dir, default_bucket=default_bucket)
    if secrets is not None:
        if session.loaded:
            import warnings

            warnings.warn(
                f'Replacing the SecretStore after {session.loaded} were loaded; '
                'they keep the credentials they were set up with. '
                'Pass reset=True, or configure before loading.',
                stacklevel=2,
            )
        session.secrets = secrets
    return session


def use_session(session: Session) -> Session:
    """Install ``session`` as the process-wide default. Returns the previous one."""
    global _default
    previous = _default
    _default = session
    return previous


def load(*names: str, strict: bool = True) -> Session:
    """Load components, defaulting to whatever ``ARROWS_COMPONENTS`` names."""
    return default_session().load(*names, strict=strict)


def login(
    *names: str,
    save: bool = False,
    include_optional: bool = False,
    prompt: bool | None = None,
    **credentials,
) -> Session:
    """Supply credentials and load — see :meth:`Session.login`."""
    return default_session().login(*names, save=save, include_optional=include_optional, prompt=prompt, **credentials)


def get(name: str) -> Component:
    return default_session().get(name)


def is_loaded(name: str) -> bool:
    return default_session().is_loaded(name)


def unload(name: str) -> None:
    default_session().unload(name)


def close() -> None:
    default_session().close()


def health() -> list[HealthStatus]:
    return default_session().health()


def _as_missing_dependency(exc: BaseException) -> MissingDependencyError | None:
    """Find a MissingDependencyError inside a ComponentSetupError wrapper."""
    if isinstance(exc, MissingDependencyError):
        return exc
    cause = getattr(exc, 'cause', None)
    return cause if isinstance(cause, MissingDependencyError) else None


def _map_credentials(components: dict[str, Component], credentials: dict) -> dict[str, str]:
    """Translate friendly keyword names into secret keys.

    An ``UPPER_CASE`` keyword is taken as a secret key verbatim, which is the
    escape hatch for anything a component has not named.
    """
    resolved: dict[str, str] = {}
    for keyword, value in credentials.items():
        if value is None:
            continue
        if keyword.isupper():
            resolved[keyword] = str(value)
            continue
        owners = [component for component in components.values() if keyword in component.login_args]
        if not owners:
            known = sorted({name for component in components.values() for name in component.login_args})
            raise TypeError(
                f'Unknown credential {keyword!r} for {sorted(components)}. '
                f'Accepted: {", ".join(known) or "(none)"}, or a raw UPPER_CASE secret key.'
            )
        keys = {component.login_args[keyword] for component in owners}
        if len(keys) > 1:
            raise TypeError(
                f'Credential {keyword!r} is ambiguous across {[c.name for c in owners]}; '
                f'pass one of {sorted(keys)} directly.'
            )
        resolved[keys.pop()] = str(value)
    return resolved


def _save_to_keyring(values: dict[str, str], service: str = 'arrows') -> None:
    try:
        import keyring
    except ImportError:
        import warnings

        warnings.warn('save=True needs the keyring package: pip install "arrows[keyring]"', stacklevel=3)
        return
    for key, value in values.items():
        try:
            keyring.set_password(service, key, value)
        except Exception as exc:
            import warnings

            warnings.warn(f'Could not save {key!r} to the keyring: {exc}', stacklevel=3)


def _with_dependencies(names: Iterable[str]) -> list[str]:
    """Expand to dependencies first, so their secrets get asked for too.

    Logging in to ``s3`` should offer to collect AWS credentials, which belong to
    the ``aws`` component it depends on.
    """
    ordered: list[str] = []

    def visit(name: str, seen: frozenset[str]) -> None:
        spec = registry.get_spec(name)
        if spec.name in ordered or spec.name in seen:
            return
        for dependency in spec.depends_on:
            visit(dependency, seen | {spec.name})
        ordered.append(spec.name)

    for name in names:
        visit(name, frozenset())
    return ordered


def _flatten(names: Iterable[str | Iterable[str]]) -> list[str]:
    """Accept load('s3', 'redshift') and load(['s3', 'redshift']) alike."""
    out: list[str] = []
    for name in names:
        if isinstance(name, str):
            out.append(name)
        else:
            out.extend(name)
    return out
