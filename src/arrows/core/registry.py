"""Component registry and third-party discovery.

Built-in components register themselves in ``arrows.components``. Anything
outside the repo plugs in through the ``arrows.components`` entry-point group::

    # in a third-party package's pyproject.toml
    [project.entry-points."arrows.components"]
    clickhouse = "arrows_clickhouse:SPEC"

The entry point must resolve to a :class:`~arrows.core.component.ComponentSpec`
(or a zero-argument callable returning one). Discovery is lazy and runs once.
"""

from __future__ import annotations

from collections.abc import Iterator

from .component import ComponentSpec
from .errors import ComponentNotFoundError

__all__ = ['ENTRY_POINT_GROUP', 'discover', 'get_spec', 'names', 'register', 'specs', 'unregister']

ENTRY_POINT_GROUP = 'arrows.components'

_SPECS: dict[str, ComponentSpec] = {}
_ALIASES: dict[str, str] = {}
_discovered = False


def register(spec: ComponentSpec, *, replace: bool = False) -> ComponentSpec:
    """Add a component to the registry.

    Re-registering the same name requires ``replace=True``, so a typo in a
    plugin cannot silently shadow a built-in component.
    """
    if spec.name in _SPECS and not replace:
        raise ValueError(f'Component {spec.name!r} is already registered; pass replace=True to override.')
    _SPECS[spec.name] = spec
    for alias in spec.aliases:
        _ALIASES[alias] = spec.name
    return spec


def unregister(name: str) -> None:
    _SPECS.pop(name, None)
    for alias, target in list(_ALIASES.items()):
        if target == name:
            _ALIASES.pop(alias)


def discover() -> None:
    """Load third-party components advertised through entry points (once)."""
    global _discovered
    if _discovered:
        return
    _discovered = True

    from importlib.metadata import entry_points

    for entry_point in entry_points(group=ENTRY_POINT_GROUP):
        try:
            loaded = entry_point.load()
            spec = loaded() if callable(loaded) and not isinstance(loaded, ComponentSpec) else loaded
            if isinstance(spec, ComponentSpec):
                register(spec, replace=False)
        except Exception as exc:  # a broken plugin must not break the library
            import warnings

            warnings.warn(f'Failed to load arrows component plugin {entry_point.name!r}: {exc}', stacklevel=2)


def get_spec(name: str) -> ComponentSpec:
    discover()
    key = _ALIASES.get(name, name)
    if key not in _SPECS:
        raise ComponentNotFoundError(name, list(_SPECS))
    return _SPECS[key]


def specs() -> dict[str, ComponentSpec]:
    discover()
    return dict(_SPECS)


def names() -> list[str]:
    return sorted(specs())


def __iter__() -> Iterator[ComponentSpec]:  # pragma: no cover - convenience only
    return iter(specs().values())
