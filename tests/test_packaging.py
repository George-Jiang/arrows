"""`pip install arrows` must be the whole installation.

There are no extras: every component's libraries are hard dependencies. These
tests keep it that way, so nobody reintroduces a half-installed state by
accident.
"""

import re
import tomllib
from pathlib import Path

import pytest

PYPROJECT = Path(__file__).resolve().parent.parent / 'pyproject.toml'

#: Libraries each built-in component imports, and therefore must be installed.
COMPONENT_LIBRARIES = {
    'boto3',
    'duckdb',
    'jinja2',
    'pyarrow',
    'psycopg2-binary',
    'awswrangler',
    'adbc-driver-postgresql',
    'google-api-python-client',
    'google-auth',
}


def _name(requirement: str) -> str:
    """Normalised distribution name of a PEP 508 requirement."""
    bare = re.split(r'[<>=!~\[;\s]', requirement.strip(), maxsplit=1)[0]
    return bare.lower().replace('_', '-')


@pytest.fixture(scope='module')
def project():
    return tomllib.loads(PYPROJECT.read_text())['project']


def test_there_are_no_extras(project):
    """An extra would imply a slimmer install that no longer exists."""
    assert not project.get('optional-dependencies'), (
        'Extras are gone: every component ships by default. Reintroducing one '
        'would make `pip install arrows[x]` promise a slim install it cannot give.'
    )


def test_every_builtin_component_library_is_installed_by_default(project):
    installed = {_name(requirement) for requirement in project['dependencies']}
    missing = COMPONENT_LIBRARIES - installed
    assert not missing, f'built-in components import {sorted(missing)}, absent from [project.dependencies]'


def test_builtin_components_do_not_advertise_an_install_hint():
    """Their libraries ship with arrows, so there is nothing to suggest installing."""
    from arrows.core.registry import specs

    for spec in specs().values():
        assert not spec.install_hint, f'built-in {spec.name!r} should not need an install hint'
