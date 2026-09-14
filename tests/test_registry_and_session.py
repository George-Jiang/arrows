import pytest

from arrows.core import registry
from arrows.core.component import Component, ComponentSpec, HealthStatus
from arrows.core.errors import ComponentNotFoundError, ComponentNotLoadedError, ComponentSetupError
from arrows.core.secrets import MappingProvider, SecretStore
from arrows.core.session import Session

EVENTS: list[str] = []


class FakeComponent(Component):
    name = 'fake'
    requires = ('FAKE_TOKEN',)

    def setup(self, secrets):
        EVENTS.append('setup:fake')
        self.token = self.require_secret('FAKE_TOKEN')

    def close(self):
        EVENTS.append('close:fake')

    def health_check(self):
        return HealthStatus(self.name, True, 'fine')


class DependentComponent(Component):
    name = 'dependent'
    depends_on = ('fake',)

    def setup(self, secrets):
        EVENTS.append('setup:dependent')


@pytest.fixture(autouse=True)
def _registered():
    EVENTS.clear()
    registry.register(ComponentSpec(name='fake', module=__name__, attr='FakeComponent'), replace=True)
    registry.register(
        ComponentSpec(name='dependent', module=__name__, attr='DependentComponent', depends_on=('fake',)),
        replace=True,
    )
    yield
    registry.unregister('fake')
    registry.unregister('dependent')


@pytest.fixture
def session():
    session = Session(secrets=SecretStore([MappingProvider({'FAKE_TOKEN': 't'}, name='m')]))
    yield session
    session.close()


def test_unknown_component_names_the_alternatives():
    with pytest.raises(ComponentNotFoundError) as excinfo:
        registry.get_spec('nope')
    assert 'fake' in str(excinfo.value)


def test_registering_twice_requires_replace():
    with pytest.raises(ValueError, match='already registered'):
        registry.register(ComponentSpec(name='fake', module=__name__, attr='FakeComponent'))


def test_only_requested_components_are_loaded(session):
    session.load('fake')
    assert session.loaded == ['fake']
    assert 'dependent' not in session


def test_dependencies_load_before_dependents(session):
    session.load('dependent')
    assert EVENTS == ['setup:fake', 'setup:dependent']
    assert session.loaded == ['dependent', 'fake']


def test_load_is_idempotent(session):
    session.load('fake')
    session.load('fake')
    assert EVENTS.count('setup:fake') == 1


def test_autoload_off_fails_fast(session):
    session.config.autoload = False
    with pytest.raises(ComponentNotLoadedError):
        session.get('fake')


def test_autoload_on_loads_on_demand(session):
    assert session.get('fake').token == 't'


def test_setup_failure_is_wrapped_with_the_component_name():
    session = Session(secrets=SecretStore([MappingProvider({}, name='empty')]))
    with pytest.raises(ComponentSetupError) as excinfo:
        session.load('fake')
    assert excinfo.value.name == 'fake'
    assert 'FAKE_TOKEN' in str(excinfo.value.cause)


def test_non_strict_load_skips_the_broken_component():
    session = Session(secrets=SecretStore([MappingProvider({}, name='empty')]))
    with pytest.warns(UserWarning, match='Skipping component'):
        session.load('fake', strict=False)
    assert session.loaded == []


def test_close_releases_components(session):
    session.load('fake')
    session.close()
    assert 'close:fake' in EVENTS
    assert session.loaded == []


def test_sessions_are_isolated():
    one = Session(secrets=SecretStore([MappingProvider({'FAKE_TOKEN': 'one'}, name='a')]))
    two = Session(secrets=SecretStore([MappingProvider({'FAKE_TOKEN': 'two'}, name='b')]))
    try:
        assert one.get('fake').token == 'one'
        assert two.get('fake').token == 'two'
    finally:
        one.close()
        two.close()


def test_session_is_a_context_manager():
    with Session(secrets=SecretStore([MappingProvider({'FAKE_TOKEN': 't'}, name='m')])) as session:
        session.load('fake')
    assert 'close:fake' in EVENTS


def test_health_reports_every_loaded_component(session):
    session.load('fake')
    assert [(s.name, s.ok) for s in session.health()] == [('fake', True)]


def test_activate_redirects_the_module_level_api():
    from arrows.core.session import default_session

    session = Session(secrets=SecretStore([MappingProvider({'FAKE_TOKEN': 'scoped'}, name='m')]))
    outer = default_session()
    with session.activate() as active:
        assert default_session() is active
        assert active.get('fake').token == 'scoped'
    assert default_session() is outer
    session.close()


def test_entry_point_plugins_are_discovered(monkeypatch):
    """A third-party package registers a component without arrows knowing it."""
    from importlib.metadata import EntryPoint

    from arrows.core import registry as reg

    registry.register(ComponentSpec(name='plugin', module=__name__, attr='DependentComponent'), replace=True)
    plugin_spec = reg.get_spec('plugin')
    registry.unregister('plugin')

    entry_point = EntryPoint(name='plugin', value=f'{__name__}:PLUGIN_SPEC', group=reg.ENTRY_POINT_GROUP)
    monkeypatch.setattr(reg, '_discovered', False)
    monkeypatch.setattr('arrows.core.registry.entry_points', lambda group: [entry_point], raising=False)
    monkeypatch.setattr('importlib.metadata.entry_points', lambda group=None: [entry_point])
    globals()['PLUGIN_SPEC'] = plugin_spec

    reg.discover()
    try:
        assert reg.get_spec('plugin').name == 'plugin'
    finally:
        registry.unregister('plugin')


def test_aliases_resolve_to_the_canonical_name():
    assert registry.get_spec('sheets').name == 'google_sheets'


def test_a_dependencys_missing_library_points_at_the_requested_component():
    """Asking for 'plugin' must report 'plugin', not the dependency that failed."""
    from arrows.core.errors import MissingDependencyError

    class NeedsAbsentLibrary(Component):
        name = 'absent'

        def setup(self, secrets):
            self.import_module('a_library_that_is_not_installed', hint='pip install arrows-absent')

    class Dependent(Component):
        name = 'dependent_on_absent'
        depends_on = ('absent',)

    registry.register(ComponentSpec(name='absent', module=__name__, attr='NeedsAbsentLibrary'), replace=True)
    registry.register(
        ComponentSpec(
            name='dependent_on_absent', module=__name__, attr='Dependent',
            depends_on=('absent',), install_hint='pip install arrows-dependent',
        ),
        replace=True,
    )
    globals().update(NeedsAbsentLibrary=NeedsAbsentLibrary, Dependent=Dependent)

    session = Session(secrets=SecretStore([MappingProvider({}, name='empty')]))
    try:
        with pytest.raises(MissingDependencyError) as excinfo:
            session.load('dependent_on_absent')
        assert 'pip install arrows-dependent' in str(excinfo.value)
        assert "dependency of 'dependent_on_absent'" in str(excinfo.value)
    finally:
        registry.unregister('absent')
        registry.unregister('dependent_on_absent')
        session.close()


def test_a_builtin_missing_library_says_the_install_is_incomplete():
    from arrows.core.errors import MissingDependencyError

    error = MissingDependencyError('redshift', 'psycopg2')
    assert 'force-reinstall arrows' in str(error)
    assert 'arrows[' not in str(error)
