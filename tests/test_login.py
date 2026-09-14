"""Interactive secret entry, as used from a notebook."""

import getpass

import pytest

from arrows.core import registry
from arrows.core.component import Component, ComponentSpec
from arrows.core.secrets import MappingProvider, PromptProvider, SecretStore
from arrows.core.session import Session


class NeedsTwo(Component):
    name = 'needs_two'
    requires = ('ALPHA', 'BETA')
    optional = ('GAMMA',)

    def setup(self, secrets):
        self.alpha = self.require_secret('ALPHA')
        self.beta = self.require_secret('BETA')


@pytest.fixture(autouse=True)
def _registered():
    registry.register(ComponentSpec(name='needs_two', module=__name__, attr='NeedsTwo'), replace=True)
    yield
    registry.unregister('needs_two')


@pytest.fixture
def answers(monkeypatch):
    """Pretend to be a notebook, and script what the user types."""
    monkeypatch.setattr('arrows.core.secrets.is_interactive', lambda: True)
    monkeypatch.setattr('arrows.core.session.is_interactive', lambda: True)
    asked: list[str] = []
    queue: list[str] = []

    def fake_getpass(prompt=''):
        asked.append(prompt.strip(': '))
        return queue.pop(0) if queue else ''

    monkeypatch.setattr(getpass, 'getpass', fake_getpass)
    return {'asked': asked, 'queue': queue}


def test_login_asks_only_for_what_is_missing(answers):
    answers['queue'].extend(['typed-beta'])
    session = Session(secrets=SecretStore([MappingProvider({'ALPHA': 'preset'}, name='preset')]))
    session.login('needs_two')
    assert answers['asked'] == ['BETA']
    assert session.get('needs_two').alpha == 'preset'
    assert session.get('needs_two').beta == 'typed-beta'
    session.close()


def test_prompted_values_do_not_override_configured_ones(answers):
    """A prompt fills a gap; it must not shadow real configuration."""
    answers['queue'].extend(['typed-beta'])
    store = SecretStore([MappingProvider({'ALPHA': 'preset'}, name='preset')])
    session = Session(secrets=store)
    session.login('needs_two')
    assert store.get('ALPHA').source == 'preset'
    assert store.get('BETA').source == 'prompt-answers'
    session.close()


def test_optional_keys_are_asked_only_on_request(answers):
    answers['queue'].extend(['a', 'b', 'g'])
    session = Session(secrets=SecretStore([MappingProvider({}, name='empty')]))
    session.login('needs_two', include_optional=True)
    assert answers['asked'] == ['ALPHA', 'BETA', 'GAMMA']
    session.close()


def test_prompt_true_refuses_to_hang_a_non_interactive_process(monkeypatch):
    monkeypatch.setattr('arrows.core.session.is_interactive', lambda: False)
    session = Session(secrets=SecretStore([MappingProvider({}, name='empty')]))
    with pytest.raises(RuntimeError, match='interactive'):
        session.login('needs_two', prompt=True)


def test_non_interactive_login_reports_the_missing_key_instead_of_prompting(monkeypatch):
    """A script gets a precise error, not a prompt nobody can answer."""
    from arrows.core.errors import ComponentSetupError

    monkeypatch.setattr('arrows.core.session.is_interactive', lambda: False)
    session = Session(secrets=SecretStore([MappingProvider({}, name='empty')]))
    with pytest.raises(ComponentSetupError) as excinfo:
        session.login('needs_two', ALPHA='given')
    assert 'BETA' in str(excinfo.value.cause)


# -- credentials passed in code ------------------------------------------------


def test_credentials_can_be_passed_in_code(monkeypatch):
    monkeypatch.setattr('arrows.core.session.is_interactive', lambda: False)
    session = Session(secrets=SecretStore([MappingProvider({}, name='empty')]))
    session.login('needs_two', ALPHA='a', BETA='b')
    assert session.get('needs_two').alpha == 'a'
    session.close()


def test_code_beats_the_environment(monkeypatch):
    """An explicit argument is an instruction, so it outranks ambient config."""
    monkeypatch.setattr('arrows.core.session.is_interactive', lambda: False)
    store = SecretStore([MappingProvider({'ALPHA': 'from-env', 'BETA': 'from-env'}, name='env-ish')])
    session = Session(secrets=store)
    session.login('needs_two', ALPHA='explicit')
    assert session.get('needs_two').alpha == 'explicit'
    assert store.get('BETA').reveal() == 'from-env'
    session.close()


def test_logging_in_again_replaces_a_loaded_component(monkeypatch):
    monkeypatch.setattr('arrows.core.session.is_interactive', lambda: False)
    session = Session(secrets=SecretStore([MappingProvider({}, name='empty')]))
    session.login('needs_two', ALPHA='first', BETA='b')
    assert session.get('needs_two').alpha == 'first'
    session.login('needs_two', ALPHA='second')
    assert session.get('needs_two').alpha == 'second'
    session.close()


def test_friendly_names_map_to_secret_keys(monkeypatch):
    monkeypatch.setattr('arrows.core.session.is_interactive', lambda: False)
    session = Session(secrets=SecretStore([MappingProvider({}, name='empty')]))
    session.login(
        'redshift',
        host='h',
        database='dev',
        user='analyst',
        password='pw',
        port='5439',
        access_key_id='k',
        secret_access_key='s',
    )
    component = session.get('redshift')
    assert (component.host, component.user, component.port) == ('h', 'analyst', 5439)
    assert session.get('aws').boto3_session.get_credentials().access_key == 'k'
    session.close()


def test_an_unknown_keyword_names_the_valid_ones(monkeypatch):
    monkeypatch.setattr('arrows.core.session.is_interactive', lambda: False)
    session = Session(secrets=SecretStore([MappingProvider({}, name='empty')]))
    with pytest.raises(TypeError, match='Unknown credential'):
        session.login('redshift', passwrd='typo')


def test_module_level_login_helpers(monkeypatch):
    monkeypatch.setattr('arrows.core.session.is_interactive', lambda: False)
    import arrows

    session = Session(secrets=SecretStore([MappingProvider({}, name='empty')]))
    with session.activate():
        arrows.redshift.login(host='h', database='dev', user='u', password='p')
        assert arrows.get('redshift').host == 'h'
    session.close()


def test_prompt_provider_is_inert_without_a_human(monkeypatch):
    monkeypatch.setattr('arrows.core.secrets.is_interactive', lambda: False)
    assert PromptProvider().get('ANYTHING') is None


def test_prompt_provider_asks_once(answers):
    provider = PromptProvider()
    answers['queue'].extend(['value'])
    assert provider.get('KEY') == 'value'
    assert provider.get('KEY') == 'value'
    assert answers['asked'] == ['KEY']


def test_a_json_key_can_be_answered_with_a_file_path(answers, tmp_path):
    path = tmp_path / 'token.json'
    path.write_text('{"refresh_token": "r"}')
    path.chmod(0o600)
    answers['queue'].extend([str(path)])
    assert PromptProvider().get('GOOGLE_TOKEN_JSON') == '{"refresh_token": "r"}'


def test_redshift_asks_for_a_password_when_there_is_no_cluster_identifier():
    from arrows.components.redshift import RedshiftComponent

    empty = SecretStore([MappingProvider({}, name='empty')])
    assert 'REDSHIFT_PASSWORD' in RedshiftComponent().missing_secrets(empty)

    with_cluster = SecretStore([MappingProvider({'REDSHIFT_CLUSTER_IDENTIFIER': 'c'}, name='m')])
    assert 'REDSHIFT_PASSWORD' not in RedshiftComponent().missing_secrets(with_cluster)
