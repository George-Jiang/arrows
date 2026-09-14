import json
import os

import pytest

from arrows.core.errors import MissingSecretError
from arrows.core.secrets import (
    DotEnvProvider,
    EnvProvider,
    JsonFileProvider,
    MappingProvider,
    Secret,
    SecretFileProvider,
    SecretStore,
)


def test_secret_never_renders_its_value():
    secret = Secret('hunter2', key='PASSWORD', source='env')
    assert str(secret) == '***'
    assert 'hunter2' not in repr(secret)
    assert 'hunter2' not in f'value={secret}'
    assert secret.reveal() == 'hunter2'


def test_secret_compares_without_leaking():
    assert Secret('a') == Secret('a')
    assert Secret('a') == 'a'
    assert Secret('a') != Secret('b')


def test_chain_order_first_hit_wins():
    store = SecretStore(
        [
            MappingProvider({'KEY': 'first'}, name='one'),
            MappingProvider({'KEY': 'second', 'OTHER': 'x'}, name='two'),
        ]
    )
    assert store.get('KEY').reveal() == 'first'
    assert store.get('KEY').source == 'one'
    assert store.get('OTHER').reveal() == 'x'


def test_missing_secret_lists_providers():
    store = SecretStore([MappingProvider({}, name='one')])
    with pytest.raises(MissingSecretError) as excinfo:
        store.require('NOPE', component='demo')
    assert 'demo' in str(excinfo.value)
    assert 'one' in str(excinfo.value)


def test_env_provider_supports_a_namespace(monkeypatch):
    monkeypatch.setenv('ACME_TOKEN', 'value')
    assert EnvProvider(prefix='ACME_').get('TOKEN') == 'value'
    assert EnvProvider().get('TOKEN') is None


def test_dotenv_provider(tmp_path):
    path = tmp_path / '.env'
    path.write_text('# comment\nexport A=1\nB="two"\nbroken\n')
    provider = DotEnvProvider(path)
    assert provider.get('A') == '1'
    assert provider.get('B') == 'two'
    assert provider.get('broken') is None


def test_json_file_provider_flat_and_wrapped(tmp_path):
    flat = tmp_path / 'flat.json'
    flat.write_text(json.dumps({'REDSHIFT_HOST': 'h'}))
    assert JsonFileProvider(flat).get('REDSHIFT_HOST') == 'h'

    token = tmp_path / 'token.json'
    token.write_text(json.dumps({'refresh_token': 'r'}))
    wrapped = JsonFileProvider(token, wrap_key='GOOGLE_TOKEN_JSON').get('GOOGLE_TOKEN_JSON')
    assert json.loads(wrapped)['refresh_token'] == 'r'


def test_secret_file_provider(tmp_path):
    (tmp_path / 'API_KEY').write_text('abc\n')
    assert SecretFileProvider(tmp_path).get('API_KEY') == 'abc'
    assert SecretFileProvider(tmp_path).get('MISSING') is None


def test_world_readable_credentials_warn(tmp_path):
    path = tmp_path / 'creds.json'
    path.write_text('{"A": "b"}')
    path.chmod(0o644)
    with pytest.warns(UserWarning, match='chmod 600'):
        JsonFileProvider(path).get('A')


def test_secrets_are_not_exported_to_the_environment_by_default(monkeypatch):
    monkeypatch.delenv('SECRET_X', raising=False)
    store = SecretStore([MappingProvider({'SECRET_X': 'v'}, name='m')])
    store.get('SECRET_X')
    assert 'SECRET_X' not in os.environ

    environ: dict[str, str] = {}
    store.export(['SECRET_X'], environ)
    assert environ == {'SECRET_X': 'v'}


def test_broken_provider_does_not_break_resolution():
    class Broken:
        name = 'broken'

        def get(self, key):
            raise RuntimeError('backend locked')

    store = SecretStore([Broken(), MappingProvider({'KEY': 'ok'}, name='m')])
    with pytest.warns(UserWarning):
        assert store.get('KEY').reveal() == 'ok'
