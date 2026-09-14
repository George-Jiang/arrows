"""Every S3 path must use the credentials the ``aws`` component resolved.

The failure these guard against is quiet and asymmetric: a library left to find
its own credentials (Polars builds its own ``boto3.Session``, pyarrow its own
filesystem) cannot see the arrows SecretStore, so a DuckDB read succeeds while a
Polars read fails on a stale profile. The second half of the problem is time —
credentials handed over once stop working when they expire, so they have to be
fetched at the moment of use.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from arrows import s3
from arrows.core.engine import reset_duckdb, set_duckdb
from arrows.core.secrets import MappingProvider, SecretStore
from arrows.core.session import Session


class FakeFrozenCredentials:
    def __init__(self, access_key: str, secret_key: str, token: str | None):
        self.access_key = access_key
        self.secret_key = secret_key
        self.token = token


class FakeCredentials:
    """Stands in for botocore's refreshable credentials.

    Like the real thing, the current keys are only visible through
    ``get_frozen_credentials()``, and they can change between calls.
    """

    def __init__(self) -> None:
        self.frozen = FakeFrozenCredentials('AKIA-first', 'secret-first', 'token-first')
        self._expiry_time = None
        self.calls = 0

    def get_frozen_credentials(self) -> FakeFrozenCredentials:
        self.calls += 1
        return self.frozen

    def rotate(self) -> None:
        self.frozen = FakeFrozenCredentials('AKIA-second', 'secret-second', 'token-second')


class FakeBoto3Session:
    def __init__(self, credentials: FakeCredentials):
        self._credentials = credentials

    def get_credentials(self) -> FakeCredentials:
        return self._credentials


class FakeConnection:
    """A DuckDB stand-in that only records what was executed."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str):
        self.statements.append(sql)
        return self

    def from_parquet(self, glob: str):
        self.statements.append(f'read_parquet({glob})')
        return self

    def secrets(self) -> list[str]:
        return [s for s in self.statements if 'SECRET arrows_s3' in s]


@pytest.fixture
def loaded(monkeypatch):
    """An s3 component whose AWS identity is a controllable fake."""
    connection = FakeConnection()
    set_duckdb(connection)
    secrets = SecretStore(
        [
            MappingProvider(
                {
                    'AWS_ACCESS_KEY_ID': 'AKIA-first',
                    'AWS_SECRET_ACCESS_KEY': 'secret-first',
                    'AWS_SESSION_TOKEN': 'token-first',
                    'AWS_REGION': 'us-east-1',
                    'ARROWS_DEFAULT_BUCKET': 'demo-bucket',
                },
                name='m',
            )
        ]
    )
    session = Session(secrets=secrets)
    credentials = FakeCredentials()
    with session.activate():
        session.load('s3')
        session.get('aws').boto3_session = FakeBoto3Session(credentials)
        yield session, credentials, connection
    session.close()
    reset_duckdb()


# -- Polars ----------------------------------------------------------------
def test_to_polars_passes_the_component_credentials(loaded, monkeypatch):
    """Without this, Polars resolves credentials from a session of its own."""
    import polars as pl

    captured: dict = {}

    def fake_read_parquet(source, **kwargs):
        captured.update(source=source, **kwargs)
        return 'frame'

    monkeypatch.setattr(pl, 'read_parquet', fake_read_parquet)

    assert s3.get_dataset('s3://demo-bucket/data/').to_polars() == 'frame'

    provider = captured['credential_provider']
    assert provider is not None, 'Polars was left to find its own credentials'
    values, _expiry = provider()
    assert values == {
        'aws_access_key_id': 'AKIA-first',
        'aws_secret_access_key': 'secret-first',
        'aws_session_token': 'token-first',
    }
    assert captured['storage_options'] == {'aws_region': 'us-east-1'}


def test_the_polars_provider_re_reads_credentials_on_every_call(loaded, monkeypatch):
    """A provider that captured its keys once would break when they rotate."""
    import polars as pl

    captured: dict = {}
    monkeypatch.setattr(pl, 'scan_parquet', lambda source, **kwargs: captured.update(kwargs) or 'lazy')

    _session, credentials, _connection = loaded
    s3.get_dataset('s3://demo-bucket/data/').to_polars(lazy=True)
    provider = captured['credential_provider']

    assert provider()[0]['aws_access_key_id'] == 'AKIA-first'
    credentials.rotate()
    assert provider()[0]['aws_access_key_id'] == 'AKIA-second'


def test_from_polars_passes_credentials_too(loaded, monkeypatch):
    """Also guards the partitioning API: it is built for real here.

    `PartitionMaxSize` was removed in the Polars release this package requires,
    which broke this method outright — an AttributeError before any credential
    was ever consulted.
    """
    import polars as pl

    captured: dict = {}
    monkeypatch.setattr(
        pl.DataFrame, 'write_parquet', lambda self, target, **kwargs: captured.update(target=target, **kwargs)
    )
    monkeypatch.setattr(s3.S3Dataset, 'clear_contents', lambda self: None)

    s3.get_dataset('s3://demo-bucket/data/').from_polars(pl.DataFrame({'a': [1]}))

    assert isinstance(captured['target'], pl.PartitionBy)
    assert captured['credential_provider'] is not None
    assert captured['storage_options'] == {'aws_region': 'us-east-1'}


# -- pyarrow ---------------------------------------------------------------
def test_to_arrow_reads_through_the_component_filesystem(loaded, monkeypatch):
    """pyarrow would otherwise build a filesystem from its own credential chain."""
    import pyarrow.parquet as pq

    captured: dict = {}

    def fake_read_table(source, **kwargs):
        captured.update(source=source, **kwargs)
        return pa.table({'a': [1]})

    monkeypatch.setattr(pq, 'read_table', fake_read_table)

    session, _credentials, _connection = loaded
    s3.get_dataset('s3://demo-bucket/data/').to_arrow()

    assert captured['filesystem'] is session.get('s3').filesystem
    # pyarrow does not strip the scheme when given a filesystem: passing the URI
    # would address a key literally named "s3:".
    assert captured['source'] == 'demo-bucket/data/'


def test_from_arrow_writes_through_the_component_filesystem(loaded, monkeypatch):
    import pyarrow.parquet as pq

    captured: dict = {}
    monkeypatch.setattr(
        pq, 'write_to_dataset', lambda table, root_path, **kwargs: captured.update(root_path=root_path, **kwargs)
    )
    monkeypatch.setattr(s3.S3Dataset, 'clear_contents', lambda self: None)

    session, _credentials, _connection = loaded
    s3.get_dataset('s3://demo-bucket/data/').from_arrow(pa.table({'a': [1]}))

    assert captured['filesystem'] is session.get('s3').filesystem
    assert captured['root_path'] == 'demo-bucket/data/'


def test_the_filesystem_is_rebuilt_when_credentials_rotate(loaded):
    """pyarrow copies the keys at construction and never refreshes them."""
    session, credentials, _connection = loaded
    component = session.get('s3')

    first = component.filesystem
    assert component.filesystem is first, 'unchanged credentials should not rebuild'

    credentials.rotate()
    assert component.filesystem is not first


# -- DuckDB ----------------------------------------------------------------
def test_the_duckdb_secret_follows_rotated_credentials(loaded):
    """Explicit keys are a snapshot inside DuckDB; they have to be refreshed."""
    _session, credentials, connection = loaded
    before = len(connection.secrets())

    s3.get_dataset('s3://demo-bucket/data/').to_duckdb()
    assert len(connection.secrets()) == before, 'unchanged credentials should not re-register'

    credentials.rotate()
    s3.get_dataset('s3://demo-bucket/data/').to_duckdb()
    assert connection.secrets()[-1].count('AKIA-second') == 1
