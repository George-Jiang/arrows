"""End-to-end check of the reference component, no credentials needed."""

import pyarrow as pa
import pytest

from arrows import sqlite
from arrows.core.secrets import MappingProvider, SecretStore
from arrows.core.session import Session


@pytest.fixture
def database(tmp_path):
    path = tmp_path / 'demo.db'
    session = Session(secrets=SecretStore([MappingProvider({'SQLITE_DATABASE': str(path)}, name='m')]))
    with session.activate():
        session.load('sqlite')
        yield path
    session.close()


def test_round_trip(database):
    table = pa.table({'id': [1, 2, 3], 'name': ['a', 'b', 'c']})
    sqlite.arrow_to_sqlite(table, 'people', mode='overwrite')
    result = sqlite.fetch_arrow('select name from people where id > {{ threshold }}', threshold=1)
    assert result.to_pydict() == {'name': ['b', 'c']}


def test_health_check(database):
    from arrows.core.session import default_session

    status = default_session().get('sqlite').health_check()
    assert status.ok
