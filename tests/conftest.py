import pytest

from arrows.core import SecretStore, Session
from arrows.core.secrets import MappingProvider


@pytest.fixture
def store():
    return SecretStore([MappingProvider({}, name='empty')])


@pytest.fixture
def session(store):
    session = Session(secrets=store)
    yield session
    session.close()
