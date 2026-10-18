"""Interactive endpoints get a Postgres statement timeout; bulk endpoints don't, and a
timed-out query becomes a 503 rather than a generic 500."""
import os
import sys
from unittest import mock

os.environ.setdefault('DATABASE_URL', 'sqlite://')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6379/0')
os.environ.setdefault('CACHE_TYPE', 'NullCache')

# api.py imports GDAL (via user_geo), which isn't needed here and isn't always installed
try:
    import osgeo  # noqa: F401
except ImportError:
    sys.modules['osgeo'] = mock.MagicMock()

import pytest
from psycopg2.errors import QueryCanceled
from sqlalchemy.exc import OperationalError

from census_extractomatic import api


@pytest.fixture
def executed_sql(monkeypatch):
    statements = []
    monkeypatch.setattr(api.db.session, 'execute', lambda stmt, *a, **kw: statements.append(str(stmt)))
    return statements


@pytest.mark.parametrize('path', [
    '/1.0/data/show/latest',
    '/1.0/geo/search',
    '/2.1/full-text/search',
])
def test_interactive_paths_get_a_statement_timeout(path, executed_sql):
    with api.app.test_request_context(path):
        api.set_interactive_statement_timeout()

    assert executed_sql == ['SET LOCAL statement_timeout = 5000']


@pytest.mark.parametrize('path', [
    '/1.0/data/download/latest',
    '/1.0/aggregate/acs/latest',
    '/1.0/user_geo/import',
    '/healthcheck',
    '/robots.txt',
])
def test_bulk_and_non_db_paths_are_left_alone(path, executed_sql):
    with api.app.test_request_context(path):
        api.set_interactive_statement_timeout()

    assert executed_sql == []


def test_statement_timeout_can_be_disabled(executed_sql, monkeypatch):
    monkeypatch.setitem(api.app.config, 'INTERACTIVE_STATEMENT_TIMEOUT_MS', 0)
    with api.app.test_request_context('/1.0/data/show/latest'):
        api.set_interactive_statement_timeout()

    assert executed_sql == []


def test_timed_out_query_returns_503():
    error = mock.Mock()
    error.original_exception = OperationalError('SELECT 1', {}, QueryCanceled('canceling statement due to statement timeout'))

    with api.app.test_request_context('/1.0/data/show/latest'):
        resp = api.jsonify_error_handler(error)

    assert resp.status_code == 503
    assert 'took too long' in resp.get_json()['error']
