"""expand_geoids should stop as soon as a request exceeds the geoid limit, instead of
running every remaining group expansion (and then repeating them for the next release)."""
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
from werkzeug.exceptions import BadRequest

from census_extractomatic import api


@pytest.fixture
def expansion_calls(monkeypatch):
    calls = []

    def fake_get_child_geoids(release, parent_geoid, child_summary_level):
        calls.append(child_summary_level)
        return [{'geoid': '%s00US%05d' % (child_summary_level, i)} for i in range(6)]

    monkeypatch.setattr(api, 'get_child_geoids', fake_get_child_geoids)
    return calls


def test_stops_expanding_once_over_the_limit(expansion_calls):
    groups = ['040|01000US', '050|01000US', '140|01000US', '150|01000US']
    with api.app.app_context(), pytest.raises(BadRequest):
        api.expand_geoids(groups, 'acs2024_5yr', max_geoids=10)

    # 6 + 6 geoids passes the limit of 10 after the second group
    assert expansion_calls == ['040', '050']


def test_repeated_groups_are_only_expanded_once(expansion_calls):
    with api.app.app_context():
        valid_geo_ids, child_parent_map = api.expand_geoids(['040|01000US'] * 5, 'acs2024_5yr')

    assert expansion_calls == ['040']
    assert len(valid_geo_ids) == 6


def test_known_large_expansion_is_rejected_without_querying(expansion_calls):
    with api.app.app_context(), pytest.raises(BadRequest):
        api.expand_geoids(['150|01000US'], 'acs2024_5yr', max_geoids=10000)

    assert expansion_calls == []


def test_known_large_expansion_is_rejected_for_1yr_releases_too(expansion_calls):
    with api.app.app_context(), pytest.raises(BadRequest):
        api.expand_geoids(['150|01000US'], 'acs2024_1yr', max_geoids=10000)

    assert expansion_calls == []


def test_known_large_expansion_under_a_higher_limit_is_expanded(expansion_calls):
    with api.app.app_context():
        api.expand_geoids(['150|04000US17'], 'acs2024_5yr', max_geoids=20000)

    assert expansion_calls == ['150']
