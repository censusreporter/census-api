"""Regression test for the redis-py/Redis-server version incompatibility that took
down production on 2026-07-28 (census-api#98).

The deployed Redis server is v5.0.7, which predates Redis 6.0 and doesn't support
the HELLO command or ACL-style `AUTH user pass`. redis-py >= 6 tries to negotiate
protocol version via HELLO whenever the connection URL includes a username (ours
does), and fails outright against this server rather than falling back. This test
runs a real redis:5.0.7 container (not a mock) so it actually exercises that
protocol behavior, and exercises the app's real flask_caching -> cachelib -> redis-py
integration path rather than a bare redis-py client.

Requires Docker to be available; fails (not skips) if it isn't, since this is the
only way to genuinely catch this class of bug.
"""
import shutil
import socket
import subprocess
import time
import tomllib
from pathlib import Path

import pytest
from flask import Flask
from flask_caching import Cache

REDIS_IMAGE = 'redis:5.0.7'
REDIS_PASSWORD = 'testpassword'


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


@pytest.fixture(scope='module')
def redis_url():
    if shutil.which('docker') is None:
        pytest.fail(
            'docker is required to run this test (it validates real redis-py '
            'wire-protocol behavior against a real Redis 5.0.7 server, which a '
            'mock cannot reproduce)'
        )

    port = _free_port()
    container_id = subprocess.run(
        [
            'docker', 'run', '-d', '--rm',
            '-p', f'{port}:6379',
            REDIS_IMAGE,
            'redis-server', '--requirepass', REDIS_PASSWORD,
        ],
        capture_output=True, text=True, check=True,
    ).stdout.strip()

    try:
        for _ in range(30):
            ping = subprocess.run(
                ['docker', 'exec', container_id, 'redis-cli', '-a', REDIS_PASSWORD,
                 '--no-auth-warning', 'ping'],
                capture_output=True, text=True,
            )
            if ping.stdout.strip() == 'PONG':
                break
            time.sleep(0.5)
        else:
            pytest.fail('redis:5.0.7 container did not become ready in time')

        # Matches the shape of the real production REDIS_URL (dokku's redis
        # plugin sets a username ahead of the password even though this Redis
        # version has no concept of named users).
        yield f'redis://censusreporter:{REDIS_PASSWORD}@127.0.0.1:{port}/0'
    finally:
        subprocess.run(['docker', 'stop', container_id], capture_output=True)


def test_cache_get_set_against_real_redis_5_0_7(redis_url):
    app = Flask(__name__)
    app.config['CACHE_TYPE'] = 'redis'
    app.config['CACHE_REDIS_URL'] = redis_url
    cache = Cache(app)

    with app.app_context():
        assert cache.get('missing-key') is None
        cache.set('some-key', {'geoid': '16000US1714000'})
        assert cache.get('some-key') == {'geoid': '16000US1714000'}


def test_redis_stays_explicitly_pinned():
    pipfile = tomllib.loads(
        (Path(__file__).parent.parent / 'Pipfile').read_text()
    )
    version = pipfile['packages']['redis']
    assert version != '*', (
        'redis is unpinned again - this silently broke production once already '
        '(see test_cache_get_set_against_real_redis_5_0_7); pin it explicitly'
    )
