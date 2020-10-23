import os


class Config(object):
    SENTRY_DSN = os.environ.get('SENTRY_DSN')
    MAX_GEOIDS_TO_SHOW = 3500
    MAX_GEOIDS_TO_DOWNLOAD = 3500
    CENSUS_REPORTER_URL_ROOT = 'https://censusreporter.org'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    BYPASS_CACHE = False
    CACHE_TYPE = os.environ.get('CACHE_TYPE', 'null')
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')


class Production(Config):
    JSONIFY_PRETTYPRINT_REGULAR = False
    CACHE_REDIS_URL = os.environ.get('REDIS_URL')


class Development(Config):
    # Maybe change for local dev:
    CENSUS_REPORTER_URL_ROOT = 'http://localhost:8000'

    MEMCACHE_ADDR = ['127.0.0.1']
    JSONIFY_PRETTYPRINT_REGULAR = False
    BYPASS_CACHE=True
