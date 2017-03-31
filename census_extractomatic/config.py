import os


class Config(object):
    SENTRY_DSN = os.environ.get('SENTRY_DSN')


class Production(Config):
    SQLALCHEMY_DATABASE_URI = 'postgresql://census:censuspassword@censusreporter.c7wefhiuybfb.us-east-1.rds.amazonaws.com:5432/census'
    MEMCACHE_ADDR = ['127.0.0.1']
    ELASTICSEARCH_HOST = ['127.0.0.1:9200']
    JSONIFY_PRETTYPRINT_REGULAR = False
    MAX_GEOIDS_TO_SHOW = 3500
    MAX_GEOIDS_TO_DOWNLOAD = 3500


class Development(Config):
    # For local dev, tunnel to the DB first:
    # ssh -i ~/.ssh/censusreporter.ec2_key.pem -L 5432:censusreporter.c7wefhiuybfb.us-east-1.rds.amazonaws.com:5432 ubuntu@52.71.251.119
    # SQLALCHEMY_DATABASE_URI = 'postgresql://census:censuspassword@localhost/census'
    SQLALCHEMY_DATABASE_URI = 'postgresql://census:censuspassword@localhost:5433/census'
    MEMCACHE_ADDR = ['127.0.0.1']
    ELASTICSEARCH_HOST = ['127.0.0.1:9200']
    JSONIFY_PRETTYPRINT_REGULAR = False
