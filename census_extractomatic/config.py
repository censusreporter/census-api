class Config(object):
    pass


class Production(Config):
    SQLALCHEMY_DATABASE_URI = 'postgresql://census:censuspassword@localhost/census'
    MEMCACHE_ADDR = ['127.0.0.1']
    ELASTICSEARCH_HOST = ['localhost:9200']
    JSONIFY_PRETTYPRINT_REGULAR = False
    MAX_GEOIDS_TO_SHOW = 3500
    MAX_GEOIDS_TO_DOWNLOAD = 3500


class Development(Config):
    # For local dev, tunnel to the DB first:
    # ssh -i ~/.ssh/censusreporter.ec2_key.pem -L 5432:localhost:5432 -L 9200:localhost:9200 ubuntu@staging.censusreporter.org
    SQLALCHEMY_DATABASE_URI = 'postgresql://census:censuspassword@localhost/census'
    MEMCACHE_ADDR = ['127.0.0.1']
    ELASTICSEARCH_HOST = ['localhost:9200']
    JSONIFY_PRETTYPRINT_REGULAR = False
