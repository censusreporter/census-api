class Config(object):
    pass


class Production(Config):
    DATABASE_URI = 'postgresql://census:censuspassword@localhost/census'
    MEMCACHE_ADDR = ['127.0.0.1']
    ELASTICSEARCH_HOST = ['localhost:9200']
    JSONIFY_PRETTYPRINT_REGULAR = False

class Development(Config):
    # For local dev, tunnel to the DB first:
    # ssh -i ~/.ssh/censusreporter.ec2_key.pem -L 5432:localhost:5432 -L 9200:localhost:9200 ubuntu@staging.censusreporter.org
    DATABASE_URI = 'postgresql://census:censuspassword@localhost/census'
    MEMCACHE_ADDR = ['127.0.0.1']
    ELASTICSEARCH_HOST = ['localhost:9200']
    JSONIFY_PRETTYPRINT_REGULAR = False
