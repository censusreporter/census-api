class Config(object):
    pass


class Production(Config):
    DATABASE_URI = 'postgresql://census:censuspassword@localhost/census'
    MEMCACHE_ADDR = ['127.0.0.1']

class Development(Config):
    DATABASE_URI = 'postgresql://census:censuspassword@localhost/census'
    MEMCACHE_ADDR = ['127.0.0.1']
