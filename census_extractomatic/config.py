class Config(object):
    pass


class Production(Config):
    DATABASE_URI = 'postgresql://census:censuspassword@localhost/census'


class Development(Config):
    DATABASE_URI = 'postgresql://census:censuspassword@staging.censusreporter.org/census'
