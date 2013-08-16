class Config(object):
    pass


class Production(Config):
    DATABASE_URI = 'postgresql://census:censuspassword@localhost/postgres'


class Development(Config):
    DATABASE_URI = 'postgresql://census:censuspassword@staging.censusreporter.org/postgres'
