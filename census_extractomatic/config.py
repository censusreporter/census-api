class Config(object):
    pass


class Production(Config):
    DATABASE_URI = 'postgresql://census:censuspassword@localhost/postgres'


class Development(Config):
    DATABASE_URI = 'postgresql://census:censuspassword@ec2-75-101-221-29.compute-1.amazonaws.com/postgres'
