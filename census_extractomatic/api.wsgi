import sys

activate_this = '/home/www-data/api_venv/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

from census_extractomatic.api import app as application