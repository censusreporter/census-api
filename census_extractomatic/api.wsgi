import sys

activate_this = '/home/www-data/api_venv/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

import newrelic.agent
newrelic.agent.initialize('/home/www-data/api_app/newrelic.ini')

from census_extractomatic.api import app as application