import newrelic.agent
newrelic.agent.initialize('newrelic.ini')

from census_extractomatic.api import app as application

if __name__ == "__main__":
    application.run()
