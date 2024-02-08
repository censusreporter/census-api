web: NEW_RELIC_CONFIG_FILE=newrelic.ini newrelic-admin run-program gunicorn --workers 3 --bind 0.0.0.0:$PORT --timeout 300 --statsd-host telegraf.web:8125 --statsd-prefix censusapi --log-level INFO  census_extractomatic.wsgi
worker: NEW_RELIC_CONFIG_FILE=newrelic.ini newrelic-admin run-program celery -A census_extractomatic.user_geo:celery_app worker
