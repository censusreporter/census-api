web: gunicorn --workers 3 --bind 0.0.0.0:$PORT --statsd-host telegraf.web:8125 --statsd-prefix censusapi census_extractomatic.wsgi
worker: celery -A census_extractomatic.user_geo:celery_app worker
