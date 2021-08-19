web: gunicorn --workers 3 --bind 0.0.0.0:$PORT --timeout 300 --statsd-host telegraf.web:8125 --statsd-prefix censusapi --log-file /var/log/gunicorn.log --log-level INFO  census_extractomatic.wsgi
worker: celery -A census_extractomatic.user_geo:celery_app worker
