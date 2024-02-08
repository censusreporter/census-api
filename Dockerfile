FROM python:3.11

RUN apt-get -qq update && \
    apt-get install -qq -y \
        gdal-bin \
        libgdal-dev \
        python3-dev \
        supervisor \
        pipenv && \
    rm -rf /var/lib/apt/lists/*

ADD Pipfile Pipfile
ADD Pipfile.lock Pipfile.lock
RUN CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal \
    pipenv install --system --deploy --ignore-pipfile

ADD . .

CMD NEW_RELIC_CONFIG_FILE=newrelic.ini newrelic-admin run-program gunicorn --workers 3 --bind 0.0.0.0:$PORT --statsd-host telegraf.web:8125 --statsd-prefix censusapi census_extractomatic.wsgi
