FROM python:3.8

MAINTAINER Ian Dees "ian.dees@gmail.com"

RUN apt-get -qq update && \
    apt-get install -qq -y \
        gdal-bin \
        libgdal-dev \
        python-dev \
        pipenv && \
    rm -rf /var/lib/apt/lists/*

ADD Pipfile Pipfile
ADD Pipfile.lock Pipfile.lock
RUN CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal \
    pipenv install --system --deploy --ignore-pipfile

ADD . .

CMD gunicorn --workers 3 --bind 0.0.0.0:$PORT census_extractomatic.wsgi
