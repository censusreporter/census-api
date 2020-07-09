FROM python:2.7.10

MAINTAINER Ian Dees "ian.dees@gmail.com"

RUN apt-get -qq update && \
    apt-get install -qq -y \
        gdal-bin \
        libgdal-dev \
        python-dev \
        python-gdal \
        libmemcached-dev && \
    rm -rf /var/lib/apt/lists/*

ADD . /census-api

WORKDIR /census-api

RUN pip install --upgrade pip
RUN CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal \
    pip install -r requirements.txt gunicorn

CMD gunicorn --workers 3 --bind 0.0.0.0:$PORT census_extractomatic.wsgi
