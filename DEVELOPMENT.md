Hacking on census-api
---------------------
The Census Reporter team welcomes contributions from the community. We'll admit that at this time we haven't set up a very simple path to local development on the API side of the application.

This document will hopefully evolve into a simple cookbook to getting your local system set up for development.

Setting up for local development
================================

Services
--------
The Census API code depends on access to a PostgreSQL database, an ElasticSearch server, and a memcache server. Census Reporter developers set up an SSH tunnel to a staging server running in the AWS cloud rather than running local postgres and elastic search.

### PostgreSQL

The easiest way to get a PostgreSQL database like ours up and running is to set one up in Amazon Web Services, [following these instructions](http://censusreporter.tumblr.com/post/55886690087/using-census-data-in-postgresql). It might be nicer to run it locally, but we haven't made that so easy yet. You can load the ACS data from [these dumps](http://censusreporter.tumblr.com/post/73727555158/easier-access-to-acs-data) but there are a few other meta-tables which aren't dumped yet. We'll try to sort that out. For most development, you could probably get away with just loading the most recent 1-year or 1- and 3-year. The 5-year data takes over 160GB uncompressed, and for most purposes, the API code should probably behave just fine if that data is missing.

Access to your local database is configured in [`census_extractomatic/config.py`](https://github.com/censusreporter/census-api/blob/master/census_extractomatic/config.py#L16) If you change this, please be careful not to include those changes in any pull requests.


Python1
------
Here's what you need to know to get a local version of the Census Reporter API up and running. These instructions assume you're using <a href="http://virtualenv.readthedocs.org/en/latest/">virtualenv</a> and <a href="http://virtualenvwrapper.readthedocs.org/en/latest/">virtualenvwrapper</a> to manage your development environments.

First, clone this repository to your machine and move into your new project directory:

    >> git clone git@github.com:censusreporter/census-api.git
    >> cd <your cloned repo dir>

Create the virtual environment for your local project, activate it and install the required libraries:

    >> mkvirtualenv census-api --no-site-packages
    >> workon census-api
    >> pip install -r requirements.txt

If you've upgraded XCode on OS X Mavericks, you may well see some compilation errors here. If so, try this:

    >> ARCHFLAGS=-Wno-error=unused-command-line-argument-hard-error-in-future pip install -r requirements.txt


Running
=======
Make sure the database is running.

Then from the root of your local copy of the repository, run

    >> flask run

This starts Flask running locally, on port 5000. If everything is configured correctly, you should be able to load a URL like `http://localhost:5000/1.0/latest/16000US1714000/profile` and see JSON data. If not, [file an issue in this repository](https://github.com/censusreporter/census-api/issues) and we'll try to help you and improve this document.


Celery
======
In August, 2021, we added a feature to support user-uploaded geographies and data aggregation to those geographies. Because this process is time-intensive, we introduced Celery to support making that process asynchronous.  

If you aren't working on that feature, you can probably ignore the rest; however, if you are, you must have Celery running before maps are uploaded, or they not be completely processed, and there will be no sign of that until you go trying to debug a problem.

To run Celery locally, you must also have Redis running, as Celery uses Redis. Details about installing and running Redis are outside the scope of this documentation. Make sure you add a local environment variable, `REDIS_URL` with the appropriate value; if you're sticking with all defaults, `redis://` should serve.  (In the simplest case, running redis might just require `redis-server`)


To run Celery, in the active python environment for this project, run
`celery -A census_extractomatic.user_geo:celery_app worker`

When Redis and Celery are both running, you should find that you can upload maps and see their status (`aggregation.user_geodata.status` in Postgres) move from `NEW` to `READY` without your intervention.
