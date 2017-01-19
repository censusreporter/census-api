### Update Postgres full text index

```
# This takes several minutes on production, but probably less than five.
PGPASSWORD=*** psql -h censusreporter.c7wefhiuybfb.us-east-1.rds.amazonaws.com -U census -d census < /home/www-data/api.censusreporter.org_app/full-text-search/metadata_script.sql

source /home/www-data/api.censusreporter.org_venv/bin/activate

PGHOST="censusreporter.c7wefhiuybfb.us-east-1.rds.amazonaws.com" PGPASSWORD=*** python /home/www-data/api.censusreporter.org_app/full-text-search/topic_scraper.py
```

TODO: figure out how to handle the thesaurus property
