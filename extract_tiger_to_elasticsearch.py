"""
Extracts TIGER geo information from the Postgres database, translates it
into something that makes sense for Elasticsearch, and loads it into Elasticsearch.

To use this, open up a tunnel to the API server (assuming you have the SSH key):

    ssh -i ~/.ssh/censusreporter.ec2_key.pem -L 5432:localhost:5432 -L 9200:localhost:9200 ubuntu@censusreporter.org

(Port 5432 is for Postgres and 9200 is for Elasticsearch)

If you need to, install the dependencies for this repo:
    mkvirtualenv --no-site-packages census-api
    pip install -r requirements.txt

Then run this script to perform the load:

    python extract_tiger_to_elasticsearch.py

You can then test the results by curling directly against the Elasticsearch HTTP search endpoint:

    curl http://localhost:9200/tiger2012/geo/_search -d '
    {
        "query": {
            "term": { "names": "spokane" }
        }
    }
    '
"""

import logging
import itertools
import sys
import pyes
import psycopg2
import psycopg2.extras
from tiger_queries import *

logging.basicConfig()

def grouper(n, iterable):
    it = iter(iterable)
    while True:
       chunk = tuple(itertools.islice(it, n))
       if not chunk:
           return
       yield chunk

def convert_rows(rows):
    for row in rows:
        lat = row.pop('lat', None)
        lon = row.pop('lon', None)
        row['location'] = [lon, lat]

        # row['name_suggest'] = {
        #     'input': row.get('names'),
        #     'output': row.get('display_name'),
        #     'payload': row.get('full_geoid'),
        #     'weight': int(round(row.get('importance'))) if row.get('importance') else 0,
        # }

        yield row

def process_single_sumlev(cur, es):

    for obj in convert_rows(cur):
        es.index(obj, index='tiger2012', doc_type='geo', id=obj['full_geoid'], bulk=True)

    es.force_bulk()

def main():
    conn = psycopg2.connect(
        host='localhost',
        user='census',
        password='censuspassword',
        database='census'
    )
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    es = pyes.ES()
    index_manager = pyes.managers.Indices(es)

    print "Deleting old index..."
    index_manager.delete_index_if_exists('tiger2012')
    print "Creating new index..."
    index_manager.create_index('tiger2012', {
        "settings": {
            "analysis": {
                "analyzer": {
                    "synonym_ngram_analyzer": {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": [
                            "synonym_filter",
                            "ngram_filter",
                            "stop",
                            "lowercase",
                        ],
                    },
                    "synonym_analyzer": {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": [
                            "synonym_filter",
                            "stop",
                            "lowercase",
                        ],
                    },
                },
                "filter": {
                    "ngram_filter": {
                        "type": "nGram",
                        "min_gram": 2,
                        "max_gram": 20,
                        "token_chars": [ "letter", "digit" ],
                    },
                    "synonym_filter": {
                        "type": "synonym",
                        "expand": True,
                        "synonyms": GEO_SYNONYMS
                    }
                }
            }
        },
        "mappings": {
            "geo": {
                "properties": {
                    "names": {
                        "type": "string",
                        "index_analyzer": "synonym_ngram_analyzer",
                        "search_analyzer": "synonym_analyzer",
                    },
                    "location": {
                        "type": "geo_point"
                    }
                }
            }
        }
    })

    QUERIES = (
        ('States', '040', STATE_QUERY),
        ('Places', '160', PLACE_QUERY),
        ('Counties', '050', COUNTY_QUERY),
        # ('County subdivisions', '060', COUSUB_QUERY),
        # ('ZCTAs', '860', ZCTA_QUERY),
        # ('CBSAs', '310', CBSA_QUERY),
        # ('Congressional districts', '500', CONGRESS_QUERY),
        # ('Combined Statistical Areas', '330', CSA_QUERY),
        # ('elementary school districts', '950', ESD_QUERY),
        # ('secondary school districts', '960', SSD_QUERY),
        # ('PUMA', '795', PUMA_QUERY),
        # ('state legislative (lower)', '620', SLDL_QUERY),
        # ('state legislative (upper)', '610', SLDU_QUERY),
        # ('AIANNH', '250', AIANNH_QUERY),
        # ('AITS', '251', AITS_QUERY),
        # ('ANRC', '230', ANRC_QUERY),
        # ('block groups', '150', BG_QUERY),
        # ('CNECTA', '335', CNECTA_QUERY),
        # ('Consolidated cities', '170', CONCIT_QUERY),
        # ('metro divisions', '314', METDIV_QUERY),
        # ('NECTA', '350', NECTA_QUERY),
        # ('NECTA divisions', '355', NECTA_DIV_QUERY),
        # ('SUBMCD', '067', SUBMCD_QUERY),
        # ('Tribal Block Group', '258', TBG_QUERY),
        # ('Tribal Tract', '256', TTRACT_QUERY),
        # ('Census tract', '140', TRACT_QUERY),
        # ('UAC', '400', UAC_QUERY),
        # ('Unified School District', '970', UNSD_QUERY),
        # ('country', '010', US_QUERY),
        # 020 regions, 030 divisions are handled separately below

)

    for label, sumlev, q in QUERIES:
        print "Loading %s" % label
        cur.execute(q)
        process_single_sumlev(cur, es)


    es.update('tiger2012', 'geo', '16000US3651000',
        document={ 
            "doc": {
                "names": [ 
                    "New York, New York", 
                    "New York City, New York", 
                    "NYC, New York" 
                ]
            }
        }
    )

    # custom = [{
    #     'names': ['new york city, ny'],
    #     'display_name': 'New York, NY',
    #     'sumlev': '160',
    #     'importance': 200.5,
    #     'geoid': '3651000',
    #     'full_geoid': '16000US3651000',
    #     'population': '8199221',
    #     'aland': 783934135,
    #     'awater': 429462763,
    #     'lat': 40.6642738,
    #     'lon': -73.9385004
    # }]
    # process_single_sumlev(custom, es)




    sys.exit(1)

    q = """SELECT
        'New England Division',
        'New England Division',
        '030',
        '',
        '03000US1',
        310, (SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid = '03000US1'),
        (SELECT SUM(aland) FROM tiger2012.state WHERE division='1'),
        (SELECT SUM(awater) FROM tiger2012.state WHERE division='1'),
        (SELECT ST_Union(the_geom) FROM tiger2012.census_name_lookup WHERE full_geoid IN ('04000US09', '04000US23', '04000US25', '04000US33', '04000US44', '04000US50'));"""
    process_single_sumlev(cur, es, q)


    q = """SELECT
        'Middle Atlantic Division',
        'Middle Atlantic Division',
        '030',
        '',
        '03000US2',
        310, (SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid = '03000US2'),
        (SELECT SUM(aland) FROM tiger2012.state WHERE division='2'),
        (SELECT SUM(awater) FROM tiger2012.state WHERE division='2'),
        (SELECT ST_Multi(ST_Union(the_geom)) FROM tiger2012.census_name_lookup WHERE full_geoid IN ('04000US34', '04000US36', '04000US42'));"""
    process_single_sumlev(cur, es, q)


    q = """SELECT
        'East North Central Division',
        'East North Central Division',
        '030',
        '',
        '03000US3',
        310, (SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid = '03000US3'),
        (SELECT SUM(aland) FROM tiger2012.state WHERE division='3'),
        (SELECT SUM(awater) FROM tiger2012.state WHERE division='3'),
        (SELECT ST_Multi(ST_Union(the_geom)) FROM tiger2012.census_name_lookup WHERE full_geoid IN ('04000US18', '04000US17', '04000US26', '04000US39', '04000US55'));"""
    process_single_sumlev(cur, es, q)


    q = """SELECT
        'West North Central Division',
        'West North Central Division',
        '030',
        '',
        '03000US4',
        310, (SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid = '03000US4'),
        (SELECT SUM(aland) FROM tiger2012.state WHERE division='4'),
        (SELECT SUM(awater) FROM tiger2012.state WHERE division='4'),
        (SELECT ST_Multi(ST_Union(the_geom)) FROM tiger2012.census_name_lookup WHERE full_geoid IN ('04000US19', '04000US20', '04000US27', '04000US29', '04000US31', '04000US38', '04000US46'));"""
    process_single_sumlev(cur, es, q)


    q = """SELECT
        'South Atlantic Division',
        'South Atlantic Division',
        '030',
        '',
        '03000US5',
        310, (SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid = '03000US5'),
        (SELECT SUM(aland) FROM tiger2012.state WHERE division='5'),
        (SELECT SUM(awater) FROM tiger2012.state WHERE division='5'),
        (SELECT ST_Multi(ST_Union(the_geom)) FROM tiger2012.census_name_lookup WHERE full_geoid IN ('04000US10', '04000US11', '04000US12', '04000US13', '04000US24', '04000US37', '04000US45', '04000US51', '04000US54'));"""
    process_single_sumlev(cur, es, q)


    q = """SELECT
        'East South Central Division',
        'East South Central Division',
        '030',
        '',
        '03000US6',
        310, (SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid = '03000US6'),
        (SELECT SUM(aland) FROM tiger2012.state WHERE division='6'),
        (SELECT SUM(awater) FROM tiger2012.state WHERE division='6'),
        (SELECT ST_Multi(ST_Union(the_geom)) FROM tiger2012.census_name_lookup WHERE full_geoid IN ('04000US01', '04000US21', '04000US28', '04000US47'));"""
    process_single_sumlev(cur, es, q)


    q = """SELECT
        'West South Central Division',
        'West South Central Division',
        '030',
        '',
        '03000US7',
        310, (SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid = '03000US7'),
        (SELECT SUM(aland) FROM tiger2012.state WHERE division='7'),
        (SELECT SUM(awater) FROM tiger2012.state WHERE division='7'),
        (SELECT ST_Multi(ST_Union(the_geom)) FROM tiger2012.census_name_lookup WHERE full_geoid IN ('04000US05', '04000US22', '04000US40', '04000US48'));"""
    process_single_sumlev(cur, es, q)


    q = """SELECT
        'Mountain Division',
        'Mountain Division',
        '030',
        '',
        '03000US8',
        310, (SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid = '03000US8'),
        (SELECT SUM(aland) FROM tiger2012.state WHERE division='8'),
        (SELECT SUM(awater) FROM tiger2012.state WHERE division='8'),
        (SELECT ST_Multi(ST_Union(the_geom)) FROM tiger2012.census_name_lookup WHERE full_geoid IN ('04000US04', '04000US08', '04000US16', '04000US35', '04000US30', '04000US49', '04000US32', '04000US56'));"""
    process_single_sumlev(cur, es, q)


    q = """SELECT
        'Pacific Division',
        'Pacific Division',
        '030',
        '',
        '03000US9',
        310, (SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid = '03000US9'),
        (SELECT SUM(aland) FROM tiger2012.state WHERE division='9'),
        (SELECT SUM(awater) FROM tiger2012.state WHERE division='9'),
        (SELECT ST_Multi(ST_Union(the_geom)) FROM tiger2012.census_name_lookup WHERE full_geoid IN ('04000US02', '04000US06', '04000US15', '04000US41', '04000US53'));"""
    process_single_sumlev(cur, es, q)


    q = """SELECT
        'Northeast Region',
        'Northeast Region',
        '020',
        '',
        '02000US1',
        320, (SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid = '02000US1'),
        (SELECT SUM(aland) FROM tiger2012.state WHERE region='1'),
        (SELECT SUM(awater) FROM tiger2012.state WHERE region='1'),
        (SELECT ST_Multi(ST_Union(the_geom)) FROM tiger2012.census_name_lookup WHERE full_geoid IN ('03000US1', '03000US2'));"""
    process_single_sumlev(cur, es, q)


    q = """SELECT
        'Midwest Region',
        'Midwest Region',
        '020',
        '',
        '02000US2',
        320, (SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid = '02000US2'),
        (SELECT SUM(aland) FROM tiger2012.state WHERE region='2'),
        (SELECT SUM(awater) FROM tiger2012.state WHERE region='2'),
        (SELECT ST_Multi(ST_Union(the_geom)) FROM tiger2012.census_name_lookup WHERE full_geoid IN ('03000US3', '03000US4'));"""
    process_single_sumlev(cur, es, q)


    q = """SELECT
        'South Region',
        'South Region',
        '020',
        '',
        '02000US3',
        320, (SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid = '02000US3'),
        (SELECT SUM(aland) FROM tiger2012.state WHERE region='3'),
        (SELECT SUM(awater) FROM tiger2012.state WHERE region='3'),
        (SELECT ST_Multi(ST_Union(the_geom)) FROM tiger2012.census_name_lookup WHERE full_geoid IN ('03000US5', '03000US6', '03000US7'));"""
    process_single_sumlev(cur, es, q)


    q = """SELECT
        'West Region',
        'West Region',
        '020',
        '',
        '02000US4',
        320, (SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid = '02000US4'),
        (SELECT SUM(aland) FROM tiger2012.state WHERE region='4'),
        (SELECT SUM(awater) FROM tiger2012.state WHERE region='4'),
        (SELECT ST_Multi(ST_Union(the_geom)) FROM tiger2012.census_name_lookup WHERE full_geoid IN ('03000US8', '03000US9'));"""
    process_single_sumlev(cur, es, q)

if __name__ == '__main__':
    main()
