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
                        "synonyms": [
                            "us, united states",
                             "ak, alaska",
                             "al, alabama",
                             "ar, arkansas",
                             "as, american samoa",
                             "az, arizona",
                             "ca, california",
                             "co, colorado",
                             "ct, connecticut",
                             "dc, district of columbia",
                             "de, delaware",
                             "fl, florida",
                             "ga, georgia",
                             "gu, guam",
                             "hi, hawaii",
                             "ia, iowa",
                             "id, idaho",
                             "il, illinois",
                             "in, indiana",
                             "ks, kansas",
                             "ky, kentucky",
                             "la, louisiana",
                             "ma, massachusetts",
                             "md, maryland",
                             "me, maine",
                             "mi, michigan",
                             "mn, minnesota",
                             "mo, missouri",
                             "ms, mississippi",
                             "mt, montana",
                             "nc, north carolina",
                             "nd, north dakota",
                             "ne, nebraska",
                             "nh, new hampshire",
                             "nj, new jersey",
                             "nm, new mexico",
                             "nv, nevada",
                             "ny, new york",
                             "oh, ohio",
                             "ok, oklahoma",
                             "or, oregon",
                             "pa, pennsylvania",
                             "pr, puerto rico",
                             "ri, rhode island",
                             "sc, south carolina",
                             "sd, south dakota",
                             "tn, tennessee",
                             "tx, texas",
                             "ut, utah",
                             "va, virginia",
                             "vt, vermont",
                             "wa, washington",
                             "wi, wisconsin",
                             "wv, west virginia",
                             "wy, wyoming",
                        ]
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

    print "Loading states..."
    q = """SELECT
        ARRAY[
            state.name
        ] as names,
        state.name as display_name,
        '040' as sumlev,
        30 * log(b01003.b01003001 + 1) as importance,
        state.geoid as geoid,
        '04000US' || state.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (state.aland :: bigint) as aland,
        (state.awater :: bigint) as awater,
        (state.intptlat :: double precision) as lat,
        (state.intptlon :: double precision) as lon
    FROM tiger2012.state LEFT OUTER JOIN acs2012_5yr.b01003 ON (('04000US' || state.geoid) = b01003.geoid)
    WHERE state.geoid NOT IN ('60', '66', '69', '78');"""
    cur.execute(q)
    process_single_sumlev(cur, es)

    print "Loading places..."
    q = """SELECT
        ARRAY[
            place.name || ', ' || state.name
        ] as names,
        place.name || ', ' || state.stusps as display_name,
        '160' as sumlev,
        29 * log(b01003.b01003001 + 1) as importance,
        place.geoid as geoid,
        '16000US' || place.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (place.aland :: bigint) as aland,
        (place.awater :: bigint) as awater,
        (place.intptlat :: double precision) as lat,
        (place.intptlon :: double precision) as lon
    FROM tiger2012.place LEFT OUTER JOIN acs2012_5yr.b01003 ON (('16000US' || place.geoid) = b01003.geoid) JOIN tiger2012.state USING (statefp)
    WHERE state.geoid NOT IN ('60', '66', '69', '78');"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading custom places..."
    custom = [{
        'names': ['new york city, ny'],
        'display_name': 'New York, NY',
        'sumlev': '160',
        'importance': 200.5,
        'geoid': '3651000',
        'full_geoid': '16000US3651000',
        'population': '8199221',
        'aland': 783934135,
        'awater': 429462763,
        'lat': 40.6642738,
        'lon': -73.9385004
    }]
    process_single_sumlev(custom, es)


    print "Loading counties..."
    q = """SELECT
        ARRAY[
            county.namelsad,
            county.namelsad || ', ' || state.name
        ] as names,
        county.namelsad || ', ' || state.stusps as display_name,
        '050' as sumlev,
        28 * log(b01003.b01003001 + 1) as importance,
        county.geoid as geoid,
        '05000US' || county.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (county.aland :: bigint) as aland,
        (county.awater :: bigint) as awater,
        (county.intptlat :: double precision) as lat,
        (county.intptlon :: double precision) as lon
    FROM tiger2012.county LEFT OUTER JOIN acs2012_5yr.b01003 ON (('05000US' || county.geoid) = b01003.geoid) JOIN tiger2012.state USING (statefp)
    WHERE state.geoid NOT IN ('60', '66', '69', '78');"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading county subdivisions..."
    q = """SELECT
        ARRAY[
            cousub.namelsad
        ] as names,
        cousub.namelsad || ', ' || county.namelsad || ', ' || state.stusps as display_name,
        '060' as sumlev,
        27 * log(b01003.b01003001 + 1) as importance,
        cousub.geoid as geoid,
        '06000US' || cousub.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (cousub.aland :: bigint) as aland,
        (cousub.awater :: bigint) as awater,
        (cousub.intptlat :: double precision) as lat,
        (cousub.intptlon :: double precision) as lon
    FROM tiger2012.cousub LEFT OUTER JOIN acs2012_5yr.b01003 ON (('06000US' || cousub.geoid) = b01003.geoid) JOIN tiger2012.county USING (statefp, countyfp) JOIN tiger2012.state USING (statefp)
    WHERE state.geoid NOT IN ('60', '66', '69', '78');"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading ZCTA..."
    q = """SELECT
        ARRAY[
            zcta5.zcta5ce10
        ] as names,
        'ZIP ' || zcta5.zcta5ce10 as display_name,
        '860' as sumlev,
        26 * log(b01003.b01003001 + 1) as importance,
        zcta5.geoid10 as geoid,
        '86000US' || zcta5.geoid10 as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (zcta5.aland10 :: bigint) as aland,
        (zcta5.awater10 :: bigint) as awater,
        (zcta5.intptlat10 :: double precision) as lat,
        (zcta5.intptlon10 :: double precision) as lon
    FROM tiger2012.zcta5 LEFT OUTER JOIN acs2012_5yr.b01003 ON (('86000US' || zcta5.geoid10) = b01003.geoid);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading CBSA..."
    q = """SELECT
        ARRAY[
            cbsa.name
        ] as names,
        cbsa.namelsad as display_name,
        '310' as sumlev,
        25 * log(b01003.b01003001 + 1) as importance,
        cbsa.geoid as geoid,
        '31000US' || cbsa.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (cbsa.aland :: bigint) as aland,
        (cbsa.awater :: bigint) as awater,
        (cbsa.intptlat :: double precision) as lat,
        (cbsa.intptlon :: double precision) as lon
    FROM tiger2012.cbsa LEFT OUTER JOIN acs2012_5yr.b01003 ON (('31000US' || cbsa.geoid) = b01003.geoid);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading congressional districts..."
    q = """SELECT
        ARRAY[
            cd.namelsad,
            state.name || ' ' || cd.namelsad
        ] as names,
        state.name || ' ' || cd.namelsad as display_name,
        '500' as sumlev,
        24 * log(b01003.b01003001 + 1) as importance,
        cd.geoid as geoid,
        '50000US' || cd.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (cd.aland :: bigint) as aland,
        (cd.awater :: bigint) as awater,
        (cd.intptlat :: double precision) as lat,
        (cd.intptlon :: double precision) as lon
    FROM tiger2012.cd LEFT OUTER JOIN acs2012_5yr.b01003 ON (('50000US' || cd.geoid) = b01003.geoid) JOIN tiger2012.state USING (statefp)
    WHERE state.geoid NOT IN ('60', '66', '69', '78');"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading CSA..."
    q = """SELECT
        ARRAY[
            csa.name
        ] as names,
        csa.namelsad as display_name,
        '330' as sumlev,
        23 * log(b01003.b01003001 + 1) as importance,
        csa.geoid as geoid,
        '33000US' || csa.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (csa.aland :: bigint) as aland,
        (csa.awater :: bigint) as awater,
        (csa.intptlat :: double precision) as lat,
        (csa.intptlon :: double precision) as lon
    FROM tiger2012.csa LEFT OUTER JOIN acs2012_5yr.b01003 ON (('33000US' || csa.geoid) = b01003.geoid);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading elementary school districts..."
    q = """SELECT
        ARRAY[
            elsd.name
        ] as names,
        elsd.name || ', ' || state.stusps as display_name,
        '950' as sumlev,
        22 * log(b01003.b01003001 + 1) as importance,
        elsd.geoid as geoid,
        '95000US' || elsd.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (elsd.aland :: bigint) as aland,
        (elsd.awater :: bigint) as awater,
        (elsd.intptlat :: double precision) as lat,
        (elsd.intptlon :: double precision) as lon
    FROM tiger2012.elsd LEFT OUTER JOIN acs2012_5yr.b01003 ON (('95000US' || elsd.geoid) = b01003.geoid) JOIN tiger2012.state USING (statefp);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading secondary school districts..."
    q = """SELECT
        ARRAY[
            scsd.name
        ] as names,
        scsd.name || ', ' || state.stusps as display_name,
        '960' as sumlev,
        21 * log(b01003.b01003001 + 1) as importance,
        scsd.geoid as geoid,
        '96000US' || scsd.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (scsd.aland :: bigint) as aland,
        (scsd.awater :: bigint) as awater,
        (scsd.intptlat :: double precision) as lat,
        (scsd.intptlon :: double precision) as lon
    FROM tiger2012.scsd LEFT OUTER JOIN acs2012_5yr.b01003 ON (('96000US' || scsd.geoid) = b01003.geoid) JOIN tiger2012.state USING (statefp);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading PUMA..."
    q = """SELECT
        ARRAY[
            puma.namelsad10
        ] as names,
        puma.namelsad10 || ', ' || state.stusps as display_name,
        '795' as sumlev,
        20 * log(b01003.b01003001 + 1) as importance,
        puma.geoid10 as geoid,
        '79500US' || puma.geoid10 as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (puma.aland10 :: bigint) as aland,
        (puma.awater10 :: bigint) as awater,
        (puma.intptlat10 :: double precision) as lat,
        (puma.intptlon10 :: double precision) as lon
    FROM tiger2012.puma LEFT OUTER JOIN acs2012_5yr.b01003 ON (('79500US' || puma.geoid10) = b01003.geoid) JOIN tiger2012.state ON (puma.statefp10=state.statefp);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading state legislative (lower)..."
    q = """SELECT
        ARRAY[
            sldl.namelsad
        ] as names,
        sldl.namelsad || ', ' || state.stusps as display_name,
        '620' as sumlev,
        19 * log(b01003.b01003001 + 1) as importance,
        sldl.geoid as geoid,
        '62000US' || sldl.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (sldl.aland :: bigint) as aland,
        (sldl.awater :: bigint) as awater,
        (sldl.intptlat :: double precision) as lat,
        (sldl.intptlon :: double precision) as lon
    FROM tiger2012.sldl LEFT OUTER JOIN acs2012_5yr.b01003 ON (('62000US' || sldl.geoid) = b01003.geoid) JOIN tiger2012.state USING (statefp);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading state legislative (upper)..."
    q = """SELECT
        ARRAY[
            sldu.namelsad
        ] as names,
        sldu.namelsad || ', ' || state.stusps as display_name,
        '610' as sumlev,
        18 * log(b01003.b01003001 + 1) as importance,
        sldu.geoid as geoid,
        '61000US' || sldu.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (sldu.aland :: bigint) as aland,
        (sldu.awater :: bigint) as awater,
        (sldu.intptlat :: double precision) as lat,
        (sldu.intptlon :: double precision) as lon
    FROM tiger2012.sldu LEFT OUTER JOIN acs2012_5yr.b01003 ON (('61000US' || sldu.geoid) = b01003.geoid) JOIN tiger2012.state USING (statefp);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading AIANNH..."
    q = """SELECT
        ARRAY[
            aiannh.namelsad
        ] as names,
        aiannh.namelsad as display_name,
        '250' as sumlev,
        17 * log(b01003.b01003001 + 1) as importance,
        aiannh.geoid as geoid,
        '25000US' || aiannh.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (aiannh.aland :: bigint) as aland,
        (aiannh.awater :: bigint) as awater,
        (aiannh.intptlat :: double precision) as lat,
        (aiannh.intptlon :: double precision) as lon
    FROM tiger2012.aiannh LEFT OUTER JOIN acs2012_5yr.b01003 ON (('25000US' || aiannh.geoid) = b01003.geoid);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading AITS..."
    q = """SELECT
        ARRAY[
            aits.namelsad
        ] as names,
        aits.namelsad as display_name,
        '251' as sumlev,
        16 * log(b01003.b01003001 + 1) as importance,
        aits.geoid as geoid,
        '25100US' || aits.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (aits.aland :: bigint) as aland,
        (aits.awater :: bigint) as awater,
        (aits.intptlat :: double precision) as lat,
        (aits.intptlon :: double precision) as lon
    FROM tiger2012.aits LEFT OUTER JOIN acs2012_5yr.b01003 ON (('25100US' || aits.geoid) = b01003.geoid);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading ANRC..."
    q = """SELECT
        ARRAY[
            anrc.namelsad
        ] as names,
        anrc.namelsad as display_name,
        '230' as sumlev,
        15 * log(b01003.b01003001 + 1) as importance,
        anrc.geoid as geoid,
        '23000US' || anrc.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (anrc.aland :: bigint) as aland,
        (anrc.awater :: bigint) as awater,
        (anrc.intptlat :: double precision) as lat,
        (anrc.intptlon :: double precision) as lon
    FROM tiger2012.anrc LEFT OUTER JOIN acs2012_5yr.b01003 ON (('23000US' || anrc.geoid) = b01003.geoid);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading block groups..."
    q = """SELECT
        ARRAY[
            bg.namelsad
        ] as names,
        bg.namelsad || ', ' || county.name || ', ' || state.stusps as display_name,
        '150' as sumlev,
        14 * log(b01003.b01003001 + 1) as importance,
        bg.geoid as geoid,
        '15000US' || bg.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (bg.aland :: bigint) as aland,
        (bg.awater :: bigint) as awater,
        (bg.intptlat :: double precision) as lat,
        (bg.intptlon :: double precision) as lon
    FROM tiger2012.bg LEFT OUTER JOIN acs2012_5yr.b01003 ON (('15000US' || bg.geoid) = b01003.geoid) JOIN tiger2012.county USING (statefp, countyfp) JOIN tiger2012.state USING (statefp);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading CNECTA..."
    q = """SELECT
        ARRAY[
            cnecta.namelsad
        ] as names,
        cnecta.namelsad as display_name,
        '335' as sumlev,
        13 * log(b01003.b01003001 + 1) as importance,
        cnecta.geoid as geoid,
        '33500US' || cnecta.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (cnecta.aland :: bigint) as aland,
        (cnecta.awater :: bigint) as awater,
        (cnecta.intptlat :: double precision) as lat,
        (cnecta.intptlon :: double precision) as lon
    FROM tiger2012.cnecta LEFT OUTER JOIN acs2012_5yr.b01003 ON (('33500US' || cnecta.geoid) = b01003.geoid);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading CONCITY..."
    q = """SELECT
        ARRAY[
            concity.namelsad
        ] as names,
        concity.namelsad || ', ' || state.stusps as display_name,
        '170' as sumlev,
        12 * log(b01003.b01003001 + 1) as importance,
        concity.geoid as geoid,
        '17000US' || concity.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (concity.aland :: bigint) as aland,
        (concity.awater :: bigint) as awater,
        (concity.intptlat :: double precision) as lat,
        (concity.intptlon :: double precision) as lon
    FROM tiger2012.concity LEFT OUTER JOIN acs2012_5yr.b01003 ON (('17000US' || concity.geoid) = b01003.geoid) JOIN tiger2012.state USING (statefp);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading metro divisions..."
    q = """SELECT
        ARRAY[
            metdiv.namelsad
        ] as names,
        metdiv.namelsad as display_name,
        '314' as sumlev,
        11 * log(b01003.b01003001 + 1) as importance,
        metdiv.geoid as geoid,
        '31400US' || metdiv.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (metdiv.aland :: bigint) as aland,
        (metdiv.awater :: bigint) as awater,
        (metdiv.intptlat :: double precision) as lat,
        (metdiv.intptlon :: double precision) as lon
    FROM tiger2012.metdiv LEFT OUTER JOIN acs2012_5yr.b01003 ON (('31400US' || metdiv.geoid) = b01003.geoid);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading NECTA..."
    q = """SELECT
        ARRAY[
            necta.namelsad
        ] as names,
        necta.namelsad as display_name,
        '350' as sumlev,
        10 * log(b01003.b01003001 + 1) as importance,
        necta.geoid as geoid,
        '35000US' || necta.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (necta.aland :: bigint) as aland,
        (necta.awater :: bigint) as awater,
        (necta.intptlat :: double precision) as lat,
        (necta.intptlon :: double precision) as lon
    FROM tiger2012.necta LEFT OUTER JOIN acs2012_5yr.b01003 ON (('35000US' || necta.geoid) = b01003.geoid);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading NECTA divisions..."
    q = """SELECT
        ARRAY[
            nectadiv.namelsad
        ] as names,
        nectadiv.namelsad as display_name,
        '355' as sumlev,
        9 * log(b01003.b01003001 + 1) as importance,
        nectadiv.geoid as geoid,
        '35500US' || nectadiv.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (nectadiv.aland :: bigint) as aland,
        (nectadiv.awater :: bigint) as awater,
        (nectadiv.intptlat :: double precision) as lat,
        (nectadiv.intptlon :: double precision) as lon
    FROM tiger2012.nectadiv LEFT OUTER JOIN acs2012_5yr.b01003 ON (('35500US' || nectadiv.geoid) = b01003.geoid);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading SUBMCD..."
    q = """SELECT
        ARRAY[
            submcd.namelsad
        ] as names,
        submcd.namelsad as display_name,
        '067' as sumlev,
        8 * log(b01003.b01003001 + 1) as importance,
        submcd.geoid as geoid,
        '06700US' || submcd.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (submcd.aland :: bigint) as aland,
        (submcd.awater :: bigint) as awater,
        (submcd.intptlat :: double precision) as lat,
        (submcd.intptlon :: double precision) as lon
    FROM tiger2012.submcd LEFT OUTER JOIN acs2012_5yr.b01003 ON (('06700US' || submcd.geoid) = b01003.geoid);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading TBG..."
    q = """SELECT
        ARRAY[
            tbg.namelsad
        ] as names,
        tbg.namelsad as display_name,
        '258' as sumlev,
        7 * log(b01003.b01003001 + 1) as importance,
        tbg.geoid as geoid,
        '25800US' || tbg.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (tbg.aland :: bigint) as aland,
        (tbg.awater :: bigint) as awater,
        (tbg.intptlat :: double precision) as lat,
        (tbg.intptlon :: double precision) as lon
    FROM tiger2012.tbg LEFT OUTER JOIN acs2012_5yr.b01003 ON (('25800US' || tbg.geoid) = b01003.geoid);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading TTRACT..."
    q = """SELECT
        ARRAY[
            ttract.namelsad
        ] as names,
        ttract.namelsad as display_name,
        '256' as sumlev,
        6 * log(b01003.b01003001 + 1) as importance,
        ttract.geoid as geoid,
        '25600US' || ttract.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (ttract.aland :: bigint) as aland,
        (ttract.awater :: bigint) as awater,
        (ttract.intptlat :: double precision) as lat,
        (ttract.intptlon :: double precision) as lon
    FROM tiger2012.ttract LEFT OUTER JOIN acs2012_5yr.b01003 ON (('25600US' || ttract.geoid) = b01003.geoid);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading TRACT..."
    q = """SELECT
        ARRAY[
            tract.namelsad
        ] as names,
        tract.namelsad || ', ' || county.name || ', ' || state.stusps as display_name,
        '140' as sumlev,
        5 * log(b01003.b01003001 + 1) as importance,
        tract.geoid as geoid,
        '14000US' || tract.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (tract.aland :: bigint) as aland,
        (tract.awater :: bigint) as awater,
        (tract.intptlat :: double precision) as lat,
        (tract.intptlon :: double precision) as lon
    FROM tiger2012.tract LEFT OUTER JOIN acs2012_5yr.b01003 ON (('14000US' || tract.geoid) = b01003.geoid) JOIN tiger2012.county USING (statefp, countyfp) JOIN tiger2012.state USING (statefp);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading UAC..."
    q = """SELECT
        ARRAY[
            uac.name10
        ] as names,
        uac.namelsad10 as display_name,
        '400' as sumlev,
        4 * log(b01003.b01003001 + 1) as importance,
        uac.geoid10 as geoid,
        '40000US' || uac.geoid10 as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (uac.aland10 :: bigint) as aland,
        (uac.awater10 :: bigint) as awater,
        (uac.intptlat10 :: double precision) as lat,
        (uac.intptlon10 :: double precision) as lon
    FROM tiger2012.uac LEFT OUTER JOIN acs2012_5yr.b01003 ON (('40000US' || uac.geoid10) = b01003.geoid);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading UNSD..."
    q = """SELECT
        ARRAY[
            unsd.name
        ] as names,
        unsd.name || ', ' || state.stusps as display_name,
        '970' as sumlev,
        3 * log(b01003.b01003001 + 1) as importance,
        unsd.geoid as geoid,
        '97000US' || unsd.geoid as full_geoid,
        (b01003.b01003001 :: bigint) as population,
        (unsd.aland :: bigint) as aland,
        (unsd.awater :: bigint) as awater,
        (unsd.intptlat :: double precision) as lat,
        (unsd.intptlon :: double precision) as lon
    FROM tiger2012.unsd LEFT OUTER JOIN acs2012_5yr.b01003 ON (('97000US' || unsd.geoid) = b01003.geoid) JOIN tiger2012.state USING (statefp);"""
    cur.execute(q)
    process_single_sumlev(cur, es)


    print "Loading country..."
    q = """SELECT
        ARRAY[
            'United States'
        ] as names,
        'United States' as display_name,
        '010' as sumlev,
        1 * log((SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid='01000US')) as importance,
        '' as geoid,
        '01000US' as full_geoid,
        (SELECT b01003.b01003001 FROM acs2012_5yr.b01003 WHERE geoid='01000US') as population,
        (SELECT SUM(aland) FROM tiger2012.state) as aland,
        (SELECT SUM(awater) FROM tiger2012.state) as awater,
        (SELECT ST_Y(ST_Centroid(ST_Union(the_geom))) FROM tiger2012.state) as lat,
        (SELECT ST_X(ST_Centroid(ST_Union(the_geom))) FROM tiger2012.state) as lon;"""
    cur.execute(q)
    process_single_sumlev(cur, es)

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
