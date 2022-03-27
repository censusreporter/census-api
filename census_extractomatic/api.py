from __future__ import division

from flask import (
    Flask,
    abort,
    current_app,
    g,
    jsonify,
    make_response,
    redirect,
    request,
    send_file,
)
import simplejson as json # for easiest serialization of decimal.Decimal
from decimal import Decimal
from collections import OrderedDict
from flask_caching import Cache
from flask_cors import CORS, cross_origin
from flask_sqlalchemy import SQLAlchemy
from itertools import groupby
from raven.contrib.flask import Sentry
from werkzeug.exceptions import HTTPException
import math
import os
import re
import shutil
import tempfile
import zipfile
import hashlib
import logging
import requests
from datetime import datetime
from .validation import (
    qwarg_validate,
    Bool,
    ClientRequestValidationException,
    FloatRange,
    IntegerRange,
    NonemptyString,
    OneOf,
    Regex,
    StringList,
)
from .user_geo import (
    COMPARISON_RELEASE_CODE,
    build_filename,
    create_block_xref_download,
    fetch_user_geodata,
    join_user_geo_to_blocks_task,
    list_user_geographies,
    save_user_geojson,
    fetch_user_geog_as_geojson,
    create_aggregate_download,
)
from census_extractomatic.full_text_search import perform_full_text_search

from census_extractomatic.exporters import supported_formats

from timeit import default_timer as timer

app = Flask(__name__)
app.config.from_object(os.environ.get('EXTRACTOMATIC_CONFIG_MODULE', 'census_extractomatic.config.Development'))

gunicorn_error_logger = logging.getLogger('gunicorn.error')
app.logger.handlers.extend(gunicorn_error_logger.handlers)

# decimal.Decimal is supposed to be automatically handled when simplejson is installed
# but that is not proving the case (chk /1.0/geo/show/tiger2020?geo_ids=16000US1714000 to verify)
from flask.json import JSONEncoder
class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return JSONEncoder.default(self, obj)
app.json_encoder = CustomJSONEncoder


db = SQLAlchemy(app)
cache = Cache(app)
cors = CORS(app)
sentry = Sentry(app)

# Allowed ACS's in "best" order (newest and smallest range preferred)
#
# In 2020 there wasn't a 1-year release, so we put 5-year first because it is newest.
allowed_acs = [
    'acs2020_5yr',
    'acs2019_1yr',
]
# When table searches happen without a specified release, use this
# release to do the table search.
default_table_search_release = allowed_acs[0]

release_to_expand_with = allowed_acs[0]

# Allowed TIGER releases in newest order
allowed_tiger = [
    'tiger2020',
]

allowed_searches = [
    'table',
    'profile',
    'topic',
    'all'
]

ACS_NAMES = {
    'acs2020_5yr': {'name': 'ACS 2020 5-year', 'years': '2016-2020'},
    'acs2019_1yr': {'name': 'ACS 2019 1-year', 'years': '2019'},
}

PARENT_CHILD_CONTAINMENT = {
    '040': ['050', '060', '101', '140', '150', '160', '500', '610', '620', '950', '960', '970'],
    '050': ['060', '101', '140', '150'],
    '140': ['101', '150'],
    '150': ['101'],
}

SUMLEV_NAMES = {
    "010": {"name": "nation", "plural": ""},
    "020": {"name": "region", "plural": "regions"},
    "030": {"name": "division", "plural": "divisions"},
    "040": {"name": "state", "plural": "states", "tiger_table": "state"},
    "050": {"name": "county", "plural": "counties", "tiger_table": "county"},
    "060": {"name": "county subdivision", "plural": "county subdivisions", "tiger_table": "cousub"},
    "101": {"name": "block", "plural": "blocks", "tiger_table": "tabblock"},
    "140": {"name": "census tract", "plural": "census tracts", "tiger_table": "tract"},
    "150": {"name": "block group", "plural": "block groups", "tiger_table": "bg"},
    "160": {"name": "place", "plural": "places", "tiger_table": "place"},
    "170": {"name": "consolidated city", "plural": "consolidated cities", "tiger_table": "concity"},
    "230": {"name": "Alaska native regional corporation", "plural": "Alaska native regional corporations", "tiger_table": "anrc"},
    "250": {"name": "native area", "plural": "native areas", "tiger_table": "aiannh250"},
    "251": {"name": "tribal subdivision", "plural": "tribal subdivisions", "tiger_table": "aits"},
    "252": {"name": "native area (reservation)", "plural": "native areas (reservation)", "tiger_table": "aiannh252"},
    "254": {"name": "native area (off-trust land)", "plural": "native areas (off-trust land)", "tiger_table": "aiannh254"},
    "256": {"name": "tribal census tract", "plural": "tribal census tracts", "tiger_table": "ttract"},
    "300": {"name": "MSA", "plural": "MSAs", "tiger_table": "metdiv"},
    "310": {"name": "CBSA", "plural": "CBSAs", "tiger_table": "cbsa"},
    "314": {"name": "metropolitan division", "plural": "metropolitan divisions", "tiger_table": "metdiv"},
    "330": {"name": "CSA", "plural": "CSAs", "tiger_table": "csa"},
    "335": {"name": "combined NECTA", "plural": "combined NECTAs", "tiger_table": "cnecta"},
    "350": {"name": "NECTA", "plural": "NECTAs", "tiger_table": "necta"},
    "364": {"name": "NECTA division", "plural": "NECTA divisions", "tiger_table": "nectadiv"},
    "400": {"name": "urban area", "plural": "urban areas", "tiger_table": "uac"},
    "500": {"name": "congressional district", "plural": "congressional districts", "tiger_table": "cd"},
    "610": {"name": "state senate district", "plural": "state senate districts", "tiger_table": "sldu"},
    "620": {"name": "state house district", "plural": "state house districts", "tiger_table": "sldl"},
    "795": {"name": "PUMA", "plural": "PUMAs", "tiger_table": "puma"},
    "850": {"name": "ZCTA3", "plural": "ZCTA3s"},
    "860": {"name": "ZCTA5", "plural": "ZCTA5s", "tiger_table": "zcta5"},
    "950": {"name": "elementary school district", "plural": "elementary school districts", "tiger_table": "elsd"},
    "960": {"name": "secondary school district", "plural": "secondary school districts", "tiger_table": "scsd"},
    "970": {"name": "unified school district", "plural": "unified school districts", "tiger_table": "unsd"},
}

state_fips = {
    "01": "Alabama",
    "02": "Alaska",
    "04": "Arizona",
    "05": "Arkansas",
    "06": "California",
    "08": "Colorado",
    "09": "Connecticut",
    "10": "Delaware",
    "11": "District of Columbia",
    "12": "Florida",
    "13": "Georgia",
    "15": "Hawaii",
    "16": "Idaho",
    "17": "Illinois",
    "18": "Indiana",
    "19": "Iowa",
    "20": "Kansas",
    "21": "Kentucky",
    "22": "Louisiana",
    "23": "Maine",
    "24": "Maryland",
    "25": "Massachusetts",
    "26": "Michigan",
    "27": "Minnesota",
    "28": "Mississippi",
    "29": "Missouri",
    "30": "Montana",
    "31": "Nebraska",
    "32": "Nevada",
    "33": "New Hampshire",
    "34": "New Jersey",
    "35": "New Mexico",
    "36": "New York",
    "37": "North Carolina",
    "38": "North Dakota",
    "39": "Ohio",
    "40": "Oklahoma",
    "41": "Oregon",
    "42": "Pennsylvania",
    "44": "Rhode Island",
    "45": "South Carolina",
    "46": "South Dakota",
    "47": "Tennessee",
    "48": "Texas",
    "49": "Utah",
    "50": "Vermont",
    "51": "Virginia",
    "53": "Washington",
    "54": "West Virginia",
    "55": "Wisconsin",
    "56": "Wyoming",
    "60": "American Samoa",
    "66": "Guam",
    "69": "Commonwealth of the Northern Mariana Islands",
    "72": "Puerto Rico",
    "78": "United States Virgin Islands"
}

# A regex to match geoids (e.g. 31000US33340) or expandable geoids (e.g. 310|33000US376)
expandable_geoid_re = re.compile(r"^((\d{3}\|))?([\dA-Z]{5}US[\d\-A-Z]*)$")
# A regex that will only match bare geoids (e.g. 31000US33340)
geoid_re = re.compile(r"^[\dA-Z]{5}US[\d\-A-Z]*$")
# A regex that matches things that look like table IDs
table_re = re.compile(r"^[BC]\d{5,6}(?:[A-Z]{1,3})?$")


@app.errorhandler(400)
@app.errorhandler(404)
@app.errorhandler(500)
@cross_origin(origin='*')
def jsonify_error_handler(error):
    if isinstance(error, ClientRequestValidationException):
        resp = jsonify(error=error.description, errors=error.errors)
        resp.status_code = error.code
    elif isinstance(error, HTTPException):
        resp = jsonify(error=error.description)
        resp.status_code = error.code
    else:
        resp = jsonify(error=error.args[0])
        resp.status_code = 500

    if resp.status_code >= 500:
        app.logger.exception("Handling exception %s, %s", error, error.args)

    return resp


def add_metadata(dictionary, table_id, universe, acs_release):
    val = dict(
        table_id=table_id,
        universe=universe,
        acs_release=acs_release,
    )

    dictionary['metadata'] = val


def find_geoid(geoid, acs=None):
    "Find the best acs to use for a given geoid or None if the geoid is not found."

    if acs:
        if acs not in allowed_acs:
            abort(404, "We don't have data for that release.")
        acs_to_search = [acs]
    else:
        acs_to_search = allowed_acs

    for acs in acs_to_search:

        result = db.session.execute(
            """SELECT geoid
               FROM %s.geoheader
               WHERE geoid=:geoid""" % acs,
            {'geoid': geoid}
        )
        if result.rowcount == 1:
            result = result.first()
            return (acs, result['geoid'])
    return (None, None)


def get_data_fallback(table_ids, geoids, acs=None):
    if type(geoids) != list:
        geoids = [geoids]

    if type(table_ids) != list:
        table_ids = [table_ids]

    from_stmt = '%%(acs)s.%s_moe' % (table_ids[0])
    if len(table_ids) > 1:
        from_stmt += ' '
        from_stmt += ' '.join(['JOIN %%(acs)s.%s_moe USING (geoid)' % (table_id) for table_id in table_ids[1:]])

    sql = 'SELECT * FROM %s WHERE geoid IN :geoids;' % (from_stmt,)

    # if acs is specified, we'll use that one and not go searching for data.
    if acs in allowed_acs:
        sql = sql % {'acs': acs}
        result = db.session.execute(
            sql,
            {'geoids': tuple(geoids)},
        )
        data = {}
        for row in result.fetchall():
            row = dict(row)
            geoid = row.pop('geoid')
            data[geoid] = dict([(col, val) for (col, val) in row.items()])

        return data, acs

    else:
        # otherwise we'll start at the best/most recent acs and move down til we have the data we want
        for acs in allowed_acs:
            sql = sql % {'acs': acs}
            result = db.session.execute(
                sql,
                {'geoids': tuple(geoids)},
            )
            data = {}
            for row in result.fetchall():
                row = dict(row)
                geoid = row.pop('geoid')
                data[geoid] = dict([(col, val) for (col, val) in row.items()])

            # Check to see if this release has our data
            data_with_values = [geoid_data for geoid_data in list(data.values()) if list(geoid_data.values())[0] is not None]
            if len(geoids) == len(data) and len(geoids) == len(data_with_values):
                return data, acs
            else:
                # Doesn't contain data for all geoids, so keep going.
                continue

    return None, acs


def special_case_parents(geoid, levels):
    '''
    Update/adjust the parents list for special-cased geographies.
    '''
    if geoid == '16000US1150000':
        # compare Washington, D.C., to "parent" state of VA,
        # rather than comparing to self as own parent state

        target = next((index for (index, d) in enumerate(levels) if d['geoid'] == '04000US11'))
        levels[target].update({
            'coverage': 0,
            'display_name': 'Virginia',
            'geoid': '04000US51'
        })

    # Louisville is not in Census 160 data but 170 consolidated city is equivalent
    # we could try to convert 160 L-ville into 170, but that would overlap with
    # 050 Jefferson which should already be in there so we'll just pluck it out.
    levels = [level for level in levels if not level['geoid'] == '16000US2148000']

    return levels


def compute_profile_item_levels(geoid):
    levels = []
    geoid_parts = []

    if geoid:
        geoid = geoid.upper()
        geoid_parts = geoid.split('US')

    if len(geoid_parts) != 2:
        raise Exception('Invalid geoid')

    levels.append({
        'relation': 'this',
        'geoid': geoid,
        'coverage': 100.0,
    })

    sumlevel = geoid_parts[0][:3]
    id_part = geoid_parts[1]

    if sumlevel in ('140', '150', '160', '310', '330', '350', '860', '950', '960', '970'):
        result = db.session.execute(
            """SELECT * FROM tiger2020.census_geo_containment
               WHERE child_geoid=:geoid
               ORDER BY percent_covered ASC
            """,
            {'geoid': geoid},
        )
        for row in result:
            parent_sumlevel_name = SUMLEV_NAMES.get(row['parent_geoid'][:3])['name']

            levels.append({
                'relation': parent_sumlevel_name,
                'geoid': row['parent_geoid'],
                'coverage': row['percent_covered'],
            })

    if sumlevel in ('060', '140', '150'):
        levels.append({
            'relation': 'county',
            'geoid': '05000US' + id_part[:5],
            'coverage': 100.0,
        })

    if sumlevel in ('050', '060', '140', '150', '160', '500', '610', '620', '795', '950', '960', '970'):
        levels.append({
            'relation': 'state',
            'geoid': '04000US' + id_part[:2],
            'coverage': 100.0,
        })

    if sumlevel in ('314'):
        levels.append({
            'relation': 'CBSA',
            'geoid': '31000US' + id_part[:5],
            'coverage': 100.0,
        })

    if sumlevel != '010':
        levels.append({
            'relation': 'nation',
            'geoid': '01000US',
            'coverage': 100.0,
        })

    levels = special_case_parents(geoid, levels)

    return levels


def get_acs_name(acs_slug):
    if acs_slug in ACS_NAMES:
        acs_name = ACS_NAMES[acs_slug]['name']
    else:
        acs_name = acs_slug
    return acs_name


#
# GEO LOOKUPS
#

def convert_row(row):
    data = dict()
    data['sumlevel'] = row['sumlevel']
    data['full_geoid'] = row['full_geoid']
    data['full_name'] = row['display_name']
    if 'geom' in row and row['geom']:
        data['geom'] = json.loads(row['geom'])
    return data

# Example: /1.0/geo/search?q=spok
# Example: /1.0/geo/search?q=spok&sumlevs=050,160
@app.route("/1.0/geo/search")
@qwarg_validate({
    'lat': {'valid': FloatRange(-90.0, 90.0)},
    'lon': {'valid': FloatRange(-180.0, 180.0)},
    'q': {'valid': NonemptyString()},
    'sumlevs': {'valid': StringList(item_validator=OneOf(SUMLEV_NAMES))},
    'geom': {'valid': Bool()}
})
@cross_origin(origin='*')
def geo_search():
    lat = request.qwargs.lat
    lon = request.qwargs.lon
    q = request.qwargs.q
    sumlevs = request.qwargs.sumlevs
    with_geom = request.qwargs.geom

    if lat and lon:
        where = "ST_Intersects(geom, ST_SetSRID(ST_Point(:lon, :lat),4326))"
        where_args = {'lon': lon, 'lat': lat}
    elif q:
        q = re.sub(r'[^a-zA-Z\,\.\-0-9]', ' ', q)
        q = re.sub(r'\s+', ' ', q)
        where = "lower(prefix_match_name) LIKE lower(:q)"
        q += '%'
        where_args = {'q': q}
    else:
        abort(400, "Must provide either a lat/lon OR a query term.")

    where += " AND lower(display_name) not like '%%not defined%%' "

    if sumlevs:
        where += " AND sumlevel IN :sumlevs"
        where_args['sumlevs'] = tuple(sumlevs)

    if with_geom:
        sql = """SELECT DISTINCT geoid,sumlevel,population,display_name,full_geoid,priority,ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom,0.001), 5) as geom
            FROM tiger2020.census_name_lookup
            WHERE %s
            ORDER BY priority, population DESC NULLS LAST
            LIMIT 25;""" % (where)
    else:
        sql = """SELECT DISTINCT geoid,sumlevel,population,display_name,full_geoid,priority
            FROM tiger2020.census_name_lookup
            WHERE %s
            ORDER BY priority, population DESC NULLS LAST
            LIMIT 25;""" % (where)
    result = db.session.execute(sql, where_args)

    return jsonify(results=[convert_row(row) for row in result])


def num2deg(xtile, ytile, zoom):
    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return (lat_deg, lon_deg)


# Example: /1.0/geo/tiger2014/tiles/160/10/261/373.geojson
# Example: /1.0/geo/tiger2013/tiles/160/10/261/373.geojson
@app.route("/1.0/geo/<release>/tiles/<sumlevel>/<int:zoom>/<int:x>/<int:y>.geojson")
@cross_origin(origin='*')
def geo_tiles(release, sumlevel, zoom, x, y):
    if release not in allowed_tiger:
        abort(404, "Unknown TIGER release")
    if sumlevel not in SUMLEV_NAMES:
        abort(404, "Unknown sumlevel")
    if sumlevel == '010':
        abort(400, "Don't support US tiles")

    cache_key = str('1.0/geo/%s/tiles/%s/%s/%s/%s.geojson' % (release, sumlevel, zoom, x, y))
    cached = cache.get(cache_key)
    if cached:
        resp = make_response(cached)
    else:
        (miny, minx) = num2deg(x, y, zoom)
        (maxy, maxx) = num2deg(x + 1, y + 1, zoom)

        tiles_across = 2**zoom
        deg_per_tile = 360.0 / tiles_across
        deg_per_pixel = deg_per_tile / 256
        tile_buffer = 10 * deg_per_pixel  # ~ 10 pixel buffer
        simplify_threshold = deg_per_pixel / 5

        result = db.session.execute(
            """SELECT
                ST_AsGeoJSON(ST_SimplifyPreserveTopology(
                    ST_Intersection(ST_Buffer(ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 4326), %f, 'join=mitre'), geom),
                    %f), 5) as geom,
                full_geoid,
                display_name
               FROM %s.census_name_lookup
               WHERE sumlevel=:sumlev AND ST_Intersects(ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 4326), geom)""" % (
                tile_buffer, simplify_threshold, release,),
            {'minx': minx, 'miny': miny, 'maxx': maxx, 'maxy': maxy, 'sumlev': sumlevel}
        )

        results = []
        for row in result:
            results.append({
                "type": "Feature",
                "properties": {
                    "geoid": row['full_geoid'],
                    "name": row['display_name']
                },
                "geometry": json.loads(row['geom']) if row['geom'] else None
            })

        result = json.dumps(dict(type="FeatureCollection", features=results), separators=(',', ':'))

        resp = make_response(result)
        try:
            cache.set(cache_key, result)
        except Exception as e:
            app.logger.warn('Skipping cache set for {} because {}'.format(cache_key, e.args))

    resp.headers.set('Content-Type', 'application/json')
    resp.headers.set('Cache-Control', 'public,max-age=86400')  # 1 day
    return resp


# Example: /1.0/geo/tiger2014/04000US53
# Example: /1.0/geo/tiger2013/04000US53
@app.route("/1.0/geo/<release>/<geoid>")
@qwarg_validate({
    'geom': {'valid': Bool(), 'default': False}
})
@cross_origin(origin='*')
def geo_lookup(release, geoid):
    if release not in allowed_tiger:
        abort(404, "Unknown TIGER release")

    if not expandable_geoid_re.match(geoid):
        abort(404, 'Invalid GeoID')

    cache_key = str('1.0/geo/%s/show/%s.json?geom=%s' % (release, geoid, request.qwargs.geom))
    cached = cache.get(cache_key)
    if cached:
        resp = make_response(cached)
    else:
        if request.qwargs.geom:
            result = db.session.execute(
                """SELECT display_name,simple_name,sumlevel,full_geoid,population,aland,awater,
                   ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, 0.00005), 6) as geom
                   FROM %s.census_name_lookup
                   WHERE full_geoid=:geoid
                   LIMIT 1""" % (release,),
                {'geoid': geoid}
            )
        else:
            result = db.session.execute(
                """SELECT display_name,simple_name,sumlevel,full_geoid,population,aland,awater
                   FROM %s.census_name_lookup
                   WHERE full_geoid=:geoid
                   LIMIT 1""" % (release,),
                {'geoid': geoid}
            )

        result = result.fetchone()

        if not result:
            abort(404, 'Unknown GeoID')

        result = dict(result)
        geom = result.pop('geom', None)
        if geom:
            geom = json.loads(geom)

        result = json.dumps(dict(type="Feature", properties=result, geometry=geom), separators=(',', ':'))

        resp = make_response(result)
        cache.set(cache_key, result)

    resp.headers.set('Content-Type', 'application/json')
    resp.headers.set('Cache-Control', 'public,max-age=%d' % int(3600 * 4))

    return resp


# Example: /1.0/geo/tiger2014/04000US53/parents
# Example: /1.0/geo/tiger2013/04000US53/parents
@app.route("/1.0/geo/<release>/<geoid>/parents")
@cross_origin(origin='*')
def geo_parent(release, geoid):
    if release not in allowed_tiger:
        abort(404, "Unknown TIGER release")

    if not geoid_re.match(geoid):
        abort(404, 'Invalid GeoID')

    cache_key = str('%s/show/%s.parents.json' % (release, geoid))
    cached = cache.get(cache_key)

    if cached:
        resp = make_response(cached)
    else:
        try:
            parents = compute_profile_item_levels(geoid)
        except Exception as e:
            abort(400, "Could not compute parents: " + e.args[0])
        parent_geoids = [p['geoid'] for p in parents]

        def build_item(p):
            return (p['full_geoid'], {
                "display_name": p['display_name'],
                "sumlevel": p['sumlevel'],
                "geoid": p['full_geoid'],
            })

        if parent_geoids:
            result = db.session.execute(
                """SELECT display_name,sumlevel,full_geoid
                   FROM %s.census_name_lookup
                   WHERE full_geoid IN :geoids
                   ORDER BY sumlevel DESC""" % (release,),
                {'geoids': tuple(parent_geoids)}
            )
            parent_list = dict([build_item(p) for p in result])

            for parent in parents:
                parent.update(parent_list.get(parent['geoid'], {}))

        result = json.dumps(dict(parents=parents))

        resp = make_response(result)
        cache.set(cache_key, result)

    resp.headers.set('Content-Type', 'application/json')
    resp.headers.set('Cache-Control', 'public,max-age=%d' % int(3600 * 4))

    return resp


# Example: /1.0/geo/show/tiger2014?geo_ids=04000US55,04000US56
# Example: /1.0/geo/show/tiger2014?geo_ids=160|04000US17,04000US56
@app.route("/1.0/geo/show/<release>")
@qwarg_validate({
    'geo_ids': {'valid': StringList(item_validator=Regex(expandable_geoid_re)), 'required': True},
})
@cross_origin(origin='*')
def show_specified_geo_data(release):
    if release not in allowed_tiger:
        abort(404, "Unknown TIGER release")
    geo_ids, child_parent_map = expand_geoids(request.qwargs.geo_ids, release_to_expand_with)

    if not geo_ids:
        abort(404, 'None of the geo_ids specified were valid: %s' % ', '.join(geo_ids))

    max_geoids = current_app.config.get('MAX_GEOIDS_TO_SHOW', 3000)
    if len(geo_ids) > max_geoids:
        abort(400, 'You requested %s geoids. The maximum is %s. Please contact us for bulk data.' % (len(geo_ids), max_geoids))

    result = []
    if geo_ids:
        result = db.session.execute(
            """SELECT full_geoid,
                display_name,
                aland,
                awater,
                population,
                ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom,ST_Perimeter(geom) / 2500)) as geom
            FROM %s.census_name_lookup
            WHERE geom is not null and full_geoid IN :geoids;""" % (release,),
            {'geoids': tuple(geo_ids)}
        )

    results = []
    valid_geo_ids = []
    for row in result:
        valid_geo_ids.append(row['full_geoid'])
        results.append({
            "type": "Feature",
            "properties": {
                "geoid": row['full_geoid'],
                "name": row['display_name'],
                "aland": row['aland'],
                "awater": row['awater'],
                "2013_population_estimate": row['population'],
            },
            "geometry": json.loads(row['geom'])
        })

    invalid_geo_ids = set(geo_ids) - set(valid_geo_ids)
    if invalid_geo_ids:
        abort(404, "GeoID(s) %s are not valid." % (','.join(invalid_geo_ids)))

    resp_data = {
        'type': 'FeatureCollection',
        'features': results
    }

    return jsonify(**resp_data)


#
# TABLE LOOKUPS
#

def format_table_search_result(obj, obj_type):
    '''internal util for formatting each object in `table_search` API response'''
    result = {
        'type': obj_type,
        'table_id': obj['table_id'],
        'table_name': obj['table_title'],
        'simple_table_name': obj['simple_table_title'],
        'topics': obj['topics'],
        'universe': obj['universe'],
    }

    if obj_type == 'table':
        result.update({
            'id': obj['table_id'],
            'unique_key': obj['table_id'],
        })
    elif obj_type == 'column':
        result.update({
            'id': obj['column_id'],
            'unique_key': '%s|%s' % (obj['table_id'], obj['column_id']),
            'column_id': obj['column_id'],
            'column_name': obj['column_title'],
        })

    return result


# Example: /1.0/table/search?q=norweg
# Example: /1.0/table/search?q=norweg&topics=age,sex
# Example: /1.0/table/search?topics=housing,poverty
@app.route("/1.0/table/search")
@qwarg_validate({
    'acs': {'valid': OneOf(allowed_acs), 'default': default_table_search_release},
    'q': {'valid': NonemptyString()},
    'topics': {'valid': StringList()}
})
@cross_origin(origin='*')
def table_search():
    # allow choice of release, default to allowed_acs[0]
    acs = request.qwargs.acs
    q = request.qwargs.q
    topics = request.qwargs.topics

    if not (q or topics):
        abort(400, "Must provide a query term or topics for filtering.")

    data = []

    if re.match(r'^\w\d{2,}$', q, flags=re.IGNORECASE):
        # we need to search 'em all because not every table is in every release...
        # might be better to have a shared table like census_tabulation_metadata?
        table_id_acs = acs
        acs_to_search = allowed_acs[:]
        acs_to_search.remove(table_id_acs)
        ids_found = set()
        while table_id_acs:
            # Matching for table id
            db.session.execute("SET search_path=:acs, public;", {'acs': table_id_acs})
            result = db.session.execute(
                """SELECT tab.table_id,
                          tab.table_title,
                          tab.simple_table_title,
                          tab.universe,
                          tab.topics
                   FROM census_table_metadata tab
                   WHERE lower(table_id) like lower(:table_id)""",
                {'table_id': '{}%'.format(q)}
            )
            for row in result:
                if row['table_id'] not in ids_found:
                    data.append(format_table_search_result(row, 'table'))
                    ids_found.add(row['table_id'])
            try:
                table_id_acs = acs_to_search.pop(0)
            except IndexError:
                table_id_acs = None
        if data:
            data.sort(key=lambda x: x['unique_key'])
            return json.dumps(data)

    db.session.execute("SET search_path=:acs, public;", {'acs': acs})
    table_where_parts = []
    table_where_args = {}
    column_where_parts = []
    column_where_args = {}

    if q and q != '*':
        q = '%%%s%%' % q
        table_where_parts.append("lower(tab.table_title) LIKE lower(:query)")
        table_where_args['query'] = q
        column_where_parts.append("lower(col.column_title) LIKE lower(:query)")
        column_where_args['query'] = q

    if topics:
        table_where_parts.append('tab.topics @> :topics')
        table_where_args['topics'] = topics
        column_where_parts.append('tab.topics @> :topics')
        column_where_args['topics'] = topics

    if table_where_parts:
        table_where = ' AND '.join(table_where_parts)
        column_where = ' AND '.join(column_where_parts)
    else:
        table_where = 'TRUE'
        column_where = 'TRUE'

    # retrieve matching tables.
    result = db.session.execute(
        """SELECT tab.tabulation_code,
                  tab.table_title,
                  tab.simple_table_title,
                  tab.universe,
                  tab.topics,
                  tab.tables_in_one_yr,
                  tab.tables_in_three_yr,
                  tab.tables_in_five_yr
           FROM census_tabulation_metadata tab
           WHERE %s
           ORDER BY tab.weight DESC""" % (table_where),
        table_where_args
    )
    for tabulation in result:
        tabulation = dict(tabulation)
        for tables_for_release_col in ('tables_in_one_yr', 'tables_in_three_yr', 'tables_in_five_yr'):
            if tabulation[tables_for_release_col]:
                tabulation['table_id'] = tabulation[tables_for_release_col][0]
            else:
                continue
            break
        data.append(format_table_search_result(tabulation, 'table'))

    # retrieve matching columns.
    if q != '*':
        # Special case for when we want ALL the tables (but not all the columns)
        result = db.session.execute(
            """SELECT col.column_id,
                      col.column_title,
                      tab.table_id,
                      tab.table_title,
                      tab.simple_table_title,
                      tab.universe,
                      tab.topics
               FROM census_column_metadata col
               LEFT OUTER JOIN census_table_metadata tab USING (table_id)
               WHERE %s
               ORDER BY char_length(tab.table_id), tab.table_id""" % (column_where),
            column_where_args
        )
        data.extend([format_table_search_result(column, 'column') for column in result])

    text = json.dumps(data)
    resp = make_response(text)
    resp.headers.set('Content-Type', 'application/json')
    return resp

# Example: /1.0/tabulation/01001
@app.route("/1.0/tabulation/<tabulation_id>")
@cross_origin(origin='*')
def tabulation_details(tabulation_id):
    if not tabulation_id.isdigit():
        abort(404, "Invalid tabulation ID")

    result = db.session.execute(
        """SELECT *
           FROM census_tabulation_metadata
           WHERE tabulation_code=:tabulation""",
        {'tabulation': tabulation_id}
    )

    row = result.fetchone()

    if not row:
        abort(404, "Tabulation %s not found." % tabulation_id)

    row = dict(row)

    row['tables_by_release'] = {
        'one_yr': row.pop('tables_in_one_yr', []),
        'three_yr': row.pop('tables_in_three_yr', []),
        'five_yr': row.pop('tables_in_five_yr', []),
    }

    row.pop('weight', None)

    return jsonify(**row)

# Example: /1.0/tabulations/?topics=comma,separated,string
# Example: /1.0/tabulations/?prefix=digits
# Example: /1.0/tabulations/?q=query+string (LIKE)
# Example: /1.0/tabulations/?codes=01001,01002
@app.route("/1.0/tabulations/")
@qwarg_validate({
    'prefix': {'valid': NonemptyString()},
    'topics': {'valid': StringList()},
    'codes': {'valid': StringList()},
    'q': {'valid': NonemptyString()}
})
@cross_origin(origin='*')
def search_tabulations():

    prefix = request.qwargs.prefix
    topics = request.qwargs.topics
    codes = request.qwargs.codes
    q = request.qwargs.q

    table_where_parts = []
    table_where_args = {}

    if topics:
        table_where_parts.append('tab.topics @> :topics')
        table_where_args['topics'] = topics

    if prefix:
        table_where_parts.append('tab.tabulation_code like :prefix')
        table_where_args['prefix'] = "{}%".format(prefix)

    if q:
        table_where_parts.append('lower(tab.table_title) like lower(:q)')
        table_where_args['q'] = "%{}%".format(q)

    if codes:
        table_where_parts.append('tab.tabulation_code = any(:codes)')
        table_where_args['codes'] = codes

    if table_where_parts:
        table_where = ' AND '.join(table_where_parts)
    else:
        table_where = 'TRUE'

    # retrieve matching tables.
    result = db.session.execute(
        """SELECT tab.tabulation_code,
                  tab.table_title,
                  tab.simple_table_title,
                  tab.universe,
                  tab.topics,
                  tab.tables_in_one_yr,
                  tab.tables_in_three_yr,
                  tab.tables_in_five_yr
           FROM census_tabulation_metadata tab
           WHERE %s
           ORDER BY tab.tabulation_code""" % (table_where),
        table_where_args
    )

    data = []

    for tabulation in result:
        data.append(dict(tabulation))

    text = json.dumps(data)
    resp = make_response(text)
    resp.headers.set('Content-Type', 'application/json')

    return resp

# Example: /1.0/table/B28001?release=acs2013_1yr
@app.route("/1.0/table/<table_id>")
@qwarg_validate({
    'acs': {'valid': OneOf(allowed_acs), 'default': default_table_search_release}
})
@cross_origin(origin='*')
def table_details(table_id):
    release = request.qwargs.acs

    if not table_re.match(table_id):
        abort(404, "Invalid table ID")

    cache_key = str('tables/%s/%s.json' % (release, table_id))
    cached = cache.get(cache_key)
    if cached:
        resp = make_response(cached)
    else:
        db.session.execute("SET search_path=:acs, public;", {'acs': request.qwargs.acs})

        result = db.session.execute(
            """SELECT *
               FROM census_table_metadata tab
               WHERE table_id=:table_id""",
            {'table_id': table_id}
        )
        row = result.fetchone()

        if not row:
            abort(404, "Table %s not found in release %s. Try specifying another release." % (table_id.upper(), release))

        data = OrderedDict([
            ("table_id", row['table_id']),
            ("table_title", row['table_title']),
            ("simple_table_title", row['simple_table_title']),
            ("subject_area", row['subject_area']),
            ("universe", row['universe']),
            ("denominator_column_id", row['denominator_column_id']),
            ("topics", row['topics'])
        ])

        result = db.session.execute(
            """SELECT *
               FROM census_column_metadata
               WHERE table_id=:table_id""",
            {'table_id': row['table_id']}
        )

        rows = []
        for row in result:
            rows.append((row['column_id'], dict(
                column_title=row['column_title'],
                indent=row['indent'],
                parent_column_id=row['parent_column_id']
            )))
        data['columns'] = OrderedDict(rows)

        result = json.dumps(data)

        resp = make_response(result)
        cache.set(cache_key, result)

    resp.headers.set('Content-Type', 'application/json')
    resp.headers.set('Cache-Control', 'public,max-age=%d' % int(3600 * 4))

    return resp


# Example: /2.0/table/latest/B28001
@app.route("/2.0/table/<release>/<table_id>")
@cross_origin(origin='*')
def table_details_with_release(release, table_id):
    if release in allowed_acs:
        acs_to_try = [release]
    elif release == 'latest':
        acs_to_try = list(allowed_acs)
    else:
        abort(404, 'The %s release isn\'t supported.' % get_acs_name(release))

    if not table_re.match(table_id):
        abort(404, "Invalid table ID")

    for release in acs_to_try:
        cache_key = str('tables/%s/%s.json' % (release, table_id))
        cached = cache.get(cache_key)
        if cached:
            resp = make_response(cached)
        else:
            db.session.execute("SET search_path=:acs, public;", {'acs': release})

            result = db.session.execute(
                """SELECT *
                   FROM census_table_metadata tab
                   WHERE table_id=:table_id""",
                {'table_id': table_id}
            )
            row = result.fetchone()

            if not row:
                continue

            data = OrderedDict([
                ("table_id", row['table_id']),
                ("table_title", row['table_title']),
                ("simple_table_title", row['simple_table_title']),
                ("subject_area", row['subject_area']),
                ("universe", row['universe']),
                ("denominator_column_id", row['denominator_column_id']),
                ("topics", row['topics'])
            ])

            result = db.session.execute(
                """SELECT *
                   FROM census_column_metadata
                   WHERE table_id=:table_id
                   ORDER By line_number""",
                {'table_id': row['table_id']}
            )

            rows = []
            for row in result:
                rows.append((row['column_id'], dict(
                    column_title=row['column_title'],
                    indent=row['indent'],
                    parent_column_id=row['parent_column_id']
                )))
            data['columns'] = OrderedDict(rows)

            result = json.dumps(data)

            resp = make_response(result)
            cache.set(cache_key, result)

        resp.headers.set('Content-Type', 'application/json')
        resp.headers.set('Cache-Control', 'public,max-age=%d' % int(3600 * 4))

        return resp

    abort(404, "Table %s not found in releases %s. Try specifying another release." % (table_id, ', '.join(acs_to_try)))


# Example: /1.0/table/compare/rowcounts/B01001?year=2011&sumlevel=050&within=04000US53
@app.route("/1.0/table/compare/rowcounts/<table_id>")
@qwarg_validate({
    'year': {'valid': NonemptyString()},
    'sumlevel': {'valid': OneOf(SUMLEV_NAMES), 'required': True},
    'within': {'valid': Regex(table_re), 'required': True},
    'topics': {'valid': StringList()}
})
@cross_origin(origin='*')
def table_geo_comparison_rowcount(table_id):
    years = request.qwargs.year.split(',')
    child_summary_level = request.qwargs.sumlevel
    parent_geoid = request.qwargs.within

    if not table_re.match(table_id):
        abort(404, "Invalid table_id")

    data = OrderedDict()

    releases = []
    for year in years:
        releases += [name for name in allowed_acs if year in name]
    releases = sorted(releases)

    for acs in releases:
        db.session.execute("SET search_path=:acs, public;", {'acs': acs})
        release = OrderedDict()
        release['release_name'] = ACS_NAMES[acs]['name']
        release['release_slug'] = acs
        release['results'] = 0

        result = db.session.execute(
            """SELECT *
               FROM census_table_metadata
               WHERE table_id=:table_id;""",
            {'table_id': table_id}
        )
        table_record = result.fetchone()
        if table_record:
            validated_table_id = table_record['table_id']
            release['table_name'] = table_record['table_title']
            release['table_universe'] = table_record['universe']

            child_geoheaders = get_child_geoids(parent_geoid, child_summary_level)

            if child_geoheaders:
                child_geoids = [child['geoid'] for child in child_geoheaders]
                result = db.session.execute(
                    """SELECT COUNT(*)
                       FROM %s.%s
                       WHERE geoid IN :geoids""" % (acs, validated_table_id),
                    {'geoids': tuple(child_geoids)}
                )
                acs_rowcount = result.fetchone()
                release['results'] = acs_rowcount['count']

        data[acs] = release

    return jsonify(**data)


def build_profile_url(row):
    ''' Builds the censusreporter URL out of the geoid.

    Format: https://censusreporter.org/profiles/full_geoid
    Note that this format is a valid link, and will redirect to the
    "proper" URL with geoid and display name.

    >>> build_profile_url("31000US18020")
    "https://censusreporter.org/profiles/31000US18020/"

    '''
    URL_ROOT = app.config.get('CENSUS_REPORTER_URL_ROOT', 'https://censusreporter.org')
    return "{}/profiles/{}/".format(URL_ROOT, row['full_geoid'])

def build_table_url(row):
    ''' Builds the CensusReporter URL out of table_id.

    Format: https://censusreporter.org/tables/table_id/"

    >>> build_table_url("B06009")
    "http://censusreporter.org/tables/B06009/"
    '''

    URL_ROOT = app.config.get('CENSUS_REPORTER_URL_ROOT', 'https://censusreporter.org')
    return "{}/tables/{}/".format(URL_ROOT, row['table_id'])

FTS_URL_BUILDERS = {
    'profile': build_profile_url,
    'table': build_table_url
}
@app.route("/2.1/full-text/search")
@qwarg_validate({
    'q': {'valid': NonemptyString()},
    'type': {'valid': OneOf(allowed_searches), 'default': allowed_searches[3]},
    'limit': {'valid': IntegerRange(1, 1000), 'default': 10},
})
@cross_origin(origin='*')
def full_text_search():

    q = request.qwargs.q
    search_type = request.qwargs.type
    limit = request.qwargs.limit

    prepared_result = perform_full_text_search(db, q, search_type, limit)
    # some results need their URLs qualified, which can only be done based on the app config
    # so post-process the results before serving them
    for row in prepared_result:
        url_func = FTS_URL_BUILDERS.get(row['type'])
        if url_func:
            row['url'] = url_func(row)

    return jsonify(results=prepared_result)



#
#  DATA RETRIEVAL
#


# get geoheader data for children at the requested summary level
def get_child_geoids(release, parent_geoid, child_summary_level):
    parent_sumlevel = parent_geoid[0:3]
    if parent_sumlevel == '010':
        return get_all_child_geoids(release, child_summary_level)
    elif parent_sumlevel in PARENT_CHILD_CONTAINMENT and child_summary_level in PARENT_CHILD_CONTAINMENT[parent_sumlevel]:
        return get_child_geoids_by_prefix(release, parent_geoid, child_summary_level)
    elif parent_sumlevel == '160' and child_summary_level in ('140', '150'):
        return get_child_geoids_by_coverage(release, parent_geoid, child_summary_level)
    elif parent_sumlevel == '310' and child_summary_level in ('160', '860'):
        return get_child_geoids_by_coverage(release, parent_geoid, child_summary_level)
    elif parent_sumlevel == '040' and child_summary_level in ('310', '860'):
        return get_child_geoids_by_coverage(release, parent_geoid, child_summary_level)
    elif parent_sumlevel == '050' and child_summary_level in ('160', '860', '950', '960', '970'):
        return get_child_geoids_by_coverage(release, parent_geoid, child_summary_level)
    else:
        return get_child_geoids_by_gis(release, parent_geoid, child_summary_level)


def get_all_child_geoids(release, child_summary_level):
    db.session.execute("SET search_path=:acs,public;", {'acs': release})
    result = db.session.execute(
        """SELECT geoid,name
           FROM geoheader
           WHERE sumlevel=:sumlev AND component='00' AND geoid NOT IN ('04000US72')
           ORDER BY name""",
        {'sumlev': int(child_summary_level)}
    )

    return result.fetchall()


def get_child_geoids_by_coverage(release, parent_geoid, child_summary_level):
    # Use the "worst"/biggest ACS to find all child geoids
    db.session.execute("SET search_path=:acs,public;", {'acs': release})
    result = db.session.execute(
        """SELECT geoid, name
           FROM tiger2020.census_geo_containment, geoheader
           WHERE geoheader.geoid = census_geo_containment.child_geoid
             AND census_geo_containment.parent_geoid = :parent_geoid
             AND census_geo_containment.child_geoid LIKE :child_geoids""",
        {'parent_geoid': parent_geoid, 'child_geoids': child_summary_level + '%'}
    )

    rowdicts = []
    seen_geoids = set()
    for row in result:
        if not row['geoid'] in seen_geoids:
            rowdicts.append(row)
            seen_geoids.add(row['geoid'])

    return rowdicts


def get_child_geoids_by_gis(release, parent_geoid, child_summary_level):
    parent_sumlevel = parent_geoid[0:3]
    child_geoids = []
    result = db.session.execute(
        """SELECT child.full_geoid
           FROM tiger2020.census_name_lookup parent
           JOIN tiger2020.census_name_lookup child ON ST_Intersects(parent.geom, child.geom) AND child.sumlevel=:child_sumlevel
           WHERE parent.full_geoid=:parent_geoid AND parent.sumlevel=:parent_sumlevel""",
        {'child_sumlevel': child_summary_level, 'parent_geoid': parent_geoid, 'parent_sumlevel': parent_sumlevel}
    )
    child_geoids = [r['full_geoid'] for r in result]

    if child_geoids:
        # Use the "worst"/biggest ACS to find all child geoids
        db.session.execute("SET search_path=:acs,public;", {'acs': release})
        result = db.session.execute(
            """SELECT geoid,name
               FROM geoheader
               WHERE geoid IN :child_geoids
               ORDER BY name""",
            {'child_geoids': tuple(child_geoids)}
        )
        return result.fetchall()
    else:
        return []


def get_child_geoids_by_prefix(release, parent_geoid, child_summary_level):
    child_geoid_prefix = '%s00US%s%%' % (child_summary_level, parent_geoid.upper().split('US')[1])

    # Use the "worst"/biggest ACS to find all child geoids
    db.session.execute("SET search_path=:acs,public;", {'acs': release})
    result = db.session.execute(
        """SELECT geoid,name
           FROM geoheader
           WHERE geoid LIKE :geoid_prefix
             AND name NOT LIKE :not_name
           ORDER BY geoid""",
        {'geoid_prefix': child_geoid_prefix, 'not_name': '%%not defined%%'}
    )
    return result.fetchall()


def expand_geoids(geoid_list, release):
    # Look for geoid "groups" of the form `child_sumlevel|parent_geoid`.
    # These will expand into a list of geoids like the old comparison endpoint used to
    expanded_geoids = set()
    explicit_geoids = set()
    child_parent_map = {}
    for geoid_str in geoid_list:
        if not expandable_geoid_re.match(geoid_str):
            continue

        geoid_split = geoid_str.split('|')
        if len(geoid_split) == 2 and len(geoid_split[0]) == 3:
            (child_summary_level, parent_geoid) = geoid_split
            child_geoid_list = [child_geoid['geoid'] for child_geoid in get_child_geoids(release, parent_geoid, child_summary_level)]
            expanded_geoids.update(child_geoid_list)
            for child_geoid in child_geoid_list:
                child_parent_map[child_geoid] = parent_geoid
        else:
            explicit_geoids.add(geoid_str)

    # Since the expanded geoids were sourced from the database they don't need to be checked
    valid_geo_ids = set(expanded_geoids)

    release_to_use = None

    # Check to make sure the geo ids the user entered are valid
    if explicit_geoids:
        db.session.execute("SET search_path=:acs,public;", {'acs': release})
        result = db.session.execute(
            """SELECT geoid
            FROM geoheader
            WHERE geoid IN :geoids;""",
            {'geoids': tuple(explicit_geoids)}
        )
        valid_geo_ids.update(geo['geoid'] for geo in result)

    invalid_geo_ids = expanded_geoids.union(explicit_geoids) - valid_geo_ids
    if invalid_geo_ids:
        raise ShowDataException("The %s release doesn't include GeoID(s) %s." % (get_acs_name(release), ','.join(invalid_geo_ids)))

    return valid_geo_ids, child_parent_map


class ShowDataException(Exception):
    pass


# Example: /1.0/data/show/acs2012_5yr?table_ids=B01001,B01003&geo_ids=04000US55,04000US56
# Example: /1.0/data/show/latest?table_ids=B01001&geo_ids=160|04000US17,04000US56
@app.route("/1.0/data/show/<acs>")
@qwarg_validate({
    'table_ids': {'valid': StringList(item_validator=Regex(table_re)), 'required': True},
    'geo_ids': {'valid': StringList(item_validator=Regex(expandable_geoid_re)), 'required': True},
})
@cross_origin(origin='*')
def show_specified_data(acs):
    if acs in allowed_acs:
        acs_to_try = [acs]
    elif acs == 'latest':
        acs_to_try = allowed_acs
    else:
        abort(404, 'The %s release isn\'t supported.' % get_acs_name(acs))

    # look for the releases that have the requested geoids
    releases_to_use = []
    expand_errors = []
    valid_geo_ids = set()
    requested_geo_ids = request.qwargs.geo_ids
    for release in acs_to_try:
        try:
            this_valid_geo_ids, child_parent_map = expand_geoids(requested_geo_ids, release)

            if this_valid_geo_ids:
                releases_to_use.append(release)
                valid_geo_ids.update(this_valid_geo_ids)
        except ShowDataException as e:
            expand_errors.append(e)
            continue

    if not releases_to_use:
        abort(400, 'None of the releases had all the requested geo_ids: %s' % ', '.join(str(e) for e in expand_errors))

    if not valid_geo_ids:
        abort(404, 'None of the geo_ids specified were valid: %s' % ', '.join(requested_geo_ids))

    max_geoids = current_app.config.get('MAX_GEOIDS_TO_SHOW', 1000)
    if len(valid_geo_ids) > max_geoids:
        abort(400, 'You requested %s geoids. The maximum is %s. Please contact us for bulk data.' % (len(valid_geo_ids), max_geoids))

    # expand_geoids has validated parents of groups by getting children;
    # this will include those parent names in the reponse `geography` list
    # but leave them out of the response `data` list
    grouped_geo_ids = [item for item in requested_geo_ids if "|" in item]
    parents_of_groups = set([item_group.split('|')[1] for item_group in grouped_geo_ids])
    named_geo_ids = valid_geo_ids | parents_of_groups

    # Fill in the display name for the geos
    result = db.session.execute(
        """SELECT full_geoid,population,display_name
           FROM tiger2020.census_name_lookup
           WHERE full_geoid IN :geoids;""",
        {'geoids': tuple(named_geo_ids)}
    )

    geo_metadata = OrderedDict()
    for geo in result:
        geo_metadata[geo['full_geoid']] = {
            'name': geo['display_name'],
        }
        # let children know who their parents are to distinguish between
        # groups at the same summary level
        if geo['full_geoid'] in child_parent_map:
            geo_metadata[geo['full_geoid']]['parent_geoid'] = child_parent_map[geo['full_geoid']]

    for release_to_use in releases_to_use:
        db.session.execute("SET search_path=:acs, public;", {'acs': release_to_use})

        # Check to make sure the tables requested are valid
        result = db.session.execute(
            """SELECT tab.table_id,
                        tab.table_title,
                        tab.universe,
                        tab.denominator_column_id,
                        col.column_id,
                        col.column_title,
                        col.indent
                FROM census_column_metadata col
                LEFT JOIN census_table_metadata tab USING (table_id)
                WHERE table_id IN :table_ids
                ORDER BY column_id;""",
            {'table_ids': tuple(request.qwargs.table_ids)}
        )

        valid_table_ids = []
        table_metadata = OrderedDict()
        for table, columns in groupby(result, lambda x: (x['table_id'], x['table_title'], x['universe'], x['denominator_column_id'])):
            valid_table_ids.append(table[0])
            table_metadata[table[0]] = OrderedDict([
                ("title", table[1]),
                ("universe", table[2]),
                ("denominator_column_id", table[3]),
                ("columns", OrderedDict([(
                    column['column_id'],
                    OrderedDict([
                        ("name", column['column_title']),
                        ("indent", column['indent'])
                    ])
                ) for column in columns]))
            ])

        invalid_table_ids = set(request.qwargs.table_ids) - set(valid_table_ids)
        if invalid_table_ids:
            resp = jsonify(error="The %s release doesn't include table(s) %s." % (get_acs_name(release_to_use), ','.join(invalid_table_ids)))
            resp.status_code = 404
            return resp

        # Now fetch the actual data
        from_stmt = '%s_moe' % (valid_table_ids[0])
        if len(valid_table_ids) > 1:
            from_stmt += ' '
            from_stmt += ' '.join(['JOIN %s_moe USING (geoid)' % (table_id) for table_id in valid_table_ids[1:]])

        sql = 'SELECT * FROM %s WHERE geoid IN :geoids;' % (from_stmt,)

        result = db.session.execute(sql, {'geoids': tuple(valid_geo_ids)})
        data = OrderedDict()

        if result.rowcount != len(valid_geo_ids):
            returned_geo_ids = set([row['geoid'] for row in result])
            app.logger.info(
                "show_specified_data: The %s release doesn't include GeoID(s) %s. for table(s) %s"
                % (get_acs_name(release_to_use),
                ','.join(set(valid_geo_ids) - returned_geo_ids),
                ','.join(valid_table_ids)))
            continue

        for row in result:
            row = dict(row)
            geoid = row.pop('geoid')
            data_for_geoid = OrderedDict()

            # If we end up at the 'most complete' release, we should include every bit of
            # data we can instead of erroring out on the user.
            # See https://www.pivotaltracker.com/story/show/70906084
            this_geo_has_data = False or release_to_use == allowed_acs[-1]

            cols_iter = iter(sorted(list(row.items()), key=lambda tup: tup[0]))
            for table_id, data_iter in groupby(cols_iter, lambda x: x[0][:-3].upper()):
                table_for_geoid = OrderedDict()
                table_for_geoid['estimate'] = OrderedDict()
                table_for_geoid['error'] = OrderedDict()

                for (col_name, value) in data_iter:
                    col_name = col_name.upper()
                    (moe_name, moe_value) = next(cols_iter)

                    if value is not None and moe_value is not None:
                        this_geo_has_data = True

                    table_for_geoid['estimate'][col_name] = value
                    table_for_geoid['error'][col_name] = moe_value

                if this_geo_has_data:
                    data_for_geoid[table_id] = table_for_geoid
                else:
                    current_app.logger.debug(f"The {release_to_use} release doesn't have data for table {table_id}, geoid {geoid} so we'll skip it and rely on the next release to cover this case")
                    continue

            data[geoid] = data_for_geoid

        # if we have data for all geographies, send it back...
        valid_geos_for_release = set(geoid for geoid,geo_data in data.items()
                                     if len(geo_data) == len(valid_table_ids))
        if len(valid_geos_for_release) == len(valid_geo_ids):
            resp_data = {
                'tables': table_metadata,
                'geography': geo_metadata,
                'data': data,
                'release': {
                    'id': release_to_use,
                    'years': ACS_NAMES[release_to_use]['years'],
                    'name': ACS_NAMES[release_to_use]['name']
                }
            }
            return jsonify(**resp_data)
        else:
            missing_geos = valid_geo_ids.difference(valid_geos_for_release)
            app.logger.debug(f"[release {release_to_use}] [table {','.join(valid_table_ids)}] missing data for [{','.join(missing_geos)}]")

    return abort(400, "None of the releases had the requested geo_ids and table_ids")


# Example: /1.0/data/download/acs2012_5yr?format=shp&table_ids=B01001,B01003&geo_ids=04000US55,04000US56
# Example: /1.0/data/download/latest?table_ids=B01001&geo_ids=160|04000US17,04000US56
@app.route("/1.0/data/download/<acs>")
@qwarg_validate({
    'table_ids': {'valid': StringList(item_validator=Regex(table_re)), 'required': True},
    'geo_ids': {'valid': StringList(item_validator=Regex(expandable_geoid_re)), 'required': True},
    'format': {'valid': OneOf(supported_formats), 'required': True},
})
@cross_origin(origin='*')
def download_specified_data(acs):
    if acs in allowed_acs:
        acs_to_try = [acs]
    elif acs == 'latest':
        acs_to_try = allowed_acs
    else:
        abort(404, 'The %s release isn\'t supported.' % get_acs_name(acs))

    # look for the releases that have the requested geoids
    releases_to_use = []
    expand_errors = []
    valid_geo_ids = set()
    requested_geo_ids = request.qwargs.geo_ids
    for release in acs_to_try:
        try:
            this_valid_geo_ids, child_parent_map = expand_geoids(requested_geo_ids, release)

            if this_valid_geo_ids:
                releases_to_use.append(release)
                valid_geo_ids.update(this_valid_geo_ids)
        except ShowDataException as e:
            expand_errors.append(e)
            continue

    if not releases_to_use:
        abort(400, 'None of the releases had all the requested geo_ids: %s' % ', '.join(str(e) for e in expand_errors))

    if not valid_geo_ids:
        abort(404, 'None of the geo_ids specified were valid: %s' % ', '.join(valid_geo_ids))

    max_geoids = current_app.config.get('MAX_GEOIDS_TO_DOWNLOAD', 1000)
    if len(valid_geo_ids) > max_geoids:
        abort(400, 'You requested %s geoids. The maximum is %s. Please contact us for bulk data.' % (len(valid_geo_ids), max_geoids))

    # Fill in the display name for the geos
    result = db.session.execute(
        """SELECT full_geoid,
                  population,
                  display_name
           FROM tiger2020.census_name_lookup
           WHERE full_geoid IN :geo_ids;""",
        {'geo_ids': tuple(valid_geo_ids)}
    )

    geo_metadata = OrderedDict()
    for geo in result:
        geo_metadata[geo['full_geoid']] = {
            "name": geo['display_name'],
        }

    for release_to_use in releases_to_use:
        db.session.execute("SET search_path=:acs, public;", {'acs': release_to_use})

        # Check to make sure the tables requested are valid
        result = db.session.execute(
            """SELECT tab.table_id,
                        tab.table_title,
                        tab.universe,
                        tab.denominator_column_id,
                        col.column_id,
                        col.column_title,
                        col.indent
                FROM census_column_metadata col
                LEFT JOIN census_table_metadata tab USING (table_id)
                WHERE table_id IN :table_ids
                ORDER BY column_id;""",
            {'table_ids': tuple(request.qwargs.table_ids)}
        )

        valid_table_ids = []
        table_metadata = OrderedDict()
        for table, columns in groupby(result, lambda x: (x['table_id'], x['table_title'], x['universe'], x['denominator_column_id'])):
            valid_table_ids.append(table[0])
            table_metadata[table[0]] = OrderedDict([
                ("title", table[1]),
                ("universe", table[2]),
                ("denominator_column_id", table[3]),
                ("columns", OrderedDict([(
                    column['column_id'],
                    OrderedDict([
                        ("name", column['column_title']),
                        ("indent", column['indent'])
                    ])
                ) for column in columns]))
            ])

        invalid_table_ids = set(request.qwargs.table_ids) - set(valid_table_ids)
        if invalid_table_ids:
            resp = jsonify(error="The %s release doesn't include table(s) %s." % (get_acs_name(release_to_use), ','.join(invalid_table_ids)))
            resp.status_code = 404
            return resp

        # Now fetch the actual data
        from_stmt = '%s_moe' % (valid_table_ids[0])
        if len(valid_table_ids) > 1:
            from_stmt += ' '
            from_stmt += ' '.join(['JOIN %s_moe USING (geoid)' % (table_id) for table_id in valid_table_ids[1:]])

        sql = 'SELECT * FROM %s WHERE geoid IN :geo_ids;' % (from_stmt,)

        result = db.session.execute(sql, {'geo_ids': tuple(valid_geo_ids)})
        data = OrderedDict()

        if result.rowcount != len(valid_geo_ids):
            returned_geo_ids = set([row['geoid'] for row in result])
            raise ShowDataException("The %s release doesn't include GeoID(s) %s." % (get_acs_name(release_to_use), ','.join(set(valid_geo_ids) - returned_geo_ids)))

        for row in result.fetchall():
            row = dict(row)
            geoid = row.pop('geoid')
            data_for_geoid = OrderedDict()

            cols_iter = iter(sorted(list(row.items()), key=lambda tup: tup[0]))
            for table_id, data_iter in groupby(cols_iter, lambda x: x[0][:-3].upper()):
                table_for_geoid = OrderedDict()
                table_for_geoid['estimate'] = OrderedDict()
                table_for_geoid['error'] = OrderedDict()

                for (col_name, value) in data_iter:
                    col_name = col_name.upper()
                    (moe_name, moe_value) = next(cols_iter)

                    table_for_geoid['estimate'][col_name] = value
                    table_for_geoid['error'][col_name] = moe_value

                data_for_geoid[table_id] = table_for_geoid

            data[geoid] = data_for_geoid

        temp_path = tempfile.mkdtemp()
        file_ident = "%s_%s_%s" % (acs, next(iter(valid_table_ids)), next(iter(valid_geo_ids)))
        inner_path = os.path.join(temp_path, file_ident)
        os.mkdir(inner_path)
        out_filename = os.path.join(inner_path, '%s.%s' % (file_ident, request.qwargs.format))
        format_info = supported_formats.get(request.qwargs.format)
        builder_func = format_info['function']
        builder_func(db.session, data, table_metadata, valid_geo_ids, file_ident, out_filename, request.qwargs.format)

        metadata_dict = {
            'release': {
                'id': release_to_use,
                'years': ACS_NAMES[release_to_use]['years'],
                'name': ACS_NAMES[release_to_use]['name']
            },
            'tables': table_metadata
        }
        json.dump(metadata_dict, open(os.path.join(inner_path, 'metadata.json'), 'w'), indent=4)

        zfile_path = os.path.join(temp_path, file_ident + '.zip')
        zfile = zipfile.ZipFile(zfile_path, 'w', zipfile.ZIP_DEFLATED)
        for root, dirs, files in os.walk(inner_path):
            for f in files:
                zfile.write(os.path.join(root, f), os.path.join(file_ident, f))
        zfile.close()

        resp = send_file(zfile_path, as_attachment=True, attachment_filename=file_ident + '.zip')

        shutil.rmtree(temp_path)

        return resp

    return abort(400, "None of the releases had the requested geo_ids and table_ids")


# Example: /1.0/data/compare/acs2012_5yr/B01001?sumlevel=050&within=04000US53
@app.route("/1.0/data/compare/<acs>/<table_id>")
@qwarg_validate({
    'within': {'valid': Regex(geoid_re), 'required': True},
    'sumlevel': {'valid': OneOf(SUMLEV_NAMES), 'required': True},
    'geom': {'valid': Bool(), 'default': False}
})
@cross_origin(origin='*')
def data_compare_geographies_within_parent(acs, table_id):
    # make sure we support the requested ACS release
    if acs not in allowed_acs:
        abort(404, 'The %s release isn\'t supported.' % get_acs_name(acs))
    db.session.execute("SET search_path=:acs, public;", {'acs': acs})

    parent_geoid = request.qwargs.within
    child_summary_level = request.qwargs.sumlevel

    # create the containers we need for our response
    comparison = OrderedDict()
    table = OrderedDict()
    parent_geography = OrderedDict()
    child_geographies = OrderedDict()

    # add some basic metadata about the comparison and data table requested.
    comparison['child_summary_level'] = child_summary_level
    comparison['child_geography_name'] = SUMLEV_NAMES.get(child_summary_level, {}).get('name')
    comparison['child_geography_name_plural'] = SUMLEV_NAMES.get(child_summary_level, {}).get('plural')

    result = db.session.execute(
        """SELECT tab.table_id,
                  tab.table_title,
                  tab.universe,
                  tab.denominator_column_id,
                  col.column_id,
                  col.column_title,
                  col.indent
           FROM census_column_metadata col
           LEFT JOIN census_table_metadata tab USING (table_id)
           WHERE table_id=:table_ids
           ORDER BY column_id;""",
        {'table_ids': table_id}
    )
    table_metadata = result.fetchall()

    if not table_metadata:
        abort(404, 'Table %s isn\'t available in the %s release.' % (table_id.upper(), get_acs_name(acs)))

    validated_table_id = table_metadata[0]['table_id']

    # get the basic table record, and add a map of columnID -> column name
    table_record = table_metadata[0]
    column_map = OrderedDict()
    for record in table_metadata:
        if record['column_id']:
            column_map[record['column_id']] = OrderedDict()
            column_map[record['column_id']]['name'] = record['column_title']
            column_map[record['column_id']]['indent'] = record['indent']

    table['census_release'] = ACS_NAMES.get(acs).get('name')
    table['table_id'] = validated_table_id
    table['table_name'] = table_record['table_title']
    table['table_universe'] = table_record['universe']
    table['denominator_column_id'] = table_record['denominator_column_id']
    table['columns'] = column_map

    # add some data about the parent geography
    result = db.session.execute("SELECT * FROM geoheader WHERE geoid=:geoid;", {'geoid': parent_geoid})
    parent_geoheader = result.fetchone()
    parent_sumlevel = '%03d' % parent_geoheader['sumlevel']

    parent_geography['geography'] = OrderedDict()
    parent_geography['geography']['name'] = parent_geoheader['name']
    parent_geography['geography']['summary_level'] = parent_sumlevel

    comparison['parent_summary_level'] = parent_sumlevel
    comparison['parent_geography_name'] = SUMLEV_NAMES.get(parent_sumlevel, {}).get('name')
    comparison['parent_name'] = parent_geoheader['name']
    comparison['parent_geoid'] = parent_geoid

    child_geoheaders = get_child_geoids(parent_geoid, child_summary_level)

    # start compiling child data for our response
    child_geoid_list = [geoheader['geoid'] for geoheader in child_geoheaders]
    child_geoid_names = dict([(geoheader['geoid'], geoheader['name']) for geoheader in child_geoheaders])

    # get geographical data if requested
    child_geodata_map = {}
    if request.qwargs.geom:
        # get the parent geometry and add to API response
        result = db.session.execute(
            """SELECT ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom,0.001), 5) as geometry
               FROM tiger2020.census_name_lookup
               WHERE full_geoid=:geo_ids;""",
            {'geo_ids': parent_geoid}
        )
        parent_geometry = result.fetchone()
        try:
            parent_geography['geography']['geometry'] = json.loads(parent_geometry['geometry'])
        except Exception:
            # we may not have geometries for all sumlevs
            pass

        # get the child geometries and store for later
        result = db.session.execute(
            """SELECT geoid, ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom,0.001), 5) as geometry
               FROM tiger2020.census_name_lookup
               WHERE full_geoid IN :geo_ids
               ORDER BY full_geoid;""",
            {'geo_ids': tuple(child_geoid_list)}
        )
        child_geodata = result.fetchall()
        child_geodata_map = dict([(record['geoid'], json.loads(record['geometry'])) for record in child_geodata])

    # make the where clause and query the requested census data table
    # get parent data first...
    result = db.session.execute("SELECT * FROM %s_moe WHERE geoid=:geoid" % (validated_table_id), {'geoid': parent_geoheader['geoid']})
    parent_data = result.fetchone()
    parent_data.pop('geoid', None)
    column_data = []
    column_moe = []
    sorted_data = iter(sorted(list(parent_data.items()), key=lambda tup: tup[0]))
    for (k, v) in sorted_data:
        (moe_k, moe_v) = next(sorted_data)
        column_data.append((k.upper(), v))
        column_moe.append((k.upper(), moe_v))
    parent_geography['data'] = OrderedDict(column_data)
    parent_geography['error'] = OrderedDict(column_moe)

    if child_geoheaders:
        # ... and then children so we can loop through with cursor
        child_geoids = [child['geoid'] for child in child_geoheaders]
        result = db.session.execute("SELECT * FROM %s_moe WHERE geoid IN :geo_ids" % (validated_table_id), {'geo_ids': tuple(child_geoids)})

        # grab one row at a time
        for record in result:
            child_geoid = record.pop('geoid')

            child_data = OrderedDict()
            this_geo_has_data = False

            # build the child item
            child_data['geography'] = OrderedDict()
            child_data['geography']['name'] = child_geoid_names[child_geoid]
            child_data['geography']['summary_level'] = child_summary_level

            column_data = []
            column_moe = []
            sorted_data = iter(sorted(list(record.items()), key=lambda tup: tup[0]))
            for (k, v) in sorted_data:

                if v is not None and moe_v is not None:
                    this_geo_has_data = True

                (moe_k, moe_v) = next(sorted_data)
                column_data.append((k.upper(), v))
                column_moe.append((k.upper(), moe_v))
            child_data['data'] = OrderedDict(column_data)
            child_data['error'] = OrderedDict(column_moe)

            if child_geodata_map:
                try:
                    child_data['geography']['geometry'] = child_geodata_map[child_geoid.split('US')[1]]
                except Exception:
                    # we may not have geometries for all sumlevs
                    pass

            if this_geo_has_data:
                child_geographies[child_geoid] = child_data

            # TODO Do we really need this?
            comparison['results'] = len(child_geographies)
    else:
        comparison['results'] = 0

    return jsonify(comparison=comparison, table=table, parent_geography=parent_geography, child_geographies=child_geographies)


@app.route('/healthcheck')
def healthcheck():
    return 'OK'


@app.route('/robots.txt')
def robots_txt():
    response = make_response('User-agent: *\nDisallow: /\n')
    response.headers["Content-type"] = "text/plain"
    return response


@app.route('/')
def index():
    return redirect('https://github.com/censusreporter/census-api/blob/master/API.md')

# Begin User Geography aggregation routes
@app.route("/1.0/user_geo/import",methods=['POST'])
@cross_origin(origin='*')
def import_geography():
    start = datetime.now()
    result = {
        'ok': False,
        'message': 'Not processed yet',
        'existing': False,
        'dataset_id': None,
        'hash_digest': None
    }
    if request.is_json:
        # TODO: validate that it's GeoJSON; validate size and such
        try:
            request_data = request.json
            geojson_str = json.dumps(request_data['geojson'])
            hash_digest = hashlib.md5(geojson_str.encode('utf-8')).hexdigest()

            existing = fetch_user_geodata(db, hash_digest)
            if existing:
                result['hash_digest'] = hash_digest
                result['dataset_id'] = existing['user_geodata_id']
                result['message'] = 'Dataset previously imported'
                result['existing'] = True
                result['ok'] = True
                return jsonify(result)

            dataset_id = save_user_geojson(db,
                    geojson_str,
                    hash_digest,
                    request_data.get('dataset_name'),
                    request_data.get('name_field'),
                    request_data.get('id_field'),
                    request_data.get('source_url'),
                    request_data.get('share_checked',False)
                    )
            if dataset_id is not None:
                result['ok'] = True
                result['message'] = 'Dataset loaded'

            result['dataset_id'] = dataset_id
            result['hash_digest'] = hash_digest

        except Exception as e:
            result['message'] = f"Exception: {e}"
    else:
        result['message'] = 'This endpoint only accepts JSON data.'

    result['elapsed_time'] = str(datetime.now()-start)
    return jsonify(result)


# Begin User Geography aggregation routes
@app.route("/1.0/user_geo/list")
@cross_origin(origin='*')
def fetch_user_geographies():

    result = {
        'ok': False,
        'message': 'Not processed yet',
        'geos': []
    }

    try:
        geos = list_user_geographies(db)
        result['ok'] = True
        result['message'] = f'Found {len(geos)} public geographies.'
        result['geos'] = geos
    except Exception as e:
        result['message'] = f'Error {e}'

    return jsonify(result)

@app.route('/1.0/user_geo/<string:hash_digest>')
@cross_origin(origin='*')
def fetch_user_geo(hash_digest):
    result = fetch_user_geodata(db, hash_digest)
    if result is None:
        abort(404)
    if result['status'] == 'NEW':
        join_user_geo_to_blocks_task.delay(result['user_geodata_id'])
        result['status'] = 'PROCESSING'
        result['message'] = "Found status NEW so requested processing."
    result['geojson'] = fetch_user_geog_as_geojson(db, hash_digest)
    return jsonify(result)

@app.route('/1.0/user_geo/<string:hash_digest>.geojson')
@cross_origin(origin='*')
def fetch_user_geojson(hash_digest):
    result = fetch_user_geog_as_geojson(db, hash_digest)
    if result is None:
        abort(404)
    return jsonify(result)

# some browsers weren't liking the CNAME form for some reason...
AGGREGATION_S3_ROOT = 'https://s3.amazonaws.com/files.censusreporter.org/aggregation'

@app.route('/1.0/user_geo/<string:hash_digest>/blocks/<string:year>')
@cross_origin(origin='*')
def fetch_user_blocks_by_year(hash_digest, year):

    # this is entangled with the S3 upload in user_geo, so if the name or S3 prefix change,
    # check that too, or refactor for single point of control
    zipfile_name = build_filename(hash_digest, year, 'block_assignments', 'zip')
    precomputed_url = f"{AGGREGATION_S3_ROOT}/{hash_digest}/{zipfile_name}"
    if url_exists(precomputed_url):
        return redirect(precomputed_url)

    try:
        start = timer()
        zf = create_block_xref_download(db, hash_digest, year)
        end = timer()
        return send_file(zf.name, 'application/zip', attachment_filename=zipfile_name)
    except ValueError:
        abort(404)


def url_exists(url):
    try:
        resp = requests.head(url)
        return resp.ok
    except:
        app.logger.warn("Error testing URL existence")
        return False

@app.route('/1.0/aggregate/<string:hash_digest>/<string:release>/<string:table_code>',methods=['GET'])
@cross_origin(origin='*')
def aggregate(hash_digest, release, table_code):

    if table_code.lower() not in ['p1', 'p2', 'p3', 'p4', 'p5', 'h1']:
        abort(404)

    if release.lower() not in ['dec2010_pl94', 'dec2020_pl94', COMPARISON_RELEASE_CODE]:
        abort(404)

    # the one we can't compare
    if table_code.lower() == 'p5' and release.lower() == COMPARISON_RELEASE_CODE:
        abort(404)

    if not re.match('[A-Fa-f0-9]{32}', hash_digest):
        abort(404)

    # this is entangled with the S3 upload in user_geo, so if the name or S3 prefix change,
    # check that too, or refactor for single point of control
    zipfile_name = build_filename(hash_digest, release, table_code, 'zip')
    precomputed_url = f"{AGGREGATION_S3_ROOT}/{hash_digest}/{zipfile_name}"
    if url_exists(precomputed_url):
        return redirect(precomputed_url)

    start = timer()
    zf = create_aggregate_download(db, hash_digest, release, table_code)
    end = timer()
    return send_file(zf.name, 'application/zip', attachment_filename=zipfile_name)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
