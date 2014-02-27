# For real division instead of sometimes-integer
from __future__ import division

from flask import Flask
from flask import abort, request, g
from flask import make_response, current_app, send_file
from flask import jsonify
from werkzeug.exceptions import HTTPException
from functools import update_wrapper
from itertools import groupby
import psycopg2
import psycopg2.extras
import simplejson as json
from collections import OrderedDict
import decimal
import operator
import math
from datetime import timedelta
import re
import os
import shutil
import tempfile
import urlparse
import zipfile
from validation import qwarg_validate, NonemptyString, FloatRange, StringList, Bool, OneOf


app = Flask(__name__)
app.config.from_object(os.environ.get('EXTRACTOMATIC_CONFIG_MODULE', 'census_extractomatic.config.Development'))

if not app.debug:
    import logging
    file_handler = logging.FileHandler('/tmp/api.censusreporter.org.wsgi_error.log')
    file_handler.setLevel(logging.WARNING)
    app.logger.addHandler(file_handler)

# Allowed ACS's in "best" order (newest and smallest range preferred)
allowed_acs = [
    'acs2012_1yr',
    'acs2012_3yr',
    'acs2012_5yr',
]

ACS_NAMES = {
    'acs2012_1yr': {'name': 'ACS 2012 1-year', 'years': '2012'},
    'acs2012_3yr': {'name': 'ACS 2012 3-year', 'years': '2010-2012'},
    'acs2012_5yr': {'name': 'ACS 2012 5-year', 'years': '2008-2012'},
}

PARENT_CHILD_CONTAINMENT = {
    '040': ['050', '060', '101', '140','150', '160', '500', '610', '620', '950', '960', '970'],
    '050': ['060', '101', '140', '150'],
    '140': ['101','150'],
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
    "250": {"name": "native area", "plural": "native areas", "tiger_table": "aiannh"},
    "300": {"name": "MSA", "plural": "MSAs", "tiger_table": "metdiv"},
    "310": {"name": "CBSA", "plural": "CBSAs", "tiger_table": "cbsa"},
    "330": {"name": "CSA", "plural": "CSAs", "tiger_table": "csa"},
    "350": {"name": "NECTA", "plural": "NECTAs", "tiger_table": "necta"},
    "400": {"name": "urban area", "plural": "urban areas", "tiger_table": "uac"},
    "500": {"name": "congressional district", "plural": "congressional districts", "tiger_table": "cd"},
    "610": {"name": "state senate district", "plural": "state senate districts", "tiger_table": "sldu"},
    "620": {"name": "state house district", "plural": "state house districts", "tiger_table": "sldl"},
    "700": {"name": "VTD", "plural": "VTDs", "tiger_table": "vtd"},
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

supported_formats = {
    'shp':      {"type": "ogr", "driver": "ESRI Shapefile"},
    'kml':      {"type": "ogr", "driver": "KML"},
    'geojson':  {"type": "ogr", "driver": "GeoJSON"},
    'xlsx':     {"type": "ogr", "driver": "XLSX"},
    'csv':      {"type": "ogr", "driver": "CSV"},
}

def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator

@app.errorhandler(400)
@app.errorhandler(500)
@crossdomain(origin='*')
def jsonify_error_handler(error):
    if isinstance(error, HTTPException):
        resp = jsonify(error=error.description)
        resp.status_code = error.code
    else:
        resp = jsonify(error=error.message)
        resp.status_code = 500
    return resp

def maybe_int(i):
    return int(i) if i else i

def percentify(val):
    return val * 100

def rateify(val):
    return val * 1000

def moe_add(moe_a, moe_b):
    # From http://www.census.gov/acs/www/Downloads/handbooks/ACSGeneralHandbook.pdf
    return math.sqrt(moe_a**2 + moe_b**2)

def moe_ratio(numerator, denominator, numerator_moe, denominator_moe):
    # From http://www.census.gov/acs/www/Downloads/handbooks/ACSGeneralHandbook.pdf
    estimated_ratio = numerator / denominator
    return math.sqrt(numerator_moe**2 + (estimated_ratio**2 * denominator_moe**2)) / denominator

ops = {
    '+': operator.add,
    '-': operator.sub,
    '/': operator.div,
    '%': percentify,
    '%%': rateify,
}
moe_ops = {
    '+': moe_add,
    '-': moe_add,
    '/': moe_ratio,
    '%': percentify,
    '%%': rateify,
}
def value_rpn_calc(data, rpn_string):
    stack = []
    moe_stack = []
    numerator = None
    numerator_moe = None

    for token in rpn_string.split():
        if token in ops:
            b = stack.pop()
            b_moe = moe_stack.pop()

            if token in ('%', '%%'):
                # Single-argument operators
                if b is None:
                    c = None
                    c_moe = None
                else:
                    c = ops[token](b)
                    c_moe = moe_ops[token](b_moe)
            else:
                a = stack.pop()
                a_moe = moe_stack.pop()

                if a is None or b is None:
                    c = None
                    c_moe = None
                elif token == '/':
                    # Broken out because MOE ratio needs both MOE and estimates
                    try:
                        c = ops[token](a, b)
                        c_moe = moe_ratio(a, b, a_moe, b_moe)
                        numerator = a
                        numerator_moe = round(a_moe, 1)
                    except ZeroDivisionError:
                        c = None
                        c_moe = None
                else:
                    c = ops[token](a, b)
                    c_moe = moe_ops[token](a_moe, b_moe)
        elif token.startswith('b'):
            c = data[token]
            c_moe = data[token + '_moe']
        else:
            c = float(token)
            c_moe = float(token)
        stack.append(c)
        moe_stack.append(c_moe)

    value = stack.pop()
    error = moe_stack.pop()

    return (value, error, numerator, numerator_moe)

def build_item(name, data, parents, rpn_string):
    val = OrderedDict([('name', name),
        ('values', dict()),
        ('error', dict()),
        ('numerators', dict()),
        ('numerator_errors', dict())])

    for parent in parents:
        label = parent['relation']
        geoid = parent['geoid']
        data_for_geoid = data.get(geoid) if data else {}

        value = None
        error = None
        numerator = None
        numerator_moe = None

        if data_for_geoid:
            (value, error, numerator, numerator_moe) = value_rpn_calc(data_for_geoid, rpn_string)

        # provide 2 decimals of precision, let client decide how much to use
        if value is not None:
            value = round(value, 2)
            error = round(error, 2)

        if numerator is not None:
            numerator = round(numerator, 2)
            numerator_moe = round(numerator_moe, 2)

        val['values'][label] = value
        val['error'][label] = error
        val['numerators'][label] = numerator
        val['numerator_errors'][label] = numerator_moe

    return val

def add_metadata(dictionary, table_id, universe, acs_release):
    val = dict(table_id=table_id,
        universe=universe,
        acs_release=acs_release,)

    dictionary['metadata'] = val

def find_geoid(geoid, acs=None):
    "Find the best acs to use for a given geoid or None if the geoid is not found."

    if acs:
        acs_to_search = [acs]
    else:
        acs_to_search = allowed_acs

    for acs in acs_to_search:
        g.cur.execute("SELECT geoid FROM %s.geoheader WHERE geoid=%%s" % acs, [geoid])
        if g.cur.rowcount == 1:
            result = g.cur.fetchone()
            return (acs, result['geoid'])
    return (None, None)


@app.before_request
def before_request():
    db_details = urlparse.urlparse(app.config['DATABASE_URI'])

    conn = psycopg2.connect(
        host=db_details.hostname,
        user=db_details.username,
        password=db_details.password,
        database=db_details.path[1:]
    )

    g.cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


@app.teardown_request
def teardown_request(exception):
    g.cur.close()

def get_data_fallback(table_ids, geoids, acs=None):
    if type(geoids) != list:
        geoids = [geoids]

    if type(table_ids) != list:
        table_ids = [table_ids]

    from_stmt = '%%(acs)s.%s_moe' % (table_ids[0])
    if len(table_ids) > 1:
        from_stmt += ' '
        from_stmt += ' '.join(['JOIN %%(acs)s.%s_moe USING (geoid)' % (table_id) for table_id in table_ids[1:]])

    where_stmt = g.cur.mogrify('geoid IN %s', [tuple(geoids)])

    sql = 'SELECT * FROM %s WHERE %s;' % (from_stmt, where_stmt)

    # if acs is specified, we'll use that one and not go searching for data.
    if acs in allowed_acs:
        g.cur.execute(sql % {'acs': acs})
        data = {}
        for row in g.cur:
            geoid = row.pop('geoid')
            data[geoid] = dict([(col, val) for (col, val) in row.iteritems()])

        return data, acs

    else:
        # otherwise we'll start at the best/most recent acs and move down til we have the data we want
        for acs in allowed_acs:
            g.cur.execute(sql % {'acs': acs})

            data = {}
            for row in g.cur:
                geoid = row.pop('geoid')
                data[geoid] = dict([(col, val) for (col, val) in row.iteritems()])

            # Check to see if this release has our data
            data_with_values = filter(lambda geoid_data: geoid_data.values()[0] is not None, data.values())
            if len(geoids) == len(data) and len(geoids) == len(data_with_values):
                return data, acs
            else:
                # Doesn't contain data for all geoids, so keep going.
                continue

    return None, acs


def compute_profile_item_levels(geoid):
    levels = []

    geoid_parts = geoid.split('US')
    if len(geoid_parts) is not 2:
        raise Exception('Invalid geoid')

    levels.append({
        'relation': 'this',
        'geoid': geoid,
        'coverage': 100.0,
    })

    sumlevel = geoid_parts[0][:3]
    id_part = geoid_parts[1]

    if sumlevel in ('140', '150', '160', '310', '330', '700', '860', '950', '960', '970'):
        g.cur.execute("""SELECT * FROM tiger2012.census_geo_containment WHERE child_geoid=%s ORDER BY percent_covered ASC""", [geoid])
        for row in g.cur:
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

    if sumlevel != '010':
        levels.append({
            'relation': 'nation',
            'geoid': '01000US',
            'coverage': 100.0,
        })

    return levels


def geo_profile(acs, geoid):
    acs_default = acs

    item_levels = compute_profile_item_levels(geoid)
    comparison_geoids = [level['geoid'] for level in item_levels]

    doc = OrderedDict([('geography', OrderedDict()),
                       ('demographics', dict()),
                       ('economics', dict()),
                       ('families', dict()),
                       ('housing', dict()),
                       ('social', dict())])


    # Demographics: Age
    # multiple data points, suitable for visualization
    data, acs = get_data_fallback('B01001', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')
    doc['geography']['census_release'] = acs_name

    g.cur.execute("""SELECT DISTINCT full_geoid,sumlevel,display_name,simple_name,aland
                     FROM tiger2012.census_name_lookup
                     WHERE full_geoid IN %s;""", [tuple(comparison_geoids)])

    def convert_geography_data(row):
        return dict(full_name=row['display_name'],
                    short_name=row['simple_name'],
                    sumlevel=row['sumlevel'],
                    land_area=row['aland'],
                    full_geoid=row['full_geoid'])

    lookup_data = {}
    doc['geography']['parents'] = OrderedDict()
    for row in g.cur:
        lookup_data[row['full_geoid']] = row

    for item_level in item_levels:
        name = item_level['relation']
        the_geoid = item_level['geoid']
        if name == 'this':
            doc['geography'][name] = convert_geography_data(lookup_data[the_geoid])
            doc['geography'][name]['total_population'] = maybe_int(data[the_geoid]['b01001001'])
        else:
            doc['geography']['parents'][name] = convert_geography_data(lookup_data[the_geoid])
            doc['geography']['parents'][name]['total_population'] = maybe_int(data[the_geoid]['b01001001'])

    age_dict = dict()
    doc['demographics']['age'] = age_dict

    cat_dict = OrderedDict()
    age_dict['distribution_by_category'] = cat_dict
    add_metadata(age_dict['distribution_by_category'], 'b01001', 'Total population', acs_name)

    cat_dict['percent_under_18'] = build_item('Under 18', data, item_levels,
        'b01001003 b01001004 + b01001005 + b01001006 + b01001027 + b01001028 + b01001029 + b01001030 + b01001001 / %')
    cat_dict['percent_18_to_64'] = build_item('18 to 64', data, item_levels,
        'b01001007 b01001008 + b01001009 + b01001010 + b01001011 + b01001012 + b01001013 + b01001014 + b01001015 + b01001016 + b01001017 + b01001018 + b01001019 + b01001031 + b01001032 + b01001033 + b01001034 + b01001035 + b01001036 + b01001037 + b01001038 + b01001039 + b01001040 + b01001041 + b01001042 + b01001043 + b01001001 / %')
    cat_dict['percent_over_65'] = build_item('65 and over', data, item_levels,
        'b01001020 b01001021 + b01001022 + b01001023 + b01001024 + b01001025 + b01001044 + b01001045 + b01001046 + b01001047 + b01001048 + b01001049 + b01001001 / %')

    pop_dict = dict()
    age_dict['distribution_by_decade'] = pop_dict
    population_by_age_total = OrderedDict()
    population_by_age_male = OrderedDict()
    population_by_age_female = OrderedDict()
    pop_dict['total'] = population_by_age_total
    add_metadata(pop_dict['total'], 'b01001', 'Total population', acs_name)
    pop_dict['male'] = population_by_age_male
    add_metadata(pop_dict['male'], 'b01001', 'Total population', acs_name)
    pop_dict['female'] = population_by_age_female
    add_metadata(pop_dict['female'], 'b01001', 'Total population', acs_name)

    population_by_age_male['0-9'] = build_item('0-9', data, item_levels,
        'b01001003 b01001004 + b01001002 / %')
    population_by_age_female['0-9'] = build_item('0-9', data, item_levels,
        'b01001027 b01001028 + b01001026 / %')
    population_by_age_total['0-9'] = build_item('0-9', data, item_levels,
        'b01001003 b01001004 + b01001027 + b01001028 + b01001001 / %')

    population_by_age_male['10-19'] = build_item('10-19', data, item_levels,
        'b01001005 b01001006 + b01001007 + b01001002 / %')
    population_by_age_female['10-19'] = build_item('10-19', data, item_levels,
        'b01001029 b01001030 + b01001031 + b01001026 / %')
    population_by_age_total['10-19'] = build_item('10-19', data, item_levels,
        'b01001005 b01001006 + b01001007 + b01001029 + b01001030 + b01001031 + b01001001 / %')

    population_by_age_male['20-29'] = build_item('20-29', data, item_levels,
        'b01001008 b01001009 + b01001010 + b01001011 + b01001002 / %')
    population_by_age_female['20-29'] = build_item('20-29', data, item_levels,
        'b01001032 b01001033 + b01001034 + b01001035 + b01001026 / %')
    population_by_age_total['20-29'] = build_item('20-29', data, item_levels,
        'b01001008 b01001009 + b01001010 + b01001011 + b01001032 + b01001033 + b01001034 + b01001035 + b01001001 / %')

    population_by_age_male['30-39'] = build_item('30-39', data, item_levels,
        'b01001012 b01001013 + b01001002 / %')
    population_by_age_female['30-39'] = build_item('30-39', data, item_levels,
        'b01001036 b01001037 + b01001026 / %')
    population_by_age_total['30-39'] = build_item('30-39', data, item_levels,
        'b01001012 b01001013 + b01001036 + b01001037 + b01001001 / %')

    population_by_age_male['40-49'] = build_item('40-49', data, item_levels,
        'b01001014 b01001015 + b01001002 / %')
    population_by_age_female['40-49'] = build_item('40-49', data, item_levels,
        'b01001038 b01001039 + b01001026 / %')
    population_by_age_total['40-49'] = build_item('40-49', data, item_levels,
        'b01001014 b01001015 + b01001038 + b01001039 + b01001001 / %')

    population_by_age_male['50-59'] = build_item('50-59', data, item_levels,
        'b01001016 b01001017 + b01001002 / %')
    population_by_age_female['50-59'] = build_item('50-59', data, item_levels,
        'b01001040 b01001041 + b01001026 / %')
    population_by_age_total['50-59'] = build_item('50-59', data, item_levels,
        'b01001016 b01001017 + b01001040 + b01001041 + b01001001 / %')

    population_by_age_male['60-69'] = build_item('60-69', data, item_levels,
        'b01001018 b01001019 + b01001020 + b01001021 + b01001002 / %')
    population_by_age_female['60-69'] = build_item('60-69', data, item_levels,
        'b01001042 b01001043 + b01001044 + b01001045 + b01001026 / %')
    population_by_age_total['60-69'] = build_item('60-69', data, item_levels,
        'b01001018 b01001019 + b01001020 + b01001021 + b01001042 + b01001043 + b01001044 + b01001045 + b01001001 / %')

    population_by_age_male['70-79'] = build_item('70-79', data, item_levels,
        'b01001022 b01001023 + b01001002 / %')
    population_by_age_female['70-79'] = build_item('70-79', data, item_levels,
        'b01001046 b01001047 + b01001026 / %')
    population_by_age_total['70-79'] = build_item('70-79', data, item_levels,
        'b01001022 b01001023 + b01001046 + b01001047 + b01001001 / %')

    population_by_age_male['80+'] = build_item('80+', data, item_levels,
        'b01001024 b01001025 + b01001002 / %')
    population_by_age_female['80+'] = build_item('80+', data, item_levels,
        'b01001048 b01001049 + b01001026 / %')
    population_by_age_total['80+'] = build_item('80+', data, item_levels,
        'b01001024 b01001025 + b01001048 + b01001049 + b01001001 / %')

    # Demographics: Sex
    # multiple data points, suitable for visualization
    sex_dict = OrderedDict()
    doc['demographics']['sex'] = sex_dict
    add_metadata(sex_dict, 'b01001', 'Total population', acs_name)
    sex_dict['percent_male'] = build_item('Male', data, item_levels,
        'b01001002 b01001001 / %')
    sex_dict['percent_female'] = build_item('Female', data, item_levels,
        'b01001026 b01001001 / %')

    data, acs = get_data_fallback('B01002', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    median_age_dict = dict()
    age_dict['median_age'] = median_age_dict
    median_age_dict['total'] = build_item('Median age', data, item_levels,
        'b01002001')
    add_metadata(median_age_dict['total'], 'b01001', 'Total population', acs_name)
    median_age_dict['male'] = build_item('Median age male', data, item_levels,
        'b01002002')
    add_metadata(median_age_dict['male'], 'b01001', 'Total population', acs_name)
    median_age_dict['female'] = build_item('Median age female', data, item_levels,
        'b01002003')
    add_metadata(median_age_dict['female'], 'b01001', 'Total population', acs_name)

    # Demographics: Race
    # multiple data points, suitable for visualization
    # uses Table B03002 (HISPANIC OR LATINO ORIGIN BY RACE), pulling race numbers from "Not Hispanic or Latino" columns
    # also collapses smaller groups into "Other"
    data, acs = get_data_fallback('B03002', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    race_dict = OrderedDict()
    doc['demographics']['race'] = race_dict
    add_metadata(race_dict, 'b03002', 'Total population', acs_name)

    race_dict['percent_white'] = build_item('White', data, item_levels,
        'b03002003 b03002001 / %')

    race_dict['percent_black'] = build_item('Black', data, item_levels,
        'b03002004 b03002001 / %')

    race_dict['percent_native'] = build_item('Native', data, item_levels,
        'b03002005 b03002001 / %')

    race_dict['percent_asian'] = build_item('Asian', data, item_levels,
        'b03002006 b03002001 / %')

    race_dict['percent_islander'] = build_item('Islander', data, item_levels,
        'b03002007 b03002001 / %')

    race_dict['percent_other'] = build_item('Other', data, item_levels,
        'b03002008 b03002001 / %')

    race_dict['percent_two_or_more'] = build_item('Two+', data, item_levels,
        'b03002009 b03002001 / %')

#    # collapsed version of "other"
#    race_dict['percent_other'] = build_item('Other', data, item_levels,
#        'b03002005 b03002007 + b03002008 + b03002009 + b03002001 / %')

    race_dict['percent_hispanic'] = build_item('Hispanic', data, item_levels,
        'b03002012 b03002001 / %')

    # Economics: Per-Capita Income
    # single data point
    data, acs = get_data_fallback('B19301', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    income_dict = dict()
    doc['economics']['income'] = income_dict

    income_dict['per_capita_income_in_the_last_12_months'] = build_item('Per capita income', data, item_levels,
        'b19301001')
    add_metadata(income_dict['per_capita_income_in_the_last_12_months'], 'b19301', 'Total population', acs_name)

    # Economics: Median Household Income
    # single data point
    data, acs = get_data_fallback('B19013', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    income_dict['median_household_income'] = build_item('Median household income', data, item_levels,
        'b19013001')
    add_metadata(income_dict['median_household_income'], 'b19013', 'Households', acs_name)

    # Economics: Household Income Distribution
    # multiple data points, suitable for visualization
    data, acs = get_data_fallback('B19001', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    income_distribution = OrderedDict()
    income_dict['household_distribution'] = income_distribution
    add_metadata(income_dict['household_distribution'], 'b19001', 'Households', acs_name)

    income_distribution['under_50'] = build_item('Under $50K', data, item_levels,
        'b19001002 b19001003 + b19001004 + b19001005 + b19001006 + b19001007 + b19001008 + b19001009 + b19001010 + b19001001 / %')
    income_distribution['50_to_100'] = build_item('$50K - $100K', data, item_levels,
        'b19001011 b19001012 + b19001013 + b19001001 / %')
    income_distribution['100_to_200'] = build_item('$100K - $200K', data, item_levels,
        'b19001014 b19001015 + b19001016 + b19001001 / %')
    income_distribution['over_200'] = build_item('Over $200K', data, item_levels,
        'b19001017 b19001001 / %')

    # Economics: Poverty Rate
    # provides separate dicts for children and seniors, with multiple data points, suitable for visualization
    data, acs = get_data_fallback('B17001', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    poverty_dict = dict()
    doc['economics']['poverty'] = poverty_dict

    poverty_dict['percent_below_poverty_line'] = build_item('Persons below poverty line', data, item_levels,
        'b17001002 b17001001 / %')
    add_metadata(poverty_dict['percent_below_poverty_line'], 'b17001', 'Population for whom poverty status is determined', acs_name)

    poverty_children = OrderedDict()
    poverty_seniors = OrderedDict()
    poverty_dict['children'] = poverty_children
    add_metadata(poverty_dict['children'], 'b17001', 'Population for whom poverty status is determined', acs_name)
    poverty_dict['seniors'] = poverty_seniors
    add_metadata(poverty_dict['seniors'], 'b17001', 'Population for whom poverty status is determined', acs_name)

    poverty_children['below'] = build_item('Poverty', data, item_levels,
        'b17001004 b17001005 + b17001006 + b17001007 + b17001008 + b17001009 + b17001018 + b17001019 + b17001020 + b17001021 + b17001022 + b17001023 + b17001004 b17001005 + b17001006 + b17001007 + b17001008 + b17001009 + b17001018 + b17001019 + b17001020 + b17001021 + b17001022 + b17001023 + b17001033 + b17001034 + b17001035 + b17001036 + b17001037 + b17001038 + b17001047 + b17001048 + b17001049 + b17001050 + b17001051 + b17001052 + / %')
    poverty_children['above'] = build_item('Non-poverty', data, item_levels,
        'b17001033 b17001034 + b17001035 + b17001036 + b17001037 + b17001038 + b17001047 + b17001048 + b17001049 + b17001050 + b17001051 + b17001052 + b17001004 b17001005 + b17001006 + b17001007 + b17001008 + b17001009 + b17001018 + b17001019 + b17001020 + b17001021 + b17001022 + b17001023 + b17001033 + b17001034 + b17001035 + b17001036 + b17001037 + b17001038 + b17001047 + b17001048 + b17001049 + b17001050 + b17001051 + b17001052 + / %')

    poverty_seniors['below'] = build_item('Poverty', data, item_levels,
        'b17001015 b17001016 + b17001029 + b17001030 + b17001015 b17001016 + b17001029 + b17001030 + b17001044 + b17001045 + b17001058 + b17001059 + / %')
    poverty_seniors['above'] = build_item('Non-poverty', data, item_levels,
        'b17001044 b17001045 + b17001058 + b17001059 + b17001015 b17001016 + b17001029 + b17001030 + b17001044 + b17001045 + b17001058 + b17001059 + / %')

    # Economics: Mean Travel Time to Work, Means of Transportation to Work
    # uses two different tables for calculation, so make sure they draw from same ACS release
    data, acs = get_data_fallback(['B08006', 'B08013'], comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    employment_dict = dict()
    doc['economics']['employment'] = employment_dict

    employment_dict['mean_travel_time'] = build_item('Mean travel time to work', data, item_levels,
        'b08013001 b08006001 b08006017 - /')
    add_metadata(employment_dict['mean_travel_time'], 'b08006, b08013', 'Workers 16 years and over who did not work at home', acs_name)

    data, acs = get_data_fallback('B08006', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    transportation_dict = OrderedDict()
    employment_dict['transportation_distribution'] = transportation_dict
    add_metadata(employment_dict['transportation_distribution'], 'b08006', 'Workers 16 years and over', acs_name)

    transportation_dict['drove_alone'] = build_item('Drove alone', data, item_levels,
        'b08006003 b08006001 / %')
    transportation_dict['carpooled'] = build_item('Carpooled', data, item_levels,
        'b08006004 b08006001 / %')
    transportation_dict['public_transit'] = build_item('Public transit', data, item_levels,
        'b08006008 b08006001 / %')
    transportation_dict['bicycle'] = build_item('Bicycle', data, item_levels,
        'b08006014 b08006001 / %')
    transportation_dict['walked'] = build_item('Walked', data, item_levels,
        'b08006015 b08006001 / %')
    transportation_dict['other'] = build_item('Other', data, item_levels,
        'b08006016 b08006001 / %')
    transportation_dict['worked_at_home'] = build_item('Worked at home', data, item_levels,
        'b08006017 b08006001 / %')

    # Families: Marital Status by Sex
    data, acs = get_data_fallback('B12001', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    marital_status = OrderedDict()
    doc['families']['marital_status'] = marital_status
    add_metadata(marital_status, 'b12001', 'Population 15 years and over', acs_name)

    marital_status['married'] = build_item('Married', data, item_levels,
        'b12001004 b12001013 + b12001001 / %')
    marital_status['single'] = build_item('Single', data, item_levels,
        'b12001003 b12001009 + b12001010 + b12001012 + b12001018 + b12001019 + b12001001 / %')

    marital_status_grouped = OrderedDict()
    doc['families']['marital_status_grouped'] = marital_status_grouped
    add_metadata(marital_status_grouped, 'b12001', 'Population 15 years and over', acs_name)

    # repeating data temporarily to develop grouped column chart format
    marital_status_grouped['never_married'] = OrderedDict()
    marital_status_grouped['never_married']['acs_release'] = acs_name
    marital_status_grouped['never_married']['metadata'] = {
        'universe': 'Population 15 years and over',
        'table_id': 'b12001',
        'name': 'Never married'
    }
    marital_status_grouped['never_married']['male'] = build_item('Male', data, item_levels,
        'b12001003 b12001002 / %')
    marital_status_grouped['never_married']['female'] = build_item('Female', data, item_levels,
        'b12001012 b12001011 / %')

    marital_status_grouped['married'] = OrderedDict()
    marital_status_grouped['married']['acs_release'] = acs_name
    marital_status_grouped['married']['metadata'] = {
        'universe': 'Population 15 years and over',
        'table_id': 'b12001',
        'name': 'Now married'
    }
    marital_status_grouped['married']['male'] = build_item('Male', data, item_levels,
        'b12001004 b12001002 / %')
    marital_status_grouped['married']['female'] = build_item('Female', data, item_levels,
        'b12001013 b12001011 / %')

    marital_status_grouped['divorced'] = OrderedDict()
    marital_status_grouped['divorced']['acs_release'] = acs_name
    marital_status_grouped['divorced']['metadata'] = {
        'universe': 'Population 15 years and over',
        'table_id': 'b12001',
        'name': 'Divorced'
    }
    marital_status_grouped['divorced']['male'] = build_item('Male', data, item_levels,
        'b12001010 b12001002 / %')
    marital_status_grouped['divorced']['female'] = build_item('Female', data, item_levels,
        'b12001019 b12001011 / %')

    marital_status_grouped['widowed'] = OrderedDict()
    marital_status_grouped['widowed']['acs_release'] = acs_name
    marital_status_grouped['widowed']['metadata'] = {
        'universe': 'Population 15 years and over',
        'table_id': 'b12001',
        'name': 'Widowed'
    }
    marital_status_grouped['widowed']['male'] = build_item('Male', data, item_levels,
        'b12001009 b12001002 / %')
    marital_status_grouped['widowed']['female'] = build_item('Female', data, item_levels,
        'b12001018 b12001011 / %')


    # Families: Family Types with Children
    data, acs = get_data_fallback('B09002', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    family_types = dict()
    doc['families']['family_types'] = family_types

    children_family_type_dict = OrderedDict()
    family_types['children'] = children_family_type_dict
    add_metadata(children_family_type_dict, 'b09002', 'Own children under 18 years', acs_name)

    children_family_type_dict['married_couple'] = build_item('Married couple', data, item_levels,
        'b09002002 b09002001 / %')
    children_family_type_dict['male_householder'] = build_item('Male householder', data, item_levels,
        'b09002009 b09002001 / %')
    children_family_type_dict['female_householder'] = build_item('Female householder', data, item_levels,
        'b09002015 b09002001 / %')

    # Families: Birth Rate by Women's Age
    data, acs = get_data_fallback('B13016', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    fertility = dict()
    doc['families']['fertility'] = fertility

    fertility['total'] = build_item('Women 15-50 who gave birth during past year', data, item_levels,
        'b13016002 b13016001 / %')
    add_metadata(fertility['total'], 'b13016', 'Women 15 to 50 years', acs_name)

    fertility_by_age_dict = OrderedDict()
    fertility['by_age'] = fertility_by_age_dict
    add_metadata(fertility['by_age'], 'b13016', 'Women 15 to 50 years', acs_name)

    fertility_by_age_dict['15_to_19'] = build_item('15-19', data, item_levels,
        'b13016003 b13016003 b13016011 + / %')
    fertility_by_age_dict['20_to_24'] = build_item('20-24', data, item_levels,
        'b13016004 b13016004 b13016012 + / %')
    fertility_by_age_dict['25_to_29'] = build_item('25-29', data, item_levels,
        'b13016005 b13016005 b13016013 + / %')
    fertility_by_age_dict['30_to_34'] = build_item('30-35', data, item_levels,
        'b13016006 b13016006 b13016014 + / %')
    fertility_by_age_dict['35_to_39'] = build_item('35-39', data, item_levels,
        'b13016007 b13016007 b13016015 + / %')
    fertility_by_age_dict['40_to_44'] = build_item('40-44', data, item_levels,
        'b13016008 b13016008 b13016016 + / %')
    fertility_by_age_dict['45_to_50'] = build_item('45-50', data, item_levels,
        'b13016009 b13016009 b13016017 + / %')

    # Families: Number of Households, Persons per Household, Household type distribution
    data, acs = get_data_fallback(['B11001', 'B11002'], comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    households_dict = dict()
    doc['families']['households'] = households_dict

    households_dict['number_of_households'] = build_item('Number of households', data, item_levels,
        'b11001001')
    add_metadata(households_dict['number_of_households'], 'b11001', 'Households', acs_name)

    households_dict['persons_per_household'] = build_item('Persons per household', data, item_levels,
        'b11002001 b11001001 /')
    add_metadata(households_dict['persons_per_household'], 'b11001,b11002', 'Households', acs_name)

    households_distribution_dict = OrderedDict()
    households_dict['distribution'] = households_distribution_dict
    add_metadata(households_dict['distribution'], 'b11001', 'Households', acs_name)

    households_distribution_dict['married_couples'] = build_item('Married couples', data, item_levels,
        'b11002003 b11002001 / %')

    households_distribution_dict['male_householder'] = build_item('Male householder', data, item_levels,
        'b11002006 b11002001 / %')

    households_distribution_dict['female_householder'] = build_item('Female householder', data, item_levels,
        'b11002009 b11002001 / %')

    households_distribution_dict['nonfamily'] = build_item('Non-family', data, item_levels,
        'b11002012 b11002001 / %')


    # Housing: Number of Housing Units, Occupancy Distribution, Vacancy Distribution
    data, acs = get_data_fallback('B25002', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    units_dict = dict()
    doc['housing']['units'] = units_dict

    units_dict['number'] = build_item('Number of housing units', data, item_levels,
        'b25002001')
    add_metadata(units_dict['number'], 'b25002', 'Housing units', acs_name)

    occupancy_distribution_dict = OrderedDict()
    units_dict['occupancy_distribution'] = occupancy_distribution_dict
    add_metadata(units_dict['occupancy_distribution'], 'b25002', 'Housing units', acs_name)

    occupancy_distribution_dict['occupied'] = build_item('Occupied', data, item_levels,
        'b25002002 b25002001 / %')
    occupancy_distribution_dict['vacant'] = build_item('Vacant', data, item_levels,
        'b25002003 b25002001 / %')

    # Housing: Structure Distribution
    data, acs = get_data_fallback('B25024', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    structure_distribution_dict = OrderedDict()
    units_dict['structure_distribution'] = structure_distribution_dict
    add_metadata(units_dict['structure_distribution'], 'b25024', 'Housing units', acs_name)

    structure_distribution_dict['single_unit'] = build_item('Single unit', data, item_levels,
        'b25024002 b25024003 + b25024001 / %')
    structure_distribution_dict['multi_unit'] = build_item('Multi-unit', data, item_levels,
        'b25024004 b25024005 + b25024006 + b25024007 + b25024008 + b25024009 + b25024001 / %')
    structure_distribution_dict['mobile_home'] = build_item('Mobile home', data, item_levels,
        'b25024010 b25024001 / %')
    structure_distribution_dict['vehicle'] = build_item('Boat, RV, van, etc.', data, item_levels,
        'b25024011 b25024001 / %')

    # Housing: Tenure
    data, acs = get_data_fallback('B25003', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    ownership_dict = dict()
    doc['housing']['ownership'] = ownership_dict

    ownership_distribution_dict = OrderedDict()
    ownership_dict['distribution'] = ownership_distribution_dict
    add_metadata(ownership_dict['distribution'], 'b25003', 'Occupied housing units', acs_name)

    ownership_distribution_dict['owner'] = build_item('Owner occupied', data, item_levels,
        'b25003002 b25003001 / %')
    ownership_distribution_dict['renter'] = build_item('Renter occupied', data, item_levels,
        'b25003003 b25003001 / %')

    data, acs = get_data_fallback('B25026', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    length_of_tenure_dict = OrderedDict()
    doc['housing']['length_of_tenure'] = length_of_tenure_dict
    add_metadata(length_of_tenure_dict, 'b25026', 'Total population in occupied housing units', acs_name)

    length_of_tenure_dict['before_1970'] = build_item('Before 1970', data, item_levels,
        'b25026008 b25026015 + b25026001 / %')
    length_of_tenure_dict['1970s'] = build_item('1970s', data, item_levels,
        'b25026007 b25026014 + b25026001 / %')
    length_of_tenure_dict['1980s'] = build_item('1980s', data, item_levels,
        'b25026006 b25026013 + b25026001 / %')
    length_of_tenure_dict['1990s'] = build_item('1990s', data, item_levels,
        'b25026005 b25026012 + b25026001 / %')
    length_of_tenure_dict['2000_to_2004'] = build_item('2000-2004', data, item_levels,
        'b25026004 b25026011 + b25026001 / %')
    length_of_tenure_dict['since_2005'] = build_item('Since 2005', data, item_levels,
        'b25026003 b25026010 + b25026001 / %')

    # Housing: Mobility
    data, acs = get_data_fallback('B07003', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    migration_dict = dict()
    doc['housing']['migration'] = migration_dict

    migration_dict['moved_since_previous_year'] = build_item('Moved since previous year', data, item_levels,
        'b07003007 b07003010 + b07003013 + b07003016 + b07003001 / %')
    add_metadata(migration_dict['moved_since_previous_year'], 'b07003', 'Population 1 year and over in the United States', acs_name)

    migration_distribution_dict = OrderedDict()
    doc['housing']['migration_distribution'] = migration_distribution_dict
    add_metadata(migration_distribution_dict, 'b07003', 'Population 1 year and over in the United States', acs_name)

    migration_distribution_dict['same_house_year_ago'] = build_item('Same house year ago', data, item_levels,
        'b07003004 b07003001 / %')
    migration_distribution_dict['moved_same_county'] = build_item('From same county', data, item_levels,
        'b07003007 b07003001 / %')
    migration_distribution_dict['moved_different_county'] = build_item('From different county', data, item_levels,
        'b07003010 b07003001 / %')
    migration_distribution_dict['moved_different_state'] = build_item('From different state', data, item_levels,
        'b07003013 b07003001 / %')
    migration_distribution_dict['moved_from_abroad'] = build_item('From abroad', data, item_levels,
        'b07003016 b07003001 / %')

    # Housing: Median Value and Distribution of Values
    data, acs = get_data_fallback('B25077', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    ownership_dict['median_value'] = build_item('Median value of owner-occupied housing units', data, item_levels,
        'b25077001')
    add_metadata(ownership_dict['median_value'], 'b25077', 'Owner-occupied housing units', acs_name)

    data, acs = get_data_fallback('B25075', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    value_distribution = OrderedDict()
    ownership_dict['value_distribution'] = value_distribution
    add_metadata(value_distribution, 'b25075', 'Owner-occupied housing units', acs_name)

    ownership_dict['total_value'] = build_item('Total value of owner-occupied housing units', data, item_levels,
        'b25075001')

    value_distribution['under_100'] = build_item('Under $100K', data, item_levels,
        'b25075002 b25075003 + b25075004 + b25075005 + b25075006 + b25075007 + b25075008 + b25075009 + b25075010 + b25075011 + b25075012 + b25075013 + b25075014 + b25075001 / %')
    value_distribution['100_to_200'] = build_item('$100K - $200K', data, item_levels,
        'b25075015 b25075016 + b25075017 + b25075018 + b25075001 / %')
    value_distribution['200_to_300'] = build_item('$200K - $300K', data, item_levels,
        'b25075019 b25075020 + b25075001 / %')
    value_distribution['300_to_400'] = build_item('$300K - $400K', data, item_levels,
        'b25075021 b25075001 / %')
    value_distribution['400_to_500'] = build_item('$400K - $500K', data, item_levels,
        'b25075022 b25075001 / %')
    value_distribution['500_to_1000000'] = build_item('$500K - $1M', data, item_levels,
        'b25075023 b25075024 + b25075001 / %')
    value_distribution['over_1000000'] = build_item('Over $1M', data, item_levels,
        'b25075025 b25075001 / %')


    # Social: Educational Attainment
    # Two aggregated data points for "high school and higher," "college degree and higher"
    # and distribution dict for chart
    data, acs = get_data_fallback('B15002', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    attainment_dict = dict()
    doc['social']['educational_attainment'] = attainment_dict

    attainment_dict['percent_high_school_grad_or_higher'] = build_item('High school grad or higher', data, item_levels,
        'b15002011 b15002012 + b15002013 + b15002014 + b15002015 + b15002016 + b15002017 + b15002018 + b15002028 + b15002029 + b15002030 + b15002031 + b15002032 + b15002033 + b15002034 + b15002035 + b15002001 / %')
    add_metadata(attainment_dict['percent_high_school_grad_or_higher'], 'b15002', 'Population 25 years and over', acs_name)

    attainment_dict['percent_bachelor_degree_or_higher'] = build_item('Bachelor\'s degree or higher', data, item_levels,
        'b15002015 b15002016 + b15002017 + b15002018 + b15002032 + b15002033 + b15002034 + b15002035 + b15002001 / %')
    add_metadata(attainment_dict['percent_bachelor_degree_or_higher'], 'b15002', 'Population 25 years and over', acs_name)

    attainment_distribution_dict = OrderedDict()
    doc['social']['educational_attainment_distribution'] = attainment_distribution_dict
    add_metadata(attainment_distribution_dict, 'b15002', 'Population 25 years and over', acs_name)

    attainment_distribution_dict['non_high_school_grad'] = build_item('No degree', data, item_levels,
        'b15002003 b15002004 + b15002005 + b15002006 + b15002007 + b15002008 + b15002009 + b15002010 + b15002020 + b15002021 + b15002022 + b15002023 + b15002024 + b15002025 + b15002026 + b15002027 + b15002001 / %')

    attainment_distribution_dict['high_school_grad'] = build_item('High school', data, item_levels,
        'b15002011 b15002028 + b15002001 / %')

    attainment_distribution_dict['some_college'] = build_item('Some college', data, item_levels,
        'b15002012 b15002013 + b15002014 + b15002029 + b15002030 + b15002031 + b15002001 / %')

    attainment_distribution_dict['bachelor_degree'] = build_item('Bachelor\'s', data, item_levels,
        'b15002015 b15002032 + b15002001 / %')

    attainment_distribution_dict['post_grad_degree'] = build_item('Post-grad', data, item_levels,
        'b15002016 b15002017 + b15002018 + b15002033 + b15002034 + b15002035 + b15002001 / %')

    # Social: Place of Birth
    data, acs = get_data_fallback('B05002', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    foreign_dict = dict()
    doc['social']['place_of_birth'] = foreign_dict

    foreign_dict['percent_foreign_born'] = build_item('Foreign-born population', data, item_levels,
        'b05002013 b05002001 / %')
    add_metadata(foreign_dict['percent_foreign_born'], 'b05002', 'Total population', acs_name)

    data, acs = get_data_fallback('B05006', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    place_of_birth_dict = OrderedDict()
    foreign_dict['distribution'] = place_of_birth_dict
    add_metadata(place_of_birth_dict, 'b05006', 'Foreign-born population', acs_name)

    place_of_birth_dict['europe'] = build_item('Europe', data, item_levels,
        'b05006002 b05006001 / %')
    place_of_birth_dict['asia'] = build_item('Asia', data, item_levels,
        'b05006046 b05006001 / %')
    place_of_birth_dict['africa'] = build_item('Africa', data, item_levels,
        'b05006090 b05006001 / %')
    place_of_birth_dict['oceania'] = build_item('Oceania', data, item_levels,
        'b05006115 b05006001 / %')
    place_of_birth_dict['latin_america'] = build_item('Latin America', data, item_levels,
        'b05006122 b05006001 / %')
    place_of_birth_dict['north_america'] = build_item('North America', data, item_levels,
        'b05006158 b05006001 / %')

    # Social: Percentage of Non-English Spoken at Home, Language Spoken at Home for Children, Adults
    data, acs = get_data_fallback('B16001', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    language_dict = dict()
    doc['social']['language'] = language_dict

    language_dict['percent_non_english_at_home'] = build_item('Persons with language other than English spoken at home', data, item_levels,
        'b16001001 b16001002 - b16001001 / %')
    add_metadata(language_dict['percent_non_english_at_home'], 'b16001', 'Population 5 years and over', acs_name)


    data, acs = get_data_fallback('B16007', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    language_children = OrderedDict()
    language_adults = OrderedDict()
    language_dict['children'] = language_children
    add_metadata(language_dict['children'], 'b16007', 'Population 5 years and over', acs_name)
    language_dict['adults'] = language_adults
    add_metadata(language_dict['adults'], 'b16007', 'Population 5 years and over', acs_name)

    language_children['english'] = build_item('English only', data, item_levels,
        'b16007003 b16007002 / %')
    language_adults['english'] = build_item('English only', data, item_levels,
        'b16007009 b16007015 + b16007008 b16007014 + / %')

    language_children['spanish'] = build_item('Spanish', data, item_levels,
        'b16007004 b16007002 / %')
    language_adults['spanish'] = build_item('Spanish', data, item_levels,
        'b16007010 b16007016 + b16007008 b16007014 + / %')

    language_children['indoeuropean'] = build_item('Indo-European', data, item_levels,
        'b16007005 b16007002 / %')
    language_adults['indoeuropean'] = build_item('Indo-European', data, item_levels,
        'b16007011 b16007017 + b16007008 b16007014 + / %')

    language_children['asian_islander'] = build_item('Asian/Islander', data, item_levels,
        'b16007006 b16007002 / %')
    language_adults['asian_islander'] = build_item('Asian/Islander', data, item_levels,
        'b16007012 b16007018 + b16007008 b16007014 + / %')

    language_children['other'] = build_item('Other', data, item_levels,
        'b16007007 b16007002 / %')
    language_adults['other'] = build_item('Other', data, item_levels,
        'b16007013 b16007019 + b16007008 b16007014 + / %')


    # Social: Number of Veterans, Wartime Service, Sex of Veterans
    data, acs = get_data_fallback('B21002', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    veterans_dict = dict()
    doc['social']['veterans'] = veterans_dict

    veterans_service_dict = OrderedDict()
    veterans_dict['wartime_service'] = veterans_service_dict
    add_metadata(veterans_service_dict, 'b21002', 'Civilian veterans 18 years and over', acs_name)

    veterans_service_dict['wwii'] = build_item('WWII', data, item_levels,
        'b21002009 b21002011 + b21002012 +')
    veterans_service_dict['korea'] = build_item('Korea', data, item_levels,
        'b21002008 b21002009 + b21002010 + b21002011 +')
    veterans_service_dict['vietnam'] = build_item('Vietnam', data, item_levels,
        'b21002004 b21002006 + b21002007 + b21002008 + b21002009 +')
    veterans_service_dict['gulf_1990s'] = build_item('Gulf (1990s)', data, item_levels,
        'b21002003 b21002004 + b21002005 + b21002006 +')
    veterans_service_dict['gulf_2001'] = build_item('Gulf (2001-)', data, item_levels,
        'b21002002 b21002003 + b21002004 +')

    data, acs = get_data_fallback('B21001', comparison_geoids, acs_default)
    acs_name = ACS_NAMES.get(acs).get('name')

    veterans_sex_dict = OrderedDict()
    veterans_dict['sex'] = veterans_sex_dict

    veterans_sex_dict['male'] = build_item('Male', data, item_levels,
        'b21001005')
    add_metadata(veterans_sex_dict['male'], 'b21001', 'Civilian population 18 years and over', acs_name)
    veterans_sex_dict['female'] = build_item('Female', data, item_levels,
        'b21001023')
    add_metadata(veterans_sex_dict['female'], 'b21001', 'Civilian population 18 years and over', acs_name)

    veterans_dict['number'] = build_item('Total veterans', data, item_levels,
        'b21001002')
    add_metadata(veterans_dict['number'], 'b21001', 'Civilian population 18 years and over', acs_name)

    veterans_dict['percentage'] = build_item('Population with veteran status', data, item_levels,
        'b21001002 b21001001 / %')
    add_metadata(veterans_dict['percentage'], 'b21001', 'Civilian population 18 years and over', acs_name)

    def default(obj):
        if type(obj) == decimal.Decimal:
            return int(obj)

    return json.dumps(doc, default=default)

def get_acs_name(acs_slug):
    if acs_slug in ACS_NAMES:
        acs_name = ACS_NAMES[acs_slug]['name']
    else:
        acs_name = acs_slug
    return acs_name

@app.route("/1.0/<acs>/<geoid>/profile")
def acs_geo_profile(acs, geoid):
    valid_acs, valid_geoid = find_geoid(geoid, acs)

    if not valid_acs:
        abort(400, 'GeoID %s isn\'t included in the %s release.' % (geoid, get_acs_name(acs)))

    return geo_profile(valid_acs, valid_geoid)


@app.route("/1.0/latest/<geoid>/profile")
def latest_geo_profile(geoid):
    valid_acs, valid_geoid = find_geoid(geoid)

    if not valid_acs:
        abort(400, 'None of the supported ACS releases include GeoID %s.' % (geoid))

    return geo_profile("latest", valid_geoid)


## GEO LOOKUPS ##

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
@crossdomain(origin='*')
def geo_search():
    lat = request.qwargs.lat
    lon = request.qwargs.lon
    q = request.qwargs.q
    sumlevs = request.qwargs.sumlevs
    with_geom = request.qwargs.geom

    if lat and lon:
        where = "ST_Intersects(the_geom, ST_SetSRID(ST_Point(%s, %s),4326))"
        where_args = [lon, lat]
    elif q:
        q = re.sub(r'\W', ' ', q)
        q = re.sub(r'\W+', ' ', q)
        where = "lower(prefix_match_name) LIKE lower(%s)"
        q += '%'
        where_args = [q]
    else:
        abort(400, "Must provide either a lat/lon OR a query term.")

    if sumlevs:
        where += " AND sumlevel IN %s"
        where_args.append(tuple(sumlevs))

    if with_geom:
        sql = """SELECT DISTINCT geoid,sumlevel,population,display_name,full_geoid,priority,ST_AsGeoJSON(ST_Simplify(the_geom,0.001)) as geom
            FROM tiger2012.census_name_lookup
            WHERE %s
            ORDER BY priority, population DESC NULLS LAST
            LIMIT 25;""" % (where)
    else:
        sql = """SELECT DISTINCT geoid,sumlevel,population,display_name,full_geoid,priority
            FROM tiger2012.census_name_lookup
            WHERE %s
            ORDER BY priority, population DESC NULLS LAST
            LIMIT 25;""" % (where)
    g.cur.execute(sql, where_args)

    def convert_row(row):
        data = dict()
        data['sumlevel'] = row['sumlevel']
        data['full_geoid'] = row['full_geoid']
        data['full_name'] = row['display_name']
        if 'geom' in row and row['geom']:
            data['geom'] = json.loads(row['geom'])
        return data

    return jsonify(results=[convert_row(row) for row in g.cur])


def num2deg(xtile, ytile, zoom):
  n = 2.0 ** zoom
  lon_deg = xtile / n * 360.0 - 180.0
  lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
  lat_deg = math.degrees(lat_rad)
  return (lat_deg, lon_deg)


# Example: /1.0/geo/tiger2012/tiles/160/12/1444/2424.json
@app.route("/1.0/geo/tiger2012/tiles/<sumlevel>/<int:zoom>/<int:x>/<int:y>.geojson")
@crossdomain(origin='*')
def geo_tiles(sumlevel, zoom, x, y):
    if sumlevel not in SUMLEV_NAMES:
        abort(400, "Unknown sumlevel")

    (miny, minx) = num2deg(x, y, zoom)
    (maxy, maxx) = num2deg(x + 1, y + 1, zoom)

    g.cur.execute("""SELECT
                ST_AsGeoJSON(ST_SimplifyPreserveTopology(
                    ST_Intersection(ST_Buffer(ST_MakeEnvelope(%s, %s, %s, %s, 4326), 0.01, 'endcap=square'), the_geom),
                    ST_Perimeter(the_geom) / 2500), 6) as geom,
                full_geoid,
                display_name
            FROM tiger2012.census_name_lookup
            WHERE sumlevel=%s AND ST_Intersects(ST_MakeEnvelope(%s, %s, %s, %s, 4326), the_geom)""",
            [minx, miny, maxx, maxy, sumlevel, minx, miny, maxx, maxy])

    results = []
    for row in g.cur:
        results.append({
            "type": "Feature",
            "properties": {
                "geoid": row['full_geoid'],
                "name": row['display_name']
            },
            "geometry": json.loads(row['geom'])
        })

    return jsonify(type="FeatureCollection", features=results)

# Example: /1.0/geo/tiger2012/04000US53
@app.route("/1.0/geo/tiger2012/<geoid>")
@qwarg_validate({
    'geom': {'valid': Bool()}
})
@crossdomain(origin='*')
def geo_lookup(geoid):
    geoid_parts = geoid.split('US')
    if len(geoid_parts) is not 2:
        abort(400, 'Invalid GeoID')

    if request.qwargs.geom:
        g.cur.execute("""SELECT display_name,simple_name,sumlevel,full_geoid,population,aland,awater,
            ST_AsGeoJSON(ST_Simplify(the_geom,ST_Perimeter(the_geom) / 1700)) as geom
            FROM tiger2012.census_name_lookup
            WHERE full_geoid=%s
            LIMIT 1""", [geoid])
    else:
        g.cur.execute("""SELECT display_name,simple_name,sumlevel,full_geoid,population,aland,awater
            FROM tiger2012.census_name_lookup
            WHERE full_geoid=%s
            LIMIT 1""", [geoid])

    result = g.cur.fetchone()

    if not result:
        abort(400, 'Unknown GeoID')

    geom = result.pop('geom', None)
    if geom:
        geom = json.loads(geom)

    return jsonify(type="Feature", properties=result, geometry=geom)


# Example: /1.0/geo/tiger2012/04000US53/parents
@app.route("/1.0/geo/tiger2012/<geoid>/parents")
@crossdomain(origin='*')
def geo_parent(geoid):
    parents = filter(lambda i: i['relation']!='this', compute_profile_item_levels(geoid))
    parent_geoids = [p['geoid'] for p in parents]

    def build_item(p):
        return (p['full_geoid'], {
            "display_name": p['display_name'],
            "sumlevel": p['sumlevel'],
            "geoid": p['full_geoid'],
        })

    if parent_geoids:
        g.cur.execute("SELECT display_name,sumlevel,full_geoid FROM tiger2012.census_name_lookup WHERE full_geoid IN %s ORDER BY sumlevel DESC", [tuple(parent_geoids)])
        parent_list = dict([build_item(p) for p in g.cur])

        for parent in parents:
            parent.update(parent_list[parent['geoid']])

    return jsonify(parents=parents)


# Example: /1.0/geo/show/tiger2012?geo_ids=04000US55,04000US56
# Example: /1.0/geo/show/tiger2012?geo_ids=160|04000US17,04000US56
@app.route("/1.0/geo/show/tiger2012")
@qwarg_validate({
    'geo_ids': {'valid': StringList(), 'required': True},
})
@crossdomain(origin='*')
def show_specified_geo_data():
    geo_ids = expand_geoids(request.qwargs.geo_ids)

    g.cur.execute("""SELECT full_geoid,display_name,ST_AsGeoJSON(ST_Simplify(the_geom,ST_Perimeter(the_geom) / 2500)) as geom
        FROM tiger2012.census_name_lookup
        WHERE full_geoid IN %s;""", [tuple(geo_ids)])

    results = []
    valid_geo_ids = []
    for row in g.cur:
        valid_geo_ids.append(row['full_geoid'])
        results.append({
            "type": "Feature",
            "properties": {
                "geoid": row['full_geoid'],
                "name": row['display_name']
            },
            "geometry": json.loads(row['geom'])
        })

    invalid_geo_ids = set(geo_ids) - set(valid_geo_ids)
    if invalid_geo_ids:
        abort(400, "GeoID(s) %s are not valid." % (','.join(invalid_geo_ids)))

    return jsonify(type="FeatureCollection", features=results)


## TABLE LOOKUPS ##

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
    'acs': {'valid': OneOf(allowed_acs), 'default': 'acs2012_1yr'},
    'q':   {'valid': NonemptyString()},
    'topics': {'valid': StringList()}
})
@crossdomain(origin='*')
def table_search():
    # allow choice of release, default to 2011 1-year
    acs = request.qwargs.acs
    q = request.qwargs.q
    topics = request.qwargs.topics

    if not (q or topics):
        abort(400, "Must provide a query term or topics for filtering.")

    g.cur.execute("SET search_path=%s,public;", [acs])
    data = []

    if re.match(r'^\w\d+\w{0,3}$', q, flags=re.IGNORECASE):
        # Matching for table id
        g.cur.execute("""SELECT tab.table_id,
                                tab.table_title,
                                tab.simple_table_title,
                                tab.universe,
                                tab.topics
                     FROM census_table_metadata tab
                     WHERE table_id=%s""", [q])
        for row in g.cur:
            data.append(format_table_search_result(row, 'table'))

        return json.dumps(data)

    table_where_parts = []
    table_where_args = []
    column_where_parts = []
    column_where_args = []

    if q and q != '*':
        q = '%%%s%%' % q
        table_where_parts.append("lower(tab.table_title) LIKE lower(%s)")
        table_where_args.append(q)
        column_where_parts.append("lower(col.column_title) LIKE lower(%s)")
        column_where_args.append(q)

    if topics:
        table_where_parts.append('tab.topics @> %s')
        table_where_args.append(topics)
        column_where_parts.append('tab.topics @> %s')
        column_where_args.append(topics)

    if table_where_parts:
        table_where = ' AND '.join(table_where_parts)
        column_where = ' AND '.join(column_where_parts)
    else:
        table_where = 'TRUE'
        column_where = 'TRUE'

    # retrieve matching tables.
    g.cur.execute("""SELECT tab.tabulation_code,
                            tab.table_title,
                            tab.simple_table_title,
                            tab.universe,
                            tab.topics,
                            tab.tables_in_one_yr,
                            tab.tables_in_three_yr,
                            tab.tables_in_five_yr
                     FROM census_tabulation_metadata tab
                     WHERE %s
                     ORDER BY tab.weight DESC""" % (table_where), table_where_args)
    for tabulation in g.cur:
        for tables_for_release_col in ('tables_in_one_yr', 'tables_in_three_yr', 'tables_in_five_yr'):
            tabulation['table_id'] = next(iter(tabulation[tables_for_release_col]))
            break
        data.append(format_table_search_result(tabulation, 'table'))

    # retrieve matching columns.
    if q != '*':
        # Special case for when we want ALL the tables (but not all the columns)
        g.cur.execute("""SELECT col.column_id,
                                col.column_title,
                                tab.table_id,
                                tab.table_title,
                                tab.simple_table_title,
                                tab.universe,
                                tab.topics
                         FROM census_column_metadata col
                         LEFT OUTER JOIN census_table_metadata tab USING (table_id)
                         WHERE %s
                         ORDER BY char_length(tab.table_id), tab.table_id""" % (column_where), column_where_args)
        data.extend([format_table_search_result(column, 'column') for column in g.cur])

    return json.dumps(data)


# Example: /1.0/table/B01001?release=acs2012_1yr
@app.route("/1.0/table/<table_id>")
@qwarg_validate({
    'acs': {'valid': OneOf(allowed_acs), 'default': 'acs2012_1yr'}
})
@crossdomain(origin='*')
def table_details(table_id):
    g.cur.execute("SET search_path=%s,public;", [request.qwargs.acs])

    g.cur.execute("""SELECT *
                     FROM census_table_metadata tab
                     WHERE table_id=%s""", [table_id])
    row = g.cur.fetchone()

    if not row:
        abort(400, "Table %s not found." % table_id.upper())

    data = OrderedDict([
        ("table_id", row['table_id']),
        ("table_title", row['table_title']),
        ("simple_table_title", row['simple_table_title']),
        ("subject_area", row['subject_area']),
        ("universe", row['universe']),
        ("denominator_column_id", row['denominator_column_id']),
        ("topics", row['topics'])
    ])

    g.cur.execute("""SELECT *
                     FROM census_column_metadata
                     WHERE table_id=%s""", [row['table_id']])

    rows = []
    for row in g.cur:
        rows.append((row['column_id'], dict(
            column_title=row['column_title'],
            indent=row['indent'],
            parent_column_id=row['parent_column_id']
        )))
    data['columns'] = OrderedDict(rows)

    return json.dumps(data)


# Example: /1.0/table/compare/rowcounts/B01001?year=2011&sumlevel=050&within=04000US53
@app.route("/1.0/table/compare/rowcounts/<table_id>")
@qwarg_validate({
    'year': {'valid': NonemptyString()},
    'sumlevel': {'valid': OneOf(SUMLEV_NAMES), 'required': True},
    'within': {'valid': NonemptyString(), 'required': True},
    'topics': {'valid': StringList()}
})
@crossdomain(origin='*')
def table_geo_comparison_rowcount(table_id):
    years = request.qwargs.year.split(',')
    child_summary_level = request.qwargs.sumlevel
    parent_geoid = request.qwargs.within
    parent_sumlevel = parent_geoid[:3]

    data = OrderedDict()

    releases = []
    for year in years:
        releases += [name for name in allowed_acs if year in name]
    releases = sorted(releases)

    for acs in releases:
        g.cur.execute("SET search_path=%s,public;", [acs])
        release = OrderedDict()
        release['release_name'] = ACS_NAMES[acs]['name']
        release['release_slug'] = acs
        release['results'] = 0

        g.cur.execute("SELECT * FROM census_table_metadata WHERE table_id=%s;", [table_id])
        table_record = g.cur.fetchone()
        if table_record:
            validated_table_id = table_record['table_id']
            release['table_name'] = table_record['table_title']
            release['table_universe'] = table_record['universe']

            child_geoheaders = get_child_geoids(parent_geoid, child_summary_level)

            if child_geoheaders:
                child_geoids = [child['geoid'] for child in child_geoheaders]
                g.cur.execute("SELECT COUNT(*) FROM %s.%s WHERE geoid IN %%s" % (acs, validated_table_id), [tuple(child_geoids)])
                acs_rowcount = g.cur.fetchone()
                release['results'] = acs_rowcount['count']

        data[acs] = release

    return json.dumps(data)


## DATA RETRIEVAL ##

# get geoheader data for children at the requested summary level
def get_child_geoids(parent_geoid, child_summary_level):
    parent_sumlevel = parent_geoid[0:3]
    if parent_sumlevel == '010':
        return get_all_child_geoids(child_summary_level)
    elif parent_sumlevel in PARENT_CHILD_CONTAINMENT and child_summary_level in PARENT_CHILD_CONTAINMENT[parent_sumlevel]:
        return get_child_geoids_by_prefix(parent_geoid, child_summary_level)
    elif parent_sumlevel == '160' and child_summary_level in ('140', '150'):
        return get_child_geoids_by_coverage(parent_geoid, child_summary_level)
    elif parent_sumlevel == '310' and child_summary_level in ('160', '860'):
        return get_child_geoids_by_coverage(parent_geoid, child_summary_level)
    elif parent_sumlevel == '040' and child_summary_level in ('310', '700', '860'):
        return get_child_geoids_by_coverage(parent_geoid, child_summary_level)
    elif parent_sumlevel == '050' and child_summary_level in ('160', '700', '860', '950', '960', '970'):
        return get_child_geoids_by_coverage(parent_geoid, child_summary_level)
    else:
        return get_child_geoids_by_gis(parent_geoid, child_summary_level)

def get_all_child_geoids(child_summary_level):
    # Use the "worst"/biggest ACS to find all child geoids
    g.cur.execute("SET search_path=%s,public;", [allowed_acs[-1]])
    g.cur.execute("""SELECT geoid,name
        FROM geoheader
        WHERE sumlevel=%s AND component='00' AND geoid NOT IN ('04000US72')
        ORDER BY name""", [int(child_summary_level)])

    return g.cur.fetchall()

def get_child_geoids_by_coverage(parent_geoid, child_summary_level):
    # Use the "worst"/biggest ACS to find all child geoids
    g.cur.execute("SET search_path=%s,public;", [allowed_acs[-1]])
    g.cur.execute("""SELECT DISTINCT(child_geoid)
        FROM tiger2012.census_geo_containment
        WHERE census_geo_containment.parent_geoid = %s AND census_geo_containment.child_geoid LIKE %s""", [parent_geoid, child_summary_level+'%'])
    child_geoids = [r['child_geoid'] for r in g.cur]

    if child_geoids:
        g.cur.execute("""SELECT geoid,name
            FROM geoheader
            WHERE geoid IN %s
            ORDER BY name""", [tuple(child_geoids)])
        return g.cur.fetchall()
    else:
        return []

def get_child_geoids_by_gis(parent_geoid, child_summary_level):
    parent_sumlevel = parent_geoid[0:3]
    child_geoids = []
    g.cur.execute("""SELECT child.full_geoid
        FROM tiger2012.census_name_lookup parent
        JOIN tiger2012.census_name_lookup child ON ST_Intersects(parent.the_geom, child.the_geom) AND child.sumlevel=%s
        WHERE parent.full_geoid=%s AND parent.sumlevel=%s;""", [child_summary_level, parent_geoid, parent_sumlevel])
    child_geoids = [r['full_geoid'] for r in g.cur]

    if child_geoids:
        g.cur.execute("""SELECT geoid,name
            FROM geoheader
            WHERE geoid IN %s
            ORDER BY name""", [tuple(child_geoids)])
        return g.cur.fetchall()
    else:
        return []


def get_child_geoids_by_prefix(parent_geoid, child_summary_level):
    child_geoid_prefix = '%s00US%s%%' % (child_summary_level, parent_geoid.split('US')[1])

    # Use the "worst"/biggest ACS to find all child geoids
    g.cur.execute("SET search_path=%s,public;", [allowed_acs[-1]])
    g.cur.execute("""SELECT geoid,name
        FROM geoheader
        WHERE geoid LIKE %s AND name NOT LIKE %s
        ORDER BY geoid""", [child_geoid_prefix, '%%not defined%%'])
    return g.cur.fetchall()


def expand_geoids(geoid_list, release=None):
    if not release:
        release = allowed_acs[-1]

    # Look for geoid "groups" of the form `child_sumlevel|parent_geoid`.
    # These will expand into a list of geoids like the old comparison endpoint used to
    geo_ids = []
    for geoid_str in geoid_list:
        geoid_split = geoid_str.split('|')
        if len(geoid_split) == 2 and len(geoid_split[0]) == 3:
            (child_summary_level, parent_geoid) = geoid_split
            child_geoids = get_child_geoids(parent_geoid, child_summary_level)
            for child_geoid in child_geoids:
                geo_ids.append(child_geoid['geoid'])
        else:
            geo_ids.append(geoid_str)

    # Check to make sure the geos requested are valid
    if not geo_ids:
        raise ShowDataException("No geo_ids for release %s." % (release))

    valid_geo_ids = []
    g.cur.execute("SET search_path=%s,public;", [release])
    g.cur.execute("SELECT geoid FROM geoheader WHERE geoid IN %s;", [tuple(geo_ids)])
    for geo in g.cur:
        valid_geo_ids.append(geo['geoid'])

    invalid_geo_ids = set(geo_ids) - set(valid_geo_ids)
    if invalid_geo_ids:
        raise ShowDataException("The %s release doesn't include GeoID(s) %s." % (get_acs_name(release), ','.join(invalid_geo_ids)))

    return valid_geo_ids


class ShowDataException(Exception):
    pass


# Example: /1.0/data/show/acs2012_5yr?table_ids=B01001,B01003&geo_ids=04000US55,04000US56
# Example: /1.0/data/show/latest?table_ids=B01001&geo_ids=160|04000US17,04000US56
@app.route("/1.0/data/show/<acs>")
@qwarg_validate({
    'table_ids': {'valid': StringList(), 'required': True},
    'geo_ids': {'valid': StringList(), 'required': True},
})
@crossdomain(origin='*')
def show_specified_data(acs):
    if acs in allowed_acs:
        acs_to_try = [acs]
    elif acs == 'latest':
        acs_to_try = allowed_acs[:3]
    else:
        abort(400, 'The %s release isn\'t supported.' % get_acs_name(acs))

    # valid_geo_ids only contains geos for which we want data
    requested_geo_ids = request.qwargs.geo_ids
    try:
        valid_geo_ids = expand_geoids(requested_geo_ids)
    except ShowDataException, e:
        abort(400, e.message)

    # expand_geoids has validated parents of groups by getting children;
    # this will include those parent names in the reponse `geography` list
    # but leave them out of the response `data` list
    grouped_geo_ids = [item for item in requested_geo_ids if "|" in item]
    parents_of_groups = [item_group.split('|')[1] for item_group in grouped_geo_ids]
    named_geo_ids = valid_geo_ids + parents_of_groups

    # Fill in the display name for the geos
    g.cur.execute("SELECT full_geoid,population,display_name FROM tiger2012.census_name_lookup WHERE full_geoid IN %s;", [tuple(named_geo_ids)])

    geo_metadata = OrderedDict()
    for geo in g.cur:
        geo_metadata[geo['full_geoid']] = {
            "name": geo['display_name'],
        }

    for acs in acs_to_try:
        try:
            g.cur.execute("SET search_path=%s,public;", [acs])

            # Check to make sure the tables requested are valid
            g.cur.execute("""SELECT tab.table_id,tab.table_title,tab.universe,tab.denominator_column_id,col.column_id,col.column_title,col.indent
                FROM census_column_metadata col
                LEFT JOIN census_table_metadata tab USING (table_id)
                WHERE table_id IN %s
                ORDER BY column_id;""", [tuple(request.qwargs.table_ids)])

            valid_table_ids = []
            table_metadata = OrderedDict()
            for table, columns in groupby(g.cur, lambda x: (x['table_id'], x['table_title'], x['universe'], x['denominator_column_id'])):
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
                    ) for column in columns ]))
                ])

            invalid_table_ids = set(request.qwargs.table_ids) - set(valid_table_ids)
            if invalid_table_ids:
                raise ShowDataException("The %s release doesn't include table(s) %s." % (get_acs_name(acs), ','.join(invalid_table_ids)))

            # Now fetch the actual data
            from_stmt = '%s_moe' % (valid_table_ids[0])
            if len(valid_table_ids) > 1:
                from_stmt += ' '
                from_stmt += ' '.join(['JOIN %s_moe USING (geoid)' % (table_id) for table_id in valid_table_ids[1:]])

            where_stmt = g.cur.mogrify('geoid IN %s', [tuple(valid_geo_ids)])

            sql = 'SELECT * FROM %s WHERE %s;' % (from_stmt, where_stmt)

            g.cur.execute(sql)
            data = OrderedDict()

            if g.cur.rowcount != len(valid_geo_ids):
                returned_geo_ids = set([row['geoid'] for row in g.cur])
                raise ShowDataException("The %s release doesn't include GeoID(s) %s." % (get_acs_name(acs), ','.join(set(valid_geo_ids) - returned_geo_ids)))

            for row in g.cur:
                geoid = row.pop('geoid')
                data[geoid] = OrderedDict()

                cols_iter = iter(sorted(row.items(), key=lambda tup: tup[0]))
                for table_id, data_iter in groupby(cols_iter, lambda x: x[0][:-3].upper()):
                    data[geoid][table_id] = OrderedDict()
                    data[geoid][table_id]['estimate'] = OrderedDict()
                    data[geoid][table_id]['error'] = OrderedDict()
                    for (col_name, value) in data_iter:
                        col_name = col_name.upper()
                        (moe_name, moe_value) = next(cols_iter)

                        if value is None:
                            continue

                        data[geoid][table_id]['estimate'][col_name] = value
                        data[geoid][table_id]['error'][col_name] = moe_value

                    if not data[geoid][table_id]['estimate']:
                        raise ShowDataException("No data for table %s, geo %s in ACS %s." % (table_id, geoid, acs))

            return jsonify(tables=table_metadata, geography=geo_metadata, data=data, release={'id': acs, 'years': ACS_NAMES[acs]['years'], 'name': ACS_NAMES[acs]['name']})
        except ShowDataException, e:
            continue
    abort(400, str(e))


# Example: /1.0/data/download/acs2012_5yr?format=shp&table_ids=B01001,B01003&geo_ids=04000US55,04000US56
# Example: /1.0/data/download/latest?table_ids=B01001&geo_ids=160|04000US17,04000US56
@app.route("/1.0/data/download/<acs>")
@qwarg_validate({
    'table_ids': {'valid': StringList(), 'required': True},
    'geo_ids': {'valid': StringList(), 'required': True},
    'format': {'valid': OneOf(supported_formats), 'required': True},
})
@crossdomain(origin='*')
def download_specified_data(acs):
    if acs in allowed_acs:
        acs_to_try = [acs]
    elif acs == 'latest':
        acs_to_try = allowed_acs[:3]
    else:
        abort(400, 'The %s release isn\'t supported.' % get_acs_name(acs))

    try:
        valid_geo_ids = expand_geoids(request.qwargs.geo_ids)
    except ShowDataException, e:
        abort(400, e.message)

    # Fill in the display name for the geos
    g.cur.execute("SELECT full_geoid,population,display_name FROM tiger2012.census_name_lookup WHERE full_geoid IN %s;", [tuple(valid_geo_ids)])

    geo_metadata = OrderedDict()
    for geo in g.cur:
        geo_metadata[geo['full_geoid']] = {
            "name": geo['display_name'],
        }

    for acs in acs_to_try:
        try:
            g.cur.execute("SET search_path=%s,public;", [acs])

            # Check to make sure the tables requested are valid
            g.cur.execute("""SELECT tab.table_id,tab.table_title,tab.universe,tab.denominator_column_id,col.column_id,col.column_title,col.indent
                FROM census_column_metadata col
                LEFT JOIN census_table_metadata tab USING (table_id)
                WHERE table_id IN %s
                ORDER BY column_id;""", [tuple(request.qwargs.table_ids)])

            valid_table_ids = []
            table_metadata = OrderedDict()
            for table, columns in groupby(g.cur, lambda x: (x['table_id'], x['table_title'], x['universe'], x['denominator_column_id'])):
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
                    ) for column in columns ]))
                ])

            invalid_table_ids = set(request.qwargs.table_ids) - set(valid_table_ids)
            if invalid_table_ids:
                raise ShowDataException("The %s release doesn't include table(s) %s." % (get_acs_name(acs), ','.join(invalid_table_ids)))

            # Now fetch the actual data
            from_stmt = '%s_moe' % (valid_table_ids[0])
            if len(valid_table_ids) > 1:
                from_stmt += ' '
                from_stmt += ' '.join(['JOIN %s_moe USING (geoid)' % (table_id) for table_id in valid_table_ids[1:]])

            where_stmt = g.cur.mogrify('geoid IN %s', [tuple(valid_geo_ids)])

            sql = 'SELECT * FROM %s WHERE %s;' % (from_stmt, where_stmt)

            g.cur.execute(sql)
            data = OrderedDict()

            if g.cur.rowcount != len(valid_geo_ids):
                returned_geo_ids = set([row['geoid'] for row in g.cur])
                raise ShowDataException("The %s release doesn't include GeoID(s) %s." % (get_acs_name(acs), ','.join(set(valid_geo_ids) - returned_geo_ids)))

            for row in g.cur:
                geoid = row.pop('geoid')
                data[geoid] = OrderedDict()

                cols_iter = iter(sorted(row.items(), key=lambda tup: tup[0]))
                for table_id, data_iter in groupby(cols_iter, lambda x: x[0][:-3].upper()):
                    data[geoid][table_id] = OrderedDict()
                    data[geoid][table_id]['estimate'] = OrderedDict()
                    data[geoid][table_id]['error'] = OrderedDict()
                    for (col_name, value) in data_iter:
                        col_name = col_name.upper()
                        (moe_name, moe_value) = next(cols_iter)

                        if value is None:
                            continue

                        data[geoid][table_id]['estimate'][col_name] = value
                        data[geoid][table_id]['error'][col_name] = moe_value

                    if not data[geoid][table_id]['estimate']:
                        raise ShowDataException("No data for table %s, geo %s in ACS %s." % (table_id, geoid, acs))

            temp_path = tempfile.mkdtemp()
            file_ident = "%s_%s_%s" % (acs, next(iter(valid_table_ids)), next(iter(valid_geo_ids)))
            inner_path = os.path.join(temp_path, file_ident)
            os.mkdir(inner_path)
            out_filename = os.path.join(inner_path, '%s.%s' % (file_ident, request.qwargs.format))
            format_info = supported_formats.get(request.qwargs.format)

            if format_info['type'] == 'ogr':
                import ogr
                import osr
                db_details = urlparse.urlparse(app.config['DATABASE_URI'])
                host = db_details.hostname
                user = db_details.username
                password = db_details.password
                database = db_details.path[1:]
                in_driver = ogr.GetDriverByName("PostgreSQL")
                conn = in_driver.Open("PG: host=%s dbname=%s user=%s password=%s" % (host, database, user, password))

                if conn is None:
                    raise Exception("Could not connect to database to generate download.")

                driver_name = format_info['driver']
                out_driver = ogr.GetDriverByName(driver_name)
                out_srs = osr.SpatialReference()
                out_srs.ImportFromEPSG(4326)
                out_data = out_driver.CreateDataSource(out_filename)
                # See http://gis.stackexchange.com/questions/53920/ogr-createlayer-returns-typeerror
                out_layer = out_data.CreateLayer(file_ident.encode('utf-8'), srs=out_srs, geom_type=ogr.wkbMultiPolygon)
                out_layer.CreateField(ogr.FieldDefn('geoid', ogr.OFTString))
                out_layer.CreateField(ogr.FieldDefn('name', ogr.OFTString))
                for (table_id, table) in table_metadata.iteritems():
                    for column_id, column_info in table['columns'].iteritems():
                        if request.qwargs.format == 'shp':
                            # Work around the Shapefile column name length limits
                            out_layer.CreateField(ogr.FieldDefn(column_id, ogr.OFTReal))
                            out_layer.CreateField(ogr.FieldDefn(column_id+"e", ogr.OFTReal))
                        else:
                            out_layer.CreateField(ogr.FieldDefn(column_info['name'], ogr.OFTReal))
                            out_layer.CreateField(ogr.FieldDefn(column_info['name']+", Error", ogr.OFTReal))

                sql = g.cur.mogrify("""SELECT the_geom,full_geoid,display_name
                    FROM tiger2012.census_name_lookup
                    WHERE full_geoid IN %s""", [tuple(valid_geo_ids)])
                in_layer = conn.ExecuteSQL(sql)

                in_feat = in_layer.GetNextFeature()
                while in_feat is not None:
                    out_feat = ogr.Feature(out_layer.GetLayerDefn())
                    out_feat.SetGeometry(in_feat.GetGeometryRef())
                    geoid = in_feat.GetField('full_geoid')
                    out_feat.SetField('geoid', geoid)
                    out_feat.SetField('name', in_feat.GetField('display_name'))
                    for (table_id, table) in table_metadata.iteritems():
                        table_estimates = data[geoid][table_id]['estimate']
                        table_errors = data[geoid][table_id]['error']
                        for column_id, column_info in table['columns'].iteritems():
                            if column_id in table_estimates:
                                if request.qwargs.format == 'shp':
                                    # Work around the Shapefile column name length limits
                                    estimate_col_name = column_id
                                    error_col_name = column_id+"e"
                                else:
                                    estimate_col_name = column_info['name']
                                    error_col_name = column_info['name']+", Error"

                                out_feat.SetField(estimate_col_name, table_estimates[column_id])
                                out_feat.SetField(error_col_name, table_errors[column_id])

                    out_layer.CreateFeature(out_feat)
                    in_feat.Destroy()
                    in_feat = in_layer.GetNextFeature()
                out_data.Destroy()

            metadata_dict = {
                'release': {
                    'id': acs,
                    'years': ACS_NAMES[acs]['years'],
                    'name': ACS_NAMES[acs]['name']
                },
                'tables': table_metadata
            }
            json.dump(metadata_dict, open(os.path.join(inner_path, 'metadata.json'), 'w'))

            zfile_path = os.path.join(temp_path, file_ident + '.zip')
            zfile = zipfile.ZipFile(zfile_path, 'w', zipfile.ZIP_DEFLATED)
            for root, dirs, files in os.walk(inner_path):
                for f in files:
                    zfile.write(os.path.join(root, f), os.path.join(file_ident, f))
            zfile.close()

            resp = send_file(zfile_path, as_attachment=True, attachment_filename=file_ident + '.zip')

            shutil.rmtree(temp_path)

            return resp
        except ShowDataException, e:
            continue
    abort(400, str(e))

# Example: /1.0/data/compare/acs2012_5yr/B01001?sumlevel=050&within=04000US53
@app.route("/1.0/data/compare/<acs>/<table_id>")
@qwarg_validate({
    'within': {'valid': NonemptyString(), 'required': True},
    'sumlevel': {'valid': OneOf(SUMLEV_NAMES), 'required': True},
    'geom': {'valid': Bool(), 'default': False}
})
@crossdomain(origin='*')
def data_compare_geographies_within_parent(acs, table_id):
    # make sure we support the requested ACS release
    if acs not in allowed_acs:
        abort(400, 'The %s release isn\'t supported.' % get_acs_name(acs))
    g.cur.execute("SET search_path=%s,public;", [acs])

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

    g.cur.execute("""SELECT tab.table_id,tab.table_title,tab.universe,tab.denominator_column_id,col.column_id,col.column_title,col.indent
        FROM census_column_metadata col
        LEFT JOIN census_table_metadata tab USING (table_id)
        WHERE table_id=%s
        ORDER BY column_id;""", [table_id])
    table_metadata = g.cur.fetchall()

    if not table_metadata:
        abort(400, 'Table %s isn\'t available in the %s release.' % (table_id.upper(), get_acs_name(acs)))

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
    g.cur.execute("SELECT * FROM geoheader WHERE geoid=%s;", [parent_geoid])
    parent_geoheader = g.cur.fetchone()
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
        g.cur.execute("""SELECT ST_AsGeoJSON(ST_Simplify(the_geom,0.001), 5) as geometry
            FROM tiger2012.census_name_lookup
            WHERE full_geoid=%s;""", [parent_geoid])
        parent_geometry = g.cur.fetchone()
        try:
            parent_geography['geography']['geometry'] = json.loads(parent_geometry['geometry'])
        except:
            # we may not have geometries for all sumlevs
            pass

        # get the child geometries and store for later
        g.cur.execute("""SELECT geoid, ST_AsGeoJSON(ST_Simplify(the_geom,0.001), 5) as geometry
            FROM tiger2012.census_name_lookup
            WHERE full_geoid IN %s
            ORDER BY full_geoid;""", [tuple(child_geoid_list)])
        child_geodata = g.cur.fetchall()
        child_geodata_map = dict([(record['geoid'], json.loads(record['geometry'])) for record in child_geodata])

    # make the where clause and query the requested census data table
    # get parent data first...
    g.cur.execute("SELECT * FROM %s_moe WHERE geoid=%%s" % (validated_table_id), [parent_geoheader['geoid']])
    parent_data = g.cur.fetchone()
    parent_data.pop('geoid', None)
    column_data = []
    column_moe = []
    sorted_data = iter(sorted(parent_data.items(), key=lambda tup: tup[0]))
    for (k, v) in sorted_data:
        (moe_k, moe_v) = next(sorted_data)
        column_data.append((k.upper(), v))
        column_moe.append((k.upper(), moe_v))
    parent_geography['data'] = OrderedDict(column_data)
    parent_geography['error'] = OrderedDict(column_moe)

    if child_geoheaders:
        # ... and then children so we can loop through with cursor
        child_geoids = [child['geoid'] for child in child_geoheaders]
        g.cur.execute("SELECT * FROM %s_moe WHERE geoid IN %%s" % (validated_table_id), [tuple(child_geoids)])

        # grab one row at a time
        for record in g.cur:
            child_geoid = record.pop('geoid')

            child_data = OrderedDict()
            this_geo_has_data = False

            # build the child item
            child_data['geography'] = OrderedDict()
            child_data['geography']['name'] = child_geoid_names[child_geoid]
            child_data['geography']['summary_level'] = child_summary_level

            column_data = []
            column_moe = []
            sorted_data = iter(sorted(record.items(), key=lambda tup: tup[0]))
            for (k, v) in sorted_data:

                if v is not None and moe_v is not None:
                    this_geo_has_data =True

                (moe_k, moe_v) = next(sorted_data)
                column_data.append((k.upper(), v))
                column_moe.append((k.upper(), moe_v))
            child_data['data'] = OrderedDict(column_data)
            child_data['error'] = OrderedDict(column_moe)

            if child_geodata_map:
                try:
                    child_data['geography']['geometry'] = child_geodata_map[child_geoid.split('US')[1]]
                except:
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


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
