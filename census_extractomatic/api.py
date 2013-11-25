# For real division instead of sometimes-integer
from __future__ import division

from flask import Flask
from flask import abort, request, g
from flask import make_response, current_app
from flask import jsonify
from functools import update_wrapper
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
import urlparse
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
    'acs2011_1yr',
    'acs2011_3yr',
    'acs2011_5yr',
    'acs2010_1yr',
    'acs2010_3yr',
    'acs2010_5yr',
    'acs2009_1yr',
    'acs2009_3yr',
    'acs2008_1yr',
    'acs2008_3yr',
    'acs2007_1yr',
    'acs2007_3yr'
]

ACS_NAMES = {
    'acs2012_1yr': {'name': 'ACS 2012 1-year', 'years': '2012'},
    'acs2012_3yr': {'name': 'ACS 2012 3-year', 'years': '2010-2012'},
    'acs2011_1yr': {'name': 'ACS 2011 1-year', 'years': '2011'},
    'acs2011_3yr': {'name': 'ACS 2011 3-year', 'years': '2009-2011'},
    'acs2011_5yr': {'name': 'ACS 2011 5-year', 'years': '2007-2011'},
    'acs2010_1yr': {'name': 'ACS 2010 1-year', 'years': '2010'},
    'acs2010_3yr': {'name': 'ACS 2010 3-year', 'years': '2008-2010'},
    'acs2010_5yr': {'name': 'ACS 2010 5-year', 'years': '2006-2010'},
    'acs2009_1yr': {'name': 'ACS 2009 1-year', 'years': '2009'},
    'acs2009_3yr': {'name': 'ACS 2009 3-year', 'years': '2007-2009'},
    'acs2008_1yr': {'name': 'ACS 2008 1-year', 'years': '2008'},
    'acs2008_3yr': {'name': 'ACS 2008 3-year', 'years': '2006-2008'},
    'acs2007_1yr': {'name': 'ACS 2007 1-year', 'years': '2007'},
    'acs2007_3yr': {'name': 'ACS 2007 3-year', 'years': '2005-2007'},
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

def maybe_int(i):
    return int(i) if i else i

def percentify(val):
    return round(val * 100, 1)

def rateify(val):
    return round(val * 1000, 1)

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

    return (stack.pop(), moe_stack.pop(), numerator, numerator_moe)

def build_item(table_id, universe, name, acs_release, data, parents, rpn_string):
    val = OrderedDict([('table_id', table_id),
        ('universe', universe),
        ('name', name),
        ('acs_release', acs_release),
        ('values', dict()),
        ('error', dict()),
        ('numerators', dict()),
        ('numerator_errors', dict())])

    for (label, geoid) in parents.iteritems():
        data_for_geoid = data.get(geoid) if data else {}

        value = None
        error = None
        numerator = None
        numerator_moe = None

        if data_for_geoid:
            (value, error, numerator, numerator_moe) = value_rpn_calc(data_for_geoid, rpn_string)

        val['values'][label] = value
        val['error'][label] = error
        val['numerators'][label] = numerator
        val['numerator_errors'][label] = numerator_moe

    return val


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
    if acs:
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
    levels = {'this': geoid}

    geoid_parts = geoid.split('US')
    if len(geoid_parts) is not 2:
        raise Exception('Invalid geoid')

    sumlevel = geoid_parts[0][:3]
    id_part = geoid_parts[1]

    if sumlevel in ('050', '060', '140', '150', '160', '500', '610', '620', '795', '950', '960', '970'):
        levels['state'] = '04000US' + id_part[:2]

    if sumlevel in ('060', '140', '150'):
        levels['county'] = '05000US' + id_part[:5]

    if sumlevel != '010':
        levels['nation'] = '01000US'

    return levels


def geo_profile(acs, geoid):
    g.cur.execute("SET search_path=%s", [acs])
    acs_default = acs

    item_levels = compute_profile_item_levels(geoid)

    doc = OrderedDict([('geography', OrderedDict()),
                       ('demographics', dict()),
                       ('economics', dict()),
                       ('families', dict()),
                       ('housing', dict()),
                       ('social', dict())])

    doc['geography']['census_release'] = ACS_NAMES.get(acs_default).get('name')

    # Demographics: Age
    # multiple data points, suitable for visualization
    data, acs = get_data_fallback('B01001', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    g.cur.execute("""SELECT DISTINCT full_geoid,sumlevel,display_name,simple_name,aland
                     FROM tiger2012.census_name_lookup
                     WHERE full_geoid IN %s;""", [tuple(item_levels.values())])

    def convert_geography_data(row):
        return dict(full_name=row['display_name'],
                    short_name=row['simple_name'],
                    sumlevel=row['sumlevel'],
                    land_area=row['aland'],
                    full_geoid=row['full_geoid'])

    lookup_data = {}
    for row in g.cur:
        lookup_data[row['full_geoid']] = row

    for (name, the_geoid) in item_levels.iteritems():
        doc['geography'][name] = convert_geography_data(lookup_data[the_geoid])
        doc['geography'][name]['total_population'] = maybe_int(data[the_geoid]['b01001001'])

    age_dict = dict()
    doc['demographics']['age'] = age_dict
    age_dict['percent_under_18'] = build_item('b01001', 'Total population', 'Under 18', acs_name, data, item_levels,
        'b01001003 b01001004 + b01001005 + b01001006 + b01001027 + b01001028 + b01001029 + b01001030 + b01001001 / %')

    age_dict['percent_over_65'] = build_item('b01001', 'Total population', '65 and over', acs_name, data, item_levels,
        'b01001020 b01001021 + b01001023 + b01001024 + b01001025 + b01001044 + b01001045 + b01001046 + b01001047 + b01001048 + b01001049 + b01001001 / %')

    pop_dict = dict()
    age_dict['population_by_age'] = pop_dict
    population_by_age_total = OrderedDict()
    population_by_age_male = OrderedDict()
    population_by_age_female = OrderedDict()
    pop_dict['total'] = population_by_age_total
    pop_dict['male'] = population_by_age_male
    pop_dict['female'] = population_by_age_female

    population_by_age_male['0-9'] = build_item('b01001', 'Total population', '0-9', acs_name, data, item_levels,
        'b01001003 b01001004 + b01001002 / %')
    population_by_age_female['0-9'] = build_item('b01001', 'Total population', '0-9', acs_name, data, item_levels,
        'b01001027 b01001028 + b01001026 / %')
    population_by_age_total['0-9'] = build_item('b01001', 'Total population', '0-9', acs_name, data, item_levels,
        'b01001003 b01001004 + b01001027 + b01001028 + b01001001 / %')

    population_by_age_male['10-19'] = build_item('b01001', 'Total population', '10-19', acs_name, data, item_levels,
        'b01001005 b01001006 + b01001007 + b01001002 / %')
    population_by_age_female['10-19'] = build_item('b01001', 'Total population', '10-19', acs_name, data, item_levels,
        'b01001029 b01001030 + b01001031 + b01001026 / %')
    population_by_age_total['10-19'] = build_item('b01001', 'Total population', '10-19', acs_name, data, item_levels,
        'b01001005 b01001006 + b01001007 + b01001029 + b01001030 + b01001031 + b01001001 / %')

    population_by_age_male['20-29'] = build_item('b01001', 'Total population', '20-29', acs_name, data, item_levels,
        'b01001008 b01001009 + b01001010 + b01001011 + b01001002 / %')
    population_by_age_female['20-29'] = build_item('b01001', 'Total population', '20-29', acs_name, data, item_levels,
        'b01001032 b01001033 + b01001034 + b01001035 + b01001026 / %')
    population_by_age_total['20-29'] = build_item('b01001', 'Total population', '20-29', acs_name, data, item_levels,
        'b01001008 b01001009 + b01001010 + b01001011 + b01001032 + b01001033 + b01001034 + b01001035 + b01001001 / %')

    population_by_age_male['30-39'] = build_item('b01001', 'Total population', '30-39', acs_name, data, item_levels,
        'b01001012 b01001013 + b01001002 / %')
    population_by_age_female['30-39'] = build_item('b01001', 'Total population', '30-39', acs_name, data, item_levels,
        'b01001036 b01001037 + b01001026 / %')
    population_by_age_total['30-39'] = build_item('b01001', 'Total population', '30-39', acs_name, data, item_levels,
        'b01001012 b01001013 + b01001036 + b01001037 + b01001001 / %')

    population_by_age_male['40-49'] = build_item('b01001', 'Total population', '40-49', acs_name, data, item_levels,
        'b01001014 b01001015 + b01001002 / %')
    population_by_age_female['40-49'] = build_item('b01001', 'Total population', '40-49', acs_name, data, item_levels,
        'b01001038 b01001039 + b01001026 / %')
    population_by_age_total['40-49'] = build_item('b01001', 'Total population', '40-49', acs_name, data, item_levels,
        'b01001014 b01001015 + b01001038 + b01001039 + b01001001 / %')

    population_by_age_male['50-59'] = build_item('b01001', 'Total population', '50-59', acs_name, data, item_levels,
        'b01001016 b01001017 + b01001002 / %')
    population_by_age_female['50-59'] = build_item('b01001', 'Total population', '50-59', acs_name, data, item_levels,
        'b01001040 b01001041 + b01001026 / %')
    population_by_age_total['50-59'] = build_item('b01001', 'Total population', '50-59', acs_name, data, item_levels,
        'b01001016 b01001017 + b01001040 + b01001041 + b01001001 / %')

    population_by_age_male['60-69'] = build_item('b01001', 'Total population', '60-69', acs_name, data, item_levels,
        'b01001018 b01001019 + b01001020 + b01001021 + b01001002 / %')
    population_by_age_female['60-69'] = build_item('b01001', 'Total population', '60-69', acs_name, data, item_levels,
        'b01001042 b01001043 + b01001044 + b01001045 + b01001026 / %')
    population_by_age_total['60-69'] = build_item('b01001', 'Total population', '60-69', acs_name, data, item_levels,
        'b01001018 b01001019 + b01001020 + b01001021 + b01001042 + b01001043 + b01001044 + b01001045 + b01001001 / %')

    population_by_age_male['70-79'] = build_item('b01001', 'Total population', '70-79', acs_name, data, item_levels,
        'b01001022 b01001023 + b01001002 / %')
    population_by_age_female['70-79'] = build_item('b01001', 'Total population', '70-79', acs_name, data, item_levels,
        'b01001046 b01001047 + b01001026 / %')
    population_by_age_total['70-79'] = build_item('b01001', 'Total population', '70-79', acs_name, data, item_levels,
        'b01001022 b01001023 + b01001046 + b01001047 + b01001001 / %')

    population_by_age_male['80+'] = build_item('b01001', 'Total population', '80+', acs_name, data, item_levels,
        'b01001024 b01001025 + b01001002 / %')
    population_by_age_female['80+'] = build_item('b01001', 'Total population', '80+', acs_name, data, item_levels,
        'b01001048 b01001049 + b01001026 / %')
    population_by_age_total['80+'] = build_item('b01001', 'Total population', '80+', acs_name, data, item_levels,
        'b01001024 b01001025 + b01001048 + b01001049 + b01001001 / %')

    # Demographics: Sex
    # multiple data points, suitable for visualization
    sex_dict = dict()
    doc['demographics']['sex'] = sex_dict
    sex_dict['percent_male'] = build_item('b01001', 'Total population', 'Male', acs_name, data, item_levels,
        'b01001002 b01001001 / %')

    sex_dict['percent_female'] = build_item('b01001', 'Total population', 'Female', acs_name, data, item_levels,
        'b01001026 b01001001 / %')

    data, acs = get_data_fallback('B01002', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    age_dict['median_age'] = build_item('b01002', 'Total population', 'Median age', acs_name, data, item_levels,
        'b01002001')

    age_dict['median_age_male'] = build_item('b01002', 'Total population', 'Median age male', acs_name, data, item_levels,
        'b01002002')

    age_dict['median_age_female'] = build_item('b01002', 'Total population', 'Median age female', acs_name, data, item_levels,
        'b01002003')

    # Demographics: Race
    # multiple data points, suitable for visualization
    # uses Table B03002 (HISPANIC OR LATINO ORIGIN BY RACE), pulling race numbers from "Not Hispanic or Latino" columns
    # also collapses smaller groups into "Other"
    data, acs = get_data_fallback('B03002', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    race_dict = OrderedDict()
    doc['demographics']['race'] = race_dict
    race_dict['percent_white'] = build_item('b03002', 'Total population', 'White', acs_name, data, item_levels,
        'b03002003 b03002001 / %')

    race_dict['percent_black'] = build_item('b03002', 'Total population', 'Black', acs_name, data, item_levels,
        'b03002004 b03002001 / %')

    race_dict['percent_asian'] = build_item('b03002', 'Total population', 'Asian', acs_name, data, item_levels,
        'b03002006 b03002001 / %')

    race_dict['percent_hispanic'] = build_item('b03002', 'Total population', 'Hispanic', acs_name, data, item_levels,
        'b03002012 b03002001 / %')

    race_dict['percent_other'] = build_item('b03002', 'Total population', 'Other', acs_name, data, item_levels,
        'b03002005 b03002007 + b03002008 + b03002009 + b03002001 / %')

    # Economics: Per-Capita Income
    # single data point
    data, acs = get_data_fallback('B19301', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    income_dict = dict()
    doc['economics']['income'] = income_dict

    income_dict['per_capita_income_in_the_last_12_months'] = build_item('b19301', 'Total population', 'Per capita income', acs_name, data, item_levels,
        'b19301001')

    # Economics: Median Household Income
    # single data point
    data, acs = get_data_fallback('B19013', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    income_dict['median_household_income'] = build_item('b19013', 'Households', 'Median household income', acs_name, data, item_levels,
        'b19013001')

    # Economics: Household Income Distribution
    # multiple data points, suitable for visualization
    data, acs = get_data_fallback('B19001', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    income_distribution = OrderedDict()
    income_dict['household_distribution'] = income_distribution

    income_distribution['under_50'] = build_item('b19001', 'Households', 'Under $50K', acs_name, data, item_levels,
        'b19001002 b19001003 + b19001004 + b19001005 + b19001006 + b19001007 + b19001008 + b19001009 + b19001010 + b19001001 / %')
    income_distribution['50_to_100'] = build_item('b19001', 'Households', '$50K - $100K', acs_name, data, item_levels,
        'b19001011 b19001012 + b19001013 + b19001001 / %')
    income_distribution['100_to_200'] = build_item('b19001', 'Households', '$100K - $200K', acs_name, data, item_levels,
        'b19001014 b19001015 + b19001016 + b19001001 / %')
    income_distribution['over_200'] = build_item('b19001', 'Households', 'Over $200K', acs_name, data, item_levels,
        'b19001017 b19001001 / %')

    # Economics: Poverty Rate
    # provides separate dicts for children and seniors, with multiple data points, suitable for visualization
    data, acs = get_data_fallback('B17001', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    poverty_dict = dict()
    doc['economics']['poverty'] = poverty_dict

    poverty_dict['percent_below_poverty_line'] = build_item('b17001', 'Population for whom poverty status is determined', 'Persons below poverty line', acs_name, data, item_levels,
        'b17001002 b17001001 / %')

    poverty_children = OrderedDict()
    poverty_seniors = OrderedDict()
    poverty_dict['children'] = poverty_children
    poverty_dict['seniors'] = poverty_seniors

    poverty_children['below'] = build_item('b17001', 'Population for whom poverty status is determined', 'Poverty', acs_name, data, item_levels,
        'b17001004 b17001005 + b17001006 + b17001007 + b17001008 + b17001009 + b17001018 + b17001019 + b17001020 + b17001021 + b17001022 + b17001023 + b17001004 b17001005 + b17001006 + b17001007 + b17001008 + b17001009 + b17001018 + b17001019 + b17001020 + b17001021 + b17001022 + b17001023 + b17001033 + b17001034 + b17001035 + b17001036 + b17001037 + b17001038 + b17001047 + b17001048 + b17001049 + b17001050 + b17001051 + b17001052 + / %')
    poverty_children['above'] = build_item('b17001', 'Population for whom poverty status is determined', 'Non-poverty', acs_name, data, item_levels,
        'b17001033 b17001034 + b17001035 + b17001036 + b17001037 + b17001038 + b17001047 + b17001048 + b17001049 + b17001050 + b17001051 + b17001052 + b17001004 b17001005 + b17001006 + b17001007 + b17001008 + b17001009 + b17001018 + b17001019 + b17001020 + b17001021 + b17001022 + b17001023 + b17001033 + b17001034 + b17001035 + b17001036 + b17001037 + b17001038 + b17001047 + b17001048 + b17001049 + b17001050 + b17001051 + b17001052 + / %')

    poverty_seniors['below'] = build_item('b17001', 'Population for whom poverty status is determined', 'Poverty', acs_name, data, item_levels,
        'b17001015 b17001016 + b17001029 + b17001030 + b17001015 b17001016 + b17001029 + b17001030 + b17001044 + b17001045 + b17001058 + b17001059 + / %')
    poverty_seniors['above'] = build_item('b17001', 'Population for whom poverty status is determined', 'Non-poverty', acs_name, data, item_levels,
        'b17001044 b17001045 + b17001058 + b17001059 + b17001015 b17001016 + b17001029 + b17001030 + b17001044 + b17001045 + b17001058 + b17001059 + / %')

    # Economics: Mean Travel Time to Work, Means of Transportation to Work
    # uses two different tables for calculation, so make sure they draw from same ACS release
    data, acs = get_data_fallback(['B08006', 'B08013'], item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    employment_dict = dict()
    doc['economics']['employment'] = employment_dict

    employment_dict['mean_travel_time'] = build_item('b08006, b08013', 'Workers 16 years and over who did not work at home', 'Mean travel time to work', acs_name, data, item_levels,
        'b08013001 b08006001 b08006017 - /')

    data, acs = get_data_fallback('B08006', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    transportation_dict = OrderedDict()
    employment_dict['transportation_distribution'] = transportation_dict

    transportation_dict['drove_alone'] = build_item('b08006', 'Workers 16 years and over', 'Drove alone', acs_name, data, item_levels,
        'b08006003 b08006001 / %')
    transportation_dict['carpooled'] = build_item('b08006', 'Workers 16 years and over', 'Carpooled', acs_name, data, item_levels,
        'b08006004 b08006001 / %')
    transportation_dict['public_transit'] = build_item('b08006', 'Workers 16 years and over', 'Public transit', acs_name, data, item_levels,
        'b08006008 b08006001 / %')
    transportation_dict['bicycle'] = build_item('b08006', 'Workers 16 years and over', 'Bicycle', acs_name, data, item_levels,
        'b08006014 b08006001 / %')
    transportation_dict['walked'] = build_item('b08006', 'Workers 16 years and over', 'Walked', acs_name, data, item_levels,
        'b08006015 b08006001 / %')
    transportation_dict['other'] = build_item('b08006', 'Workers 16 years and over', 'Other', acs_name, data, item_levels,
        'b08006016 b08006001 / %')
    transportation_dict['worked_at_home'] = build_item('b08006', 'Workers 16 years and over', 'Worked at home', acs_name, data, item_levels,
        'b08006017 b08006001 / %')

    # Families: Marital Status by Sex
    data, acs = get_data_fallback('B12001', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    marital_status = OrderedDict()
    doc['families']['marital_status'] = marital_status

    marital_status['married'] = build_item('b12001', 'Population 15 years and over', 'Married', acs_name, data, item_levels,
        'b12001004 b12001013 + b12001001 / %')
    marital_status['single'] = build_item('b12001', 'Population 15 years and over', 'Single', acs_name, data, item_levels,
        'b12001003 b12001009 + b12001010 + b12001012 + b12001018 + b12001019 + b12001001 / %')

    marital_status_grouped = OrderedDict()
    doc['families']['marital_status_grouped'] = marital_status_grouped

    # repeating data temporarily to develop grouped column chart format
    marital_status_grouped['never_married'] = OrderedDict()
    marital_status_grouped['never_married']['acs_release'] = acs_name
    marital_status_grouped['never_married']['metadata'] = {
        'universe': 'Population 15 years and over',
        'table_id': 'b12001',
        'name': 'Never married'
    }
    marital_status_grouped['never_married']['male'] = build_item('b12001', 'Population 15 years and over', 'Male', acs_name, data, item_levels,
        'b12001003 b12001002 / %')
    marital_status_grouped['never_married']['female'] = build_item('b12001', 'Population 15 years and over', 'Female', acs_name, data, item_levels,
        'b12001012 b12001011 / %')

    marital_status_grouped['married'] = OrderedDict()
    marital_status_grouped['married']['acs_release'] = acs_name
    marital_status_grouped['married']['metadata'] = {
        'universe': 'Population 15 years and over',
        'table_id': 'b12001',
        'name': 'Now married'
    }
    marital_status_grouped['married']['male'] = build_item('b12001', 'Population 15 years and over', 'Male', acs_name, data, item_levels,
        'b12001004 b12001002 / %')
    marital_status_grouped['married']['female'] = build_item('b12001', 'Population 15 years and over', 'Female', acs_name, data, item_levels,
        'b12001013 b12001011 / %')

    marital_status_grouped['divorced'] = OrderedDict()
    marital_status_grouped['divorced']['acs_release'] = acs_name
    marital_status_grouped['divorced']['metadata'] = {
        'universe': 'Population 15 years and over',
        'table_id': 'b12001',
        'name': 'Divorced'
    }
    marital_status_grouped['divorced']['male'] = build_item('b12001', 'Population 15 years and over', 'Male', acs_name, data, item_levels,
        'b12001010 b12001002 / %')
    marital_status_grouped['divorced']['female'] = build_item('b12001', 'Population 15 years and over', 'Female', acs_name, data, item_levels,
        'b12001019 b12001011 / %')

    marital_status_grouped['widowed'] = OrderedDict()
    marital_status_grouped['widowed']['acs_release'] = acs_name
    marital_status_grouped['widowed']['metadata'] = {
        'universe': 'Population 15 years and over',
        'table_id': 'b12001',
        'name': 'Widowed'
    }
    marital_status_grouped['widowed']['male'] = build_item('b12001', 'Population 15 years and over', 'Male', acs_name, data, item_levels,
        'b12001009 b12001002 / %')
    marital_status_grouped['widowed']['female'] = build_item('b12001', 'Population 15 years and over', 'Female', acs_name, data, item_levels,
        'b12001018 b12001011 / %')


    # Families: Family Types with Children
    data, acs = get_data_fallback('B09002', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    family_types = dict()
    doc['families']['family_types'] = family_types

    children_family_type_dict = OrderedDict()
    family_types['children'] = children_family_type_dict

    children_family_type_dict['married_couple'] = build_item('b09002', 'Own children under 18 years', 'Married couple', acs_name, data, item_levels,
        'b09002002 b09002001 / %')
    children_family_type_dict['male_householder'] = build_item('b09002', 'Own children under 18 years', 'Male householder', acs_name, data, item_levels,
        'b09002009 b09002001 / %')
    children_family_type_dict['female_householder'] = build_item('b09002', 'Own children under 18 years', 'Female householder', acs_name, data, item_levels,
        'b09002015 b09002001 / %')

    # Families: Birth Rate by Women's Age
    data, acs = get_data_fallback('B13016', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    birth_rate = dict()
    doc['families']['birth_rate'] = birth_rate

    birth_rate['total'] = build_item('b13016', 'Women 15 to 50 years', 'Births per 1,000 women', acs_name, data, item_levels,
        'b13016002 b13016001 / %%')

    birth_rate_by_age_dict = OrderedDict()
    birth_rate['by_age'] = birth_rate_by_age_dict

    birth_rate_by_age_dict['15_to_19'] = build_item('b13016', 'Women 15 to 50 years', '15-19', acs_name, data, item_levels,
        'b13016003 b13016003 b13016011 + / %%')
    birth_rate_by_age_dict['20_to_24'] = build_item('b13016', 'Women 15 to 50 years', '20-24', acs_name, data, item_levels,
        'b13016004 b13016004 b13016012 + / %%')
    birth_rate_by_age_dict['25_to_29'] = build_item('b13016', 'Women 15 to 50 years', '25-29', acs_name, data, item_levels,
        'b13016005 b13016005 b13016013 + / %%')
    birth_rate_by_age_dict['30_to_34'] = build_item('b13016', 'Women 15 to 50 years', '30-35', acs_name, data, item_levels,
        'b13016006 b13016006 b13016014 + / %%')
    birth_rate_by_age_dict['35_to_39'] = build_item('b13016', 'Women 15 to 50 years', '35-39', acs_name, data, item_levels,
        'b13016007 b13016007 b13016015 + / %%')
    birth_rate_by_age_dict['40_to_44'] = build_item('b13016', 'Women 15 to 50 years', '40-44', acs_name, data, item_levels,
        'b13016008 b13016008 b13016016 + / %%')
    birth_rate_by_age_dict['45_to_50'] = build_item('b13016', 'Women 15 to 50 years', '45-50', acs_name, data, item_levels,
        'b13016009 b13016009 b13016017 + / %%')

    # Families: Number of Households, Persons per Household, Household type distribution
    data, acs = get_data_fallback(['B11001', 'B11002'], item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    households_dict = dict()
    doc['families']['households'] = households_dict

    households_dict['number_of_households'] = build_item('b11001', 'Households', 'Number of households', acs_name, data, item_levels,
        'b11001001')

    households_dict['persons_per_household'] = build_item('b11001,b11002', 'Households', 'Persons per household', acs_name, data, item_levels,
        'b11002001 b11001001 /')

    households_distribution_dict = OrderedDict()
    households_dict['distribution'] = households_distribution_dict

    households_distribution_dict['married_couples'] = build_item('b11001', 'Households', 'Married couples', acs_name, data, item_levels,
        'b11002003 b11002001 / %')

    households_distribution_dict['male_householder'] = build_item('b11001', 'Households', 'Male householder', acs_name, data, item_levels,
        'b11002006 b11002001 / %')

    households_distribution_dict['female_householder'] = build_item('b11001', 'Households', 'Female householder', acs_name, data, item_levels,
        'b11002009 b11002001 / %')

    households_distribution_dict['nonfamily'] = build_item('b11001', 'Households', 'Non-family', acs_name, data, item_levels,
        'b11002012 b11002001 / %')


    # Housing: Number of Housing Units, Occupancy Distribution, Vacancy Distribution
    data, acs = get_data_fallback('B25002', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    units_dict = dict()
    doc['housing']['units'] = units_dict

    units_dict['number'] = build_item('b25002', 'Housing units', 'Number of housing units', acs_name, data, item_levels,
        'b25002001')

    occupancy_distribution_dict = OrderedDict()
    units_dict['occupancy_distribution'] = occupancy_distribution_dict

    occupancy_distribution_dict['occupied'] = build_item('b25002', 'Housing units', 'Occupied', acs_name, data, item_levels,
        'b25002002 b25002001 / %')
    occupancy_distribution_dict['vacant'] = build_item('b25002', 'Housing units', 'Vacant', acs_name, data, item_levels,
        'b25002003 b25002001 / %')

    # Housing: Structure Distribution
    data, acs = get_data_fallback('B25024', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    structure_distribution_dict = OrderedDict()
    units_dict['structure_distribution'] = structure_distribution_dict

    structure_distribution_dict['single_unit'] = build_item('b25024', 'Housing units', 'Single unit', acs_name, data, item_levels,
        'b25024002 b25024003 + b25024001 / %')
    structure_distribution_dict['multi_unit'] = build_item('b25024', 'Housing units', 'Multi-unit', acs_name, data, item_levels,
        'b25024004 b25024005 + b25024006 + b25024007 + b25024008 + b25024009 + b25024001 / %')
    structure_distribution_dict['mobile_home'] = build_item('b25024', 'Housing units', 'Mobile home', acs_name, data, item_levels,
        'b25024010 b25024001 / %')
    structure_distribution_dict['vehicle'] = build_item('b25024', 'Housing units', 'Boat, RV, van, etc.', acs_name, data, item_levels,
        'b25024011 b25024001 / %')

    # Housing: Tenure
    data, acs = get_data_fallback('B25003', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    ownership_dict = dict()
    doc['housing']['ownership'] = ownership_dict

    ownership_distribution_dict = OrderedDict()
    ownership_dict['distribution'] = ownership_distribution_dict
    ownership_distribution_dict['owner'] = build_item('b25003', 'Occupied housing units', 'Owner occupied', acs_name, data, item_levels,
        'b25003002 b25003001 / %')
    ownership_distribution_dict['renter'] = build_item('b25003', 'Occupied housing units', 'Renter occupied', acs_name, data, item_levels,
        'b25003003 b25003001 / %')

    data, acs = get_data_fallback('B25026', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    length_of_tenure_dict = OrderedDict()
    doc['housing']['length_of_tenure'] = length_of_tenure_dict

    length_of_tenure_dict['before_1970'] = build_item('b25026', 'Total population in occupied housing units', 'Before 1970', acs_name, data, item_levels,
        'b25026008 b25026015 + b25026001 / %')
    length_of_tenure_dict['1970s'] = build_item('b25026', 'Total population in occupied housing units', '1970s', acs_name, data, item_levels,
        'b25026007 b25026014 + b25026001 / %')
    length_of_tenure_dict['1980s'] = build_item('b25026', 'Total population in occupied housing units', '1980s', acs_name, data, item_levels,
        'b25026006 b25026013 + b25026001 / %')
    length_of_tenure_dict['1990s'] = build_item('b25026', 'Total population in occupied housing units', '1990s', acs_name, data, item_levels,
        'b25026005 b25026012 + b25026001 / %')
    length_of_tenure_dict['2000_to_2004'] = build_item('b25026', 'Total population in occupied housing units', '2000-2004', acs_name, data, item_levels,
        'b25026004 b25026011 + b25026001 / %')
    length_of_tenure_dict['since_2005'] = build_item('b25026', 'Total population in occupied housing units', 'Since 2005', acs_name, data, item_levels,
        'b25026003 b25026010 + b25026001 / %')

    # Housing: Mobility
    data, acs = get_data_fallback('B07003', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    migration_dict = dict()
    doc['housing']['migration'] = migration_dict

    migration_dict['same_house_year_ago'] = build_item('b07003', 'Population 1 year and over in the United States', 'Same house year ago', acs_name, data, item_levels,
        'b07003004 b07003001 / %')
    migration_dict['moved_since_previous_year'] = build_item('b07003', 'Population 1 year and over in the United States', 'Moved since previous year', acs_name, data, item_levels,
        'b07003007 b07003010 + b07003013 + b07003016 + b07003001 / %')

    migration_distribution_dict = OrderedDict()
    doc['housing']['migration_distribution'] = migration_distribution_dict

    migration_distribution_dict['same_house_year_ago'] = build_item('b07003', 'Population 1 year and over in the United States', 'Same house year ago', acs_name, data, item_levels,
        'b07003004 b07003001 / %')
    migration_distribution_dict['moved_same_county'] = build_item('b07003', 'Population 1 year and over in the United States', 'From same county', acs_name, data, item_levels,
        'b07003007 b07003001 / %')
    migration_distribution_dict['moved_different_county'] = build_item('b07003', 'Population 1 year and over in the United States', 'From different county', acs_name, data, item_levels,
        'b07003010 b07003001 / %')
    migration_distribution_dict['moved_different_state'] = build_item('b07003', 'Population 1 year and over in the United States', 'From different state', acs_name, data, item_levels,
        'b07003013 b07003001 / %')
    migration_distribution_dict['moved_from_abroad'] = build_item('b07003', 'Population 1 year and over in the United States', 'From abroad', acs_name, data, item_levels,
        'b07003016 b07003001 / %')

    # Housing: Median Value and Distribution of Values
    data, acs = get_data_fallback('B25077', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    ownership_dict['median_value'] = build_item('b25077', 'Owner-occupied housing units', 'Median value of owner-occupied housing units', acs_name, data, item_levels,
        'b25077001')

    data, acs = get_data_fallback('B25075', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    value_distribution = OrderedDict()
    ownership_dict['value_distribution'] = value_distribution

    ownership_dict['total_value'] = build_item('b25075', 'Owner-occupied housing units', 'Total value of owner-occupied housing units', acs_name, data, item_levels,
        'b25075001')

    value_distribution['under_100'] = build_item('b25075', 'Owner-occupied housing units', 'Under $100K', acs_name, data, item_levels,
        'b25075002 b25075003 + b25075004 + b25075005 + b25075006 + b25075007 + b25075008 + b25075009 + b25075010 + b25075011 + b25075012 + b25075013 + b25075014 + b25075001 / %')
    value_distribution['100_to_200'] = build_item('b25075', 'Owner-occupied housing units', '$100K - $200K', acs_name, data, item_levels,
        'b25075015 b25075016 + b25075017 + b25075018 + b25075001 / %')
    value_distribution['200_to_300'] = build_item('b25075', 'Owner-occupied housing units', '$200K - $300K', acs_name, data, item_levels,
        'b25075019 b25075020 + b25075001 / %')
    value_distribution['300_to_400'] = build_item('b25075', 'Owner-occupied housing units', '$300K - $400K', acs_name, data, item_levels,
        'b25075021 b25075001 / %')
    value_distribution['400_to_500'] = build_item('b25075', 'Owner-occupied housing units', '$400K - $500K', acs_name, data, item_levels,
        'b25075022 b25075001 / %')
    value_distribution['500_to_1000000'] = build_item('b25075', 'Owner-occupied housing units', '$500K - $1M', acs_name, data, item_levels,
        'b25075023 b25075024 + b25075001 / %')
    value_distribution['over_1000000'] = build_item('b25075', 'Owner-occupied housing units', 'Over $1M', acs_name, data, item_levels,
        'b25075025 b25075001 / %')


    # Social: Educational Attainment
    # Two aggregated data points for "high school and higher," "college degree and higher"
    # and distribution dict for chart
    data, acs = get_data_fallback('B15002', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    attainment_dict = dict()
    doc['social']['educational_attainment'] = attainment_dict

    attainment_dict['percent_high_school_grad_or_higher'] = build_item('b15002', 'Population 25 years and over', 'High school grad or higher', acs_name, data, item_levels,
        'b15002011 b15002012 + b15002013 + b15002014 + b15002015 + b15002016 + b15002017 + b15002018 + b15002028 + b15002029 + b15002030 + b15002031 + b15002032 + b15002033 + b15002034 + b15002035 + b15002001 / %')

    attainment_dict['percent_bachelor_degree_or_higher'] = build_item('b15002', 'Population 25 years and over', 'Bachelor\'s degree or higher', acs_name, data, item_levels,
        'b15002015 b15002016 + b15002017 + b15002018 + b15002032 + b15002033 + b15002034 + b15002035 + b15002001 / %')

    attainment_distribution_dict = OrderedDict()
    doc['social']['educational_attainment_distribution'] = attainment_distribution_dict

    attainment_distribution_dict['non_high_school_grad'] = build_item('b15002', 'Population 25 years and over', 'No degree', acs_name, data, item_levels,
        'b15002003 b15002004 + b15002005 + b15002006 + b15002007 + b15002008 + b15002009 + b15002010 + b15002020 + b15002021 + b15002022 + b15002023 + b15002024 + b15002025 + b15002026 + b15002027 + b15002001 / %')

    attainment_distribution_dict['high_school_grad'] = build_item('b15002', 'Population 25 years and over', 'High school', acs_name, data, item_levels,
        'b15002011 b15002028 + b15002001 / %')

    attainment_distribution_dict['some_college'] = build_item('b15002', 'Population 25 years and over', 'Some college', acs_name, data, item_levels,
        'b15002012 b15002013 + b15002014 + b15002029 + b15002030 + b15002031 + b15002001 / %')

    attainment_distribution_dict['bachelor_degree'] = build_item('b15002', 'Population 25 years and over', 'Bachelor\'s', acs_name, data, item_levels,
        'b15002015 b15002032 + b15002001 / %')

    attainment_distribution_dict['post_grad_degree'] = build_item('b15002', 'Population 25 years and over', 'Post-grad', acs_name, data, item_levels,
        'b15002016 b15002017 + b15002018 + b15002033 + b15002034 + b15002035 + b15002001 / %')

    # Social: Place of Birth
    data, acs = get_data_fallback('B05002', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    foreign_dict = dict()
    doc['social']['place_of_birth'] = foreign_dict

    foreign_dict['percent_foreign_born'] = build_item('b05002', 'Total population', 'Foreign-born population', acs_name, data, item_levels,
        'b05002013 b05002001 / %')

    data, acs = get_data_fallback('B05006', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    place_of_birth_dict = OrderedDict()
    foreign_dict['distribution'] = place_of_birth_dict

    place_of_birth_dict['europe'] = build_item('b05006', 'Foreign-born population', 'Europe', acs_name, data, item_levels,
        'b05006002 b05006001 / %')
    place_of_birth_dict['asia'] = build_item('b05006', 'Foreign-born population', 'Asia', acs_name, data, item_levels,
        'b05006047 b05006001 / %')
    place_of_birth_dict['africa'] = build_item('b05006', 'Foreign-born population', 'Africa', acs_name, data, item_levels,
        'b05006091 b05006001 / %')
    place_of_birth_dict['oceania'] = build_item('b05006', 'Foreign-born population', 'Oceania', acs_name, data, item_levels,
        'b05006116 b05006001 / %')
    place_of_birth_dict['latin_america'] = build_item('b05006', 'Foreign-born population', 'Latin America', acs_name, data, item_levels,
        'b05006123 b05006001 / %')
    place_of_birth_dict['north_america'] = build_item('b05006', 'Foreign-born population', 'North America', acs_name, data, item_levels,
        'b05006159 b05006001 / %')

    # Social: Percentage of Non-English Spoken at Home, Language Spoken at Home for Children, Adults
    data, acs = get_data_fallback('B16001', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    language_dict = dict()
    doc['social']['language'] = language_dict

    language_dict['percent_non_english_at_home'] = build_item('b16001', 'Population 5 years and over', 'Persons with language other than English spoken at home', acs_name, data, item_levels,
        'b16001001 b16001002 - b16001001 / %')


    data, acs = get_data_fallback('B16007', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    language_children = OrderedDict()
    language_adults = OrderedDict()
    language_dict['children'] = language_children
    language_dict['adults'] = language_adults

    language_children['english'] = build_item('b16007', 'Population 5 years and over', 'English only', acs_name, data, item_levels,
        'b16007003 b16007002 / %')
    language_adults['english'] = build_item('b16007', 'Population 5 years and over', 'English only', acs_name, data, item_levels,
        'b16007009 b16007015 + b16007008 b16007014 + / %')

    language_children['spanish'] = build_item('b16007', 'Population 5 years and over', 'Spanish', acs_name, data, item_levels,
        'b16007004 b16007002 / %')
    language_adults['spanish'] = build_item('b16007', 'Population 5 years and over', 'Spanish', acs_name, data, item_levels,
        'b16007010 b16007016 + b16007008 b16007014 + / %')

    language_children['indoeuropean'] = build_item('b16007', 'Population 5 years and over', 'Indo-European', acs_name, data, item_levels,
        'b16007005 b16007002 / %')
    language_adults['indoeuropean'] = build_item('b16007', 'Population 5 years and over', 'Indo-European', acs_name, data, item_levels,
        'b16007011 b16007017 + b16007008 b16007014 + / %')

    language_children['asian_islander'] = build_item('b16007', 'Population 5 years and over', 'Asian/Islander', acs_name, data, item_levels,
        'b16007006 b16007002 / %')
    language_adults['asian_islander'] = build_item('b16007', 'Population 5 years and over', 'Asian/Islander', acs_name, data, item_levels,
        'b16007012 b16007018 + b16007008 b16007014 + / %')

    language_children['other'] = build_item('b16007', 'Population 5 years and over', 'Other', acs_name, data, item_levels,
        'b16007007 b16007002 / %')
    language_adults['other'] = build_item('b16007', 'Population 5 years and over', 'Other', acs_name, data, item_levels,
        'b16007013 b16007019 + b16007008 b16007014 + / %')


    # Social: Number of Veterans, Wartime Service, Sex of Veterans
    data, acs = get_data_fallback('B21002', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    veterans_dict = dict()
    doc['social']['veterans'] = veterans_dict

    veterans_service_dict = OrderedDict()
    veterans_dict['wartime_service'] = veterans_service_dict

    veterans_service_dict['wwii'] = build_item('b21002', 'Civilian veterans 18 years and over', 'WWII', acs_name, data, item_levels,
        'b21002009 b21002011 + b21002012 +')
    veterans_service_dict['korea'] = build_item('b21002', 'Civilian veterans 18 years and over', 'Korea', acs_name, data, item_levels,
        'b21002008 b21002009 + b21002010 + b21002011 +')
    veterans_service_dict['vietnam'] = build_item('b21002', 'Civilian veterans 18 years and over', 'Vietnam', acs_name, data, item_levels,
        'b21002004 b21002006 + b21002007 + b21002008 + b21002009 +')
    veterans_service_dict['gulf_1990s'] = build_item('b21002', 'Civilian veterans 18 years and over', 'Gulf (1990s)', acs_name, data, item_levels,
        'b21002003 b21002004 + b21002005 + b21002006 +')
    veterans_service_dict['gulf_2001'] = build_item('b21002', 'Civilian veterans 18 years and over', 'Gulf (2001-)', acs_name, data, item_levels,
        'b21002002 b21002003 + b21002004 +')

    data, acs = get_data_fallback('B21001', item_levels.values())
    acs_name = ACS_NAMES.get(acs).get('name')

    veterans_sex_dict = OrderedDict()
    veterans_dict['sex'] = veterans_sex_dict

    veterans_sex_dict['male'] = build_item('b21001', 'Civilian population 18 years and over', 'Male', acs_name, data, item_levels,
        'b21001005')
    veterans_sex_dict['female'] = build_item('b21001', 'Civilian population 18 years and over', 'Female', acs_name, data, item_levels,
        'b21001023')

    veterans_dict['number'] = build_item('b21002', 'Civilian veterans 18 years and over', 'Total veterans', acs_name, data, item_levels,
        'b21001002')

    veterans_dict['percentage'] = build_item('b21001', 'Civilian population 18 years and over', 'Population with veteran status', acs_name, data, item_levels,
        'b21001002 b21001001 / %')

    def default(obj):
        if type(obj) == decimal.Decimal:
            return int(obj)

    return json.dumps(doc, default=default)


@app.route("/1.0/<acs>/<geoid>/profile")
def acs_geo_profile(acs, geoid):
    acs, geoid = find_geoid(geoid, acs)

    if not acs:
        abort(404, 'That ACS doesn\'t know about have that geoid.')

    return geo_profile(acs, geoid)


@app.route("/1.0/latest/<geoid>/profile")
def latest_geo_profile(geoid):
    acs, geoid = find_geoid(geoid)

    if not acs:
        abort(404, 'None of the ACS I know about have that geoid.')

    return geo_profile(acs, geoid)


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


# Example: /1.0/geo/tiger2012/04000US53
@app.route("/1.0/geo/tiger2012/<geoid>")
@qwarg_validate({
    'geom': {'valid': Bool()}
})
@crossdomain(origin='*')
def geo_lookup(geoid):
    geoid_parts = geoid.split('US')
    if len(geoid_parts) is not 2:
        abort(400, 'Invalid geoid')

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
        abort(404, 'Unknown geoid')

    geom = result.pop('geom', None)
    if geom:
        geom = json.loads(geom)

    return jsonify(type="Feature", properties=result, geometry=geom)


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
    'acs': {'valid': OneOf(allowed_acs), 'default': 'acs2011_1yr'},
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

    g.cur.execute("SET search_path=%s,public;", [acs])

    data = []
    # retrieve matching tables.
    g.cur.execute("""SELECT tab.table_id,tab.table_title,tab.simple_table_title,tab.universe,tab.topics
                     FROM census_table_metadata tab
                     WHERE %s
                     ORDER BY char_length(tab.table_id), tab.table_id""" % (table_where), table_where_args)

    data.extend([format_table_search_result(table, 'table') for table in g.cur])

    # retrieve matching columns.
    if q != '*':
        # Special case for when we want ALL the tables (but not all the columns)
        g.cur.execute("""SELECT col.column_id,col.column_title,tab.table_id,tab.table_title,tab.simple_table_title,tab.universe,tab.topics
                         FROM census_column_metadata col
                         LEFT OUTER JOIN census_table_metadata tab USING (table_id)
                         WHERE %s
                         ORDER BY char_length(tab.table_id), tab.table_id""" % (column_where), column_where_args)
        data.extend([format_table_search_result(column, 'column') for column in g.cur])

    return json.dumps(data)


# Example: /1.0/table/B01001?release=acs2011_1yr
@app.route("/1.0/table/<table_id>")
@qwarg_validate({
    'acs': {'valid': OneOf(allowed_acs), 'default': 'acs2011_1yr'}
})
@crossdomain(origin='*')
def table_details(table_id):
    g.cur.execute("SET search_path=%s,public;", [request.qwargs.acs])

    g.cur.execute("""SELECT *
                     FROM census_table_metadata tab
                     WHERE table_id=%s""", [table_id])
    row = g.cur.fetchone()

    if not row:
        abort(404, "Table %s not found." % table_id)

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

        g.cur.execute("SELECT * FROM census_table_metadata WHERE table_id=%s;", [table_id])
        table_record = g.cur.fetchone()
        if table_record:
            validated_table_id = table_record['table_id']
            release['table_name'] = table_record['table_title']
            release['table_universe'] = table_record['universe']

            if parent_sumlevel == '010':
                child_geoheaders = get_all_child_geoids(child_summary_level)
            elif parent_sumlevel in PARENT_CHILD_CONTAINMENT and child_summary_level in PARENT_CHILD_CONTAINMENT[parent_sumlevel]:
                child_geoheaders = get_child_geoids_by_prefix(parent_geoid, child_summary_level)
            else:
                child_geoheaders = get_child_geoids_by_gis(parent_geoid, child_summary_level)

            if child_geoheaders:
                child_geoids = [child['geoid'] for child in child_geoheaders]
                g.cur.execute("SELECT COUNT(*) FROM %s.%s WHERE geoid IN %%s" % (acs, validated_table_id), [tuple(child_geoids)])
                acs_rowcount = g.cur.fetchone()
                release['results'] = acs_rowcount['count']
            else:
                release['results'] = 0

        data[acs] = release

    return json.dumps(data)


## DATA RETRIEVAL ##

def get_all_child_geoids(child_summary_level):
    g.cur.execute("""SELECT geoid,name
        FROM geoheader
        WHERE sumlevel=%s AND component='00'
        ORDER BY name""", [int(child_summary_level)])

    return g.cur.fetchall()

# get geoheader data for children at the requested summary level
def get_child_geoids_by_gis(parent_geoid, child_summary_level):
    parent_sumlevel = parent_geoid[0:3]
    child_geoids = []
    parent_tiger_geoid = parent_geoid.split('US')[1]
    g.cur.execute("""SELECT child.geoid
        FROM tiger2012.census_names parent
        JOIN tiger2012.census_names child ON ST_Intersects(parent.the_geom, child.the_geom) AND child.sumlevel=%s
        WHERE parent.geoid=%s AND parent.sumlevel=%s;""", [child_summary_level, parent_tiger_geoid, parent_sumlevel])
    child_geoids = ['%s00US%s' % (child_summary_level, r['geoid']) for r in g.cur]

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

    g.cur.execute("""SELECT geoid,name
        FROM geoheader
        WHERE geoid LIKE %s
        ORDER BY name""", [child_geoid_prefix])
    return g.cur.fetchall()

# Example: /1.0/data/rank/acs2011_5yr/B01001001?geoid=04000US53
# Example: /1.0/data/rank/acs2011_5yr/B01001001?geoid=16000US5367000&within=04000US53
# Example: /1.0/data/rank/acs2011_5yr/B01001001?geoid=05000US53063
@app.route("/1.0/data/rank/<acs>/<column_id>")
@qwarg_validate({
    'within': {'valid': NonemptyString(), 'required': False, 'default': '01000US'},
    'geoid': {'valid': NonemptyString(), 'required': True}
})
@crossdomain(origin='*')
def data_rank_geographies_within_parent(acs, column_id):
    # make sure we support the requested ACS release
    if acs not in allowed_acs:
        abort(404, 'ACS %s is not supported.' % acs)
    g.cur.execute("SET search_path=%s,public;", [acs])

    table_id = column_id[:-3]
    parent_geoid = request.qwargs.within
    geoid_of_interest = request.qwargs.geoid

    # TODO should validate the parent and geoid of interest.

    child_summary_level = geoid_of_interest[:3]
    parent_sumlevel = parent_geoid[:3]
    if parent_sumlevel == '010':
        child_geoheaders = get_all_child_geoids(child_summary_level)
    elif parent_sumlevel in PARENT_CHILD_CONTAINMENT and child_summary_level in PARENT_CHILD_CONTAINMENT[parent_sumlevel]:
        child_geoheaders = get_child_geoids_by_prefix(parent_geoid, child_summary_level)
    else:
        child_geoheaders = get_child_geoids_by_gis(parent_geoid, child_summary_level)

    child_geoids = [child['geoid'] for child in child_geoheaders]

    g.cur.execute("""SELECT rank() OVER (ORDER BY %(column_id)s DESC),%(column_id)s,g.geoid,g.name
        FROM %(table_id)s
        JOIN geoheader g USING (geoid)
        WHERE geoid IN %%s""" % {'column_id': column_id, 'table_id': table_id}, [tuple(child_geoids)])

    ranks = []

    ranks.extend(g.cur.fetchmany(3))

    for r in g.cur:
        if r['geoid'] == geoid_of_interest or g.cur.rownumber >= g.cur.rowcount - 2:
            ranks.append(r)

    return json.dumps(ranks)

# Example: /1.0/data/histogram/acs2011_5yr/B01001001?geoid=04000US53
# Example: /1.0/data/histogram/acs2011_5yr/B01001001?geoid=16000US5367000&within=04000US53
# Example: /1.0/data/histogram/acs2011_5yr/B01001001?geoid=05000US53063
@app.route("/1.0/data/histogram/<acs>/<column_id>")
@qwarg_validate({
    'within': {'valid': NonemptyString(), 'required': False, 'default': '01000US'},
    'geoid': {'valid': NonemptyString(), 'required': True}
})
@crossdomain(origin='*')
def data_histogram_geographies_within_parent(acs, column_id):
    # make sure we support the requested ACS release
    if acs not in allowed_acs:
        abort(404, 'ACS %s is not supported.' % acs)
    g.cur.execute("SET search_path=%s,public;", [acs])

    table_id = column_id[:-3]
    parent_geoid = request.qwargs.within
    geoid_of_interest = request.qwargs.geoid

    # TODO should validate the parent and geoid of interest.

    child_summary_level = geoid_of_interest[:3]
    parent_sumlevel = parent_geoid[:3]
    if parent_sumlevel == '010':
        child_geoheaders = get_all_child_geoids(child_summary_level)
    elif parent_sumlevel in PARENT_CHILD_CONTAINMENT and child_summary_level in PARENT_CHILD_CONTAINMENT[parent_sumlevel]:
        child_geoheaders = get_child_geoids_by_prefix(parent_geoid, child_summary_level)
    else:
        child_geoheaders = get_child_geoids_by_gis(parent_geoid, child_summary_level)

    child_geoids = [child['geoid'] for child in child_geoheaders]

    g.cur.execute("""SELECT percentile,count(percentile)
        FROM (SELECT ntile(100) OVER (ORDER BY %(column_id)s DESC) AS percentile
            FROM %(table_id)s
            WHERE %%s) x
        GROUP BY x.percentile
        ORDER BY x.percentile""" % {'column_id': column_id, 'table_id': table_id}, [tuple(child_geoids)])
    # g.cur.execute("""SELECT percentile,COUNT(percentile)
    #     FROM (SELECT ntile(100) OVER (ORDER BY %(column_id)s DESC) AS percentile FROM %(table_id)s WHERE %(where)s) x
    #     GROUP BY percentile
    #     ORDER BY percentile""" % {'column_id': column_id, 'table_id': table_id, 'where': where})

    return json.dumps(g.cur.fetchall())

# Example: /1.0/data/compare/acs2011_5yr/B01001?sumlevel=050&within=04000US53
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
        abort(404, 'ACS %s is not supported.' % acs)
    g.cur.execute("SET search_path=%s,public;", [acs])

    parent_geoid = request.qwargs.within
    child_summary_level = request.qwargs.sumlevel

    # create the containers we need for our response
    data = OrderedDict([
        ('comparison', OrderedDict()),
        ('table', OrderedDict()),
        ('parent_geography', OrderedDict()),
        ('child_geographies', OrderedDict())
    ])

    # add some basic metadata about the comparison and data table requested.
    data['comparison']['child_summary_level'] = child_summary_level
    data['comparison']['child_geography_name'] = SUMLEV_NAMES.get(child_summary_level, {}).get('name')
    data['comparison']['child_geography_name_plural'] = SUMLEV_NAMES.get(child_summary_level, {}).get('plural')

    g.cur.execute("""SELECT tab.table_id,tab.table_title,tab.universe,tab.denominator_column_id,col.column_id,col.column_title,col.indent
        FROM census_column_metadata col
        LEFT JOIN census_table_metadata tab USING (table_id)
        WHERE table_id=%s
        ORDER BY column_id;""", [table_id])
    table_metadata = g.cur.fetchall()

    if not table_metadata:
        abort(404, 'Table id %s is not available in %s.' % (table_id, acs))

    validated_table_id = table_metadata[0]['table_id']

    # get the basic table record, and add a map of columnID -> column name
    table_record = table_metadata[0]
    column_map = OrderedDict()
    for record in table_metadata:
        if record['column_id']:
            column_map[record['column_id']] = OrderedDict()
            column_map[record['column_id']]['name'] = record['column_title']
            column_map[record['column_id']]['indent'] = record['indent']

    data['table']['census_release'] = ACS_NAMES.get(acs).get('name')
    data['table']['table_id'] = validated_table_id
    data['table']['table_name'] = table_record['table_title']
    data['table']['table_universe'] = table_record['universe']
    data['table']['denominator_column_id'] = table_record['denominator_column_id']
    data['table']['columns'] = column_map

    # add some data about the parent geography
    g.cur.execute("SELECT * FROM geoheader WHERE geoid=%s;", [parent_geoid])
    parent_geography = g.cur.fetchone()
    parent_sumlevel = '%03d' % parent_geography['sumlevel']

    data['parent_geography']['geography'] = OrderedDict()
    data['parent_geography']['geography']['name'] = parent_geography['name']
    data['parent_geography']['geography']['summary_level'] = parent_sumlevel

    data['comparison']['parent_summary_level'] = parent_sumlevel
    data['comparison']['parent_geography_name'] = SUMLEV_NAMES.get(parent_sumlevel, {}).get('name')
    data['comparison']['parent_name'] = parent_geography['name']
    data['comparison']['parent_geoid'] = parent_geoid

    if parent_sumlevel == '010':
        child_geoheaders = get_all_child_geoids(child_summary_level)
    elif parent_sumlevel in PARENT_CHILD_CONTAINMENT and child_summary_level in PARENT_CHILD_CONTAINMENT[parent_sumlevel]:
        child_geoheaders = get_child_geoids_by_prefix(parent_geoid, child_summary_level)
    else:
        child_geoheaders = get_child_geoids_by_gis(parent_geoid, child_summary_level)

    # start compiling child data for our response
    child_geoid_list = list()
    for geoheader in child_geoheaders:
        # store some mapping to make our next query easier
        child_geoid_list.append(geoheader['geoid'].split('US')[1])

        # build the child item
        data['child_geographies'][geoheader['geoid']] = OrderedDict()
        data['child_geographies'][geoheader['geoid']]['geography'] = OrderedDict()
        data['child_geographies'][geoheader['geoid']]['geography']['name'] = geoheader['name']
        data['child_geographies'][geoheader['geoid']]['geography']['summary_level'] = child_summary_level
        data['child_geographies'][geoheader['geoid']]['data'] = {}

    # get geographical data if requested
    geometries = request.qwargs.geom
    child_geodata_map = {}
    if geometries:
        # get the parent geometry and add to API response
        g.cur.execute("""SELECT ST_AsGeoJSON(ST_Simplify(the_geom,0.001)) as geometry
            FROM tiger2012.census_names
            WHERE sumlevel=%s AND geoid=%s;""", [parent_sumlevel, parent_geoid.split('US')[1]])
        parent_geometry = g.cur.fetchone()
        try:
            data['parent_geography']['geography']['geometry'] = json.loads(parent_geometry['geometry'])
        except:
            # we may not have geometries for all sumlevs
            pass

        # get the child geometries and store for later
        g.cur.execute("""SELECT geoid, ST_AsGeoJSON(ST_Simplify(the_geom,0.001)) as geometry
            FROM tiger2012.census_names
            WHERE sumlevel=%s AND geoid IN %s
            ORDER BY geoid;""", [child_summary_level, tuple(child_geoid_list)])
        child_geodata = g.cur.fetchall()
        child_geodata_map = dict([(record['geoid'], json.loads(record['geometry'])) for record in child_geodata])

    # make the where clause and query the requested census data table
    # get parent data first...
    g.cur.execute("SELECT * FROM %s WHERE geoid=%%s" % (validated_table_id), [parent_geography['geoid']])
    parent_data = g.cur.fetchone()
    parent_data.pop('geoid', None)
    column_data = []
    for (k, v) in sorted(parent_data.items(), key=lambda tup: tup[0]):
        column_data.append((k.upper(), v))
    data['parent_geography']['data'] = OrderedDict(column_data)

    if child_geoheaders:
        # ... and then children so we can loop through with cursor
        child_geoids = [child['geoid'] for child in child_geoheaders]
        g.cur.execute("SELECT * FROM %s WHERE geoid IN %%s" % (validated_table_id), [tuple(child_geoids)])
        # store the number of rows returned in comparison object
        data['comparison']['results'] = g.cur.rowcount

        # grab one row at a time
        for record in g.cur:
            child_geoid = record.pop('geoid')

            column_data = []
            for (k, v) in sorted(record.items(), key=lambda tup: tup[0]):
                column_data.append((k.upper(), v))
            data['child_geographies'][child_geoid]['data'] = OrderedDict(column_data)

            if child_geodata_map:
                try:
                    data['child_geographies'][child_geoid]['geography']['geometry'] = child_geodata_map[child_geoid.split('US')[1]]
                except:
                    # we may not have geometries for all sumlevs
                    pass
    else:
        data['comparison']['results'] = 0

    return json.dumps(data, indent=4, separators=(',', ': '))


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000, debug=True)
