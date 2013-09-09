# For real division instead of sometimes-integer
from __future__ import division

from flask import Flask
from flask import abort, request, g
from flask import make_response, current_app
from functools import update_wrapper
import json
import psycopg2
import psycopg2.extras
from collections import OrderedDict
from datetime import timedelta
from urllib2 import unquote
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


def sum(data, *columns):
    def reduce_fn(x, y):
        if x and y:
            return x + y
        elif x and not y:
            return x
        elif y and not x:
            return y
        else:
            return None

    return reduce(reduce_fn, map(lambda col: data[col], columns))


def dif(minuend, subtrahend):
    if minuend and subtrahend:
        return minuend - subtrahend
    else:
        return None


def maybe_int(i):
    return int(i) if i else i


def maybe_float(i, decimals=1):
    return round(float(i), decimals) if i else i


def div(numerator, denominator):
    if numerator and denominator:
        return numerator / denominator
    else:
        return None


def maybe_percent(numerator, denominator, decimals=1):
    if not numerator or not denominator:
        return None

    return round(numerator / denominator * 100, decimals)


def build_item(table_id, universe, name, data_years, data, transform):
    val = dict(table_id=table_id,
        universe=universe,
        name=name,
        data_years=data_years,
        values=dict(this=transform(data),
                    county=transform(data),
                    state=transform(data),
                    nation=transform(data)))

    return val


def find_geoid(geoid, acs=None):
    "Find the best acs to use for a given geoid or None if the geoid is not found."

    if acs:
        acs_to_search = [acs]
    else:
        acs_to_search = allowed_acs

    for acs in acs_to_search:
        g.cur.execute("SELECT stusab,logrecno FROM %s.geoheader WHERE geoid=%%s" % acs, [geoid])
        if g.cur.rowcount == 1:
            result = g.cur.fetchone()
            return (acs, result['stusab'], result['logrecno'])
    return (None, None, None)


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


@app.route("/1.0/latest/geoid/search")
@qwarg_validate({
    'name': {'valid': NonemptyString(), 'required': True}
})
def latest_geoid_search():
    term = "%s%%" % request.qwargs.name

    result = []
    for acs in allowed_acs:
        g.cur.execute("SELECT geoid,stusab as state,name FROM %s.geoheader WHERE name LIKE %%s LIMIT 5" % acs, [term])
        if g.cur.rowcount > 0:
            result = g.cur.fetchall()
            for r in result:
                r['acs'] = acs
            break

    return json.dumps(result)


@app.route("/1.0/<acs>/geoid/search")
@qwarg_validate({
    'name': {'valid': NonemptyString()}
})
def acs_geoid_search(acs):
    if acs not in allowed_acs:
        abort(404, "I don't know anything about that ACS.")

    term = "%s%%" % request.qwargs.name

    result = []
    g.cur.execute("SELECT geoid,stusab as state,name FROM %s.geoheader WHERE name LIKE %%s LIMIT 5" % acs, [term])
    if g.cur.rowcount > 0:
        result = g.cur.fetchall()
        for r in result:
            r['acs'] = acs

    return json.dumps(result)


def geo_profile(acs, state, logrecno):
    g.cur.execute("SET search_path=%s", [acs])

    doc = OrderedDict([('geography', dict()),
                       ('demographics', dict()),
                       ('economics', dict()),
                       ('families', dict()),
                       ('housing', dict()),
                       ('social', dict())])

    doc['geography']['census_release'] = ACS_NAMES.get(acs).get('name')
    default_data_years = ACS_NAMES.get(acs).get('years')

    g.cur.execute("SELECT * FROM geoheader WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()
    doc['geography'].update(dict(name=data['name'],
                                 pretty_name=None,
                                 stusab=data['stusab'],
                                 sumlevel=data['sumlevel'],
                                 land_area=None))

    # Demographics: Age
    g.cur.execute("SELECT * FROM B01001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    doc['geography']['total_population'] = maybe_int(data['b01001001'])

    age_dict = dict()
    doc['demographics']['age'] = age_dict
    age_dict['percent_under_18'] = build_item('b01001', 'Total population', 'Under 18', default_data_years, data,
                                        lambda data: maybe_percent((sum(data, 'b01001003', 'b01001004', 'b01001005', 'b01001006') +
                                                                    sum(data, 'b01001027', 'b01001028', 'b01001029', 'b01001030')),
                                                                    data['b01001001']))

    age_dict['percent_over_65'] = build_item('b01001', 'Total population', '65 and over', default_data_years, data,
                                        lambda data: maybe_percent((sum(data, 'b01001020', 'b01001021', 'b01001022', 'b01001023', 'b01001024', 'b01001025') +
                                                                    sum(data, 'b01001044', 'b01001045', 'b01001046', 'b01001047', 'b01001048', 'b01001049')),
                                                                    data['b01001001']))

    pop_dict = dict()
    age_dict['population_by_age'] = pop_dict
    population_by_age_total = OrderedDict()
    population_by_age_male = OrderedDict()
    population_by_age_female = OrderedDict()
    pop_dict['total'] = population_by_age_total
    pop_dict['male'] = population_by_age_male
    pop_dict['female'] = population_by_age_female

    population_by_age_male['0-9'] = build_item('b01001', 'Total population', '0-9', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001003', 'b01001004')))
    population_by_age_female['0-9'] = build_item('b01001', 'Total population', '0-9', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001027', 'b01001028')))
    population_by_age_total['0-9'] = build_item('b01001', 'Total population', '0-9', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001003', 'b01001004', 'b01001027', 'b01001028')))

    population_by_age_male['10-19'] = build_item('b01001', 'Total population', '10-19', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001005', 'b01001006', 'b01001007')))
    population_by_age_female['10-19'] = build_item('b01001', 'Total population', '10-19', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001029', 'b01001030', 'b01001031')))
    population_by_age_total['10-19'] = build_item('b01001', 'Total population', '10-19', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001005', 'b01001006', 'b01001007', 'b01001029', 'b01001030', 'b01001031')))

    population_by_age_male['20-29'] = build_item('b01001', 'Total population', '20-29', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001008', 'b01001009', 'b01001010', 'b01001011')))
    population_by_age_female['20-29'] = build_item('b01001', 'Total population', '20-29', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001032', 'b01001033', 'b01001034', 'b01001035')))
    population_by_age_total['20-29'] = build_item('b01001', 'Total population', '20-29', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001008', 'b01001009', 'b01001010', 'b01001011', 'b01001032', 'b01001033', 'b01001034', 'b01001035')))

    population_by_age_male['30-39'] = build_item('b01001', 'Total population', '30-39', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001012', 'b01001013')))
    population_by_age_female['30-39'] = build_item('b01001', 'Total population', '30-39', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001036', 'b01001037')))
    population_by_age_total['30-39'] = build_item('b01001', 'Total population', '30-39', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001012', 'b01001013', 'b01001036', 'b01001037')))

    population_by_age_male['40-49'] = build_item('b01001', 'Total population', '40-49', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001014', 'b01001015')))
    population_by_age_female['40-49'] = build_item('b01001', 'Total population', '40-49', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001038', 'b01001039')))
    population_by_age_total['40-49'] = build_item('b01001', 'Total population', '40-49', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001014', 'b01001015', 'b01001038', 'b01001039')))

    population_by_age_male['50-59'] = build_item('b01001', 'Total population', '50-59', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001016', 'b01001017')))
    population_by_age_female['50-59'] = build_item('b01001', 'Total population', '50-59', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001040', 'b01001041')))
    population_by_age_total['50-59'] = build_item('b01001', 'Total population', '50-59', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001016', 'b01001017', 'b01001040', 'b01001041')))

    population_by_age_male['60-69'] = build_item('b01001', 'Total population', '60-69', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001018', 'b01001019', 'b01001020', 'b01001021')))
    population_by_age_female['60-69'] = build_item('b01001', 'Total population', '60-69', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001042', 'b01001043', 'b01001044', 'b01001045')))
    population_by_age_total['60-69'] = build_item('b01001', 'Total population', '60-69', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001018', 'b01001019', 'b01001020', 'b01001021', 'b01001042', 'b01001043', 'b01001044', 'b01001045')))

    population_by_age_male['70-79'] = build_item('b01001', 'Total population', '70-79', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001022', 'b01001023')))
    population_by_age_female['70-79'] = build_item('b01001', 'Total population', '70-79', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001046', 'b01001047')))
    population_by_age_total['70-79'] = build_item('b01001', 'Total population', '70-79', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001022', 'b01001023', 'b01001046', 'b01001047')))

    population_by_age_male['80+'] = build_item('b01001', 'Total population', '80+', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001024', 'b01001025')))
    population_by_age_female['80+'] = build_item('b01001', 'Total population', '80+', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001048', 'b01001049')))
    population_by_age_total['80+'] = build_item('b01001', 'Total population', '80+', default_data_years, data,
                                        lambda data: maybe_int(sum(data, 'b01001024', 'b01001025', 'b01001048', 'b01001049')))

    # Demographics: Sex
    sex_dict = dict()
    doc['demographics']['sex'] = sex_dict
    sex_dict['percent_male'] = build_item('b01001', 'Total population', 'Male', default_data_years, data,
                                        lambda data: maybe_percent(data['b01001002'], data['b01001001']))

    sex_dict['percent_female'] = build_item('b01001', 'Total population', 'Female', default_data_years, data,
                                        lambda data: maybe_percent(data['b01001026'], data['b01001001']))

    g.cur.execute("SELECT * FROM B01002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    age_dict['median_age'] = build_item('b01002', 'Total population', 'Median age', default_data_years, data,
                                        lambda data: maybe_float(data['b01002001']))

    age_dict['median_age_male'] = build_item('b01002', 'Total population', 'Median age male', default_data_years, data,
                                        lambda data: maybe_float(data['b01002002']))

    age_dict['median_age_female'] = build_item('b01002', 'Total population', 'Median age female', default_data_years, data,
                                        lambda data: maybe_float(data['b01002003']))

    # Demographics: Race
    g.cur.execute("SELECT * FROM B02001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    race_dict = OrderedDict()
    doc['demographics']['race'] = race_dict
    race_dict['percent_white'] = build_item('b02001', 'Total population', 'White', default_data_years, data,
                                        lambda data: maybe_percent(data['b02001002'], data['b02001001']))

    race_dict['percent_black'] = build_item('b02001', 'Total population', 'Black', default_data_years, data,
                                        lambda data: maybe_percent(data['b02001003'], data['b02001001']))

    race_dict['percent_native_american'] = build_item('b02001', 'Total population', 'Native', default_data_years, data,
                                        lambda data: maybe_percent(data['b02001004'], data['b02001001']))

    race_dict['percent_asian'] = build_item('b02001', 'Total population', 'Asian', default_data_years, data,
                                        lambda data: maybe_percent(data['b02001005'], data['b02001001']))

    race_dict['percent_native_islander'] = build_item('b02001', 'Total population', 'Islander', default_data_years, data,
                                        lambda data: maybe_percent(data['b02001006'], data['b02001001']))

    race_dict['percent_other'] = build_item('b02001', 'Total population', 'Other race', default_data_years, data,
                                        lambda data: maybe_percent(data['b02001007'], data['b02001001']))

    race_dict['percent_two_or_more'] = build_item('b02001', 'Total population', 'Two+ races', default_data_years, data,
                                        lambda data: maybe_percent(data['b02001008'], data['b02001001']))

    g.cur.execute("SELECT * FROM B03003 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    ethnicity_dict = dict()
    doc['demographics']['ethnicity'] = ethnicity_dict

    ethnicity_dict['percent_hispanic'] = build_item('b03003', 'Total population', 'Hispanic/Latino', default_data_years, data,
                                        lambda data: maybe_percent(data['b03003003'], data['b03003001']))

    # Economics: Per-Capita Income
    g.cur.execute("SELECT * FROM B19301 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    income_dict = dict()
    doc['economics']['income'] = income_dict

    income_dict['per_capita_income_in_the_last_12_months'] = build_item('b19301', 'Total population', 'Per capita income in past year', default_data_years, data,
                                        lambda data: maybe_int(data['b19301001']))

    # Economics: Median Household Income
    g.cur.execute("SELECT * FROM B19013 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    income_dict['median_household_income'] = build_item('b19013', 'Households', 'Median household income', default_data_years, data,
                                        lambda data: maybe_int(data['b19013001']))

    # Economics: Poverty Rate
    g.cur.execute("SELECT * FROM B17001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    poverty_dict = dict()
    doc['economics']['poverty'] = poverty_dict

    poverty_dict['percent_below_poverty_line'] = build_item('b17001', 'Population for whom poverty status is determined', 'Persons below poverty line', default_data_years, data,
                                        lambda data: maybe_percent(data['b17001002'], data['b17001001']))

    # Economics: Median Travel Time to Work
    g.cur.execute("SELECT * FROM B08006 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    _total_workers_16_and_over = maybe_int(data['b08006001'])
    _workers_who_worked_at_home = maybe_int(data['b08006017'])

    g.cur.execute("SELECT * FROM B08013 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    _aggregate_minutes = maybe_int(data['b08013001'])

    travel_time_dict = dict()
    doc['economics']['travel_time'] = travel_time_dict

    travel_time_dict['mean_travel_time'] = build_item('b08006, b08013', 'Workers 16 years and over', 'Mean travel time to work', default_data_years, data,
                                        lambda data: maybe_float(div(_aggregate_minutes, dif(_total_workers_16_and_over, _workers_who_worked_at_home))))

    # Families: Number of Households
    g.cur.execute("SELECT * FROM B11001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    households_dict = dict()
    doc['families']['households'] = households_dict
    # store so we can use this for the next calculation too
    _number_of_households = maybe_int(data['b11001001'])

    households_dict['number_of_households'] = build_item('b11001', 'Households', 'Number of households', default_data_years, data,
                                        lambda data: _number_of_households)


    # Families: Persons per Household
    g.cur.execute("SELECT * FROM B11002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    _total_persons_in_households = maybe_int(data['b11002001'])

    households_dict['persons_per_household'] = build_item('b11001,b11002', 'Households', 'Persons per household', default_data_years, data,
                                        lambda data: maybe_float(div(_total_persons_in_households, _number_of_households)))

    # Housing: Mobility
    g.cur.execute("SELECT * FROM B07001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    migration_dict = dict()
    doc['housing']['mobility'] = migration_dict

    migration_dict['percent_living_in_same_house_1_year'] = build_item('b07001', 'Population 1 year and over in the United States', 'People living in same house for 1 year or more', default_data_years, data,
                                        lambda data: maybe_percent(data['b07001017'], data['b07001001']))

    # Housing: Number of Housing Units
    g.cur.execute("SELECT * FROM B25001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    units_dict = dict()
    doc['housing']['units'] = units_dict

    units_dict['number_of_housing_units'] = build_item('b25001', 'Housing units', 'Number of housing units', default_data_years, data,
                                        lambda data: maybe_int(data['b25001001']))

    # Housing: Percentage of Units in Multi-Unit Structure
    g.cur.execute("SELECT * FROM B25024 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    units_dict['percent_units_in_multi_unit_structure'] = build_item('b25024', 'Housing units', 'Housing units in multi-unit structures', default_data_years, data,
                                        lambda data: maybe_percent(sum(data, 'b25024004', 'b25024005', 'b25024006', 'b25024007', 'b25024008', 'b25024009'),
                                                                    data['b25024001']))

    # Housing: Percentage of Homeownership
    g.cur.execute("SELECT * FROM B25003 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    ownership_dict = dict()
    doc['housing']['ownership'] = ownership_dict

    ownership_dict['percent_homeownership'] = build_item('b25003', 'Occupied housing units', 'Rate of homeownership', default_data_years, data,
                                        lambda data: maybe_percent(data['b25003002'], data['b25003001']))

    # Housing: Median Value
    g.cur.execute("SELECT * FROM B25077 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    ownership_dict['median_value_of_owner_occupied_housing_unit'] = build_item('b25077', 'Owner-occupied housing units', 'Median value of owner-occupied housing units', default_data_years, data,
                                        lambda data: maybe_int(data['b25077001']))

    # Social: Educational Attainment
    g.cur.execute("SELECT * FROM B15002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    attainment_dict = dict()
    doc['social']['educational_attainment'] = attainment_dict

    attainment_dict['percent_high_school_or_higher'] = build_item('b15002', 'Population 25 years and over', 'High school grad or higher', default_data_years, data,
                                        lambda data: maybe_percent((sum(data, 'b15002011', 'b15002012', 'b15002013', 'b15002014', 'b15002015', 'b15002016', 'b15002017', 'b15002018') +
                                                                     sum(data, 'b15002028', 'b15002029', 'b15002030', 'b15002031', 'b15002032', 'b15002033', 'b15002034', 'b15002035')),
                                                                     data['b15002001']))

    attainment_dict['percent_bachelor_degree_or_higher'] = build_item('b15002', 'Population 25 years and over', 'Bachelor\'s degree or higher', default_data_years, data,
                                        lambda data: maybe_percent((sum(data, 'b15002015', 'b15002016', 'b15002017', 'b15002018') +
                                                                     sum(data, 'b15002032', 'b15002033', 'b15002034', 'b15002035')),
                                                                     data['b15002001']))

    # Social: Place of Birth
    g.cur.execute("SELECT * FROM B05002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    foreign_dict = dict()
    doc['social']['place_of_birth'] = foreign_dict

    foreign_dict['percent_foreign_born'] = build_item('b05002', 'Total population', 'Foreign-born persons', default_data_years, data,
                                        lambda data: maybe_percent(data['b05002013'], data['b05002001']))

    # Social: Percentage of Non-English Spoken at Home
    g.cur.execute("SELECT * FROM B16001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    language_dict = dict()
    doc['social']['language'] = language_dict

    language_dict['percent_non_english_at_home'] = build_item('b16001', 'Population 5 years and over', 'Persons with language other than English spoken at home', default_data_years, data,
                                        lambda data: maybe_float(maybe_percent(dif(data['b16001001'], data['b16001002']), data['b16001001'])))

    # Social: Number of Veterans
    g.cur.execute("SELECT * FROM B21002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    veterans_dict = dict()
    doc['social']['veteran_status'] = veterans_dict

    veterans_dict['number_of_veterans'] = build_item('b21002', 'Civilian veterans 18 years and over', 'Number of veterans', default_data_years, data,
                                        lambda data: maybe_int(data['b21002001']))

    return json.dumps(doc)


@app.route("/1.0/<acs>/<geoid>/profile")
def acs_geo_profile(acs, geoid):
    acs, state, logrecno = find_geoid(geoid, acs)

    if not acs:
        abort(404, 'That ACS doesn\'t know about have that geoid.')

    return geo_profile(acs, state, logrecno)


@app.route("/1.0/latest/<geoid>/profile")
def latest_geo_profile(geoid):
    acs, state, logrecno = find_geoid(geoid)

    if not acs:
        abort(404, 'None of the ACS I know about have that geoid.')

    return geo_profile(acs, state, logrecno)


## GEO LOOKUPS ##

def build_geo_full_name(row):
    geoid = row['geoid']
    sumlevel = row['sumlevel']
    if sumlevel in ('500', '610', '620'):
        return "%s %s" % (state_fips[geoid[:2]], row['name'])
    elif sumlevel in ('050', '950', '960', '970', '160'):
        return "%s, %s" % (row['name'], state_fips[geoid[:2]])
    elif sumlevel == '860':
        return "Zip Code: %s" % row['name']
    else:
        return row['name']

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
        q += "%"
        where = "lower(name) LIKE lower(%s)"
        where_args = [q]
    else:
        abort(400, "Must provide either a lat/lon OR a query term.")

    if sumlevs:
        where += " AND sumlevel IN %s"
        where_args.append(tuple(sumlevs))

    if with_geom:
        g.cur.execute("""SELECT awater,aland,sumlevel,geoid,name,ST_AsGeoJSON(ST_Simplify(the_geom,0.001)) as geom
            FROM tiger2012.census_names
            WHERE %s
            ORDER BY sumlevel, aland DESC
            LIMIT 25;""" % where, where_args)
    else:
        g.cur.execute("""SELECT awater,aland,sumlevel,geoid,name
            FROM tiger2012.census_names
            WHERE %s
            ORDER BY sumlevel, aland DESC
            LIMIT 25;""" % where, where_args)

    data = []

    for row in g.cur:
        row['full_geoid'] = "%s00US%s" % (row['sumlevel'], row['geoid'])
        row['full_name'] = build_geo_full_name(row)
        if 'geom' in row and row['geom']:
            row['geom'] = json.loads(row['geom'])
        data.append(row)

    return json.dumps(data)


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

    sumlevel_part = geoid_parts[0][:3]
    id_part = geoid_parts[1]

    if request.qwargs.geom:
        g.cur.execute("""SELECT awater,aland,name,intptlat,intptlon,ST_AsGeoJSON(ST_Simplify(the_geom,0.001)) as geom
            FROM tiger2012.census_names
            WHERE sumlevel=%s AND geoid=%s
            LIMIT 1""", [sumlevel_part, id_part])
    else:
        g.cur.execute("""SELECT awater,aland,name,intptlat,intptlon
            FROM tiger2012.census_names
            WHERE sumlevel=%s AND geoid=%s
            LIMIT 1""", [sumlevel_part, id_part])

    result = g.cur.fetchone()

    if not result:
        abort(404, 'Unknown geoid')

    intptlon = result.pop('intptlon')
    result['intptlon'] = round(float(intptlon), 7)
    intptlat = result.pop('intptlat')
    result['intptlat'] = round(float(intptlat), 7)

    if 'geom' in result:
        result['geom'] = json.loads(result['geom'])

    return json.dumps(result)


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
                         LEFT OUTER JOIN census_table_metadata tab USING (table_id, sequence_number)
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
                     WHERE table_id=%s AND sequence_number=%s""", [row['table_id'], row['sequence_number']])

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
    year = request.qwargs.year
    child_summary_level = request.qwargs.sumlevel
    parent_geoid = request.qwargs.within
    parent_sumlevel = parent_geoid[:3]

    data = OrderedDict()

    releases = sorted([name for name in ACS_NAMES if year in name])
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
                where = " OR ".join(["(stusab='%s' AND logrecno='%s')" % (child['stusab'], child['logrecno']) for child in child_geoheaders])
                g.cur.execute("SELECT COUNT(*) FROM %s.%s WHERE %s" % (acs, validated_table_id, where))
                acs_rowcount = g.cur.fetchone()
                release['results'] = acs_rowcount['count']
            else:
                release['results'] = 0

        data[acs] = release

    return json.dumps(data)


## DATA RETRIEVAL ##

def get_all_child_geoids(child_summary_level):
    g.cur.execute("""SELECT geoid,stusab,logrecno,name
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

    g.cur.execute("""SELECT geoid,stusab,logrecno,name
        FROM geoheader
        WHERE geoid IN %s
        ORDER BY name""", [tuple(child_geoids)])
    return g.cur.fetchall()


def get_child_geoids_by_prefix(parent_geoid, child_summary_level):
    child_geoid_prefix = '%s00US%s%%' % (child_summary_level, parent_geoid.split('US')[1])

    g.cur.execute("""SELECT geoid,stusab,logrecno,name
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

    where = " OR ".join(["(stusab='%s' AND logrecno='%s')" % (child['stusab'], child['logrecno']) for child in child_geoheaders])

    g.cur.execute("""SELECT rank() OVER (ORDER BY %(column_id)s DESC),%(column_id)s,g.geoid,g.name
        FROM %(table_id)s
        JOIN geoheader g USING (stusab, logrecno)
        WHERE %(where)s""" % {'column_id': column_id, 'table_id': table_id, 'where': where})

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

    where = " OR ".join(["(stusab='%s' AND logrecno='%s')" % (child['stusab'], child['logrecno']) for child in child_geoheaders])

    g.cur.execute("""SELECT percentile,count(percentile)
        FROM (SELECT ntile(100) OVER (ORDER BY %(column_id)s DESC) AS percentile
            FROM %(table_id)s
            WHERE %(where)s) x
        GROUP BY x.percentile
        ORDER BY x.percentile""" % {'column_id': column_id, 'table_id': table_id, 'where': where})
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
        LEFT JOIN census_table_metadata tab USING (table_id, sequence_number)
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
    child_geoid_map = dict()
    child_geoid_list = list()
    for geoheader in child_geoheaders:
        # store some mapping to make our next query easier
        child_geoid_map[(geoheader['stusab'], geoheader['logrecno'])] = geoheader['geoid']
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
    g.cur.execute("SELECT * FROM %s WHERE (stusab=%%s AND logrecno=%%s)" % (validated_table_id), [parent_geography['stusab'], parent_geography['logrecno']])
    parent_data = g.cur.fetchone()
    stusab = parent_data.pop('stusab')
    logrecno = parent_data.pop('logrecno')
    column_data = []
    for (k, v) in sorted(parent_data.items(), key=lambda tup: tup[0]):
        column_data.append((k.upper(), v))
    data['parent_geography']['data'] = OrderedDict(column_data)

    if child_geoheaders:
        # ... and then children so we can loop through with cursor
        where = " OR ".join(["(stusab='%s' AND logrecno='%s')" % (child['stusab'], child['logrecno']) for child in child_geoheaders])
        g.cur.execute("SELECT * FROM %s WHERE %s" % (validated_table_id, where))
        # store the number of rows returned in comparison object
        data['comparison']['results'] = g.cur.rowcount

        # grab one row at a time
        for record in g.cur:
            stusab = record.pop('stusab')
            logrecno = record.pop('logrecno')
            child_geoid = child_geoid_map[(stusab, logrecno)]

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
