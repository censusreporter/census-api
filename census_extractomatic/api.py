# For real division instead of sometimes-integer
from __future__ import division

from flask import Flask
from flask import abort, request, g
from flask import make_response, request, current_app
from functools import update_wrapper
import json
import psycopg2
import psycopg2.extras
from collections import OrderedDict
from datetime import timedelta

app = Flask(__name__)

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
    conn = psycopg2.connect(database='postgres', user='census', password='censuspassword', host='localhost')
    g.cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


@app.teardown_request
def teardown_request(exception):
    g.cur.close()


@app.route("/")
def hello():
    return "Hello World!"


@app.route("/1.0/latest/geoid/search")
def latest_geoid_search():
    term = request.args.get('name')

    if not term:
        abort(400, "Provide a 'name' argument to search for.")

    term = "%s%%" % term

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
def acs_geoid_search(acs):
    term = request.args.get('name')

    if not term:
        abort(400, "Provide a 'name' argument to search for.")

    if acs not in allowed_acs:
        abort(404, "I don't know anything about that ACS.")

    term = "%s%%" % term

    result = []
    g.cur.execute("SELECT geoid,stusab as state,name FROM %s.geoheader WHERE name LIKE %%s LIMIT 5" % acs, [term])
    if g.cur.rowcount > 0:
        result = g.cur.fetchall()
        for r in result:
            r['acs'] = acs

    return json.dumps(result)


def geo_comparison(acs, parent_geoid, comparison_sumlev):
    g.cur.execute("SET search_path=%s", [acs])

    # Builds something like: '05000US17%'
    geoid_prefix = '%s00US%s%%' % (comparison_sumlev, parent_geoid.split('US')[1])
    g.cur.execute("SELECT * FROM geoheader WHERE geoid LIKE %s;", [geoid_prefix])
    geoheaders = g.cur.fetchall()

    doc = OrderedDict([('comparison', dict()),
                       ('geographies', list())])

    doc['comparison']['census_release'] = ACS_NAMES.get(acs).get('name')

    for geo in geoheaders:
        state = geo['stusab']
        logrecno = geo['logrecno']

        one_geom = dict(population=dict(), geography=dict())
        one_geom['geography'] = dict(name=geo['name'],
                                geoid=geo['geoid'],
                                stusab=geo['stusab'],
                                sumlevel=geo['sumlevel'],
                                census_release=ACS_NAMES.get(acs).get('name'))

        g.cur.execute("SELECT * FROM B01001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
        data = g.cur.fetchone()

        one_geom['population']['total'] = maybe_int(data['b01001001'])

        one_geom['population']['gender'] = OrderedDict([
            ('0-9',   dict(male=maybe_int(sum(data, 'b01001003', 'b01001004')),
                         female=maybe_int(sum(data, 'b01001027', 'b01001028')),
                          total=maybe_int(sum(data, 'b01001003', 'b01001004', 'b01001027', 'b01001028')))),

            ('10-19', dict(male=maybe_int(sum(data, 'b01001005', 'b01001006', 'b01001007')),
                         female=maybe_int(sum(data, 'b01001029', 'b01001030', 'b01001031')),
                          total=maybe_int(sum(data, 'b01001005', 'b01001006', 'b01001007', 'b01001029', 'b01001030', 'b01001031')))),

            ('20-29', dict(male=maybe_int(sum(data, 'b01001008', 'b01001009', 'b01001010', 'b01001011')),
                         female=maybe_int(sum(data, 'b01001032', 'b01001033', 'b01001034', 'b01001035')),
                          total=maybe_int(sum(data, 'b01001008', 'b01001009', 'b01001010', 'b01001011', 'b01001032', 'b01001033', 'b01001034', 'b01001035')))),

            ('30-39', dict(male=maybe_int(sum(data, 'b01001012', 'b01001013')),
                         female=maybe_int(sum(data, 'b01001036', 'b01001037')),
                          total=maybe_int(sum(data, 'b01001012', 'b01001013', 'b01001036', 'b01001037')))),

            ('40-49', dict(male=maybe_int(sum(data, 'b01001014', 'b01001015')),
                         female=maybe_int(sum(data, 'b01001038', 'b01001039')),
                          total=maybe_int(sum(data, 'b01001014', 'b01001015', 'b01001038', 'b01001039')))),

            ('50-59', dict(male=maybe_int(sum(data, 'b01001016', 'b01001017')),
                         female=maybe_int(sum(data, 'b01001040', 'b01001041')),
                          total=maybe_int(sum(data, 'b01001016', 'b01001017', 'b01001040', 'b01001041')))),

            ('60-69', dict(male=maybe_int(sum(data, 'b01001018', 'b01001019', 'b01001020', 'b01001021')),
                         female=maybe_int(sum(data, 'b01001042', 'b01001043', 'b01001044', 'b01001045')),
                          total=maybe_int(sum(data, 'b01001018', 'b01001019', 'b01001020', 'b01001021', 'b01001042', 'b01001043', 'b01001044', 'b01001045')))),

            ('70-79', dict(male=maybe_int(sum(data, 'b01001022', 'b01001023')),
                         female=maybe_int(sum(data, 'b01001046', 'b01001047')),
                          total=maybe_int(sum(data, 'b01001022', 'b01001023', 'b01001046', 'b01001047')))),

            ('80+',   dict(male=maybe_int(sum(data, 'b01001024', 'b01001025')),
                         female=maybe_int(sum(data, 'b01001048', 'b01001049')),
                          total=maybe_int(sum(data, 'b01001024', 'b01001025', 'b01001048', 'b01001049'))))
        ])

        doc['geographies'].append(one_geom)

    return json.dumps(doc)


def geo_profile(acs, state, logrecno):
    g.cur.execute("SET search_path=%s", [acs])

    doc = OrderedDict([('geography', dict()),
                       ('demographics', dict()),
                       ('economics', dict()),
                       ('education', dict()),
                       ('employment', dict()),
                       ('families', dict()),
                       ('health', dict()),
                       ('housing', dict()),
                       ('sociocultural', dict()),
                       ('veterans', dict())])

    doc['geography']['census_release'] = ACS_NAMES.get(acs).get('name')
    default_data_years = ACS_NAMES.get(acs).get('years')

    g.cur.execute("SELECT * FROM geoheader WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()
    doc['geography'].update(dict(name=data['name'],
                                 pretty_name=None,
                                 stusab=data['stusab'],
                                 sumlevel=data['sumlevel'],
                                 land_area=None))

    g.cur.execute("SELECT * FROM B01001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    pop_dict = dict()
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

    gender_dict = dict()
    doc['demographics']['gender'] = gender_dict
    gender_dict['percent_male'] = build_item('b01001', 'Total population', 'Male', default_data_years, data,
                                        lambda data: maybe_percent(data['b01001002'], data['b01001001']))

    gender_dict['percent_female'] = build_item('b01001', 'Total population', 'Female', default_data_years, data,
                                        lambda data: maybe_percent(data['b01001026'], data['b01001001']))

    g.cur.execute("SELECT * FROM B01002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    age_dict['median_age'] = build_item('b01002', 'Total population', 'Median age', default_data_years, data,
                                        lambda data: maybe_float(data['b01002001']))

    age_dict['median_age_male'] = build_item('b01002', 'Total population', 'Median age male', default_data_years, data,
                                        lambda data: maybe_float(data['b01002002']))

    age_dict['median_age_female'] = build_item('b01002', 'Total population', 'Median age female', default_data_years, data,
                                        lambda data: maybe_float(data['b01002003']))

    g.cur.execute("SELECT * FROM B02001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    race_dict = dict()
    doc['demographics']['race'] = race_dict
    race_dict['percent_white'] = build_item('b02001', 'Total population', 'White', default_data_years, data,
                                        lambda data: maybe_percent(data['b02001002'], data['b02001001']))

    race_dict['percent_black'] = build_item('b02001', 'Total population', 'Black', default_data_years, data,
                                        lambda data: maybe_percent(data['b02001003'], data['b02001001']))

    race_dict['percent_native_american'] = build_item('b02001', 'Total population', 'Native', default_data_years, data,
                                        lambda data: maybe_percent(data['b02001004'], data['b02001001']))

    race_dict['percent_asian'] = build_item('b02001', 'Total population', 'Asian', default_data_years, data,
                                        lambda data: maybe_percent(data['b02001005'], data['b02001001']))

    race_dict['percent_other'] = build_item('b02001', 'Total population', 'Islander', default_data_years, data,
                                        lambda data: maybe_percent(data['b02001006'], data['b02001001']))

    race_dict['percent_native_islander'] = build_item('b02001', 'Total population', 'Other race', default_data_years, data,
                                        lambda data: maybe_percent(data['b02001007'], data['b02001001']))

    race_dict['percent_two_or_more'] = build_item('b02001', 'Total population', 'Two+ races', default_data_years, data,
                                        lambda data: maybe_percent(data['b02001008'], data['b02001001']))

    g.cur.execute("SELECT * FROM B03003 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    ethnicity_dict = dict()
    doc['demographics']['ethnicity'] = ethnicity_dict

    ethnicity_dict['percent_hispanic'] = build_item('b03003', 'Total population', 'Hispanic/Latino', default_data_years, data,
                                        lambda data: maybe_percent(data['b03003003'], data['b03003001']))

    g.cur.execute("SELECT * FROM B19301 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    income_dict = dict()
    doc['economics']['income'] = income_dict

    income_dict['per_capita_income_in_the_last_12_months'] = build_item('b19301', 'Total population', 'Per capita income in past year', default_data_years, data,
                                        lambda data: maybe_int(data['b19301001']))

    g.cur.execute("SELECT * FROM B19013 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    income_dict['median_household_income'] = build_item('b19013', 'Households', 'Median household income', default_data_years, data,
                                        lambda data: maybe_int(data['b19013001']))

    g.cur.execute("SELECT * FROM B17001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    poverty_dict = dict()
    doc['economics']['poverty'] = poverty_dict

    poverty_dict['percent_below_poverty_line'] = build_item('b17001', 'Population for whom poverty status is determined', 'Persons below poverty line', default_data_years, data,
                                        lambda data: maybe_percent(data['b17001002'], data['b17001001']))

    g.cur.execute("SELECT * FROM B15002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    attainment_dict = dict()
    doc['education']['attainment'] = attainment_dict

    attainment_dict['percent_high_school_or_higher'] = build_item('b15002', 'Population 25 years and over', 'High school grad or higher', default_data_years, data,
                                        lambda data: maybe_percent((sum(data, 'b15002011', 'b15002012', 'b15002013', 'b15002014', 'b15002015', 'b15002016', 'b15002017', 'b15002018') +
                                                                     sum(data, 'b15002028', 'b15002029', 'b15002030', 'b15002031', 'b15002032', 'b15002033', 'b15002034', 'b15002035')),
                                                                     data['b15002001']))

    attainment_dict['percent_bachelor_degree_or_higher'] = build_item('b15002', 'Population 25 years and over', 'Bachelor\'s degree or higher', default_data_years, data,
                                        lambda data: maybe_percent((sum(data, 'b15002015', 'b15002016', 'b15002017', 'b15002018') +
                                                                     sum(data, 'b15002032', 'b15002033', 'b15002034', 'b15002035')),
                                                                     data['b15002001']))

    g.cur.execute("SELECT * FROM B08006 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    _total_workers_16_and_over = maybe_int(data['b08006001'])
    _workers_who_worked_at_home = maybe_int(data['b08006017'])

    g.cur.execute("SELECT * FROM B08013 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    _aggregate_minutes = maybe_int(data['b08013001'])

    travel_time_dict = dict()
    doc['employment']['travel_time'] = travel_time_dict

    travel_time_dict['mean_travel_time'] = build_item('b08006, b08013', 'Workers 16 years and over', 'Mean travel time to work', default_data_years, data,
                                        lambda data: maybe_float(div(_aggregate_minutes, dif(_total_workers_16_and_over, _workers_who_worked_at_home))))

    g.cur.execute("SELECT * FROM B11001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    households_dict = dict()
    doc['families']['households'] = households_dict
    # store so we can use this for the next calculation too
    _number_of_households = maybe_int(data['b11001001'])

    households_dict['number_of_households'] = build_item('b11001', 'Households', 'Number of households', default_data_years, data,
                                        lambda data: _number_of_households)


    g.cur.execute("SELECT * FROM B11002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    _total_persons_in_households = maybe_int(data['b11002001'])

    households_dict['persons_per_household'] = build_item('b11001,b11002', 'Households', 'Persons per household', default_data_years, data,
                                        lambda data: maybe_float(div(_total_persons_in_households, _number_of_households)))

    g.cur.execute("SELECT * FROM B07001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    migration_dict = dict()
    doc['housing']['mobility'] = migration_dict

    migration_dict['percent_living_in_same_house_1_year'] = build_item('b07001', 'Population 1 year and over in the United States', 'People living in same house for 1 year or more', default_data_years, data,
                                        lambda data: maybe_percent(data['b07001017'], data['b07001001']))

    g.cur.execute("SELECT * FROM B25001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    units_dict = dict()
    doc['housing']['units'] = units_dict

    units_dict['number_of_housing_units'] = build_item('b25001', 'Housing units', 'Number of housing units', default_data_years, data,
                                        lambda data: maybe_int(data['b25001001']))

    g.cur.execute("SELECT * FROM B25024 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    units_dict['percent_units_in_multi_unit_structure'] = build_item('b25024', 'Housing units', 'Housing units in multi-unit structures', default_data_years, data,
                                        lambda data: maybe_percent(sum(data, 'b25024004', 'b25024005', 'b25024006', 'b25024007', 'b25024008', 'b25024009'),
                                                                    data['b25024001']))

    g.cur.execute("SELECT * FROM B25003 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    ownership_dict = dict()
    doc['housing']['ownership'] = ownership_dict

    ownership_dict['percent_homeownership'] = build_item('b25003', 'Occupied housing units', 'Rate of homeownership', default_data_years, data,
                                        lambda data: maybe_percent(data['b25003002'], data['b25003001']))

    g.cur.execute("SELECT * FROM B25077 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    ownership_dict['median_value_of_owner_occupied_housing_unit'] = build_item('b25077', 'Owner-occupied housing units', 'Median value of owner-occupied housing units', default_data_years, data,
                                        lambda data: maybe_int(data['b25077001']))

    g.cur.execute("SELECT * FROM B05002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    foreign_dict = dict()
    doc['sociocultural']['place_of_birth'] = foreign_dict

    foreign_dict['percent_foreign_born'] = build_item('b05002', 'Total population', 'Foreign-born persons', default_data_years, data,
                                        lambda data: maybe_percent(data['b05002013'], data['b05002001']))

    g.cur.execute("SELECT * FROM B16001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    language_dict = dict()
    doc['sociocultural']['language'] = language_dict

    language_dict['percent_non_english_at_home'] = build_item('b16001', 'Population 5 years and over', 'Persons with language other than English spoken at home', default_data_years, data,
                                        lambda data: maybe_float(maybe_percent(dif(data['b16001001'], data['b16001002']), data['b16001001'])))

    g.cur.execute("SELECT * FROM B21002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    veterans_dict = dict()
    doc['veterans']['veteran_status'] = veterans_dict

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

@app.route("/1.0/<acs>/<parent_id>/<comparison_sumlev>/compare")
def acs_geo_comparison(acs, parent_id, comparison_sumlev):
    if acs not in allowed_acs:
        abort(404, 'ACS %s is not supported.' % acs)

    return geo_comparison(acs, parent_id, comparison_sumlev)

@app.route("/1.0/<acs>/<table>")
def table_details(acs, table):
    if acs not in allowed_acs:
        abort(404, 'ACS %s is not supported.' % acs)

    g.cur.execute("SET search_path=%s", [acs])

    geoids = tuple(request.args.get('geoids', '').split(','))
    if not geoids:
        abort(400, 'Must include at least one geoid separated by commas.')

    # If they specify a sumlevel, then we look for the geographies "underneath"
    # the specified geoids that sit at the specified sumlevel
    child_summary_level = request.args.get('sumlevel')
    if child_summary_level:
        # A hacky way to represent the state-county-town geography relationship line
        if child_summary_level not in ('50', '60'):
            abort(400, 'Only support child sumlevel or 50 or 60 for now.')

        if len(geoids) > 1:
            abort(400, 'Only support one parent geoid for now.')

        child_summary_level = int(child_summary_level)

        desired_geoid_prefix = '%03d00US%s%%' % (child_summary_level, geoids[0][7:])

        g.cur.execute("SELECT geoid,stusab,logrecno FROM geoheader WHERE geoid LIKE %s", [desired_geoid_prefix])
        geoids = g.cur.fetchall()
    else:
        # Find the logrecno for the geoids they asked for
        g.cur.execute("SELECT geoid,stusab,logrecno FROM geoheader WHERE geoid IN %s", [geoids, ])
        geoids = g.cur.fetchall()

    geoid_mapping = dict()
    for r in geoids:
        geoid_mapping[(r['stusab'], r['logrecno'])] = r['geoid']

    where = " OR ".join(["(stusab='%s' AND logrecno='%s')" % (r['stusab'], r['logrecno']) for r in geoids])

    # Query the table they asked for using the geometries they asked for
    data = dict()
    g.cur.execute("SELECT * FROM %s WHERE %s" % (table, where))
    for r in g.cur:
        stusab = r.pop('stusab')
        logrecno = r.pop('logrecno')

        geoid = geoid_mapping[(stusab, logrecno)]

        column_data = []
        for (k, v) in sorted(r.items(), key=lambda tup: tup[0]):
            column_data.append((k, v))
        data[geoid] = OrderedDict(column_data)

    return json.dumps(data)


@app.route("/1.0/geo/search")
@crossdomain(origin='*')
def geo_search():
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    prefix = request.args.get('prefix')
    try:
        with_geom = bool(request.args.get('geom', False))
    except ValueError:
        with_geom = False

    if lat and lon:
        try:
            lat = float(lat)
            lon = float(lon)
        except ValueError:
            abort(400, 'Lat and Lon must be numbers.')
        if not (-180.0 <= lon <= 180.0) or not (-90.0 <= lat <= 90.0):
            abort(400, 'Lat must be between [-90,90], Lon must be between [-180,180].')

        where = "ST_Intersects(the_geom, ST_SetSRID(ST_Point(%s, %s),4326))"
        where_args = [lon, lat]
    elif prefix:
        prefix += "%"
        where = "lower(name) LIKE lower(%s)"
        where_args = [prefix]
    else:
        abort(400, "Must provide either a lat/lon OR a prefix.")

    if with_geom:
        g.cur.execute("SELECT awater,aland,sumlevel,geoid,name,ST_AsGeoJSON(ST_Simplify(the_geom,0.01)) as geom FROM tiger2012.census_names_simple WHERE %s ORDER BY sumlevel, aland DESC LIMIT 5;" % where, where_args)
    else:
        g.cur.execute("SELECT awater,aland,sumlevel,geoid,name FROM tiger2012.census_names_simple WHERE %s ORDER BY sumlevel, aland DESC LIMIT 5;" % where, where_args)

    data = []

    for row in g.cur:
        row['full_geoid'] = "%s00US%s" % (row['sumlevel'], row['geoid'])
        data.append(row)

    return json.dumps(data)


@app.route("/1.0/geo/tiger2012/<geoid>")
@crossdomain(origin='*')
def geo_lookup(geoid):

    try:
        with_geom = bool(request.args.get('geom', False))
    except ValueError:
        with_geom = False

    geoid_parts = geoid.split('US')
    if len(geoid_parts) is not 2:
        abort(400, 'Invalid geoid')

    sumlevel_part = geoid_parts[0][:3]
    id_part = geoid_parts[1]

    if with_geom:
        g.cur.execute("SELECT awater,aland,name,intptlat,intptlon,ST_AsGeoJSON(ST_Simplify(the_geom,0.01)) as geom FROM tiger2012.census_names WHERE sumlevel=%s AND geoid=%s LIMIT 1", [sumlevel_part, id_part])
    else:
        g.cur.execute("SELECT awater,aland,name,intptlat,intptlon FROM tiger2012.census_names WHERE sumlevel=%s AND geoid=%s LIMIT 1", [sumlevel_part, id_part])

    result = g.cur.fetchone()

    if not result:
        abort(404, 'Unknown geoid')

    intptlon = result.pop('intptlon')
    result['intptlon'] = round(float(intptlon), 7)
    intptlat = result.pop('intptlat')
    result['intptlat'] = round(float(intptlat), 7)

    return json.dumps(result)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
