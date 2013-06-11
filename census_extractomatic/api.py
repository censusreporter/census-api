from __future__ import division

from flask import Flask
from flask import abort, request, g
import json
import psycopg2
import psycopg2.extras
from collections import OrderedDict

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
    'acs2011_1yr': 'ACS 2011 1-year',
    'acs2011_3yr': 'ACS 2011 3-year',
    'acs2011_5yr': 'ACS 2011 5-year',
    'acs2010_1yr': 'ACS 2010 1-year',
    'acs2010_3yr': 'ACS 2010 3-year',
    'acs2010_5yr': 'ACS 2010 5-year',
    'acs2009_1yr': 'ACS 2009 1-year',
    'acs2009_3yr': 'ACS 2009 3-year',
    'acs2008_1yr': 'ACS 2008 1-year',
    'acs2008_3yr': 'ACS 2008 3-year',
    'acs2007_1yr': 'ACS 2007 1-year',
    'acs2007_3yr': 'ACS 2007 3-year'
}


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


def maybe_int(i):
    return int(i) if i else i


def maybe_float(i, decimals=1):
    return round(float(i), decimals) if i else i


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

    # Builds something like: '05000US17%'
    geoid_prefix = '%s00US%s%' % (comparison_sumlev, parent_geoid)

    cur.execute("SELECT * FROM %s.geoheader WHERE geoid LIKE %s;", [acs, geoid_prefix])
    geoheaders = cur.fetchall()

    doc = []

    for geo in geoheaders:
        state = geo['stusab']
        logrecno = geo['logrecno']

        one_geom = dict(population=dict(), geography=dict(), education=dict())
        one_geom['geography'] = dict(name=geo['name'],
                                geoid=geo['geoid'],
                                stusab=geo['stusab'],
                                sumlevel=geo['sumlevel'],
                                census_release=ACS_NAMES.get(acs))

        cur.execute("SELECT * FROM %s.B01003 WHERE stusab=%s AND logrecno=%s;", [acs, state, logrecno])
        data = cur.fetchone()

        one_geom['population']['total'] = maybe_int(data['b010030001'])

        cur.execute("SELECT * FROM %s.B01001 WHERE stusab=%s AND logrecno=%s;", [acs, state, logrecno])
        data = cur.fetchone()

        one_geom['population']['gender'] = OrderedDict([
            ('0-9',   dict(male=maybe_int(sum(data, 'b010010003', 'b010010004')),
                         female=maybe_int(sum(data, 'b010010027', 'b010010028')),
                          total=maybe_int(sum(data, 'b010010003', 'b010010004', 'b010010027', 'b010010028')))),

            ('10-19', dict(male=maybe_int(sum(data, 'b010010005', 'b010010006', 'b010010007')),
                         female=maybe_int(sum(data, 'b010010029', 'b010010030', 'b010010031')),
                          total=maybe_int(sum(data, 'b010010005', 'b010010006', 'b010010007', 'b010010029', 'b010010030', 'b010010031')))),

            ('20-29', dict(male=maybe_int(sum(data, 'b010010008', 'b010010009', 'b010010010', 'b010010011')),
                         female=maybe_int(sum(data, 'b010010032', 'b010010033', 'b010010034', 'b010010035')),
                          total=maybe_int(sum(data, 'b010010008', 'b010010009', 'b010010010', 'b010010011', 'b010010032', 'b010010033', 'b010010034', 'b010010035')))),

            ('30-39', dict(male=maybe_int(sum(data, 'b010010012', 'b010010013')),
                         female=maybe_int(sum(data, 'b010010036', 'b010010037')),
                          total=maybe_int(sum(data, 'b010010012', 'b010010013', 'b010010036', 'b010010037')))),

            ('40-49', dict(male=maybe_int(sum(data, 'b010010014', 'b010010015')),
                         female=maybe_int(sum(data, 'b010010038', 'b010010039')),
                          total=maybe_int(sum(data, 'b010010014', 'b010010015', 'b010010038', 'b010010039')))),

            ('50-59', dict(male=maybe_int(sum(data, 'b010010016', 'b010010017')),
                         female=maybe_int(sum(data, 'b010010040', 'b010010041')),
                          total=maybe_int(sum(data, 'b010010016', 'b010010017', 'b010010040', 'b010010041')))),

            ('60-69', dict(male=maybe_int(sum(data, 'b010010018', 'b010010019', 'b010010020', 'b010010021')),
                         female=maybe_int(sum(data, 'b010010042', 'b010010043', 'b010010044', 'b010010045')),
                          total=maybe_int(sum(data, 'b010010018', 'b010010019', 'b010010020', 'b010010021', 'b010010042', 'b010010043', 'b010010044', 'b010010045')))),

            ('70-79', dict(male=maybe_int(sum(data, 'b010010022', 'b010010023')),
                         female=maybe_int(sum(data, 'b010010046', 'b010010047')),
                          total=maybe_int(sum(data, 'b010010022', 'b010010023', 'b010010046', 'b010010047')))),

            ('80+',   dict(male=maybe_int(sum(data, 'b010010024', 'b010010025')),
                         female=maybe_int(sum(data, 'b010010048', 'b010010049')),
                          total=maybe_int(sum(data, 'b010010024', 'b010010025', 'b010010048', 'b010010049'))))
        ])

        doc.append(one_geom)

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

    doc['geography']['census_release'] = ACS_NAMES.get(acs)

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
    age_dict['percent_under_18'] = dict(table_id='b01001',
                                        universe='Total population',
                                        name='Under 18',
                                        values=dict(this=maybe_float((sum(data, 'b01001003', 'b01001004', 'b01001005', 'b01001006') +
                                                                     sum(data, 'b01001027', 'b01001028', 'b01001029', 'b01001030')) /
                                                                     data['b01001001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    age_dict['percent_over_65'] = dict(table_id='b01001',
                                        universe='Total population',
                                        name='65 and over',
                                        values=dict(this=maybe_float((sum(data, 'b01001020', 'b01001021', 'b01001022', 'b01001023', 'b01001024', 'b01001025') +
                                                                     sum(data, 'b01001044', 'b01001045', 'b01001046', 'b01001047', 'b01001048', 'b01001049')) /
                                                                     data['b01001001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    gender_dict = dict()
    doc['demographics']['gender'] = gender_dict
    gender_dict['percent_male'] = dict(table_id='b01001',
                                        universe='Total population',
                                        name='Male',
                                        values=dict(this=maybe_float(data['b01001002'] / data['b01001001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    gender_dict['percent_female'] = dict(table_id='b01001',
                                        universe='Total population',
                                        name='Female',
                                        values=dict(this=maybe_float(data['b01001026'] / data['b01001001']* 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    g.cur.execute("SELECT * FROM B01002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    age_dict['median_age'] = dict(table_id='b01002',
                                    universe='Total population',
                                    name='Median age',
                                    values=dict(this=maybe_float(data['b01002001']),
                                                county=None,
                                                state=None,
                                                nation=None))

    age_dict['median_age_male'] = dict(table_id='b01002',
                                        universe='Total population',
                                        name='Median age male',
                                        values=dict(this=maybe_float(data['b01002002']),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    age_dict['median_age_female'] = dict(table_id='b01002',
                                        universe='Total population',
                                        name='Median age female',
                                        values=dict(this=maybe_float(data['b01002003']),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    g.cur.execute("SELECT * FROM B02001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    race_dict = dict()
    doc['demographics']['race'] = race_dict
    race_dict['percent_white'] = dict(table_id='b02001',
                                        universe='Total population',
                                        name='White',
                                        values=dict(this=maybe_float(data['b02001002'] / data['b02001001']* 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    race_dict['percent_black'] = dict(table_id='b02001',
                                        universe='Total population',
                                        name='Black',
                                        values=dict(this=maybe_float(data['b02001003'] / data['b02001001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    race_dict['percent_native_american'] = dict(table_id='b02001',
                                        universe='Total population',
                                        name='Native',
                                        values=dict(this=maybe_float(data['b02001004'] / data['b02001001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    race_dict['percent_asian'] = dict(table_id='b02001',
                                        universe='Total population',
                                        name='Asian',
                                        values=dict(this=maybe_float(data['b02001005'] / data['b02001001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    race_dict['percent_other'] = dict(table_id='b02001',
                                        universe='Total population',
                                        name='Islander',
                                        values=dict(this=maybe_float(data['b02001006'] / data['b02001001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    race_dict['percent_native_islander'] = dict(table_id='b02001',
                                        universe='Total population',
                                        name='Other race',
                                        values=dict(this=maybe_float(data['b02001007'] / data['b02001001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    race_dict['percent_two_or_more'] = dict(table_id='b02001',
                                        universe='Total population',
                                        name='Two+ races',
                                        values=dict(this=maybe_float(data['b02001008'] / data['b02001001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    g.cur.execute("SELECT * FROM B03003 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    ethnicity_dict = dict()
    doc['demographics']['ethnicity'] = ethnicity_dict

    ethnicity_dict['percent_hispanic'] = dict(table_id='b03003',
                                        universe='Total population',
                                        name='Hispanic/Latino',
                                        values=dict(this=maybe_float(data['b03003003'] / data['b03003001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    g.cur.execute("SELECT * FROM B19301 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    income_dict = dict()
    doc['economics']['income'] = income_dict

    income_dict['per_capita_income_in_the_last_12_months'] = dict(table_id='b19301',
                                        universe='Total population',
                                        name='Per capita income in past year',
                                        values=dict(this=maybe_int(data['b19301001']),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    g.cur.execute("SELECT * FROM B19013 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    income_dict['median_household_income'] = dict(table_id='b19013',
                                        universe='Households',
                                        name='Median household income',
                                        values=dict(this=maybe_int(data['b19013001']),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    g.cur.execute("SELECT * FROM B17001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    poverty_dict = dict()
    doc['economics']['poverty'] = poverty_dict

    poverty_dict['percent_below_poverty_line'] = dict(table_id='b17001',
                                        universe='Population for whom poverty status is determined',
                                        name='Persons below poverty line',
                                        values=dict(this=maybe_float(data['b17001002'] / data['b17001001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    g.cur.execute("SELECT * FROM B15002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    attainment_dict = dict()
    doc['education']['attainment'] = attainment_dict

    attainment_dict['percent_high_school_or_higher'] = dict(table_id='b15002',
                                        universe='Population 25 years and over',
                                        name='High school grad or higher',
                                        values=dict(this=maybe_float((sum(data, 'b15002011', 'b15002012', 'b15002013', 'b15002014', 'b15002015', 'b15002016', 'b15002017', 'b15002018') +
                                                                     sum(data, 'b15002028', 'b15002029', 'b15002030', 'b15002031', 'b15002032', 'b15002033', 'b15002034', 'b15002035')) /
                                                                     data['b15002001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    attainment_dict['percent_bachelor_degree_or_higher'] = dict(table_id='b15002',
                                        universe='Population 25 years and over',
                                        name='Bachelor\'s degree or higher',
                                        values=dict(this=maybe_float((sum(data, 'b15002015', 'b15002016', 'b15002017', 'b15002018') +
                                                                     sum(data, 'b15002032', 'b15002033', 'b15002034', 'b15002035')) /
                                                                     data['b15002001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    #TODO: employment.travel_time

    g.cur.execute("SELECT * FROM B11001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    households_dict = dict()
    doc['families']['households'] = households_dict

    households_dict['number_of_households'] = dict(table_id='b11001',
                                        universe='Households',
                                        name='Number of households',
                                        values=dict(this=maybe_int(data['b11001001']),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    #TODO: families.persons_per_household

    g.cur.execute("SELECT * FROM B07001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    migration_dict = dict()
    doc['housing']['mobility'] = migration_dict

    migration_dict['percent_living_in_same_house_1_year'] = dict(table_id='b07001',
                                        universe='Population 1 year and over in the United States',
                                        name='People living in same house for 1 year or more',
                                        values=dict(this=maybe_float(data['b07001017'] / data['b07001001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    g.cur.execute("SELECT * FROM B25001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    units_dict = dict()
    doc['housing']['units'] = units_dict

    units_dict['number_of_housing_units'] = dict(table_id='b25001',
                                        universe='Housing units',
                                        name='Number of housing units',
                                        values=dict(this=maybe_int(data['b25001001']),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    g.cur.execute("SELECT * FROM B25024 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    units_dict['percent_units_in_multi_unit_structure'] = dict(table_id='b25024',
                                        universe='Housing units',
                                        name='Housing units in multi-unit structures',
                                        values=dict(this=maybe_float(sum(data, 'b25024004', 'b25024005', 'b25024006', 'b25024007', 'b25024008', 'b25024009') /
                                                                    data['b25024001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    g.cur.execute("SELECT * FROM B25003 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    ownership_dict = dict()
    doc['housing']['ownership'] = ownership_dict

    ownership_dict['percent_homeownership'] = dict(table_id='b25003',
                                        universe='Occupied housing units',
                                        name='Rate of homeownership',
                                        values=dict(this=maybe_float(data['b25003002'] / data['b25003001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    g.cur.execute("SELECT * FROM B25077 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    ownership_dict['median_value_of_owner_occupied_housing_unit'] = dict(table_id='b25077',
                                        universe='Owner-occupied housing units',
                                        name='Median value of owner-occupied housing units',
                                        values=dict(this=maybe_int(data['b25077001']),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    g.cur.execute("SELECT * FROM B05002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    foreign_dict = dict()
    doc['sociocultural']['place_of_birth'] = foreign_dict

    foreign_dict['percent_foreign_born'] = dict(table_id='b05002',
                                        universe='Total population',
                                        name='Foreign-born persons',
                                        values=dict(this=maybe_float(data['b05002013'] / data['b05002001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    g.cur.execute("SELECT * FROM B16001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    language_dict = dict()
    doc['sociocultural']['language'] = language_dict

    language_dict['percent_non_english_at_home'] = dict(table_id='b16001',
                                        universe='Population 5 years and over',
                                        name='Persons with language other than English spoken at home',
                                        values=dict(this=maybe_float((data['b16001001']-data['b16001002']) / data['b16001001'] * 100),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

    g.cur.execute("SELECT * FROM B21002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    veterans_dict = dict()
    doc['veterans']['veteran_status'] = veterans_dict

    veterans_dict['number_of_veterans'] = dict(table_id='b21002',
                                        universe='Civilian veterans 18 years and over',
                                        name='Number of veterans',
                                        values=dict(this=maybe_int(data['b21002001']),
                                                    county=None,
                                                    state=None,
                                                    nation=None))

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

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
