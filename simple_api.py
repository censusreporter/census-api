from flask import Flask
from flask import abort, request, g
import json
import psycopg2
import psycopg2.extras
from collections import OrderedDict

app = Flask(__name__)

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
    conn = psycopg2.connect(database='postgres')
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


def geo_summary(acs, state, logrecno):
    g.cur.execute("SET search_path=%s", [acs])

    doc = OrderedDict([('metadata', dict()),
                       ('population', dict()),
                       ('geography', dict()),
                       ('education', dict())])

    doc['metadata']['acs'] = acs

    g.cur.execute("SELECT * FROM geoheader WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()
    doc['geography'] = dict(name=data['name'],
                            stusab=data['stusab'],
                            sumlevel=data['sumlevel'])

    g.cur.execute("SELECT * FROM B01002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    doc['population']['median_age'] = dict(total=maybe_int(data['b01002001']),
                                           male=maybe_int(data['b01002002']),
                                           female=maybe_int(data['b01002003']))

    g.cur.execute("SELECT * FROM B01003 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    doc['population']['total'] = maybe_int(data['b01003001'])

    g.cur.execute("SELECT * FROM B01001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    doc['population']['gender'] = OrderedDict([
        ('0-9',   dict(male=maybe_int(sum(data, 'b01001003', 'b01001004')),
                     female=maybe_int(sum(data, 'b01001027', 'b01001028')))),
        ('10-19', dict(male=maybe_int(sum(data, 'b01001005', 'b01001006', 'b01001007')),
                     female=maybe_int(sum(data, 'b01001029', 'b01001030', 'b01001031')))),
        ('20-29', dict(male=maybe_int(sum(data, 'b01001008', 'b01001009', 'b01001010', 'b01001011')),
                     female=maybe_int(sum(data, 'b01001032', 'b01001033', 'b01001034', 'b01001035')))),
        ('30-39', dict(male=maybe_int(sum(data, 'b01001012', 'b01001013')),
                     female=maybe_int(sum(data, 'b01001036', 'b01001037')))),
        ('40-49', dict(male=maybe_int(sum(data, 'b01001014', 'b01001015')),
                     female=maybe_int(sum(data, 'b01001038', 'b01001039')))),
        ('50-59', dict(male=maybe_int(sum(data, 'b01001016', 'b01001017')),
                     female=maybe_int(sum(data, 'b01001040', 'b01001041')))),
        ('60-69', dict(male=maybe_int(sum(data, 'b01001018', 'b01001019', 'b01001020', 'b01001021')),
                     female=maybe_int(sum(data, 'b01001042', 'b01001043', 'b01001044', 'b01001045')))),
        ('70-79', dict(male=maybe_int(sum(data, 'b01001022', 'b01001023')),
                     female=maybe_int(sum(data, 'b01001046', 'b01001047')))),
        ('80+',   dict(male=maybe_int(sum(data, 'b01001024', 'b01001025')),
                     female=maybe_int(sum(data, 'b01001048', 'b01001049'))))
    ])

    g.cur.execute("SELECT * FROM B15001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    doc['education']['attainment'] = OrderedDict([
        ('<9th Grade',                      maybe_int(sum(data, 'b15001004', 'b15001012', 'b15001020', 'b15001028', 'b15001036', 'b15001045', 'b15001053', 'b15001061', 'b15001069', 'b15001077'))),
        ('9th-12th Grade (No Diploma)',     maybe_int(sum(data, 'b15001005', 'b15001013', 'b15001021', 'b15001029', 'b15001037', 'b15001046', 'b15001054', 'b15001062', 'b15001070', 'b15001078'))),
        ('High School Grad/GED/Alt',        maybe_int(sum(data, 'b15001006', 'b15001014', 'b15001022', 'b15001030', 'b15001038', 'b15001047', 'b15001055', 'b15001063', 'b15001071', 'b15001079'))),
        ('Some College (No Degree)',        maybe_int(sum(data, 'b15001007', 'b15001015', 'b15001023', 'b15001031', 'b15001039', 'b15001048', 'b15001056', 'b15001064', 'b15001072', 'b15001080'))),
        ('Associate Degree',                maybe_int(sum(data, 'b15001008', 'b15001016', 'b15001024', 'b15001032', 'b15001040', 'b15001049', 'b15001057', 'b15001065', 'b15001073', 'b15001081'))),
        ('Bachelor Degree',                 maybe_int(sum(data, 'b15001009', 'b15001017', 'b15001025', 'b15001033', 'b15001041', 'b15001050', 'b15001058', 'b15001066', 'b15001074', 'b15001082'))),
        ('Graduate or Professional Degree', maybe_int(sum(data, 'b15001010', 'b15001018', 'b15001026', 'b15001034', 'b15001042', 'b15001051', 'b15001059', 'b15001067', 'b15001075', 'b15001083')))
    ])

    try:
        g.cur.execute("SELECT * FROM C16001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
        data = g.cur.fetchone()

        doc['language'] = OrderedDict([
            ('English Only',        maybe_int(data['c16001002'])),
            ('Spanish',             maybe_int(data['c16001003'])),
            ('French',              maybe_int(data['c16001004'])),
            ('German',              maybe_int(data['c16001005'])),
            ('Slavic',              maybe_int(data['c16001006'])),
            ('Other Indo-European', maybe_int(data['c16001007'])),
            ('Korean',              maybe_int(data['c16001008'])),
            ('Chinese',             maybe_int(data['c16001009'])),
            ('Vietnamese',          maybe_int(data['c16001010'])),
            ('Tagalong',            maybe_int(data['c16001011'])),
            ('Other Asian',         maybe_int(data['c16001012'])),
            ('Other & Unspecified', maybe_int(data['c16001013']))
        ])
    except:
        pass

    try:
        g.cur.execute("SELECT * FROM B27010 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
        data = g.cur.fetchone()

        doc['insurance'] = OrderedDict([
            ('No Insurance',                maybe_int(sum(data, 'b27010017', 'b27010033', 'b27010050', 'b27010053'))),
            ('Employer Only',               maybe_int(sum(data, 'b27010004', 'b27010020', 'b27010036', 'b27010054'))),
            ('Direct-Purchase Only',        maybe_int(sum(data, 'b27010005', 'b27010021', 'b27010037', 'b27010055'))),
            ('Medicare Only',               maybe_int(sum(data, 'b27010006', 'b27010022', 'b27010038'             ))),
            ('Medicaid/Means-Tested Only',  maybe_int(sum(data, 'b27010007', 'b27010023', 'b27010039'             ))),
            ('Tricare/Military Only',       maybe_int(sum(data, 'b27010008', 'b27010024', 'b27010040', 'b27010056'))),
            ('VA Health Care Only',         maybe_int(sum(data, 'b27010009', 'b27010025', 'b27010041', 'b27010057'))),
            ('Employer+Direct Purchase',    maybe_int(sum(data, 'b27010011', 'b27010027', 'b27010043', 'b27010058'))),
            ('Employer+Medicare',           maybe_int(sum(data, 'b27010012', 'b27010028', 'b27010044', 'b27010059'))),
            ('Direct+Medicare',             maybe_int(sum(data,                           'b27010045', 'b27010060'))),
            ('Medicare+Medicaid',           maybe_int(sum(data, 'b27010013', 'b27010029', 'b27010046', 'b27010061'))),
            ('Other Private-Only',          maybe_int(sum(data, 'b27010014', 'b27010030', 'b27010047', 'b27010062'))),
            ('Other Public-Only',           maybe_int(sum(data, 'b27010015', 'b27010031', 'b27010048', 'b27010064'))),
            ('Other',                       maybe_int(sum(data, 'b27010016', 'b27010032', 'b27010049', 'b27010065')))
        ])
    except:
        pass

    return json.dumps(doc)


@app.route("/1.0/<acs>/summary/<geoid>")
def acs_geo_summary(acs, geoid):
    acs, state, logrecno = find_geoid(geoid, acs)

    if not acs:
        abort(404, 'That ACS doesn\'t know about have that geoid.')

    return geo_summary(acs, state, logrecno)

@app.route("/1.0/latest/summary/<geoid>")
def latest_geo_summary(geoid):
    acs, state, logrecno = find_geoid(geoid)

    if not acs:
        abort(404, 'None of the ACS I know about have that geoid.')

    return geo_summary(acs, state, logrecno)


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
