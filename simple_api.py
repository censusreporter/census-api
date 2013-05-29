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


def find_geoid(geoid):
    "Find the best acs to use for a given geoid or None if the geoid is not found."
    for acs in allowed_acs:
        g.cur.execute("SELECT stusab,logrecno FROM %s.geoheader WHERE geoid=%%s" % acs, [geoid])
        if g.cur.rowcount == 1:
            result = g.cur.fetchone()
            return (acs, result['stusab'], result['logrecno'])
    return None


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


@app.route("/1.0/summary/<geoid>")
def geo_summary(geoid):
    acs, state, logrecno = find_geoid(geoid)

    if not acs:
        abort(404, 'None of the ACS I know about have that geoid.')

    g.cur.execute("SET search_path=%s", [acs])

    doc = dict(population=dict(), geography=dict(), education=dict())

    g.cur.execute("SELECT * FROM geoheader WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()
    doc['geography'] = dict(name=data['name'],
                            stusab=data['stusab'],
                            sumlevel=data['sumlevel'])

    g.cur.execute("SELECT * FROM B01002 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    doc['population']['median_age'] = dict(total=maybe_int(data['b010020001']),
                                           male=maybe_int(data['b010020002']),
                                           female=maybe_int(data['b010020003']))

    g.cur.execute("SELECT * FROM B01003 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    doc['population']['total'] = maybe_int(data['b010030001'])

    g.cur.execute("SELECT * FROM B01001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    doc['population']['gender'] = OrderedDict([
        ('0-9',   dict(male=maybe_int(sum(data, 'b010010003', 'b010010004')),
                     female=maybe_int(sum(data, 'b010010027', 'b010010028')))),
        ('10-19', dict(male=maybe_int(sum(data, 'b010010005', 'b010010006', 'b010010007')),
                     female=maybe_int(sum(data, 'b010010029', 'b010010030', 'b010010031')))),
        ('20-29', dict(male=maybe_int(sum(data, 'b010010008', 'b010010009', 'b010010010', 'b010010011')),
                     female=maybe_int(sum(data, 'b010010032', 'b010010033', 'b010010034', 'b010010035')))),
        ('30-39', dict(male=maybe_int(sum(data, 'b010010012', 'b010010013')),
                     female=maybe_int(sum(data, 'b010010036', 'b010010037')))),
        ('40-49', dict(male=maybe_int(sum(data, 'b010010014', 'b010010015')),
                     female=maybe_int(sum(data, 'b010010038', 'b010010039')))),
        ('50-59', dict(male=maybe_int(sum(data, 'b010010016', 'b010010017')),
                     female=maybe_int(sum(data, 'b010010040', 'b010010041')))),
        ('60-69', dict(male=maybe_int(sum(data, 'b010010018', 'b010010019', 'b010010020', 'b010010021')),
                     female=maybe_int(sum(data, 'b010010042', 'b010010043', 'b010010044', 'b010010045')))),
        ('70-79', dict(male=maybe_int(sum(data, 'b010010022', 'b010010023')),
                     female=maybe_int(sum(data, 'b010010046', 'b010010047')))),
        ('80+',   dict(male=maybe_int(sum(data, 'b010010024', 'b010010025')),
                     female=maybe_int(sum(data, 'b010010048', 'b010010049'))))
    ])

    g.cur.execute("SELECT * FROM B15001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    doc['education']['attainment'] = OrderedDict([
        ('<9th Grade',                      maybe_int(sum(data, 'b150010004', 'b150010012', 'b150010020', 'b150010028', 'b150010036', 'b150010045', 'b150010053', 'b150010061', 'b150010069', 'b150010077'))),
        ('9th-12th Grade (No Diploma)',     maybe_int(sum(data, 'b150010005', 'b150010013', 'b150010021', 'b150010029', 'b150010037', 'b150010046', 'b150010054', 'b150010062', 'b150010070', 'b150010078'))),
        ('High School Grad/GED/Alt',        maybe_int(sum(data, 'b150010006', 'b150010014', 'b150010022', 'b150010030', 'b150010038', 'b150010047', 'b150010055', 'b150010063', 'b150010071', 'b150010079'))),
        ('Some College (No Degree)',        maybe_int(sum(data, 'b150010007', 'b150010015', 'b150010023', 'b150010031', 'b150010039', 'b150010048', 'b150010056', 'b150010064', 'b150010072', 'b150010080'))),
        ('Associate Degree',                maybe_int(sum(data, 'b150010008', 'b150010016', 'b150010024', 'b150010032', 'b150010040', 'b150010049', 'b150010057', 'b150010065', 'b150010073', 'b150010081'))),
        ('Bachelor Degree',                 maybe_int(sum(data, 'b150010009', 'b150010017', 'b150010025', 'b150010033', 'b150010041', 'b150010050', 'b150010058', 'b150010066', 'b150010074', 'b150010082'))),
        ('Graduate or Professional Degree', maybe_int(sum(data, 'b150010010', 'b150010018', 'b150010026', 'b150010034', 'b150010042', 'b150010051', 'b150010059', 'b150010067', 'b150010075', 'b150010083')))
    ])

    g.cur.execute("SELECT * FROM C16001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    doc['language'] = OrderedDict([
        ('English Only',        maybe_int(data['c160010002'])),
        ('Spanish',             maybe_int(data['c160010003'])),
        ('French',              maybe_int(data['c160010004'])),
        ('German',              maybe_int(data['c160010005'])),
        ('Slavic',              maybe_int(data['c160010006'])),
        ('Other Indo-European', maybe_int(data['c160010007'])),
        ('Korean',              maybe_int(data['c160010008'])),
        ('Chinese',             maybe_int(data['c160010009'])),
        ('Vietnamese',          maybe_int(data['c160010010'])),
        ('Tagalong',            maybe_int(data['c160010011'])),
        ('Other Asian',         maybe_int(data['c160010012'])),
        ('Other & Unspecified', maybe_int(data['c160010013']))
    ])

    g.cur.execute("SELECT * FROM B27010 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = g.cur.fetchone()

    doc['insurance'] = OrderedDict([
        ('No Insurance',                maybe_int(sum(data, 'b270100017', 'b270100033', 'b270100050', 'b270100053'))),
        ('Employer Only',               maybe_int(sum(data, 'b270100004', 'b270100020', 'b270100036', 'b270100054'))),
        ('Direct-Purchase Only',        maybe_int(sum(data, 'b270100005', 'b270100021', 'b270100037', 'b270100055'))),
        ('Medicare Only',               maybe_int(sum(data, 'b270100006', 'b270100022', 'b270100038'              ))),
        ('Medicaid/Means-Tested Only',  maybe_int(sum(data, 'b270100007', 'b270100023', 'b270100039'              ))),
        ('Tricare/Military Only',       maybe_int(sum(data, 'b270100008', 'b270100024', 'b270100040', 'b270100056'))),
        ('VA Health Care Only',         maybe_int(sum(data, 'b270100009', 'b270100025', 'b270100041', 'b270100057'))),
        ('Employer+Direct Purchase',    maybe_int(sum(data, 'b270100011', 'b270100027', 'b270100043', 'b270100058'))),
        ('Employer+Medicare',           maybe_int(sum(data, 'b270100012', 'b270100028', 'b270100044', 'b270100059'))),
        ('Direct+Medicare',             maybe_int(sum(data,                             'b270100045', 'b270100060'))),
        ('Medicare+Medicaid',           maybe_int(sum(data, 'b270100013', 'b270100029', 'b270100046', 'b270100061'))),
        ('Other Private-Only',          maybe_int(sum(data, 'b270100014', 'b270100030', 'b270100047', 'b270100062'))),
        ('Other Public-Only',           maybe_int(sum(data, 'b270100015', 'b270100031', 'b270100048', 'b270100064'))),
        ('Other',                       maybe_int(sum(data, 'b270100016', 'b270100032', 'b270100049', 'b270100065')))
    ])

    return json.dumps(doc)


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
