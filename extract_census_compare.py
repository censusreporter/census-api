#!/bin/python

import psycopg2
import psycopg2.extras
import json
from collections import OrderedDict

conn = psycopg2.connect(database='postgres')
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# 050 is county, 060 is metro areas
# 17 is IL FIPS code
geoid_prefix = '05000US17%'


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


cur.execute("SELECT * FROM acs2010_1yr.geoheader WHERE geoid LIKE %s;", [geoid_prefix])
geoheaders = cur.fetchall()

doc = []

for geo in geoheaders:
    state = geo['stusab']
    logrecno = geo['logrecno']

    one_geom = dict(population=dict(), geography=dict(), education=dict())
    one_geom['geography'] = dict(name=geo['name'],
                            geoid=geo['geoid'],
                            stusab=geo['stusab'],
                            sumlevel=geo['sumlevel'])

    cur.execute("SELECT * FROM acs2010_1yr.B01003 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
    data = cur.fetchone()

    one_geom['population']['total'] = maybe_int(data['b010030001'])

    cur.execute("SELECT * FROM acs2010_1yr.B01001 WHERE stusab=%s AND logrecno=%s;", [state, logrecno])
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

print json.dumps(doc, indent=2)
