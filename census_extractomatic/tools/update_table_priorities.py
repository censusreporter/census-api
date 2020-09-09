from __future__ import division
import gzip
import re
import sys
try:
    # Python 3
    from urllib.parse import urlparse, parse_qs
except ImportError:
    # Python 2
    from urlparse import urlparse, parse_qs
from ..api import db


log_rx = re.compile(
    r'^(?P<ip>\d+\.\d+\.\d+\.\d+) - - \[(?P<ts>.*?)\] "(?P<req>.*?)" '
    r'(?P<status>\d+) (?P<code>\d+) "(?P<path>.*?)" "(?P<agent>.*?)"')
req_rx = re.compile('^GET (/profiles|/tables|/data/table).+$')
table_rx = re.compile(r'[BC]\d{5,6}(?:[A-I]|[A-I]?PR)?$')

agents = {}
requests = set()
paths = {}
geo_ids = {}
tables = {}
referers = {}

tables_x = re.compile('^/tables/(.+?)/?$')

def parse_log(log):
    m = log_rx.search(log)
    if not m.group('status') == '200':
        return
    if req_rx.search(m.group('req')) is not None:
        requests.add(m.group('req'))
        agent = m.group('agent')
        if agent not in agents:
            agents[agent] = 0
        agents[agent] += 1
    url = m.group('req').split()[1]
    if url.startswith('/static') or url.startswith('/healthcheck'):
        return
    parsed = urlparse(url)
    path = parsed.path
    if path not in paths:
        paths[path] = { 'count': 0, 'params': {} }
    paths[path]['count'] += 1
    qs = parse_qs(parsed.query)
    referer = m.group('path')
    if path in ['/data/map/', '/data/table/', '/data/comparison']:
        for geo in qs.get('primary_geo_id', []):
            geo_ids[geo] = geo_ids.get(geo, 0) + 1
        for geolist in qs.get('geo_ids', []):
            for geo in geolist.split(','): 
                if '|' in geo:
                    geo = geo.split('|', 1)[1]
                geo_ids[geo] = geo_ids.get(geo, 0) + 1
        for t in qs.get('table', []):
            # We get some bad requests that are still 200s. They end up not
            # parsing correctly. Skipping.
            if any([
                    '|' in t,
                    '&' in t,
                    'amp;' in t.lower(),
                    ' and ' in t.lower(),
            ]):
                continue
            tables[t] = tables.get(t, 0) + 1
            if not referer.startswith('https://censusreporter.org/') and referer != '-':
                if not t in referers:
                    referers[t] = {}
                referers[t][referer] = referers[t].get(referer, 0) + 1
    else: # and for ^/tables/(.+)/?$ we’d just want the values of the match group
        table_match = tables_x.search(path)
        if table_match is not None:
            t = table_match.group(1)
            tables[t] = tables.get(t, 0) + 1        
            if not referer.startswith('https://censusreporter.org/') and referer != '-':
                if not t in referers:
                    referers[t] = {}
                referers[t][referer] = referers[t].get(referer, 0) + 1


def prep():
    for fn in sys.argv[1:]:
        print('Reading log file: %s' % fn)
        with gzip.open(fn, 'rt') as f:
            for line in f:
                parse_log(line)

normalized_counts = {}

def calculate():
    max_count = 0
    min_count = None
    for t, count in sorted(tables.items(), key=lambda item: item[1], reverse=True):
        if table_rx.match(t):
            if count > max_count:
                max_count = count 
            elif min_count is None or count < min_count:
                min_count = count
    for t, count in sorted(tables.items(), key=lambda item: item[1], reverse=True):
        if table_rx.match(t):
            n = int(round( (count - min_count) / (max_count - min_count) * 100 ))
            normalized_counts[t] = n


def populate():
    query = """select text1,text5 from search_metadata where type='table'"""
    for row in db.session.execute(query):
        tabulation = row[0]
        tables = row[1].split()
        value = sum([ normalized_counts.get(t, 0) for t in tables ])
        print('Updating tabulation: %s, text6=%s' % (tabulation, value))
        db.session.execute(
            """UPDATE search_metadata set text6=:value where text1=:tabulation""",
            { 'value': value, 'tabulation': tabulation })
        db.session.commit()


def main():
    prep()
    calculate()
    populate()


if __name__=='__main__':
    """Usage:
        python -m census_extractomatic.tools.update_table_priorities FILES
    FILES should match a set of .gz log files.
    e.g.:
        python -m census_extractomatic.tools.update_table_priorities path/to/*.gz
    """
    # a more likely command on the main server would be
    # EXTRACTOMATIC_CONFIG_MODULE=census_extractomatic.config.Production python -m census_extractomatic.tools.update_table_priorities /var/log/nginx/censusreporter.org.access.log*gz
    # after activating the proper virtual environment
    if len(sys.argv) < 2:
        print('\n.gz log files must be specified\n')
        exit()
    main()
