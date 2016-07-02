from jinja2 import Environment, FileSystemLoader
from sets import Set
import psycopg2
import os.path

def write_table_sitemap(output_dir,db_connect_string='postgresql://census:censuspassword@localhost:5432/census'):
	''' Builds table.xml sitemap file. There are not more than
	50,000 URLs, so we can use one file without issue.

	params: none
	return: none

	'''
	table_names = query_table_list(db_connect_string)
	table_urls = build_table_page_list(table_names)
	fname = os.path.join(output_dir,'sitemap_tables.xml')
	with open(fname, 'w') as f:
		f.write(build_sitemap(table_urls))
	print 'Wrote table sitemap to file %s' % (fname)


def build_sitemap(page_data):
    ''' Builds sitemap from template in sitemap.xml using data provided
    in page_data.

    params: page_data = list of page URLs
    returns: XML template with the page URLs

    '''

    env = Environment(loader = FileSystemLoader('.'))
    template = env.get_template('sitemap.xml')

    return template.render(pages = page_data)


def query_table_list(db_connect_string):
	''' Queries the database for a list of all one-year
	and five-year tables. Removes duplicates from them,
	and returns a set of all table IDs.

	params: none
	returns: Set of table IDs

	'''
	# we oughta parameterize this but feeling lazy...
	# this is assuming that you're running the DB on a non-standard port, say
	# as if you were SSH tunneling from production to your machine
	conn = psycopg2.connect(db_connect_string)
	cur = conn.cursor()

	q1 = "SELECT DISTINCT tables_in_one_yr from census_tabulation_metadata;"
	cur.execute(q1)
	results1 = cur.fetchall()

	q2 = "SELECT DISTINCT tables_in_five_yr from census_tabulation_metadata;"
	cur.execute(q2)
	results2 = cur.fetchall()

	# results1, results2 are "lists of 1-tuples of lists"
	# i.e., [(['a'], ), (['b', 'c'], ), ( [ ... ], ), ...]
	# so add to a set (which inherently has no duplicates)

	tables = Set()
	for result in results1 + results2:
		for table in result[0]:
			tables.add(table)

	return tables


def build_table_page_list(table_names):
	''' Builds the URL/pages list for all tables.

	params: none
	return: list of CensusReporter URLs

	'''

	table_urls = []

	for table in sorted(table_names):
		table_urls.append(build_url(table))

	return table_urls


def build_url(table_name):
    ''' Builds the CensusReporter URL out of table_name.
    Format: https://censusreporter.org/tables/table_name/"

    params: table_name = table ID
    return: full URL of the relevant page

    >>> build_url("B06009")
    "http://censusreporter.org/tables/B06009/"

    >>> build_url("B25113")
    "https://censusreporter.org/tables/B25113"

    '''

    return "https://censusreporter.org/tables/" + table_name + "/"


def main():
	write_table_sitemap('.')


if __name__ == "__main__":
	main()
