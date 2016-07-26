import profile_query_script
import table_query_script
import sys


def query_both(q):
	""" Queries tables and profiles. 

	params: q = search string (e.g., "puerto rico") """

	profiles = profile_query_script.get_results(q)
	tables = table_query_script.get_results(q)

	all_objects = []

	for p in profiles:
		relevance = str(profile_query_script.compute_score(p[5], p[4]))
		obj_type = 'profile'
		name = p[0]
		full_geoid = p[3]

		p = (relevance, obj_type, full_geoid, name)
		all_objects.append(p)

	for t in tables:
		relevance = str(table_query_script.compute_score(t[5]))
		obj_type = 'table'
		name = t[1]
		table_id = t[4].split()[0]

		t = (relevance, obj_type, table_id, name)
		all_objects.append(t)

	return all_objects


if __name__ == "__main__":
	formatted_query = ' '.join(sys.argv[1:])
	results = query_both(formatted_query)

	# Each result is formatted as (relevance, type, some_code, name), where
	# some_code is full_geoid or table_id. Sort by relevance.
	results = sorted(results, key = lambda x: x[0], reverse = True)

	for entry in results:
		print ' '.join(entry)