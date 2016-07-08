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
		p = (p[0], profile_query_script.compute_score(p[6], p[5]), 'profile')
		all_objects.append(p)

	for t in tables:
		t = (t[1], table_query_script.compute_score(t[2]), 'table')
		all_objects.append(t)

	return all_objects


if __name__ == "__main__":
	formatted_query = ' '.join(sys.argv[1:])
	results = query_both(formatted_query)

	# Sort results by second entry, their score; keep top 20
	results = sorted(results, key = lambda x: x[1], reverse = True)

	for entry in results[:20]:
		print entry