import psycopg2
import sys

def query(text):
	''' Queries the database of tables, printing relevant search results.

	params: text = search terms (e.g., "gross income housing rent")
	return: None 
	'''

	conn = psycopg2.connect("dbname=census user=census")
	cur = conn.cursor()

	text = text.split()
	text = "'" +  ' & '.join(text) + "'"

   	q = ("SELECT table_id, table_title, "
    "ts_rank(table_search.document, to_tsquery({0})) as relevance "
    "FROM ("
        "SELECT table_id, table_title, "
               "setweight(to_tsvector(coalesce(table_title, ' ')), 'A') || "
               "setweight(to_tsvector(coalesce(universe, ' ')), 'B') || "
               "setweight(to_tsvector(coalesce(subject_area, ' ')), 'C') as document "
        "FROM acs2014_1yr.census_table_metadata"
        ") table_search "
    "WHERE table_search.document @@ to_tsquery({0}) "
    "ORDER BY relevance DESC;").format(text)

   	cur.execute(q)
   	results = cur.fetchall()

	for result in results:
	   	print result

	cur.close()
	conn.close()


def main(argv):
	query(' '.join(argv))

if __name__ == "__main__":
	main(sys.argv[1:])