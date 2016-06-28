import psycopg2
import sys

def query(text):
    """Queries the database of tables, printing relevant search results.

    params: text = search terms (e.g., "gross income housing rent")
    """

    conn = psycopg2.connect("dbname=census user=census")
    cur = conn.cursor()

    text = text.split()
    text = "'" +  ' & '.join(text) + "'"

    q = ("SELECT table_id, table_title, "
            "ts_rank(table_info.document, to_tsquery({0})) as relevance "
        "FROM ("
            "SELECT table_id, table_title, "
                "setweight(to_tsvector(coalesce(table_title, ' ')), 'A') || "
                "setweight(to_tsvector(coalesce(subject_area, ' ')), 'B') || "
                "setweight(to_tsvector(coalesce(string_agg(column_title, ' '), ' ')), 'C') || "
                "setweight(to_tsvector(coalesce(universe, ' ')), 'D') as document "
            "FROM ("
                "SELECT DISTINCT t.table_id, t.table_title, t.subject_area, "
                    "t.universe, c.column_title "
                "FROM acs2014_1yr.census_table_metadata t  "
                "JOIN acs2014_1yr.census_column_metadata c "
                "ON t.table_id = c.table_id) table_search "
            "WHERE table_id = table_search.table_id "
            "GROUP BY table_id, table_title, subject_area, universe "
        ") table_info "
        "WHERE table_info.document @@ to_tsquery({0}) "
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
    try:
        arg1 = sys.argv[1]
    except IndexError:
        print "Usage: python query-script.py <arg1> <arg2> etc..."
        sys.exit(1)

    main(sys.argv[1:])