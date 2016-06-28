import psycopg2
import sys

def query(text):
    """Queries the database of tables, printing relevant search results.

    params: text = search terms (e.g., "gross income housing rent")
    """

    conn = psycopg2.connect("dbname=census user=census")
    cur = conn.cursor()

    # The required query format is words separated by ampersands ('&')
    # enclosed within apostrophes, e.g., 'test string'
    text = "'" + text.replace(' ', ' & ') + "'"

    # Read query comments inside-out. For full detail, refer to
    # full-text-guide.md.

    # Outermost query returns the information relevant to a user -- the
    # table ID, table title, and a relevance score.

    q = ("SELECT table_id, table_title, "
            "ts_rank(table_info.document, to_tsquery({0})) as relevance "
        "FROM ("

            # This query transforms relevant information into a document,
            # i.e., a tsvector, by using the table title and other data.
            #
            # coalesce is used because the columns have the potential to
            # have null values. string_agg creates one string of all
            # column names. For full detail, refer to the psql docs.
            #
            # The result of this is called table_info, and it 
            # contains information for one table only.

            "SELECT table_id, table_title, "
                "setweight(to_tsvector(coalesce(table_title, ' ')), 'A') || "
                "setweight(to_tsvector(coalesce(subject_area, ' ')), 'B') || "
                "setweight(to_tsvector(coalesce(string_agg(column_title, ' '), ' ')), 'C') || "
                "setweight(to_tsvector(coalesce(universe, ' ')), 'D') as document "
            "FROM ("

                # Innermost query joins the table with all table names
                # (census_table_metadata) to the table with column titles 
                # (census_column_metadata) based on table ID.
                #
                # It selects all of the column titles, along with other table
                # info, and returns a table with one row for every column
                # in every table plus the metadata (table_id, universe, 
                # etc.) as other entries in each row.
                #
                # The result of this is called table_search.

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