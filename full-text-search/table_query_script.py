from math import log10
import psycopg2
import sys


def compute_score(relevance):
    """ Computes a ranking score in the range [0, 1].

    params: relevance - psql relevance score, which (from out testing) 
            appears to generally be in range [1E-8, 1E-2], which for 
            safety, we are generalizing to [1E-10, 1] (factor of 100)
    return: score in range [0, 1]
    """

    return log10(relevance) / 10.0 + 1


def get_results(q):
    """Queries the database of tables, printing relevant search results.

    params: q = search terms (e.g., "gross income housing rent")
    """

    conn = psycopg2.connect("dbname=census user=census")
    cur = conn.cursor()

    # The required query format is words separated by ampersands ('&')
    # enclosed within apostrophes, e.g., 'test string'
    text = "'" + q.replace(' ', ' & ') + "'"

    # Read query comments inside-out. For full detail, refer to
    # full-text-guide.md.

    # Outermost query returns the information relevant to a user -- the
    # table ID, table title, and a relevance score.

    q = ("SELECT table_id, table_title, "
            "ts_rank(table_info.document, to_tsquery({0}), 2|8|32) as relevance "
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
        "ORDER BY relevance DESC "
        "LIMIT 20;").format(text)

    cur.execute(q)
    return cur.fetchall()


def show_results(results):
    """ Print search results' names and scores. """

    # Format of data is a 3-tuple, with second entry being table name, 
    # and the last entry being the relevancy score.

    for result in results:
        print (result[1], compute_score(result[2]))


if __name__ == "__main__":
    try:
        arg1 = sys.argv[1]
    except IndexError:
        print "Usage: python query-script.py <arg1> <arg2> etc..."
        sys.exit(1)

    formatted_query = ' '.join(sys.argv[1:])
    query_result = get_results(formatted_query)
    show_results(query_result)