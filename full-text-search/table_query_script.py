from math import log10
import psycopg2
import sys


def compute_score(relevance):
    """ Computes a ranking score in the range [0, 1].

    params: relevance - psql relevance score, which (from out testing) 
            appears to always be in range [1E-8, 1E-2], which for safety
            we are generalizing to [1E-9, 1E-1] (factor of 10 on either side)
    return: score in range [0, 1]
    """

    return (log10(relevance) + 9) / 8.0


def get_results(q):
    """Queries the database of tables, printing relevant search results.

    params: q = search terms (e.g., "gross income housing rent")
    """

    conn = psycopg2.connect("dbname=census user=census")
    cur = conn.cursor()

    # Query combined metadata table, decoding columns as needed

    cur.execute("""SELECT text1 AS table_id, 
                         text2 AS table_title,
                         text3 AS topics,
                         text4 AS simple_table_title,
                         ts_rank(document, to_tsquery('{0}')) AS relevance
                   FROM search_metadata
                   WHERE document @@ to_tsquery('{0}')
                   AND type = 'table'
                   ORDER BY relevance DESC
                   LIMIT 20;
                """.format(' & '.join(q.split())))

    return cur.fetchall()


def show_results(results):
    """ Print search results' names and scores. """

    # Format of data is a 5-tuple, with second entry being table title, 
    # and the last entry being the relevancy score.

    for result in results:
        print (result[1], compute_score(result[4]))


if __name__ == "__main__":
    try:
        arg1 = sys.argv[1]
    except IndexError:
        print "Usage: python query-script.py <arg1> <arg2> etc..."
        sys.exit(1)

    formatted_query = ' '.join(sys.argv[1:])
    query_result = get_results(formatted_query)
    show_results(query_result)